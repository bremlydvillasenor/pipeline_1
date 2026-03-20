# referrals.py
# majority of the processes are using polars
from __future__ import annotations

import logging
from pathlib import Path
import re
from typing import Tuple, Dict
from datetime import date, timedelta

from contextlib import contextmanager              # [TIMER]
from time import perf_counter                      # [TIMER]

import polars as pl
import pandas as pd

from .utils.helpers import (
    latest_file
)

from .utils import funnel_referral


__all__ = [
    "run_referral_pipeline",
]


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
_LOG_FORMAT = "%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s"
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────
# simple stage timer for consistent elapsed-time logs
@contextmanager
def stage_timer(label: str):
    _t0 = perf_counter()
    try:
        yield
    finally:
        _dt = perf_counter() - _t0
        log.info("%s duration: %.2fs", label, _dt)

def _to_snake(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)        # punctuation → space
    s = re.sub(r"\s+", "_", s)            # spaces → _
    s = re.sub(r"_+", "_", s).strip("_")  # collapse _
    return s or "col"

def _read_excel_promote_header_and_normalize(
    path: Path | str,
    *,
    skip_rows: int,
    expect_cols: list[str]
) -> pl.DataFrame:
    """
    (1) Read Excel with initial rows skipped
    (2) Promote the next row as header
    (3) Select expected columns (if present)
    (4) snake_case column names; ensure uniqueness
    """
    df = pl.read_excel(path, read_options={"skip_rows": skip_rows})

    # Promote the first remaining row as header
    headers = df.row(0)
    df = df.slice(1).rename({old: new for old, new in zip(df.columns, headers)})

    # Select expected columns (ignore missing gracefully)
    present = [c for c in expect_cols if c in df.columns]
    df = df.select(present)

    # snake_case + de-dup
    raw = [_to_snake(c) for c in df.columns]
    counts: Dict[str, int] = {}
    uniq: list[str] = []
    for c in raw:
        n = counts.get(c, 0)
        uniq.append(c if n == 0 else f"{c}_{n}")
        counts[c] = n + 1

    return df.rename({old: new for old, new in zip(df.columns, uniq)})

def _parse_any_datetime_to_date(col_name: str, *, fmt: str | None = None) -> pl.Expr:
    """
    Robustly convert the named column to Date:
    - try string -> Datetime via strptime (strict=False)
    - else try a generic cast to Datetime (strict=False)
    - finally cast the result to Date
    """
    return (
        pl.coalesce([
            pl.col(col_name).str.strptime(pl.Datetime, format=fmt, strict=False, exact=False),
            pl.col(col_name).cast(pl.Datetime, strict=False),
        ])
        .cast(pl.Date)
    )

def _ensure_schema(df: pl.DataFrame, schema: Dict[str, pl.DataType]) -> pl.DataFrame:
    """
    Reorder/select columns to exactly match schema keys and cast to specified dtypes.
    Missing columns are added as nulls (then cast).
    """
    for col, dtype in schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
    df = df.select([pl.col(c).cast(schema[c]) for c in schema.keys()])
    return df


# ─────────────────────────────────────────────────────────────
# Headcount helpers
# ─────────────────────────────────────────────────────────────

def to_month_end(d: date) -> date:
    """Convert any date to the last day of its month."""
    y = d.year + (1 if d.month == 12 else 0)
    m = 1 if d.month == 12 else d.month + 1
    return date(y, m, 1) - timedelta(days=1)

def ensure_date(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Coerce a column to pl.Date if needed; accepts Date, Datetime, or string."""
    dt = df.schema.get(col)
    if dt == pl.Date:
        return df
    if dt == pl.Datetime:
        return df.with_columns(pl.col(col).dt.date().alias(col))
    return df.with_columns(pl.col(col).str.strptime(pl.Date, strict=False).alias(col))

def extend_headcount(
    mheadcount: pl.DataFrame,
    reporting_col: str = "reporting_date",
    sort_output: bool = True,
) -> pl.DataFrame:
    """
    bia always report completed months..
    copy the latest reporting month rows into new dataframe and set reporting_col = current month-end, 
    then concat the dup data frame to existing.
    """
    if mheadcount.is_empty():
        return mheadcount

    # Ensure reporting_col is Date
    mheadcount = ensure_date(mheadcount, reporting_col)

    # 1) Compute current month-end (local system date)
    target_month_end = to_month_end(date.today())

    #print(type(target_month_end))
    
    # 2) If target already present, nothing to do
    has_target = mheadcount.select(pl.col(reporting_col).is_in([target_month_end]).any()).item()
    #log.info("extending headcount df....")
    if has_target:
        return mheadcount

    #print(mheadcount.head())
    # Optional guard: if the latest reporting date is AFTER target (e.g., future data), do nothing
    latest_reporting = mheadcount.select(pl.col(reporting_col).max()).item()
    if latest_reporting is None or latest_reporting > target_month_end:
        return mheadcount

    # 3) Duplicate only rows from the latest reporting month
    latest_rows = mheadcount.filter(pl.col(reporting_col) == latest_reporting)
    #print(latest_rows.head())

    # 4) Overwrite reporting_col for the duplicated rows
    new_cols = [pl.lit(target_month_end).alias(reporting_col)]
    new_rows = latest_rows.with_columns(*new_cols)
    #print(new_rows.head())

    # 5) Concat (and optionally sort)
    out = pl.concat([mheadcount, new_rows])
    return out.sort(reporting_col) if sort_output else out

# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────
def extract_prospect_data(config: dict) -> pl.DataFrame:
    """
    Read prospect referral data from Excel using Polars.
    - Skips preamble rows
    - Promotes row as header
    - Normalizes & selects only needed columns
    - Renames to internal column set
    """
    path = latest_file(config["RAW_DATA_ROOT"], config["PROSPECT_PATTERN"])
    log.info("Reading prospect data: %s", path.name)

    raw_cols = [
        "Candidate ID",
        "Added On",
        "Reference ID",
        "Referred by Employee ID",
        "Referred by",
    ]
    df = _read_excel_promote_header_and_normalize(
        path,
        skip_rows=8,          # after skipping 8, the next row is header
        expect_cols=raw_cols
    ).rename({
        "candidate_id": "candidate_id",
        "added_on": "added_date",
        "reference_id": "job_requisition_id",
        "referred_by_employee_id": "referred_by_employee_id",
        "referred_by": "referred_by",
    })

    # normalize added_date to DATE for stability
    df = df.with_columns(
        _parse_any_datetime_to_date("added_date", fmt="%Y-%m-%d %H:%M:%S").alias("added_date")
    )
    #print(df.head())
    return df

def extract_erp_applications(config: dict) -> pl.DataFrame:
    """
    Read ERP applications parquet → ERP referrals view.
    """
    parq_path = Path(config["OUTPUT_APPS_PAR"])
    log.info("Reading erp apps: %s", parq_path.name)

    cols = [
        "candidate_id",
        "job_requisition_id",
        "referred_by_employee_id",
        "last_recruiting_stage",
        "last_stage_number",
        "candidate_recruiting_status",
        "added_date",
        "referred_by",
        "disposition_reason",
        "consolidated_disposition",
    ]
    df = (
        pl.scan_parquet(parq_path)
          .filter(pl.col("consolidated_channel") == "ERP")
          .select(cols) 
          .collect()
    )
    #print(df.head())
    return df

def extract_headcount(config: dict) -> pl.DataFrame:
    """
    Read pmd_monthly_headcount (Excel), promote header, normalize.
    """
    path = latest_file(config["HISTORICAL_DATA_ROOT"], config["HEADCOUNT_PATTERN"])
    log.info("Reading monthly headcount: %s", path.name)

    df = _read_excel_promote_header_and_normalize(
        path,
        skip_rows=1,
        expect_cols=[
            "Masterfile[Reporting Date]",
            "Masterfile[Location]",
            "Masterfile[Function]",
            "Masterfile[Sub-function]",
            "Masterfile[Job Grade]",
            "Masterfile[Employee ID]",
        ],
    ).rename({
        "masterfile_reporting_date": "reporting_date",
        "masterfile_location": "location",
        "masterfile_function": "function",
        "masterfile_sub_function": "sub_function",
        "masterfile_job_grade": "job_grade",
        "masterfile_employee_id": "employee_id",
    })

    df = df.with_columns(
        _parse_any_datetime_to_date("reporting_date", fmt="%Y-%m-%d %H:%M:%S").alias("reporting_date")
    )
    return df

# ────────────── BUNDLED EXTRACT ──────────────

def extract_all(config: dict) -> dict:
    """Return every raw DataFrame needed for transforms."""
    return {
        "prospect": extract_prospect_data(config),
        "erp": extract_erp_applications(config),
        "headcount": extract_headcount(config),
    }

# ─────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────
def transform_prospect(df: pl.DataFrame) -> pl.DataFrame:
    """
    - keep only rows with a job requisition id
    - clean referred_by (strip anything after first '(')
    """
    return (
        df
        .filter(pl.col("job_requisition_id").is_not_null())
        .with_columns(
            pl.col("referred_by")
              .str.split("(").list.get(0)
              .str.strip_chars()
              .alias("referred_by"),
            pl.lit(None).cast(pl.Utf8).alias("disposition_reason"),
            pl.lit("Prospect").cast(pl.Utf8).alias("candidate_recruiting_status"),
            pl.lit(0).cast(pl.Int16).alias("last_stage_number"),
        )
    )

def transform_referrals(
    prospect_df: pl.DataFrame,
    erp_df: pl.DataFrame
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    - Align schemas
    - Concat
    - Deduplicate on erp_id
    - Produce unique headcount flags (has_referral)
    """
    prospect_df = transform_prospect(prospect_df)

    # Minimal union schema to carry through; add more fields if you want to keep them
    target_schema: Dict[str, pl.DataType] = {
        "candidate_id": pl.Utf8,
        "job_requisition_id": pl.Utf8,
        "referred_by_employee_id": pl.Utf8,
        "referred_by": pl.Utf8,
        "added_date": pl.Date,       # unified as Date for stable yyyymm
        "disposition_reason": pl.Utf8,
        "consolidated_disposition": pl.Utf8,
        "candidate_recruiting_status": pl.Utf8,
        "last_stage_number": pl.Int16,
    }

    p = _ensure_schema(prospect_df, target_schema)
    e = _ensure_schema(erp_df,      target_schema)

    # Concat with matching width/dtypes; rechunk for tidy memory layout
    df = pl.concat([p, e], how="vertical").rechunk()

    # Build erp_id column (null-safe)
    df = df.with_columns([
        pl.col("job_requisition_id").fill_null("").cast(pl.Utf8),
        pl.col("referred_by_employee_id").fill_null("").cast(pl.Utf8),
        pl.col("candidate_id").fill_null("").cast(pl.Utf8),
    ]).with_columns(
        (pl.col("job_requisition_id") + "_" + pl.col("referred_by_employee_id") + "_" + pl.col("candidate_id"))
        .alias("erp_id")
    )

    # headcount_id = referred_by_employee_id + '_' + yyyymm(added_date)
    df = df.with_columns(
        (pl.col("added_date").dt.strftime("%Y-%m")).alias("_yyyymm")
    ).with_columns(
        (pl.col("referred_by_employee_id") + "_" + pl.col("_yyyymm")).alias("headcount_id")
    ).drop("_yyyymm")

    # Dedupe by erp_id, keep last
    df = df.unique(subset=["erp_id"], keep="last")

    # Unique headcount flags
    unique_df = (
        df
        .select("headcount_id")
        .unique()
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("has_referral"))
    )

    return df, unique_df

def transform_headcount(headcount_df: pl.DataFrame, unique_referral_df: pl.DataFrame) -> pl.DataFrame:

    headcount_df = extend_headcount(headcount_df)

    """
    - Add "Level " prefix to job_grade
    - Build headcount_id = employee_id + '_' + yyyymm(reporting_date)
    - Left join has_referral flag
    """
    out = (
        headcount_df
        .with_columns(
            (pl.lit("Level ") + pl.col("job_grade").cast(pl.Utf8)).alias("job_grade")
        )
        .with_columns(
            pl.col("reporting_date").cast(pl.Date)
        )
        .with_columns(
            pl.col("reporting_date").dt.strftime("%Y-%m").alias("_yyyymm")
        )
        .with_columns(
            (pl.col("employee_id").cast(pl.Utf8) + "_" + pl.col("_yyyymm")).alias("headcount_id")
        )
        .drop("_yyyymm")
        .join(unique_referral_df, on="headcount_id", how="left")
        .with_columns(pl.col("has_referral").fill_null(0).cast(pl.Int8))
    )
    return out

# ────────────── BUNDLED TRANSFORM ──────────────

def transform_all(extracts: dict) -> dict:

    referrals_df, unique_referrals_df = transform_referrals(extracts["prospect"], extracts["erp"])
    headcount_df = transform_headcount(extracts["headcount"], unique_referrals_df)

    # Transform for to referral funnel
    #print(referrals_df.head())
    funnel_df = funnel_referral.run(referrals_df.to_pandas())

    return {
        "headcount": headcount_df,
        "referrals": referrals_df,
        "funnel": funnel_df,
    }

# ─────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────
def load_to_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)

# ────────────── BUNDLED LOAD ──────────────
def load_outputs(config: dict, outputs: dict, ) -> None:
    # Load Polars DF
    load_to_parquet(outputs["referrals"], Path(config["OUTPUT_REFERRALS_PARQUET"]))
    load_to_parquet(outputs["funnel"], Path(config["OUTPUT_REFERRALS_FUNNEL_PARQUET"]))
    load_to_parquet(outputs["headcount"], Path(config["OUTPUT_MHEADCOUNT_PARQUET"]))

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def run_referral_pipeline(config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Runs the full pipeline and writes:
      - config["OUTPUT_REFERRALS_PARQUET"]
      - config["OUTPUT_MHEADCOUNT_PARQUET"]
    Returns pandas DataFrames for downstream code that expects pandas.
    """

    _t0_run = perf_counter() 

    log.info("──────────────────────────────")
    log.info("🚀 Referral ETL started ... ")

    # Extract
    extracted_dict = extract_all(config)

    # Transform
    tranformed_dict = transform_all(extracted_dict) 

    # Load
    load_outputs(config, tranformed_dict)

    # Overall elapsed time
    _dt_run = perf_counter() - _t0_run
    log.info("✅ Referral ETL completed • duration : %.2fs", _dt_run)         
    log.info("──────────────────────────────")

    # Return pandas for compatibility with existing code
    return tranformed_dict["referrals"], tranformed_dict["headcount"], tranformed_dict["funnel"]
