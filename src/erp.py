# erp.py  ·  ERP Headcount ETL
# Consumes:
#   - Monthly headcount Excel (raw source)
#   - unique_referrals_df produced by referral.py
# Produces:
#   - OUTPUT_MHEADCOUNT_PARQUET  (headcount rows flagged with has_referral)

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Final

import polars as pl

from .utils.helpers import latest_file


__all__ = ["run_erp_pipeline"]


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# TYPE ALIAS
# ─────────────────────────────────────────────────────────────

PipelineConfig = dict[str, Any]


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# Raw Excel column names → internal snake_case names
HEADCOUNT_RENAME_MAP: Final[Dict[str, str]] = {
    "masterfile_reporting_date": "reporting_date",
    "masterfile_location":       "location",
    "masterfile_function":       "function",
    "masterfile_sub_function":   "sub_function",
    "masterfile_job_grade":      "job_grade",
    "masterfile_employee_id":    "employee_id",
}

HEADCOUNT_RAW_COLS: Final[list[str]] = [
    "Masterfile[Reporting Date]",
    "Masterfile[Location]",
    "Masterfile[Function]",
    "Masterfile[Sub-function]",
    "Masterfile[Job Grade]",
    "Masterfile[Employee ID]",
]


# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────

@contextmanager
def stage_timer(label: str):
    _t0 = perf_counter()
    try:
        yield
    finally:
        log.info("%s duration: %.2fs", label, perf_counter() - _t0)


def _to_snake(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "col"


def _read_excel_promote_header(
    path: Path | str,
    *,
    skip_rows: int,
    expect_cols: list[str],
) -> pl.DataFrame:
    """
    Read Excel, skip preamble rows, promote the next row as header,
    select expected columns, and normalize column names to snake_case.
    """
    df = pl.read_excel(path, read_options={"skip_rows": skip_rows})

    headers = df.row(0)
    df = df.slice(1).rename({old: new for old, new in zip(df.columns, headers)})

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


def _parse_datetime_to_date(col: str, *, fmt: str | None = None) -> pl.Expr:
    return (
        pl.coalesce([
            pl.col(col).str.strptime(pl.Datetime, format=fmt, strict=False, exact=False),
            pl.col(col).cast(pl.Datetime, strict=False),
        ])
        .cast(pl.Date)
    )


# ─────────────────────────────────────────────────────────────
# HEADCOUNT DATE HELPERS
# ─────────────────────────────────────────────────────────────

def _to_month_end(d: date) -> date:
    """Return the last calendar day of d's month."""
    y = d.year + (1 if d.month == 12 else 0)
    m = 1 if d.month == 12 else d.month + 1
    return date(y, m, 1) - timedelta(days=1)


def _ensure_date_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Coerce col to pl.Date; accepts Date, Datetime, or string."""
    dt = df.schema.get(col)
    if dt == pl.Date:
        return df
    if dt == pl.Datetime:
        return df.with_columns(pl.col(col).dt.date().alias(col))
    return df.with_columns(pl.col(col).str.strptime(pl.Date, strict=False).alias(col))


def _extend_headcount(
    df: pl.DataFrame,
    reporting_col: str = "reporting_date",
) -> pl.DataFrame:
    """
    BIA always reports completed months. If the current month-end is not yet
    present, duplicate the latest month's rows and stamp them with today's
    month-end so downstream reporting sees an up-to-date snapshot.
    """
    if df.is_empty():
        return df

    df = _ensure_date_col(df, reporting_col)
    target = _to_month_end(date.today())

    has_target = df.select(pl.col(reporting_col).is_in([target]).any()).item()
    if has_target:
        return df

    latest = df.select(pl.col(reporting_col).max()).item()
    if latest is None or latest > target:
        return df

    new_rows = (
        df.filter(pl.col(reporting_col) == latest)
        .with_columns(pl.lit(target).alias(reporting_col))
    )
    return pl.concat([df, new_rows]).sort(reporting_col)


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────

def extract(config: PipelineConfig) -> pl.DataFrame:
    """Read monthly headcount Excel and return a clean DataFrame."""
    path = latest_file(config["HISTORICAL_DATA_ROOT"], config["HEADCOUNT_PATTERN"])
    log.info("Reading monthly headcount: %s", path.name)

    df = (
        _read_excel_promote_header(
            path,
            skip_rows=1,
            expect_cols=HEADCOUNT_RAW_COLS,
        )
        .rename(HEADCOUNT_RENAME_MAP)
    )

    return df.with_columns(
        _parse_datetime_to_date("reporting_date", fmt="%Y-%m-%d %H:%M:%S").alias("reporting_date")
    )


# ─────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────

def transform(
    headcount_df: pl.DataFrame,
    unique_referrals_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    1. Extend headcount to current month-end if needed.
    2. Prefix job_grade with "Level ".
    3. Build headcount_id = employee_id + '_' + YYYY-MM(reporting_date).
    4. Left-join has_referral flag from unique_referrals_df.
    5. Fill missing has_referral with 0.
    """
    headcount_df = _extend_headcount(headcount_df)

    return (
        headcount_df
        .with_columns(
            (pl.lit("Level ") + pl.col("job_grade").cast(pl.Utf8)).alias("job_grade")
        )
        .with_columns(pl.col("reporting_date").cast(pl.Date))
        .with_columns(pl.col("reporting_date").dt.strftime("%Y-%m").alias("_yyyymm"))
        .with_columns(
            (pl.col("employee_id").cast(pl.Utf8) + "_" + pl.col("_yyyymm")).alias("headcount_id")
        )
        .drop("_yyyymm")
        .join(unique_referrals_df, on="headcount_id", how="left")
        .with_columns(pl.col("has_referral").fill_null(0).cast(pl.Int8))
    )


# ─────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────

def load(headcount_df: pl.DataFrame, config: PipelineConfig) -> None:
    out_path = Path(config["OUTPUT_MHEADCOUNT_PARQUET"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    headcount_df.write_parquet(out_path, compression="zstd")
    log.info("Headcount written → %s (%d rows)", out_path.name, headcount_df.height)


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_erp_pipeline(
    config: PipelineConfig,
    unique_referrals_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Orchestrate ERP headcount ETL.

    Parameters
    ----------
    config              : pipeline configuration dict
    unique_referrals_df : output of referral.run_referral_pipeline()[1]
                          (one row per headcount_id, has_referral=1)

    Returns
    -------
    headcount_df : pl.DataFrame  – headcount with has_referral flags
    """
    _t0 = perf_counter()
    log.info("──────────────────────────────")
    log.info("🚀 ERP Headcount ETL started ...")

    with stage_timer("⏱  Extract •"):
        headcount_df = extract(config)

    with stage_timer("⏱  Transform •"):
        headcount_df = transform(headcount_df, unique_referrals_df)

    with stage_timer("⏱  Load •"):
        load(headcount_df, config)

    log.info("✅ ERP Headcount ETL completed • duration: %.2fs", perf_counter() - _t0)
    log.info("──────────────────────────────")

    return headcount_df
