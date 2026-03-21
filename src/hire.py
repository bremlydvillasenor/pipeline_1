"""
Hires Pipeline – ETL for recruitment / talent-acquisition hire records.

Flow:
1. Extract incoming fulfillment + INTX files
2. Build a cleaned incoming LazyFrame
3. Validate schema
4. Upsert-merge into master hires database (exception-aware)
5. Run discrepancy checks against job-requisition counts
6. Persist merged output to Parquet
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from dateutil.relativedelta import relativedelta

from .utils.helpers import latest_file

# ═════════════════════════════════════════════
# LOGGING
# ═════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ▸ %(levelname)-8s ▸ %(message)s",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════
# TYPE ALIAS
# ═════════════════════════════════════════════

PipelineConfig = dict[str, Any]

# ═════════════════════════════════════════════
# COLUMN RENAME MAPS
# ═════════════════════════════════════════════

FULFILLMENT_RENAME_MAP: dict[str, str] = {
    "Job Requisition ID": "job_requisition_id",
    "Position ID - Proposed": "position_id",
    "Employee ID": "employee_id",
    "Legal Name": "employee_name",
    "Business Process Reason": "business_process_reason",
    "Effective Date": "candidate_start_date",
    "Grade - Proposed": "grade_proposed"
}

INTX_RENAME_MAP: dict[str, str] = {
    "Candidate ID": "candidate_id",
    "Employee ID": "employee_id",
    "Added Date": "added_date",
    "Recruiting Start Date": "recruiting_start_date",
    "Job Application Date": "job_application_date",
    "Ready for Hire Date": "ready_for_hire_date",
    "Candidate Start Date": "candidate_start_date",
    "Recruiter Completed Offer": "recruiter_completed_offer",
    "Recruiter Employee ID": "recruiter_employee_id",
    "Referred By Employee ID": "referred_by_employee_id",
    "Source": "source",
    "Recruiting Agency": "recruiting_agency",
    "Date Completed (Offer)": "offer_accepted_date",
    "Job Requisition Unfreeze Date": "jr_unfreeze_date",
    "Total Number of Days Frozen": "days_frozen",
}

# ═════════════════════════════════════════════
# SCHEMAS
# ═════════════════════════════════════════════

HIRES_SCHEMA: dict[str, pl.DataType] = {
    "jrpos_id": pl.Utf8,
    "jree_id": pl.Utf8,
    "eehire_id": pl.Utf8,
    "job_requisition_id": pl.Utf8,
    "employee_id": pl.Utf8,
    "candidate_id": pl.Utf8,
    "employee_name": pl.Utf8,
    "position_id": pl.Utf8,
    "business_process_reason": pl.Utf8,
    "recruiting_start_date": pl.Date,
    "added_date": pl.Date,
    "job_application_date": pl.Date,
    "ready_for_hire_date": pl.Date,    
    "candidate_start_date": pl.Date,
    "is_hire_on_time_exception": pl.Utf8,
    "jr_unfreeze_date": pl.Date,
    "offer_accepted_date": pl.Date,
    "days_frozen": pl.Utf8,
    "rfh_recruiter_id": pl.Utf8,
    "rfh_recruiter_id_exception": pl.Utf8,
    "recruiter_completed_offer": pl.Utf8,
    "source": pl.Utf8,
    "source_exception": pl.Utf8,
    "recruiting_agency": pl.Utf8,
    "referred_by_employee_id": pl.Utf8,
    "remarks": pl.Utf8,
    "termination_date": pl.Date,
    "termination_reason": pl.Utf8,
    "termination_reason_category": pl.Utf8,
    "termination_tenure_days_bucket": pl.Int32,
}

JOBREQ_SCHEMA: dict[str, pl.DataType] = {
    "job_requisition_id": pl.Utf8,
    "target_hire_date": pl.Date,
    "positions_filled_hire_selected": pl.Int32,
}


# ═════════════════════════════════════════════
# SCHEMA VALIDATION
# ═════════════════════════════════════════════

def validate_and_align_schema(
    df: pl.DataFrame,
    expected_schema: dict[str, pl.DataType],
    df_name: str,
) -> pl.DataFrame:

    if df.height == 0:
        log.warning("%s: empty DataFrame — creating empty with expected schema", df_name)
        return pl.DataFrame(schema=expected_schema)

    existing_cols = set(df.columns)
    missing_cols = [c for c in expected_schema if c not in existing_cols]

    if missing_cols:
        log.warning("%s: missing columns added as NULL → %s", df_name, missing_cols)

    exprs = [
        pl.lit(None).cast(dtype).alias(col)
        if col in missing_cols
        else pl.col(col).cast(dtype, strict=False)
        for col, dtype in expected_schema.items()
    ]

    return df.select(exprs)

# ═════════════════════════════════════════════
# SCHEMA CHECK
# ═════════════════════════════════════════════
def schema_check_lf(
    lf: pl.LazyFrame,
    target_schema: dict[str, pl.DataType],
) -> None:

    # ── Schema is resolved from Parquet metadata only ──
    actual_schema = lf.collect_schema()

    missing = set(target_schema) - set(actual_schema)
    extra = set(actual_schema) - set(target_schema)

    type_mismatch = {
        col: (actual_schema[col], target_schema[col])
        for col in target_schema
        if col in actual_schema and actual_schema[col] != target_schema[col]
    }

    if missing or type_mismatch:
        raise ValueError(
            f"""
            Missing columns: {missing}
            Type mismatches: {type_mismatch}
            Extra columns (allowed but ignored): {extra}
            """
        )

# ═════════════════════════════════════════════
# EXTRACT
# ═════════════════════════════════════════════

def read_excel(path: Path, skip_rows: int) -> pl.DataFrame:
    try:
        df = pl.read_excel(
            source=path,
            has_header=False,
            read_options={"skip_rows": skip_rows},
            engine="calamine",
        )
        header = df.row(0)
        df = df.slice(1)
        df.columns = [str(h) for h in header]
        return df
    except Exception as e:
        log.error(f"Failed to read Excel {path}: {e}")
        raise

def extract_incoming_hires_files(config: PipelineConfig) -> dict[str, pl.DataFrame]:

    raw_root = config["RAW_DATA_ROOT"]

    fulfillment = read_excel(
        latest_file(raw_root, config["FULFILLMENT_PATTERN"]),
        skip_rows=7
    )

    intx = read_excel(
        latest_file(raw_root, config["INTX_HIRES_PATTERN"]),
        skip_rows=10
    )

    log.info(
        "Extracted — fulfillment: %d rows, intx: %d rows",
        fulfillment.height,
        intx.height,
    )

    return {"fulfillment": fulfillment, "intx": intx}


# ═════════════════════════════════════════════
# UPSERT MERGE (Exception-Aware)
# ═════════════════════════════════════════════

def upsert_merge(
    master: pl.LazyFrame,
    incoming: pl.LazyFrame,
    key_col: str,
) -> pl.LazyFrame:

    master_schema = master.collect_schema().names()
    incoming_schema = incoming.collect_schema().names()

    master_col_set = set(master_schema)
    incoming_cols = list(incoming_schema)

    update_cols = [
        c for c in incoming_cols
        if c != key_col and c in master_col_set
    ]

    joined = master.join(
        incoming.select(key_col, *update_cols),
        on=key_col,
        how="left",
        suffix="_upd",
    )

    coalesce_exprs = []

    for col in update_cols:
        exc_col = f"{col}_exception"
        upd_col = f"{col}_upd"

        if exc_col in master_col_set:
            coalesce_exprs.append(
                pl.when(pl.col(exc_col).is_not_null())
                .then(pl.col(exc_col))
                .when(pl.col(upd_col).is_not_null())
                .then(pl.col(upd_col))
                .otherwise(pl.col(col))
                .alias(col)
            )
        else:
            coalesce_exprs.append(
                pl.coalesce(pl.col(upd_col), pl.col(col)).alias(col)
            )

    updated = joined.with_columns(coalesce_exprs).select(master_schema)

    new_rows = incoming.join(master.select(key_col),on=key_col,how="anti")

    new_rows_aligned = new_rows.with_columns([
        pl.lit(None).cast(dtype).alias(col)
        for col, dtype in HIRES_SCHEMA.items()
        if col not in incoming_cols
    ]).select(HIRES_SCHEMA.keys())

    schema_check_lf(new_rows_aligned, HIRES_SCHEMA)

    return pl.concat([updated, new_rows_aligned], how="vertical")

# ═════════════════════════════════════════════
# SUB FUNCTIONS
# ═════════════════════════════════════════════

def select_rename(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    return df.select([
        pl.col(src).alias(dst)
        for src, dst in mapping.items()
        if src in df.columns
    ])

def _make_eehire_id(employee_col: str = "employee_id") -> pl.Expr:
    return (
        pl.col(employee_col).cast(pl.Utf8)
        + pl.lit("_")
        + pl.col("candidate_start_date").dt.strftime("%Y%m")
    ).alias("eehire_id")

def _make_jree_id(employee_col: str = "employee_id") -> pl.Expr:
    return (
        pl.col("job_requisition_id").cast(pl.Utf8)
        + pl.lit("_")
        + pl.col(employee_col).cast(pl.Utf8)
    ).alias("jree_id")

def parse_date(col: str) -> pl.Expr:

    if col == "offer_accepted_date":
        # Special format for this column
        return pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d", strict=False)

    elif col == "job_application_date" or col == "ready_for_hire_date":
        return (
            pl.col(col)
            .str.strptime(pl.Datetime,format="%Y-%m-%d %H:%M:%S%.f",strict=False)
            .dt.date()
        )
    else:
        return (
            pl.col(col)
            .cast(pl.String)
            .str.strptime(
                pl.Date, 
                format="%Y-%m-%d %H:%M:%S", 
                strict=False
            )
            .fill_null(
                pl.col(col)
                .cast(pl.String)
                .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
            )
        )

# ═════════════════════════════════════════════
# BUILD INCOMING HIRES
# ═════════════════════════════════════════════    

def build_incoming_hires_file(
    fulfillment: pl.DataFrame,
    intx: pl.DataFrame,
) -> pl.LazyFrame:

    cutoff = date.today().replace(day=1) - relativedelta(months=3)

    fulfillment_lf = select_rename(fulfillment, FULFILLMENT_RENAME_MAP).lazy()
    intx_lf = select_rename(intx, INTX_RENAME_MAP).lazy()

    fulfillment_lf = (
        fulfillment_lf
        .with_columns([
            parse_date(c)
            for c in fulfillment_lf.collect_schema().names()
            if c.endswith("_date")         
        ])
        .filter(
            (pl.col("candidate_start_date") >= cutoff) & (~pl.col("grade_proposed").is_in(["Level 9", "Level 10"]))
        )
        .with_columns(
            pl.col("business_process_reason")
            .cast(pl.Utf8)
            .str.split(" > ")
            .list.first()
            .alias("business_process_reason"),
            (
                pl.col("job_requisition_id").cast(pl.Utf8)
                + pl.lit("_")
                + pl.col("position_id").cast(pl.Utf8)
            ).alias("jrpos_id"),
            _make_eehire_id(),
            _make_jree_id(),
        )
    )

    intx_lf = (
        intx_lf
        .with_columns([
            parse_date(c)
            for c in intx_lf.collect_schema().names()
            if c.endswith("_date")     
        ])      
        .filter(pl.col("candidate_start_date") >= cutoff)
        .with_columns(
            pl.coalesce(
                pl.col("recruiter_completed_offer").str.extract(r"\((\d+)\)"),
                pl.col("recruiter_employee_id").cast(pl.Utf8),
            ).alias("rfh_recruiter_id"),

            ## Update frozen day
            pl.when(pl.col("jr_unfreeze_date").is_null())
            .then(0)
            .otherwise(pl.col("days_frozen"))
            .alias("days_frozen"),

            ## offer_accepted_date if null get ready_for_hire_date
            pl.coalesce(
                pl.col("offer_accepted_date"),
                pl.col("ready_for_hire_date"),
            ).alias("offer_accepted_date"),

            _make_eehire_id(),
        )
        .drop(
            "candidate_start_date",
            "employee_id",
            "recruiter_employee_id",
            strict=False,
        )
    )

    return (
        fulfillment_lf
        .join(intx_lf, on="eehire_id", how="left")
        .sort("candidate_start_date", "employee_id")
        .unique(subset=["eehire_id"], keep="last")
    )


# ═════════════════════════════════════════════
# DISCREPANCY REPORT
# ═════════════════════════════════════════════

def generate_discrepancy_report(
    hires_df: pl.DataFrame,
    jobreq_df: pl.DataFrame,
) -> pl.DataFrame:

    hires_agg = (
        hires_df.lazy()
        .group_by("job_requisition_id")
        .agg(pl.len().alias("hires_count"))
    )

    jobreq_sel = (
        jobreq_df.lazy()
        .select(
            "job_requisition_id",
            pl.col("positions_filled_hire_selected")
            .fill_null(0)
            .cast(pl.Int32)
            .alias("jobreq_filled_count"),
        )
        .unique(subset=["job_requisition_id"])
    )

    discrepancies = (
        jobreq_sel
        .join(hires_agg, on="job_requisition_id", how="left")
        .with_columns(pl.col("hires_count").fill_null(0))
        .with_columns(
            (pl.col("jobreq_filled_count") - pl.col("hires_count"))
            .abs()
            .alias("discrepancy")
        )
        .filter(pl.col("discrepancy") != 0)
        .collect()
    )

    if discrepancies.height > 0:
        log.warning("⚠️  %d JR discrepancies detected", discrepancies.height)
    else:
        log.info("✅ No discrepancies found.")

    return discrepancies

# ═════════════════════════════════════════════
# WRITE PARQUET ATOMIC
# ═════════════════════════════════════════════

def write_parquet_atomic(
    df: pl.DataFrame,
    target: Path,
    *,
    compression: str = "zstd",
) -> None:
    """
    Atomically write a Parquet file.
    Writes to a temporary file in the same directory
    and replaces the target on success.
    """

    if target.suffix != ".parquet":
        raise ValueError("Target path must end with '.parquet'")

    # Ensure unique temp file to avoid collisions
    tmp = target.with_name(target.name + ".tmp")

    try:
        df.write_parquet(tmp, compression=compression)
        tmp.replace(target)
    except Exception:
        # Optional: log or re-raise
        raise
    finally:
        # Delete only if file still exists as temp
        if tmp.exists():
            tmp.unlink()


# ═════════════════════════════════════════════
# MAIN PIPELINE
# ═════════════════════════════════════════════

def run_hire_pipeline(
    config: PipelineConfig,
    incoming_df: pl.DataFrame | None = None,
    master_df: pl.DataFrame | None = None,
) -> pl.DataFrame:

    log.info("🚀 Hires ETL started")

    # Build incoming
    if incoming_df is None:
        extracted = extract_incoming_hires_files(config)
        incoming_lf = build_incoming_hires_file(
            extracted["fulfillment"],
            extracted["intx"],
        )
    else:
        incoming_lf = incoming_df.lazy()

    # Load master / hiresdb
    if master_df is None:
        hiresdb_path = Path(config["OUTPUT_HIRESDB_PAR"])
        if not hiresdb_path.exists():
            raise FileNotFoundError(f"Master not found at {hiresdb_path}")
        master_df = pl.read_parquet(hiresdb_path)

    master_df = validate_and_align_schema(master_df, HIRES_SCHEMA, "Master")

    # Merge
    merged_df = (
        upsert_merge(master_df.lazy(), incoming_lf, key_col="jrpos_id")
        .collect()
    )

    # Discrepancy check
    jobreq_df = validate_and_align_schema(
        pl.read_parquet(Path(config["OUTPUT_JOBREQS_PAR"])),
        JOBREQ_SCHEMA,
        "Jobreq",
    )

    print(generate_discrepancy_report(merged_df, jobreq_df))

    incoming_lf.collect().write_parquet(Path("incoming.parquet"), compression="zstd")

    # Persist
    for dest in (
        config["OUTPUT_HIRESDB_PAR"],
        config["OUTPUT_HIRES_PAR"],
    ):
        write_parquet_atomic(merged_df, dest)
       

    log.info("✅ Hires ETL completed — %d rows", merged_df.height)

    return merged_df
