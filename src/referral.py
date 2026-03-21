# referral.py  ·  Referral Applications ETL
# Sources:
#   1. Prospect Excel        – pre-funnel prospect stage rows
#   2. Funnel parquet        – produced by app.py; ERP / Referral source rows only
# Produces:
#   - OUTPUT_REFERRALS_PARQUET  (full ERP funnel: Prospect → Review → Ready for Hire)
#   - unique_referrals_df       (one row per headcount_id, has_referral=1, for erp.py)

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Final, Tuple

import polars as pl

from .utils.helpers import latest_file


__all__ = ["run_referral_pipeline"]


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# Matches sources that contain "ERP" or "Referral" (case-insensitive)
REFERRAL_SOURCE_PATTERN: Final[str] = r"(?i)ERP|Referral"

# Stage number assigned to prospect rows (before Review = 1)
PROSPECT_STAGE_NUMBER: Final[int] = 0
REVIEW_STAGE_NUMBER: Final[int] = 1

# Raw Excel column names for the prospect file → snake_case
PROSPECT_RENAME_MAP: Final[Dict[str, str]] = {
    "Candidate ID":                 "candidate_id",
    "Job Requisition ID":           "job_requisition_id",
    "Referred By Employee ID":      "referred_by_employee_id",
    "Referred By":                  "referred_by",
    "Added Date":                   "added_date",
    "Source":                       "source",
    "Disposition Reason":           "disposition_reason",
    "Candidate Recruiting Status":  "candidate_recruiting_status",
}

# Columns projected from the funnel parquet
FUNNEL_COLS: Final[list[str]] = [
    "candidate_id",
    "job_requisition_id",
    "referred_by_employee_id",
    "referred_by",
    "added_date",
    "source",
    "disposition_reason",
    "consolidated_disposition",
    "candidate_recruiting_status",
    "last_stage_number",
]

# Aligned output schema
REFERRAL_SCHEMA: Final[Dict[str, pl.DataType]] = {
    "candidate_id":                pl.Utf8,
    "job_requisition_id":          pl.Utf8,
    "referred_by_employee_id":     pl.Utf8,
    "referred_by":                 pl.Utf8,
    "added_date":                  pl.Date,
    "source":                      pl.Utf8,
    "disposition_reason":          pl.Utf8,
    "consolidated_disposition":    pl.Utf8,
    "candidate_recruiting_status": pl.Utf8,
    "last_stage_number":           pl.Int16,
}


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


def _ensure_schema(df: pl.DataFrame, schema: Dict[str, pl.DataType]) -> pl.DataFrame:
    """Select and cast to schema; missing columns are added as nulls."""
    for col, dtype in schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
    return df.select([pl.col(c).cast(schema[c]) for c in schema])


def _parse_date_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Coerce a string/mixed date column to pl.Date."""
    return df.with_columns(
        pl.coalesce([
            pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
            pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d", strict=False),
            pl.col(col).str.strptime(pl.Date, format="%m/%d/%Y", strict=False),
            pl.col(col).cast(pl.Date, strict=False),
        ]).alias(col)
    )


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────

def _extract_prospect(config: dict) -> pl.DataFrame:
    """
    Read the prospect Excel and return a DataFrame with columns aligned to
    REFERRAL_SCHEMA.  Prospect rows are stamped with last_stage_number = 0.
    """
    path = latest_file(config["RAW_DATA_ROOT"], config["PROSPECT_PATTERN"])
    log.info("Reading prospect Excel: %s", path.name)

    df = pl.read_excel(path, engine="calamine")

    # Select & rename only the columns present in the file
    present = {k: v for k, v in PROSPECT_RENAME_MAP.items() if k in df.columns}
    df = df.select([pl.col(src).alias(dst) for src, dst in present.items()])

    if "added_date" in df.columns:
        df = _parse_date_col(df, "added_date")

    # Stamp prospect stage number (no recruiting stage yet)
    df = df.with_columns(
        pl.lit(PROSPECT_STAGE_NUMBER, dtype=pl.Int16).alias("last_stage_number")
    )

    log.info("Prospect rows extracted: %d", df.height)
    return df


def _extract_erp_funnel(config: dict) -> pl.DataFrame:
    """
    Scan the funnel parquet (app.py output), keep only ERP / Referral source
    rows, project to FUNNEL_COLS, and collect.
    Resulting rows cover Review (1) → Ready for Hire (8).
    """
    parq_path = Path(config["OUTPUT_FUNNEL_PAR"])
    if not parq_path.exists():
        raise FileNotFoundError(f"Funnel parquet not found: {parq_path}")

    log.info("Scanning funnel parquet: %s", parq_path.name)

    schema_names = set(pl.scan_parquet(parq_path).collect_schema().names())
    select_cols = [c for c in FUNNEL_COLS if c in schema_names]

    df = (
        pl.scan_parquet(parq_path)
        .filter(pl.col("source").str.contains(REFERRAL_SOURCE_PATTERN))
        .select(select_cols)
        .collect()
    )

    log.info("ERP funnel rows (Review → RfH): %d", df.height)
    return df


def extract(config: dict) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Return (prospect_df, erp_funnel_df)."""
    prospect_df   = _extract_prospect(config)
    erp_funnel_df = _extract_erp_funnel(config)
    return prospect_df, erp_funnel_df


# ─────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────

def _apply_common_transforms(df: pl.DataFrame) -> pl.DataFrame:
    """
    Shared transforms applied to both DataFrames before merging:
      - Align to REFERRAL_SCHEMA (add missing cols as null, cast types)
      - Clean referred_by: strip employee ID in parentheses
      - Null-safe key columns
    """
    df = _ensure_schema(df, REFERRAL_SCHEMA)

    # "John Smith (12345)" → "John Smith"
    df = df.with_columns(
        pl.col("referred_by")
          .str.split("(").list.get(0)
          .str.strip_chars()
          .alias("referred_by")
    )

    # Null-safe composite key parts
    df = df.with_columns([
        pl.col("job_requisition_id").fill_null("").cast(pl.Utf8),
        pl.col("referred_by_employee_id").fill_null("").cast(pl.Utf8),
        pl.col("candidate_id").fill_null("").cast(pl.Utf8),
    ])

    return df


def transform(
    prospect_df: pl.DataFrame,
    erp_funnel_df: pl.DataFrame,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Build a full ERP funnel spanning Prospect → Review → … → Ready for Hire.

    Steps
    -----
    1.  Apply common transforms to both source DataFrames.
    2.  Extract Review-stage rows from erp_funnel_df.
    3.  prospect_review_df  = prospect_df  +  review_df
    4.  full_erp_funnel_df  = erp_funnel_df  +  prospect_review_df
        → now contains all stages: Prospect (0), Review (1) … Ready for Hire (8)
    5.  Build erp_id (composite dedup key) and headcount_id.
    6.  Deduplicate on erp_id (keep last).
    7.  Return (referrals_df, unique_referrals_df).
    """
    prospect_df   = _apply_common_transforms(prospect_df)
    erp_funnel_df = _apply_common_transforms(erp_funnel_df)

    # ── Step 2: Review-stage slice from ERP funnel ──────────────────────────
    review_df = erp_funnel_df.filter(
        pl.col("last_stage_number") == REVIEW_STAGE_NUMBER
    )

    # ── Step 3: prospect + review → top-of-funnel bridge ───────────────────
    prospect_review_df = pl.concat([prospect_df, review_df], how="diagonal")
    log.info(
        "prospect_review_df: %d rows  (prospect=%d  review=%d)",
        prospect_review_df.height, prospect_df.height, review_df.height,
    )

    # ── Step 4: append to full ERP funnel ───────────────────────────────────
    full_erp_funnel_df = pl.concat([erp_funnel_df, prospect_review_df], how="diagonal")

    # ── Step 5a: erp_id ──────────────────────────────────────────────────────
    full_erp_funnel_df = full_erp_funnel_df.with_columns(
        (
            pl.col("job_requisition_id")
            + "_" + pl.col("referred_by_employee_id")
            + "_" + pl.col("candidate_id")
        ).alias("erp_id")
    )

    # ── Step 5b: headcount_id = referred_by_employee_id + '_' + YYYY-MM ─────
    full_erp_funnel_df = (
        full_erp_funnel_df
        .with_columns(pl.col("added_date").dt.strftime("%Y-%m").alias("_yyyymm"))
        .with_columns(
            (pl.col("referred_by_employee_id") + "_" + pl.col("_yyyymm")).alias("headcount_id")
        )
        .drop("_yyyymm")
    )

    # ── Step 6: deduplicate ──────────────────────────────────────────────────
    full_erp_funnel_df = full_erp_funnel_df.unique(subset=["erp_id"], keep="last")

    # ── Step 7: unique headcount flags (consumed by erp.py) ─────────────────
    unique_df = (
        full_erp_funnel_df.select("headcount_id")
        .unique()
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("has_referral"))
    )

    log.info(
        "Full ERP funnel: %d rows | Unique headcount IDs: %d",
        full_erp_funnel_df.height, unique_df.height,
    )
    return full_erp_funnel_df, unique_df


# ─────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────

def load(referrals_df: pl.DataFrame, config: dict) -> None:
    out_path = Path(config["OUTPUT_REFERRALS_PARQUET"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    referrals_df.write_parquet(out_path, compression="zstd")
    log.info("Referrals written → %s", out_path.name)


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_referral(config: dict) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Runs the referral ETL and writes OUTPUT_REFERRALS_PARQUET.

    Sources
    -------
    - Prospect Excel  (config["PROSPECT_PATTERN"])  – pre-funnel prospect rows
    - Funnel parquet  (config["OUTPUT_FUNNEL_PAR"]) – ERP/Referral source rows

    Flow
    ----
    prospect_df + review_df          →  prospect_review_df
    erp_funnel_df + prospect_review_df  →  full_erp_funnel_df
                                           (Prospect → Review → … → Ready for Hire)

    Returns
    -------
    referrals_df        : pl.DataFrame – full deduplicated ERP funnel records
    unique_referrals_df : pl.DataFrame – one row per headcount_id, has_referral=1
                          (passed to erp.py for headcount flag join)
    """
    _t0 = perf_counter()
    log.info("──────────────────────────────")
    log.info("🚀 Referral ETL started ...")

    with stage_timer("⏱  Extract •"):
        prospect_df, erp_funnel_df = extract(config)

    with stage_timer("⏱  Transform •"):
        referrals_df, unique_referrals_df = transform(prospect_df, erp_funnel_df)

    with stage_timer("⏱  Load •"):
        load(referrals_df, config)

    log.info("✅ Referral ETL completed • duration: %.2fs", perf_counter() - _t0)
    log.info("──────────────────────────────")

    return referrals_df, unique_referrals_df
