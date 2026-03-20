# referral.py  ·  Referral Applications ETL
# Reads from the funnel parquet produced by app.py, keeps only ERP / Referral
# source rows, deduplicates, and builds unique-headcount flags for erp.py.

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Final, Tuple

import polars as pl


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

# Columns selected from the funnel parquet
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

# Aligned output schema for referrals
REFERRAL_SCHEMA: Final[Dict[str, pl.DataType]] = {
    "candidate_id":               pl.Utf8,
    "job_requisition_id":         pl.Utf8,
    "referred_by_employee_id":    pl.Utf8,
    "referred_by":                pl.Utf8,
    "added_date":                 pl.Date,
    "source":                     pl.Utf8,
    "disposition_reason":         pl.Utf8,
    "consolidated_disposition":   pl.Utf8,
    "candidate_recruiting_status": pl.Utf8,
    "last_stage_number":          pl.Int16,
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


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────

def extract(config: dict) -> pl.LazyFrame:
    """
    Scan the funnel parquet (app.py output), keep only ERP / Referral sources,
    and project down to the columns needed for referral processing.
    """
    parq_path = Path(config["OUTPUT_FUNNEL_PAR"])
    if not parq_path.exists():
        raise FileNotFoundError(f"Funnel parquet not found: {parq_path}")

    log.info("Scanning funnel parquet: %s", parq_path.name)

    schema_names = set(pl.scan_parquet(parq_path).collect_schema().names())
    select_cols = [c for c in FUNNEL_COLS if c in schema_names]

    return (
        pl.scan_parquet(parq_path)
        .filter(pl.col("source").str.contains(REFERRAL_SOURCE_PATTERN))
        .select(select_cols)
    )


# ─────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────

def transform(lf: pl.LazyFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    1. Collect and align to REFERRAL_SCHEMA.
    2. Clean referred_by (strip anything after first '(').
    3. Build composite erp_id  = job_requisition_id_referred_by_employee_id_candidate_id.
    4. Build headcount_id      = referred_by_employee_id_YYYY-MM(added_date).
    5. Deduplicate on erp_id (keep last).
    6. Return (referrals_df, unique_referrals_df).
    """
    df = _ensure_schema(lf.collect(), REFERRAL_SCHEMA)

    # Clean referred_by name
    df = df.with_columns(
        pl.col("referred_by")
          .str.split("(").list.get(0)
          .str.strip_chars()
          .alias("referred_by")
    )

    # Null-safe composite key columns
    df = df.with_columns([
        pl.col("job_requisition_id").fill_null("").cast(pl.Utf8),
        pl.col("referred_by_employee_id").fill_null("").cast(pl.Utf8),
        pl.col("candidate_id").fill_null("").cast(pl.Utf8),
    ])

    # erp_id
    df = df.with_columns(
        (
            pl.col("job_requisition_id")
            + "_" + pl.col("referred_by_employee_id")
            + "_" + pl.col("candidate_id")
        ).alias("erp_id")
    )

    # headcount_id = referred_by_employee_id + '_' + YYYY-MM(added_date)
    df = df.with_columns(
        pl.col("added_date").dt.strftime("%Y-%m").alias("_yyyymm")
    ).with_columns(
        (pl.col("referred_by_employee_id") + "_" + pl.col("_yyyymm")).alias("headcount_id")
    ).drop("_yyyymm")

    # Deduplicate: one row per unique application
    df = df.unique(subset=["erp_id"], keep="last")

    # Unique headcount presence flags (consumed by erp.py)
    unique_df = (
        df.select("headcount_id")
        .unique()
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("has_referral"))
    )

    log.info("Referrals: %d rows | Unique headcount IDs: %d", df.height, unique_df.height)
    return df, unique_df


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

def run_referral_pipeline(config: dict) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Runs the referral ETL and writes OUTPUT_REFERRALS_PARQUET.

    Returns
    -------
    referrals_df      : pl.DataFrame  – full deduplicated referral records
    unique_referrals_df : pl.DataFrame  – one row per headcount_id, with has_referral=1
                         (passed to erp.py for headcount flag join)
    """
    _t0 = perf_counter()
    log.info("──────────────────────────────")
    log.info("🚀 Referral ETL started ...")

    with stage_timer("⏱  Extract •"):
        lf = extract(config)

    with stage_timer("⏱  Transform •"):
        referrals_df, unique_referrals_df = transform(lf)

    with stage_timer("⏱  Load •"):
        load(referrals_df, config)

    log.info("✅ Referral ETL completed • duration: %.2fs", perf_counter() - _t0)
    log.info("──────────────────────────────")

    return referrals_df, unique_referrals_df
