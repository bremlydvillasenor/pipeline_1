# src/referral.py  ·  Referral ETL
# Sources:
#   1. Prospect Excel        – pre-funnel prospect stage rows
#   2. Funnel parquet        – produced by app.py; ERP / Referral source rows only
# Produces:
#   - OUTPUT_REFERRALS_PARQUET  (full ERP funnel: Prospect → Review → Ready for Hire)
#   - unique_referrals_df       (one row per headcount_id, has_referral=1, for erp.py)

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Final, Tuple

import polars as pl

from ._utils import latest_file, ensure_schema

__all__ = ["run_referral"]

log = logging.getLogger(__name__)

_SEP = "─" * 50


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

REFERRAL_SOURCE_PATTERN: Final[str] = r"(?i)ERP|Referral"

PROSPECT_STAGE_NUMBER: Final[int] = 0
REVIEW_STAGE_NUMBER:   Final[int] = 1

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

FUNNEL_COLS: Final[list[str]] = [
    "candidate_id", "job_requisition_id",
    "referred_by_employee_id", "referred_by",
    "added_date", "source", "disposition_reason",
    "candidate_recruiting_status", "last_stage_number",
]

REFERRAL_SCHEMA: Final[Dict[str, pl.DataType]] = {
    "candidate_id":                pl.Utf8,
    "job_requisition_id":          pl.Utf8,
    "referred_by_employee_id":     pl.Utf8,
    "referred_by":                 pl.Utf8,
    "added_date":                  pl.Date,
    "source":                      pl.Utf8,
    "disposition_reason":          pl.Utf8,
    "candidate_recruiting_status": pl.Utf8,
    "last_stage_number":           pl.Int16,
}


# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────

def _parse_date_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Coerce a string/mixed date column to pl.Date."""
    return df.with_columns(
        pl.coalesce([
            pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d",  strict=False),
            pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d",  strict=False),
            pl.col(col).str.strptime(pl.Date, format="%m/%d/%Y",  strict=False),
            pl.col(col).cast(pl.Date, strict=False),
        ]).alias(col)
    )


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────

def _extract_prospect(config: dict) -> pl.DataFrame:
    path = latest_file(config["RAW_DATA_ROOT"], config["PROSPECT_PATTERN"])
    log.info("  source : %s", path.name)

    df = pl.read_excel(path, engine="calamine")

    present = {k: v for k, v in PROSPECT_RENAME_MAP.items() if k in df.columns}
    df = df.select([pl.col(src).alias(dst) for src, dst in present.items()])

    if "added_date" in df.columns:
        df = _parse_date_col(df, "added_date")

    df = df.with_columns(
        pl.lit(PROSPECT_STAGE_NUMBER, dtype=pl.Int16).alias("last_stage_number")
    )
    log.info("  prospect rows : %d", df.height)
    return df


def _extract_erp_funnel(config: dict) -> pl.DataFrame:
    parq_path = Path(config["OUTPUT_FUNNEL_PAR"])
    if not parq_path.exists():
        raise FileNotFoundError(f"Funnel parquet not found: {parq_path}")
    log.info("  source : %s", parq_path.name)

    schema_names = set(pl.scan_parquet(parq_path).collect_schema().names())
    select_cols  = [c for c in FUNNEL_COLS if c in schema_names]

    df = (
        pl.scan_parquet(parq_path)
        .filter(pl.col("source").str.contains(REFERRAL_SOURCE_PATTERN))
        .select(select_cols)
        .collect()
    )
    log.info("  erp funnel rows (Review → RfH) : %d", df.height)
    return df


def extract(config: dict) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Return (prospect_df, erp_funnel_df)."""
    return _extract_prospect(config), _extract_erp_funnel(config)


# ─────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────

def _apply_common_transforms(df: pl.DataFrame) -> pl.DataFrame:
    df = ensure_schema(df, REFERRAL_SCHEMA)

    # "John Smith (12345)" → "John Smith"
    df = df.with_columns(
        pl.col("referred_by").str.split("(").list.get(0).str.strip_chars()
        .alias("referred_by")
    )

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
    1. Apply common transforms to both source DataFrames.
    2. Extract Review-stage rows from erp_funnel_df.
    3. prospect_review_df  = prospect_df + review_df
    4. full_erp_funnel_df  = erp_funnel_df + prospect_review_df
    5. Build erp_id and headcount_id.
    6. Deduplicate on erp_id (keep last).
    7. Return (referrals_df, unique_referrals_df).
    """
    prospect_df   = _apply_common_transforms(prospect_df)
    erp_funnel_df = _apply_common_transforms(erp_funnel_df)

    review_df = erp_funnel_df.filter(pl.col("last_stage_number") == REVIEW_STAGE_NUMBER)

    prospect_review_df = (
        pl.concat([prospect_df, review_df], how="diagonal")
        .with_columns(pl.lit(PROSPECT_STAGE_NUMBER, dtype=pl.Int16).alias("last_stage_number"))
    )
    log.info(
        "  prospect_review : %d rows  (prospect=%d  review=%d)",
        prospect_review_df.height, prospect_df.height, review_df.height,
    )

    full_erp_funnel_df = pl.concat([erp_funnel_df, prospect_review_df], how="diagonal")

    full_erp_funnel_df = full_erp_funnel_df.with_columns(
        (
            pl.col("job_requisition_id")
            + "_" + pl.col("referred_by_employee_id")
            + "_" + pl.col("candidate_id")
        ).alias("erp_id")
    )

    full_erp_funnel_df = (
        full_erp_funnel_df
        .with_columns(pl.col("added_date").dt.strftime("%Y-%m").alias("_yyyymm"))
        .with_columns(
            (pl.col("referred_by_employee_id") + "_" + pl.col("_yyyymm")).alias("headcount_id")
        )
        .drop("_yyyymm")
    )

    full_erp_funnel_df = full_erp_funnel_df.unique(subset=["erp_id"], keep="last")

    unique_df = (
        full_erp_funnel_df.select("headcount_id")
        .unique()
        .with_columns(pl.lit(1, dtype=pl.Int8).alias("has_referral"))
    )

    log.info(
        "  stats  : erp_funnel=%d rows  unique_headcount_ids=%d",
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
    log.info("  output : %s  (%d rows)", out_path.name, referrals_df.height)


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_referral(config: dict) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Orchestrate referral ETL.

    Returns
    -------
    referrals_df        : pl.DataFrame – full deduplicated ERP funnel
    unique_referrals_df : pl.DataFrame – one row per headcount_id, has_referral=1
    """
    log.info(_SEP)
    log.info("[referral] started")

    prospect_df, erp_funnel_df     = extract(config)
    referrals_df, unique_referrals_df = transform(prospect_df, erp_funnel_df)
    load(referrals_df, config)

    log.info("[referral] done")
    log.info(_SEP)

    return referrals_df, unique_referrals_df
