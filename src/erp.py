# src/erp.py  ·  ERP Headcount ETL
# Consumes:
#   - Monthly headcount Excel (raw source)
#   - unique_referrals_df produced by referral.py
# Produces:
#   - OUTPUT_MHEADCOUNT_PARQUET  (headcount rows flagged with has_referral)

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Final

import polars as pl

from ._utils import latest_file, build_rename_map, read_excel_with_header

__all__ = ["run_erp"]

log = logging.getLogger(__name__)

_SEP = "─" * 50


# ─────────────────────────────────────────────────────────────
# TYPE ALIAS
# ─────────────────────────────────────────────────────────────

PipelineConfig = dict[str, Any]


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

HEADCOUNT_RAW_COLS: Final[list[str]] = [
    "Masterfile[Reporting Date]",
    "Masterfile[Location]",
    "Masterfile[Function]",
    "Masterfile[Sub-function]",
    "Masterfile[Job Grade]",
    "Masterfile[Employee ID]",
]

HEADCOUNT_RENAME_MAP: Final[Dict[str, str]] = {
    "masterfile_reporting_date": "reporting_date",
    "masterfile_location":       "location",
    "masterfile_function":       "function",
    "masterfile_sub_function":   "sub_function",
    "masterfile_job_grade":      "job_grade",
    "masterfile_employee_id":    "employee_id",
}


# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────

def _parse_datetime_to_date(col: str, *, fmt: str | None = None) -> pl.Expr:
    return (
        pl.coalesce([
            pl.col(col).str.strptime(pl.Datetime, format=fmt, strict=False, exact=False),
            pl.col(col).cast(pl.Datetime, strict=False),
        ])
        .cast(pl.Date)
    )


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


def _extend_headcount(df: pl.DataFrame, reporting_col: str = "reporting_date") -> pl.DataFrame:
    """
    BIA always reports completed months. If the current month-end is not yet
    present, duplicate the latest month's rows stamped with today's month-end
    so downstream reporting sees an up-to-date snapshot.
    """
    if df.is_empty():
        return df

    df     = _ensure_date_col(df, reporting_col)
    target = _to_month_end(date.today())

    if df.select(pl.col(reporting_col).is_in([target]).any()).item():
        return df

    latest = df.select(pl.col(reporting_col).max()).item()
    if latest is None or latest > target:
        return df

    new_rows = (
        df.filter(pl.col(reporting_col) == latest)
        .with_columns(pl.lit(target).alias(reporting_col))
    )
    log.info("  headcount extended to month-end %s  (+%d rows)", target, new_rows.height)
    return pl.concat([df, new_rows]).sort(reporting_col)


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────

def extract(config: PipelineConfig) -> pl.DataFrame:
    """Read monthly headcount Excel and return a clean DataFrame."""
    path = latest_file(config["HISTORICAL_DATA_ROOT"], config["HEADCOUNT_PATTERN"])
    log.info("  source : %s", path.name)

    df = read_excel_with_header(path, skip_rows=1)
    df = df.select([c for c in HEADCOUNT_RAW_COLS if c in df.columns])
    df = df.rename(build_rename_map(df.columns)).rename(HEADCOUNT_RENAME_MAP)

    df = df.with_columns(
        _parse_datetime_to_date("reporting_date", fmt="%Y-%m-%d %H:%M:%S").alias("reporting_date")
    )
    log.info("  headcount rows : %d", df.height)
    return df


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
    log.info("  output : %s  (%d rows)", out_path.name, headcount_df.height)


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_erp(
    config: PipelineConfig,
    unique_referrals_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Orchestrate ERP headcount ETL.

    Parameters
    ----------
    config              : pipeline configuration dict
    unique_referrals_df : output of run_referral()[1]
                          (one row per headcount_id, has_referral=1)

    Returns
    -------
    headcount_df : pl.DataFrame – headcount with has_referral flags
    """
    log.info(_SEP)
    log.info("[erp] started")

    headcount_df = extract(config)
    headcount_df = transform(headcount_df, unique_referrals_df)
    load(headcount_df, config)

    log.info("[erp] done")
    log.info(_SEP)

    return headcount_df
