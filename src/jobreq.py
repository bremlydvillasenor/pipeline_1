# src/jobreq.py  ·  Job Requisitions ETL
# Source:
#   - All-Status Excel (Workday requisition all-status report)
# Produces:
#   - OUTPUT_JOBREQS_PAR  (cleansed, deduplicated requisitions parquet)
#   - refresh_date.csv    (date token for downstream stages)

from __future__ import annotations

import logging
import warnings
from datetime import date
from pathlib import Path
from typing import Final

import polars as pl

from ._utils import (
    latest_file,
    extract_date_from_filename,
    build_rename_map,
    read_excel_with_header,
)

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

log = logging.getLogger(__name__)

_SEP = "─" * 50


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# All-Status Excel: 13 preamble rows before the header row
ALL_STATUS_SKIP_ROWS: Final[int] = 13

# Columns selected from the All-Status file (after snake_case rename)
INPUT_COLS: Final[list[str]] = [
    "job_requisition_id",
    "target_hire_date",
    "positions_filled_hire_selected",
    "positions_openings_available",
    "mbps_teams",
]

# Cast rules applied to each input column
INPUT_DTYPES: Final[dict[str, pl.DataType]] = {
    "job_requisition_id":             pl.Utf8,
    "target_hire_date":               pl.Date,
    "positions_filled_hire_selected": pl.Int16,
    "positions_openings_available":   pl.Int16,
    "mbps_teams":                     pl.Utf8,
}

# Final output schema — input columns + computed columns
OUTPUT_SCHEMA: Final[dict[str, pl.DataType]] = {
    **INPUT_DTYPES,
    "positions_requested_new": pl.Int16,
    "jobreq_status":           pl.Utf8,
}


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────

def extract(config: dict, cutoff: date) -> pl.LazyFrame:
    """
    Read the All-Status Excel, promote the header row, rename to snake_case,
    select INPUT_COLS, cast to INPUT_DTYPES, and filter rows where
    target_hire_date >= cutoff.
    """
    path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    log.info("  source : %s", path.name)

    df = read_excel_with_header(path, skip_rows=ALL_STATUS_SKIP_ROWS)
    df = df.rename(build_rename_map(df.columns))

    present = [c for c in INPUT_COLS if c in df.columns]
    missing = set(INPUT_COLS) - set(present)
    if missing:
        log.warning("  missing input columns: %s", sorted(missing))

    return (
        df.lazy()
        .select(present)
        .with_columns([
            pl.col(c).cast(INPUT_DTYPES[c], strict=False)
            for c in present
        ])
        .filter(pl.col("target_hire_date") >= pl.lit(cutoff))
    )


# ─────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────

def transform(lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Deduplicate on job_requisition_id (keep latest target_hire_date),
    compute positions_requested_new and jobreq_status, then enforce
    OUTPUT_SCHEMA.

    jobreq_status logic:
      CAN    → positions_requested_new == 0
      OPEN   → positions_openings_available > 0
      FILLED → otherwise
    """
    df = (
        lf
        .sort("target_hire_date", descending=True)
        .unique(subset=["job_requisition_id"], keep="first", maintain_order=True)
        .with_columns(
            (pl.col("positions_openings_available") + pl.col("positions_filled_hire_selected"))
            .cast(pl.Int16)
            .alias("positions_requested_new")
        )
        .with_columns(
            pl.when(pl.col("positions_requested_new") == 0).then(pl.lit("CAN"))
            .when(pl.col("positions_openings_available") > 0).then(pl.lit("OPEN"))
            .otherwise(pl.lit("FILLED"))
            .alias("jobreq_status")
        )
        .collect()
    )

    # Enforce output schema — cast and select in declared order
    return df.select([
        pl.col(c).cast(OUTPUT_SCHEMA[c])
        for c in OUTPUT_SCHEMA
        if c in df.columns
    ])


# ─────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────

def load(df: pl.DataFrame, config: dict, refresh_date: date) -> None:
    out_path = Path(config["OUTPUT_JOBREQS_PAR"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path, compression="zstd")
    log.info("  output : %s  (%d rows)", out_path.name, df.height)

    pl.DataFrame({"refresh_date": [refresh_date]}).write_csv(
        Path(config["PROCESSED_DATA_ROOT"]) / "refresh_date.csv"
    )


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_jobreq(config: dict) -> tuple[pl.DataFrame, date]:
    """
    End-to-end Job Requisitions ETL.

    Returns
    -------
    jobreq_df    : pl.DataFrame  – cleansed, deduplicated requisitions
    refresh_date : datetime.date – date token from the source filename
    """
    path         = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    refresh_date = extract_date_from_filename(path)
    years_scope  = config.get("REQS_YEARS_SCOPE", 1)
    cutoff       = date(refresh_date.year - years_scope, 1, 1)

    log.info(_SEP)
    log.info("[jobreq] started   refresh=%s  cutoff=%s", refresh_date, cutoff)

    lf        = extract(config, cutoff)
    jobreq_df = transform(lf)
    load(jobreq_df, config, refresh_date)

    counts = jobreq_df["jobreq_status"].value_counts().to_dicts()
    counts_str = "  ".join(f"{r['jobreq_status']}={r['count']}" for r in counts)
    log.info("  stats  : rows=%d  %s", jobreq_df.height, counts_str)
    log.info("[jobreq] done")
    log.info(_SEP)

    return jobreq_df, refresh_date
