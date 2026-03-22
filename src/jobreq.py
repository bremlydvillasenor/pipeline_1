# src/jobreq.py  ·  Job Requisitions ETL
# Source:
#   - All-Status Excel (Workday requisition all-status report)
# Produces:
#   - OUTPUT_JOBREQS_PAR  (cleansed, deduplicated requisitions parquet)
#   - refresh_date.csv    (date token for downstream stages)

from __future__ import annotations

import logging
import re
import warnings
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Final

import polars as pl

from ._utils import latest_file, extract_date_from_filename

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

_LOG_FORMAT = "%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s"
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
log = logging.getLogger(__name__)


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
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"


def _build_rename_map(columns: list[str]) -> dict[str, str]:
    """Snake_case + de-duplicate rename map for a list of raw column names."""
    counts: dict[str, int] = {}
    result: dict[str, str] = {}
    for col in columns:
        snake = _to_snake(col)
        if snake in counts:
            counts[snake] += 1
            result[col] = f"{snake}_{counts[snake]}"
        else:
            counts[snake] = 0
            result[col] = snake
    return result


# ─────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────

def extract(config: dict, cutoff: date) -> pl.LazyFrame:
    """
    Read the All-Status Excel, promote the header row, rename to snake_case,
    select INPUT_COLS, cast to INPUT_DTYPES, and filter rows where
    target_hire_date >= cutoff.

    Returns a LazyFrame ready for transform().
    """
    path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    log.info("Reading all-status: %s", path.name)

    raw = pl.read_excel(path, has_header=False, read_options={"skip_rows": ALL_STATUS_SKIP_ROWS})
    headers = [str(v) for v in raw.row(0)]
    df = raw.slice(1).rename(dict(zip(raw.columns, headers)))
    df = df.rename(_build_rename_map(df.columns))

    present = [c for c in INPUT_COLS if c in df.columns]
    missing = set(INPUT_COLS) - set(present)
    if missing:
        log.warning("Expected input columns not found: %s", sorted(missing))

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
      - CAN    → positions_requested_new == 0
      - OPEN   → positions_openings_available > 0
      - FILLED → otherwise
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
    log.info("Jobreqs written → %s (%d rows)", out_path.name, df.height)

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
    _t0 = perf_counter()

    path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    refresh_date: date = extract_date_from_filename(path).date()
    years_scope: int = config.get("REQS_YEARS_SCOPE", 1)
    cutoff = date(refresh_date.year - years_scope, 1, 1)

    log.info("──────────────────────────────")
    log.info("🚀 Jobreq ETL started  (refresh=%s  scope≥%s)", refresh_date, cutoff)

    with stage_timer("⏱  Extract •"):
        lf = extract(config, cutoff)

    with stage_timer("⏱  Transform •"):
        jobreq_df = transform(lf)

    with stage_timer("⏱  Load •"):
        load(jobreq_df, config, refresh_date)

    status_counts = jobreq_df["jobreq_status"].value_counts().to_dicts()
    counts_str = "  ".join(f"{r['jobreq_status']}={r['count']}" for r in status_counts)
    log.info("Final stats  rows=%d  %s", jobreq_df.height, counts_str)
    log.info("✅ Jobreq ETL completed • duration: %.2fs", perf_counter() - _t0)
    log.info("──────────────────────────────")

    return jobreq_df, refresh_date
