# ─────────────────────────────────────────────────────────────
# src/jobreq.py   ·   Job Requisitions ETL / cleansing
# ─────────────────────────────────────────────────────────────
from __future__ import annotations

import logging
import warnings
import re
from pathlib import Path
from contextlib import contextmanager
from time import perf_counter

import pandas as pd
import polars as pl

from src.utils.helpers import (
    latest_file,
    extract_date_from_filename,
)

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

_LOG_FORMAT = "%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s"
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Utilities
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
    """Snake_case + de-duplicate rename map for a list of column names."""
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


# ────────────── INDIVIDUAL EXTRACTS ──────────────

def _extract_refresh_date(config: dict) -> pd.Timestamp:
    """Most-recent All-Status file date token → refresh_date."""
    path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    return extract_date_from_filename(path)


def _extract_status(config: dict, jobreq_cutoff: pd.Timestamp) -> pl.LazyFrame:
    """All-Status sheet → cleansed & filtered status LazyFrame (snake_case)."""
    path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    log.info("%s", path.name)

    # Read with header row offset, extract header from first data row
    raw = pl.read_excel(path, has_header=False, read_options={"skip_rows": 13})
    headers = [str(v) for v in raw.row(0)]
    df = raw.slice(1).rename(dict(zip(raw.columns, headers)))
    df = df.rename(_build_rename_map(df.columns))

    usecols = [
        "job_requisition_id",
        "target_hire_date",
        "positions_filled_hire_selected",
        "positions_openings_available",
        "mbps_teams",
    ]
    cutoff = jobreq_cutoff.to_pydatetime().date()

    return (
        df.lazy()
        .select(usecols)
        .with_columns([
            pl.col("target_hire_date").cast(pl.Date, strict=False),
            pl.col("positions_filled_hire_selected").cast(pl.Int16, strict=False),
            pl.col("positions_openings_available").cast(pl.Int16, strict=False),
        ])
        .filter(pl.col("target_hire_date") >= pl.lit(cutoff))
        .sort("target_hire_date", descending=True)
        .unique(subset=["job_requisition_id"], keep="first", maintain_order=True)
        .with_columns(
            (pl.col("positions_openings_available") + pl.col("positions_filled_hire_selected"))
            .alias("positions_requested_new")
        )
        .with_columns(
            pl.when(pl.col("positions_requested_new") == 0).then(pl.lit("CAN"))
            .when(pl.col("positions_openings_available") > 0).then(pl.lit("OPEN"))
            .otherwise(pl.lit("FILLED"))
            .alias("jobreq_status")
        )
    )

# ────────────── BUNDLED EXTRACT ──────────────

def extract_all(config: dict, jobreq_cutoff: pd.Timestamp) -> dict:
    """Return every LazyFrame needed for transforms (already snake_case)."""
    return {
        "status": _extract_status(config, jobreq_cutoff),
        "scrum": _extract_scrum(config),
        "dmt": _extract_dmt(config),
        "thd_corr": _extract_thd_corr(config),
    }


# ────────────── LOAD ──────────────

def load_outputs(dfs: dict, refresh_date: pd.Timestamp, config: dict):
    with stage_timer("⏱  Load •"):
        jobreq: pl.DataFrame = dfs["jobreq"]
        jobreq.write_csv(config["OUTPUT_JOBREQS_CSV"], date_format="%Y-%m-%d")
        jobreq.write_parquet(config["OUTPUT_JOBREQS_PAR"])

        cols = [
            "job_requisition_id",
            "job_requisition",
            "function",
            "compensation_grade",
            "complexity",
            "main_recruiter_id",
            "main_recruiter",
            "positions_openings_available",
            "target_hire_date",
            "hiring_manager",
        ]
        jobreq_cols = [c for c in cols if c in jobreq.columns]
        open_reqs = jobreq.filter(pl.col("positions_openings_available") > 0).select(jobreq_cols)

        dmt_open: pl.DataFrame = dfs["dmt_open"]
        dmt_cols = [c for c in cols if c in dmt_open.columns]
        combined = pl.concat([open_reqs, dmt_open.select(dmt_cols)], how="diagonal")
        combined.write_csv(config["OUTPUT_JOBREQS_OPENDMT_CSV"], date_format="%Y-%m-%d")

        # Refresh marker
        pl.DataFrame({"refresh_date": [refresh_date.to_pydatetime().date()]}).write_csv(
            Path(config["PROCESSED_DATA_ROOT"]) / "refresh_date.csv"
        )


# ────────────── MISC ──────────────

def _backlog_count(df: pl.DataFrame, refresh_date: pd.Timestamp) -> int:
    rd = refresh_date.to_pydatetime().date()
    return int(df.filter(pl.col("target_hire_date") <= pl.lit(rd))["positions_openings_available"].sum() or 0)


# ────────────── MAIN PIPELINE ──────────────

def run_jobreq(config: dict) -> tuple[pl.DataFrame, pd.Timestamp]:
    """
    End-to-end Job Requisitions ETL.
    Returns the final jobreq DataFrame.
    """
    _t0_run = perf_counter()

    refresh_date = _extract_refresh_date(config)
    log.info("──────────────────────────────")
    log.info("🚀 Jobreq ETL started (refresh date=%s) ", refresh_date.date())

    years_scope = config.get("REQS_YEARS_SCOPE", 1)
    jobreq_cutoff = pd.Timestamp(refresh_date.year - years_scope, 1, 1)
    log.info("Scope THD ≥ %s", jobreq_cutoff.date())

    with stage_timer("⏱  Extract •"):
        extracts = extract_all(config, jobreq_cutoff)

    dfs = transform_all(extracts, refresh_date)

    load_outputs(dfs, refresh_date, config)

    log.info("Final Stats...")
    log.info("Backlog positions : %d", _backlog_count(dfs["jobreq"], refresh_date))
    log.info("Jobreqs rows=%d · cols=%d", dfs["jobreq"].shape[0], dfs["jobreq"].shape[1])
    log.info("DMT open rows=%d", len(dfs["dmt_open"]))
    log.info("✅ Jobreq ETL completed • duration : %.2fs", perf_counter() - _t0_run)
    log.info("──────────────────────────────")

    return dfs["jobreq"], refresh_date
