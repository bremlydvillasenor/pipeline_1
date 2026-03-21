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


def _extract_scrum(config: dict) -> pl.LazyFrame:
    df = pl.read_excel(config["SCRUM_FILE"], sheet_name="scrum", has_header=True)
    df = df.drop([
        "Hiring_Scrum[# Positions Openings Available]",
        "Hiring_Scrum[# Positions Filled (Hire selected)]",
        "Hiring_Scrum[Positions Requested_New]",
        "Hiring_Scrum[Target Hire Date]",
    ])
    bracket_map = {
        col: (m.group(1) if (m := re.search(r"\[(.*?)\]", col)) else col)
        for col in df.columns
    }
    df = df.rename(bracket_map)
    return df.rename(_build_rename_map(df.columns)).lazy()


def _extract_dmt(config: dict) -> pl.LazyFrame:
    df = pl.read_excel(
        config["SCRUM_FILE"],
        sheet_name="dmt",
        columns=[
            "Job Requisition ID",
            "Job Requisition",
            "Function",
            "Compensation Grade",
            "# Positions Openings Available",
            "Target Hire Date",
            "Main Recruiter ID",
            "Main Recruiter",
            "Hiring Manager",
            "is_logged",
            "complexity",
        ],
    )
    return df.rename(_build_rename_map(df.columns)).lazy()


def _extract_thd_corr(config: dict) -> pl.LazyFrame:
    df = pl.read_excel(
        config["SCRUM_FILE"],
        sheet_name="thd_correction",
        columns=["Job Requisition ID", "New Target Hire Date"],
    )
    return df.rename(_build_rename_map(df.columns)).lazy()


# ────────────── BUNDLED EXTRACT ──────────────

def extract_all(config: dict, jobreq_cutoff: pd.Timestamp) -> dict:
    """Return every LazyFrame needed for transforms (already snake_case)."""
    return {
        "status": _extract_status(config, jobreq_cutoff),
        "scrum": _extract_scrum(config),
        "dmt": _extract_dmt(config),
        "thd_corr": _extract_thd_corr(config),
    }


# ────────────── TRANSFORM HELPERS ──────────────

def _apply_new_thd(status_lf: pl.LazyFrame, corr_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Override THD when correction table provides a value."""
    return (
        status_lf
        .join(corr_lf, on="job_requisition_id", how="left")
        .with_columns(
            pl.when(pl.col("new_target_hire_date").is_not_null())
            .then(pl.col("new_target_hire_date"))
            .otherwise(pl.col("target_hire_date"))
            .cast(pl.Date)
            .alias("target_hire_date")
        )
        .drop("new_target_hire_date")
    )


def _add_rag_fields(lf: pl.LazyFrame, refresh_date: pd.Timestamp) -> pl.LazyFrame:
    """Business RAG metrics."""
    rd = refresh_date.to_pydatetime().date()
    return (
        lf
        .with_columns(
            (
                pl.col("target_hire_date")
                - pl.duration(days=pl.col("ots").fill_null(0).cast(pl.Int32))
            ).alias("rag_target_offer_acceptance_date")
        )
        .with_columns(
            pl.when(pl.col("positions_openings_available") > 0)
            .then(
                (pl.col("rag_target_offer_acceptance_date") - pl.lit(rd))
                .dt.total_days()
                .cast(pl.Float64)
            )
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("rag_days_remaining_offer")
        )
        .with_columns([
            pl.when(pl.col("positions_openings_available") == 0).then(pl.lit(""))
            .when(pl.col("target_hire_date") <= pl.lit(rd)).then(pl.lit("Backlog"))
            .when(pl.col("rag_days_remaining_offer") < 8).then(pl.lit("Red"))
            .when(pl.col("rag_days_remaining_offer") < 14).then(pl.lit("Amber"))
            .otherwise(pl.lit("Green"))
            .alias("rag_rating"),

            pl.when(pl.col("positions_openings_available") == 0).then(pl.lit("9"))
            .when(pl.col("target_hire_date") <= pl.lit(rd)).then(pl.lit("1"))
            .when(pl.col("rag_days_remaining_offer") < 8).then(pl.lit("2"))
            .when(pl.col("rag_days_remaining_offer") < 14).then(pl.lit("3"))
            .otherwise(pl.lit("4"))
            .alias("rag_rating_sort"),
        ])
        .with_columns(
            pl.when(pl.col("rag_target_offer_acceptance_date") < pl.lit(rd))
            .then(pl.lit(rd))
            .otherwise(pl.col("rag_target_offer_acceptance_date"))
            .alias("rag_processing_date")
        )
        .with_columns(
            pl.col("rag_processing_date").dt.strftime("%Y-%m").alias("rag_processing_month")
        )
    )


def _add_date_slices(lf: pl.LazyFrame, date_col: str, prefix: str = "") -> pl.LazyFrame:
    """Append year / month_year / yyyymm columns derived from date_col."""
    return lf.with_columns([
        pl.col(date_col).dt.year().alias(f"{prefix}year"),
        pl.col(date_col).dt.strftime("%b %Y").alias(f"{prefix}month_year"),
        pl.col(date_col).dt.strftime("%Y%m").alias(f"{prefix}yyyymm"),
    ])


# ────────────── BUNDLED TRANSFORM ──────────────

def transform_all(extracts: dict, refresh_date: pd.Timestamp) -> dict:
    """Return the final jobreq DataFrame plus DMT subset."""
    with stage_timer("⏱  Transform •"):
        status_lf = _apply_new_thd(extracts["status"], extracts["thd_corr"])

        # Keep only jobreqs present in status
        scrum_lf = extracts["scrum"].join(
            status_lf.select("job_requisition_id"),
            on="job_requisition_id",
            how="inner",
        )

        rd = refresh_date.to_pydatetime().date()
        jobreq_lf = (
            scrum_lf
            .join(status_lf, on="job_requisition_id", how="left")
            .pipe(_add_rag_fields, refresh_date=refresh_date)
            .pipe(_add_date_slices, date_col="target_hire_date", prefix="thd_")
            .with_columns([
                pl.col("complexity").fill_null("SPEC"),
                pl.when(pl.col("target_hire_date") > pl.lit(rd))
                .then(pl.lit("Yes"))
                .otherwise(pl.lit(""))
                .alias("is_future"),
            ])
        )

        jobreq_df = jobreq_lf.collect()

        dmt_lf = extracts["dmt"]
        if "is_logged" in dmt_lf.collect_schema().names():
            dmt_open = dmt_lf.filter(pl.col("is_logged") == "No").collect()
        else:
            dmt_open = dmt_lf.collect()

        return {"jobreq": jobreq_df, "dmt_open": dmt_open}


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

def run_jobreq_pipeline(config: dict) -> tuple[pl.DataFrame, pd.Timestamp]:
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
