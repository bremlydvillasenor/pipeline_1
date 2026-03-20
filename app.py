# src/pipelines/apps.py  ·  Applications ETL / cleansing

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any, Final, Sequence

import polars as pl

from .utils import funnel_app
from .utils.helpers import latest_file, extract_date_from_filename


# ═════════════════════════════════════════════
# LOGGING
# ═════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════
# TYPE ALIAS
# ═════════════════════════════════════════════

PipelineConfig = dict[str, Any]


# ═════════════════════════════════════════════
# CONSTANTS
# ═════════════════════════════════════════════

STAGE_MAP: Final[dict[str, int]] = {
    "Review": 1, "Screen": 2, "Assessment": 3, "Interview": 4,
    "Reference Check": 5, "Offer": 6, "Background Check": 7, "Ready for Hire": 8,
}

COLS_APP: Final[Sequence[str]] = (
    "Candidate ID", "Candidate Name",
    "Job Requisition ID", "Job Requisition", "Recruiting Instruction",
    "Job Family", "Compensation Grade",
    "Worker Type Hiring Requirement", "Worker Sub-Type Hiring Requirement",
    "Target Hire Date", "Added Date", "Job Application Date", "Offer Accepted Date", "Candidate Start Date",
    "Recruiter Employee ID", "Recruiter Completed Offer",
    "Disposition Reason", "Candidate Recruiting Status",
    "Last Recruiting Stage", "Hired", "Hire Transaction Status", "Source",
    "Referred By Employee ID", "Referred By", "Recruiting Agency",
    "Last Employer", "School Name", "Is cancelled?", "MBPS Teams",
)

DATE_COLS: Final[tuple[str, ...]] = (
    "added_date", "job_application_date", "offer_accepted_date",
    "candidate_start_date", "target_hire_date",
)

RAW_COLS_SNAKE: Final[list[str]] = [
    "candidate_id", "candidate_name",
    "job_requisition_id", "job_requisition", "recruiting_instruction",
    "job_family", "compensation_grade",
    "worker_type_hiring_requirement", "worker_sub_type_hiring_requirement",
    "target_hire_date", "added_date", "job_application_date", "offer_accepted_date", "candidate_start_date",
    "recruiter_employee_id", "recruiter_completed_offer",
    "disposition_reason", "candidate_recruiting_status",
    "last_recruiting_stage", "hired", "hire_transaction_status", "source",
    "referred_by_employee_id", "referred_by", "recruiting_agency",
    "last_employer", "school_name", "is_cancelled", "mbps_teams",
]

ENGINEERED_COLS: Final[list[str]] = [
    "last_recruiting_stage_orig",
    "candidate_recruiting_status_orig",
    "disposition_reason_orig",
    "recruiter_completed_offer_id",
    "last_stage_number",
    "consolidated_disposition", "consolidated_disposition_2",
    "consolidated_channel", "channel_sort", "internal_external",
    "is_non_auto_dispo", "is_candidate_driven_dispo",
    "function", "sub_function", "rag_target_offer_acceptance_date",
    "on_time_offer_accept", "complexity",
]


# ═════════════════════════════════════════════
# UTILITIES
# ═════════════════════════════════════════════

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


def _normalize_colnames(df: pl.DataFrame) -> pl.DataFrame:
    raw = [_to_snake(c) for c in df.columns]
    counts: dict[str, int] = {}
    unique: list[str] = []
    for c in raw:
        n = counts.get(c, 0)
        unique.append(c if n == 0 else f"{c}_{n}")
        counts[c] = n + 1
    return df.rename(dict(zip(df.columns, unique)))


def _parse_date(col: str) -> pl.Expr:
    """Try common date string formats; returns pl.Date, null on failure."""
    return pl.coalesce([
        pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
        pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d", strict=False),
        pl.col(col).str.strptime(pl.Date, format="%m/%d/%Y", strict=False),
    ])


# ═════════════════════════════════════════════
# EXTRACT
# ═════════════════════════════════════════════

def extract(config: PipelineConfig) -> dict:
    status_path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    refresh_date = extract_date_from_filename(status_path)
    app_cutoff = date(refresh_date.year - 4, 1, 1)

    app_path = latest_file(config["RAW_DATA_ROOT"], config["DAILY_APP_PATTERN"])
    app_df = _normalize_colnames(
        pl.scan_csv(app_path, skip_rows=16)
        .select(list(COLS_APP))
        .collect()
    )

    dispo_map = _normalize_colnames(
        pl.read_excel(config["SCRUM_FILE"], sheet_name="disposition_mapping")
    )

    source_map = _normalize_colnames(
        pl.read_excel(
            config["SCRUM_FILE"],
            sheet_name="source_mapping",
            columns=["source", "consolidated_channel", "channel_sort", "internal_external"],
        )
    )

    parq_path: Path = config["OUTPUT_JOBREQS_PAR"]
    if not parq_path.exists():
        raise FileNotFoundError(f"No jobreq parq found at {parq_path}")
    jobreq_df = _normalize_colnames(pl.read_parquet(parq_path))

    return {
        "app_df": app_df,
        "dispo_map": dispo_map,
        "source_map": source_map,
        "jobreq_df": jobreq_df,
        "app_cutoff": app_cutoff,
        "refresh_date": refresh_date,
    }


# ═════════════════════════════════════════════
# TRANSFORM
# ═════════════════════════════════════════════

def _apply_rescind_correction(df: pl.DataFrame) -> pl.DataFrame:
    """Correct stage and disposition for rescinded offers."""
    needed = {
        "hire_transaction_status", "hired",
        "last_recruiting_stage_orig", "candidate_recruiting_status_orig",
        "disposition_reason",
    }
    if not needed.issubset(df.columns):
        return df

    rescind_mask = (
        (pl.col("hire_transaction_status") == "Rescinded")
        & pl.col("hired").is_null()
        & (pl.col("last_recruiting_stage_orig") == "Ready for Hire")
        & (pl.col("candidate_recruiting_status_orig") == "Offer Accepted")
    )
    stage_update = (
        ((pl.col("last_recruiting_stage_orig") == "Ready for Hire") & pl.col("disposition_reason").is_not_null())
        | rescind_mask
    )

    return df.with_columns([
        pl.when(stage_update)
          .then(pl.lit("Offer"))
          .otherwise(pl.col("last_recruiting_stage"))
          .alias("last_recruiting_stage"),
        pl.when(rescind_mask)
          .then(pl.lit("Offer Accepted to Rescinded"))
          .otherwise(pl.col("disposition_reason"))
          .alias("disposition_reason"),
    ])


def transform(raw: dict, config: PipelineConfig) -> dict:
    df: pl.DataFrame = raw["app_df"]
    dispo_map: pl.DataFrame = raw["dispo_map"]
    source_map: pl.DataFrame = raw["source_map"]
    jobreq_df: pl.DataFrame = raw["jobreq_df"]
    app_cutoff: date = raw["app_cutoff"]

    # Fill missing source
    if "source" in df.columns:
        df = df.with_columns(
            pl.col("source").fill_null("zzz_unknown").cast(pl.Utf8)
        )

    # Parse date columns
    date_exprs = [_parse_date(c) for c in DATE_COLS if c in df.columns]
    if date_exprs:
        df = df.with_columns(date_exprs)

    # Filter by cutoff and remove Level 9
    if "job_application_date" in df.columns:
        df = df.filter(pl.col("job_application_date") >= app_cutoff)
    if "compensation_grade" in df.columns:
        df = df.filter(pl.col("compensation_grade") != "Level 9")

    # Audit originals before corrections
    orig_cols = [c for c in ("last_recruiting_stage", "candidate_recruiting_status", "disposition_reason") if c in df.columns]
    df = df.with_columns([pl.col(c).alias(f"{c}_orig") for c in orig_cols])

    # Rescind offer correction
    df = _apply_rescind_correction(df)

    # Stage number
    if "last_recruiting_stage" in df.columns:
        df = df.with_columns(
            pl.col("last_recruiting_stage").replace(STAGE_MAP, default=None).cast(pl.Int8).alias("last_stage_number")
        )

    # Enrichment joins
    if "disposition_reason" in df.columns and "disposition_reason" in dispo_map.columns:
        df = df.join(dispo_map, how="left", on="disposition_reason")

    jr_cols = {"job_requisition_id", "function", "sub_function", "rag_target_offer_acceptance_date", "complexity"}
    if jr_cols.issubset(jobreq_df.columns):
        df = df.join(jobreq_df.select(list(jr_cols)), how="left", on="job_requisition_id")

    if "source" in df.columns and "source" in source_map.columns:
        df = df.join(source_map, how="left", on="source")

    # Recruiter offer ID (digits only; blank → zzz_blank)
    if "recruiter_completed_offer" in df.columns:
        cleaned = pl.col("recruiter_completed_offer").cast(pl.Utf8).str.replace_all(r"\D+", "")
        df = df.with_columns(
            pl.when(cleaned == "").then(pl.lit("zzz_blank")).otherwise(cleaned).alias("recruiter_completed_offer_id")
        )

    # On-time offer acceptance vs RAG target (month-end inclusive)
    if {"offer_accepted_date", "rag_target_offer_acceptance_date"}.issubset(df.columns):
        rag_month_end = pl.col("rag_target_offer_acceptance_date").cast(pl.Date, strict=False).dt.month_end()
        df = df.with_columns(
            pl.when(pl.col("offer_accepted_date") <= rag_month_end)
              .then(pl.lit("Yes"))
              .otherwise(pl.lit(""))
              .alias("on_time_offer_accept")
        )

    # Final column ordering
    final_cols = [c for c in RAW_COLS_SNAKE + ENGINEERED_COLS if c in df.columns]
    df = df.select(final_cols)

    with stage_timer("⏱  Transf Funnel •"):
        funnel_df = pl.from_pandas(funnel_app.run(df.to_pandas(), config=config))

    return {
        "apps": df,
        "funnel": funnel_df,
    }


# ═════════════════════════════════════════════
# LOAD
# ═════════════════════════════════════════════

def load(dfs: dict, config: PipelineConfig) -> None:
    with stage_timer("⏱  Load apps •"):
        dfs["apps"].write_csv(config["OUTPUT_APPS_CSV"])
        dfs["apps"].write_parquet(config["OUTPUT_APPS_PAR"], compression="zstd")

    with stage_timer("⏱  Load funnel •"):
        dfs["funnel"].write_parquet(config["OUTPUT_FUNNEL_PAR"])


# ═════════════════════════════════════════════
# MAIN PIPELINE
# ═════════════════════════════════════════════

def run_app_pipeline(config: PipelineConfig) -> dict:
    """Orchestrate full ETL for applications."""
    log.info("🚀 App ETL started...")
    _t0 = perf_counter()

    with stage_timer("⏱  Extract •"):
        ext = extract(config)

    with stage_timer("⏱  Transform (overall) •"):
        dfs = transform(ext, config)

    with stage_timer("⏱  Load •"):
        load(dfs, config)

    log.info("✅ App ETL completed • duration : %.2fs", perf_counter() - _t0)
    log.info("────────────────────────────────────")

    return dfs
