# src/pipelines/apps.py  ·  Applications ETL / cleansing

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Any, Final

import polars as pl

from .utils import latest_file, extract_date_from_filename


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
# COLUMN RENAME MAPS
# ═════════════════════════════════════════════

APP_RENAME_MAP: Final[dict[str, str]] = {
    "Candidate ID":                        "candidate_id",
    "Candidate Name":                      "candidate_name",
    "Job Requisition ID":                  "job_requisition_id",
    "Job Requisition":                     "job_requisition",
    "Recruiting Instruction":              "recruiting_instruction",
    "Job Family":                          "job_family",
    "Compensation Grade":                  "compensation_grade",
    "Worker Type Hiring Requirement":      "worker_type_hiring_requirement",
    "Worker Sub-Type Hiring Requirement":  "worker_sub_type_hiring_requirement",
    "Target Hire Date":                    "target_hire_date",
    "Added Date":                          "added_date",
    "Job Application Date":                "job_application_date",
    "Offer Accepted Date":                 "offer_accepted_date",
    "Candidate Start Date":                "candidate_start_date",
    "Recruiter Employee ID":               "recruiter_employee_id",
    "Recruiter Completed Offer":           "recruiter_completed_offer",
    "Disposition Reason":                  "disposition_reason",
    "Candidate Recruiting Status":         "candidate_recruiting_status",
    "Last Recruiting Stage":               "last_recruiting_stage",
    "Hired":                               "hired",
    "Hire Transaction Status":             "hire_transaction_status",
    "Source":                              "source",
    "Referred By Employee ID":             "referred_by_employee_id",
    "Referred By":                         "referred_by",
    "Recruiting Agency":                   "recruiting_agency",
    "Last Employer":                       "last_employer",
    "School Name":                         "school_name",
    "Is cancelled?":                       "is_cancelled",
    "MBPS Teams":                          "mbps_teams",
}

DISPO_RENAME_MAP: Final[dict[str, str]] = {
    "Disposition Reason": "disposition_reason",
}


# ═════════════════════════════════════════════
# CONSTANTS
# ═════════════════════════════════════════════

STAGE_MAP: Final[dict[str, int]] = {
    "Review": 1, "Screen": 2, "Assessment": 3, "Interview": 4,
    "Reference Check": 5, "Offer": 6, "Background Check": 7, "Ready for Hire": 8,
}

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
    "last_stage_number"
]

# Offer (6) through Ready for Hire (8)
OFFER_STAGE_MIN: Final[int] = STAGE_MAP["Offer"]


# ═════════════════════════════════════════════
# FUNNEL CONSTANTS
# ═════════════════════════════════════════════

FUNNEL_REQUIRED_COLS: Final[set[str]] = {
    "candidate_id",
    "added_date",
    "job_application_date",
    "job_requisition_id",
    "recruiting_agency",
    "source",
    "consolidated_channel",
    "internal_external",
    "disposition_reason",
    "candidate_recruiting_status",
    "last_stage_number",
}

FUNNEL_VALID_STAGES: Final[list[int]] = [1, 2, 3, 4, 6, 8]

FUNNEL_STAGE_LABELS: Final[dict[int, str]] = {
    1: "Review",
    2: "Screen",
    3: "Assessment",
    4: "Interview",
    5: "Reference Check",
    6: "Offer",
    7: "Background Check",
    8: "Ready for Hire",
}

_FUNNEL_STAGE_MAP: pl.DataFrame = pl.DataFrame(
    {
        "stage_number": pl.Series(list(FUNNEL_STAGE_LABELS.keys()), dtype=pl.Int16),
        "stage": pl.Series(list(FUNNEL_STAGE_LABELS.values()), dtype=pl.Utf8),
    }
)

_APPLICATION_IN_PROCESS: Final[str] = "application in process"


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


def _parse_date(col: str) -> pl.Expr:
    """Try common date string formats; returns pl.Date, null on failure."""
    return pl.coalesce([
        pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
        pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d", strict=False),
        pl.col(col).str.strptime(pl.Date, format="%m/%d/%Y", strict=False),
    ])


# ═════════════════════════════════════════════
# FUNNEL TRANSFORM
# ═════════════════════════════════════════════

def _check_funnel_schema(df: pl.DataFrame) -> None:
    """Raise ValueError if any required funnel input column is absent."""
    missing = FUNNEL_REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"Input DataFrame is missing required columns: {sorted(missing)}"
        )


def _transform_funnel(df: pl.DataFrame, config: PipelineConfig) -> pl.DataFrame:

    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"Expected a Polars DataFrame, got {type(df).__name__}")

    if df.is_empty():
        return df

    _check_funnel_schema(df)

    df = df.select([c for c in df.columns if c in FUNNEL_REQUIRED_COLS])

    # Coerce last_stage_number; map 5→4, 7→6
    df = df.with_columns(
        pl.when(pl.col("last_stage_number").cast(pl.Int16, strict=False) == 5)
        .then(pl.lit(4, dtype=pl.Int16))
        .when(pl.col("last_stage_number").cast(pl.Int16, strict=False) == 7)
        .then(pl.lit(6, dtype=pl.Int16))
        .otherwise(pl.col("last_stage_number").cast(pl.Int16, strict=False))
        .alias("last_stage_number")
    )

    # Normalize job_application_date to month-start
    df = df.with_columns(
        pl.col("job_application_date")
        .cast(pl.Date, strict=False)
        .dt.truncate("1mo")
        .alias("job_application_date")
    )

    # Tag rows where the candidate is still in-process
    df = df.with_columns(
        pl.col("candidate_recruiting_status")
        .str.strip_chars()
        .str.to_lowercase()
        .eq(_APPLICATION_IN_PROCESS)
        .alias("_in_process")
    )

    # Build per-row list of stages to explode
    df = df.with_columns(
        pl.struct(["last_stage_number", "_in_process"])
        .map_elements(
            lambda row: (
                [s for s in FUNNEL_VALID_STAGES if s < row["last_stage_number"]]
                if row["_in_process"]
                else [s for s in FUNNEL_VALID_STAGES if s <= row["last_stage_number"]]
            )
            if row["last_stage_number"] is not None
            else [],
            return_dtype=pl.List(pl.Int16),
        )
        .alias("_stage_list")
    )

    if df.is_empty():
        return df

    # Explode to one row per stage
    long = (
        df.explode("_stage_list")
        .rename({"_stage_list": "stage_number"})
        .filter(pl.col("stage_number").is_not_null())
        .drop("_in_process")
    )

    # Add stage label
    long = long.join(_FUNNEL_STAGE_MAP, on="stage_number", how="left")

    # Status / disposition: only last stage row keeps truth
    is_last = pl.col("last_stage_number") == pl.col("stage_number")

    long = long.with_columns(
        pl.when(is_last)
        .then(pl.col("candidate_recruiting_status"))
        .otherwise(pl.lit("Passed"))
        .alias("candidate_recruiting_status"),
        pl.when(is_last)
        .then(pl.col("disposition_reason"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("disposition_reason"),
        pl.when(is_last)
        .then(pl.col("on_time_offer_accept"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("on_time_offer_accept"),
    )

    # Helper flags
    status_norm = pl.col("candidate_recruiting_status").str.strip_chars().str.to_lowercase()
    long = long.with_columns(
        pl.when(status_norm == _APPLICATION_IN_PROCESS)
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(0, dtype=pl.Int8))
        .alias("in_process_count"),
        pl.when(status_norm != _APPLICATION_IN_PROCESS)
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(0, dtype=pl.Int8))
        .alias("completed_count"),
    )

    # Final column order (only those present)
    order_cols = [
        "job_requisition_id",
        "candidate_id",
        "added_date",
        "job_application_date",
        "last_stage_number",
        "stage_number",
        "stage",
        "candidate_recruiting_status",
        "recruiting_agency",
        "source",
        "consolidated_channel",
        "internal_external",
        "disposition_reason",
        "is_dispo",
        "consolidated_disposition",
        "consolidated_disposition_2",
        "is_non_auto_dispo",
        "is_candidate_driven_dispo",
        "in_process_count",
        "completed_count",
        "on_time_offer_accept",
    ]
    return long.select([c for c in order_cols if c in long.columns])


# ═════════════════════════════════════════════
# EXTRACT
# ═════════════════════════════════════════════

def extract(config: PipelineConfig) -> dict:
    status_path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    refresh_date = extract_date_from_filename(status_path)
    app_cutoff = date(refresh_date.year - 4, 1, 1)

    app_path = latest_file(config["RAW_DATA_ROOT"], config["DAILY_APP_PATTERN"])
    app_lf = (
        pl.scan_csv(app_path, skip_rows=16)
        .select(list(APP_RENAME_MAP.keys()))
        .rename(APP_RENAME_MAP)
    )

    parq_path: Path = config["OUTPUT_JOBREQS_PAR"]
    if not parq_path.exists():
        raise FileNotFoundError(f"No jobreq parq found at {parq_path}")
    jobreq_lf = pl.scan_parquet(parq_path)

    return {
        "app_lf": app_lf,
        "app_cutoff": app_cutoff,
        "refresh_date": refresh_date,
    }

def _apply_rescind_correction(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Correct stage and disposition for rescinded offers."""
    needed = {
        "hire_transaction_status", "hired",
        "last_recruiting_stage_orig", "candidate_recruiting_status_orig",
        "disposition_reason",
    }
    if not needed.issubset(lf.collect_schema().names()):
        return lf

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

    return lf.with_columns([
        pl.when(stage_update)
          .then(pl.lit("Offer"))
          .otherwise(pl.col("last_recruiting_stage"))
          .alias("last_recruiting_stage"),
        pl.when(rescind_mask)
          .then(pl.lit("Offer Accepted to Rescinded"))
          .otherwise(pl.col("disposition_reason"))
          .alias("disposition_reason"),
    ])

# ═════════════════════════════════════════════
# TRANSFORM
# ═════════════════════════════════════════════

def transform(raw: dict, config: PipelineConfig) -> dict:
    lf: pl.LazyFrame = raw["app_lf"]
    app_cutoff: date = raw["app_cutoff"]

    # Schema of the raw CSV (cheap metadata read; used for pre-join guards)
    schema = set(lf.collect_schema().names())

    # Fill missing source
    if "source" in schema:
        lf = lf.with_columns(
            pl.col("source").fill_null("zzz_unknown").cast(pl.Utf8)
        )

    # Parse date columns
    date_exprs = [_parse_date(c) for c in DATE_COLS if c in schema]
    if date_exprs:
        lf = lf.with_columns(date_exprs)

    # Filter by cutoff and remove Level 9
    if "job_application_date" in schema:
        lf = lf.filter(pl.col("job_application_date") >= app_cutoff)
    if "compensation_grade" in schema:
        lf = lf.filter(pl.col("compensation_grade") != "Level 9")

    # Audit originals before corrections
    orig_cols = [c for c in ("last_recruiting_stage", "candidate_recruiting_status", "disposition_reason") if c in schema]
    lf = lf.with_columns([pl.col(c).alias(f"{c}_orig") for c in orig_cols])

    # Rescind offer correction
    lf = _apply_rescind_correction(lf)

    # Stage number
    if "last_recruiting_stage" in schema:
        lf = lf.with_columns(
            pl.col("last_recruiting_stage").replace(STAGE_MAP, default=None).cast(pl.Int8).alias("last_stage_number")
        )

    # Post-join schema (includes columns added by joins)
    post_schema = set(lf.collect_schema().names())

    # Recruiter offer ID (digits only; blank → zzz_blank)
    if "recruiter_completed_offer" in post_schema:
        cleaned = pl.col("recruiter_completed_offer").cast(pl.Utf8).str.replace_all(r"\D+", "")
        lf = lf.with_columns(
            pl.when(cleaned == "").then(pl.lit("zzz_blank")).otherwise(cleaned).alias("recruiter_completed_offer_id")
        )

    # Final column ordering
    final_schema = set(lf.collect_schema().names())
    final_cols = [c for c in RAW_COLS_SNAKE + ENGINEERED_COLS if c in final_schema]
    lf = lf.select(final_cols)

    # Offer-stage slice: Offer (6) → Ready for Hire (8), used for on-time offer computation
    offer_lf = lf.filter(pl.col("last_stage_number") >= OFFER_STAGE_MIN)

    with stage_timer("⏱  Transf Funnel •"):
        funnel_lf = _transform_funnel(lf.collect(), config).lazy()

    return {
        "apps": lf,
        "offer_stage": offer_lf,
        "funnel": funnel_lf,
    }


# ═════════════════════════════════════════════
# LOAD
# ═════════════════════════════════════════════

def load(dfs: dict, config: PipelineConfig) -> None:
    with stage_timer("⏱  Load apps •"):
        apps_df = dfs["apps"].collect()
        apps_df.write_csv(config["OUTPUT_APPS_CSV"])
        apps_df.write_parquet(config["OUTPUT_APPS_PAR"], compression="zstd")

    with stage_timer("⏱  Load offer stage •"):
        dfs["offer_stage"].collect().write_parquet(config["OUTPUT_APPS_OFFER_PAR"], compression="zstd")

    with stage_timer("⏱  Load funnel •"):
        dfs["funnel"].collect().write_parquet(config["OUTPUT_FUNNEL_PAR"])


# ═════════════════════════════════════════════
# MAIN PIPELINE
# ═════════════════════════════════════════════

def run_app(config: PipelineConfig) -> dict:
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
