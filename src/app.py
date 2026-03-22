# src/app.py  ·  Applications ETL / cleansing

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Final

import polars as pl

from _utils import latest_file, extract_date_from_filename


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
# COLUMN RENAME MAP
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

VALID_GRADES: Final[list[str]] = [
    "Level 1", "Level 2", "Level 3", "Level 4",
    "Level 5", "Level 6", "Level 7", "Level 8",
]

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
]


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
    "disposition_reason",
    "candidate_recruiting_status",
    "last_stage_number",
}

FUNNEL_VALID_STAGES: Final[list[int]] = [1, 2, 3, 4, 6, 7, 8]

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

    # Coerce last_stage_number; map 5→4 (Reference Check → Interview)
    df = df.with_columns(
        pl.when(pl.col("last_stage_number").cast(pl.Int16, strict=False) == 5)
        .then(pl.lit(4, dtype=pl.Int16))
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
        "disposition_reason",
    ]
    return long.select([c for c in order_cols if c in long.columns])


# ═════════════════════════════════════════════
# EXTRACT
# ═════════════════════════════════════════════

def extract(config: PipelineConfig) -> dict:
    run_date = date.today()
    app_cutoff = date(run_date.year - 4, 1, 1)

    app_path = latest_file(config["RAW_DATA_ROOT"], config["DAILY_APP_PATTERN"])
    app_lf = (
        pl.scan_csv(app_path, skip_rows=16)
        .select(list(APP_RENAME_MAP.keys()))
        .rename(APP_RENAME_MAP)
    )

    return {
        "app_lf": app_lf,
        "app_cutoff": app_cutoff,
        "run_date": run_date,
    }


# ═════════════════════════════════════════════
# RESCIND CORRECTION
# ═════════════════════════════════════════════

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

    schema = set(lf.collect_schema().names())

    # Fill null source before joins (prevents NULL key fan-out)
    if "source" in schema:
        lf = lf.with_columns(pl.col("source").fill_null("zzz_unknown"))

    # Parse date columns
    date_exprs = [_parse_date(c) for c in DATE_COLS if c in schema]
    if date_exprs:
        lf = lf.with_columns(date_exprs)

    # Filter: 4-year lookback and Level 1–8 grades only
    if "job_application_date" in schema:
        lf = lf.filter(pl.col("job_application_date") >= app_cutoff)
    if "compensation_grade" in schema:
        lf = lf.filter(pl.col("compensation_grade").is_in(VALID_GRADES))

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

    # Recruiter offer ID (digits only; blank → zzz_blank)
    if "recruiter_completed_offer" in schema:
        cleaned = pl.col("recruiter_completed_offer").cast(pl.Utf8).str.replace_all(r"\D+", "")
        lf = lf.with_columns(
            pl.when(cleaned == "").then(pl.lit("zzz_blank")).otherwise(cleaned).alias("recruiter_completed_offer_id")
        )

    # Final column ordering
    final_schema = set(lf.collect_schema().names())
    final_cols = [c for c in RAW_COLS_SNAKE + ENGINEERED_COLS if c in final_schema]
    lf = lf.select(final_cols)

    # Offer-accepts slice: candidates who accepted an offer
    offer_accepts_lf = lf.filter(pl.col("candidate_recruiting_status") == "Offer Accepted")

    # Funnel transform (collect → transform → lazy)
    funnel_lf = _transform_funnel(lf.collect(), config).lazy()

    return {
        "apps": lf,
        "offer_accepts": offer_accepts_lf,
        "funnel": funnel_lf,
    }


# ═════════════════════════════════════════════
# LOAD
# ═════════════════════════════════════════════

def load(dfs: dict, config: PipelineConfig) -> None:
    apps_df = dfs["apps"].collect()
    apps_df.write_parquet(config["OUTPUT_APPS_PAR"], compression="zstd")
    apps_df.write_csv(config["OUTPUT_APPS_CSV"])

    dfs["offer_accepts"].collect().write_parquet(config["OUTPUT_APPS_OFFER_ACCEPTS_PAR"], compression="zstd")

    dfs["funnel"].collect().write_parquet(config["OUTPUT_FUNNEL_PAR"])


# ═════════════════════════════════════════════
# MAIN PIPELINE
# ═════════════════════════════════════════════

def run_app(config: PipelineConfig) -> dict:
    """Orchestrate full ETL for applications."""
    log.info("🚀 App ETL started...")

    ext = extract(config)
    dfs = transform(ext, config)
    load(dfs, config)

    log.info("✅ App ETL completed")
    log.info("────────────────────────────────────")

    return dfs
