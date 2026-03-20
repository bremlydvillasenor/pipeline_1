# src/pipelines/apps.py   ·   Applications ETL / cleansing

from __future__ import annotations
import logging
from typing import Final, Sequence
from pathlib import Path
import pandas as pd
import polars as pl
import re
import numpy as np

from contextlib import contextmanager              # [duration]
from time import perf_counter                      # [duration]

from .utils import funnel_app

from .utils.helpers import (
    latest_file, extract_date_from_filename
)

#─────────────────────────────────────────────────────────────
# Logging setup
_LOG_FORMAT = "%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s"
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Utilities: normalization + helpers
# ─────────────────────────────────────────────────────────────

# [NEW] simple stage timer for consistent elapsed-time logs
@contextmanager
def stage_timer(label: str):
    _t0 = perf_counter()
    try:
        yield
    finally:
        _dt = perf_counter() - _t0
        log.info("%s duration: %.2fs", label, _dt)

#─────────────────────────────────────────────────────────────
# Stage labels (data values) → numbers (we only renamed column *names*, not values)
STAGE_MAP: Final[dict[str, int]] = {
    "Review": 1, "Screen": 2, "Assessment": 3, "Interview": 4,
    "Reference Check": 5, "Offer": 6, "Background Check": 7, "Ready for Hire": 8,
}

# Keep this as your raw-file column list for Polars select; these are BEFORE normalization
COLS_APP: Final[Sequence[str]] = (
    "Candidate ID", "Candidate Name",
    "Job Requisition ID", "Job Requisition", "Recruiting Instruction",
    "Job Family", "Compensation Grade",
    "Worker Type Hiring Requirement", "Worker Sub-Type Hiring Requirement",
    "Target Hire Date", "Added Date", "Job Application Date","Offer Accepted Date","Candidate Start Date",
    "Recruiter Employee ID","Recruiter Completed Offer",
    "Disposition Reason", "Candidate Recruiting Status",
    "Last Recruiting Stage", "Hired","Hire Transaction Status", "Source",
    "Referred By Employee ID", "Referred By", "Recruiting Agency",
    "Last Employer", "School Name", "Is cancelled?", "MBPS Teams"
)

#─────────────────────────────────────────────────────────────
# UTIL: column normalization (lowercase + snake_case + de-dupe)
#─────────────────────────────────────────────────────────────
def _to_snake(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)       # punctuation → space
    s = re.sub(r"\s+", "_", s)           # spaces → _
    s = re.sub(r"_+", "_", s).strip("_") # collapse _
    return s or "col"

def normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    raw = [_to_snake(c) for c in df.columns]
    counts: dict[str, int] = {}
    unique: list[str] = []
    for c in raw:
        if c in counts:
            counts[c] += 1
            unique.append(f"{c}_{counts[c]}")
        else:
            counts[c] = 0
            unique.append(c)
    out = df.copy()
    out.columns = unique
    return out

#─────────────────────────────────────────────────────────────
# DATE PARSER (unchanged)
#─────────────────────────────────────────────────────────────
def safe_parse_date(series, fmt="%Y-%m-%d", na_threshold=0.7):
    s = series.astype(str).str.strip()
    if s.replace("nan", "").replace("", "").str.len().sum() == 0:
        return pd.NaT
    iso_pattern = re.compile(r"^\d{4}[-/.]\d{2}[-/.]\d{2}$")
    looks_regular = s.apply(lambda x: bool(iso_pattern.match(x)))
    if looks_regular.mean() > (1 - na_threshold):
        normalized = s.str.replace(r"[-/.]", "-", regex=True).str[:10]
        parsed = pd.to_datetime(normalized, errors="coerce", format="%Y-%m-%d")
        return parsed
    return pd.to_datetime(s, errors="coerce")

#─────────────────────────────────────────────────────────────
# EXTRACT
#─────────────────────────────────────────────────────────────
def extract(config: dict) -> dict:
    # Dates/paths
    status_path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    refresh_date = extract_date_from_filename(status_path)
    app_cutoff = pd.Timestamp(year=refresh_date.year - 4, month=1, day=1)

    # Raw Applications (Polars scan_csv, then normalize columns)
    app_path = latest_file(config["RAW_DATA_ROOT"], config["DAILY_APP_PATTERN"])

    #log.info("Reading Daily Applications file (polars scan_csv): %s", app_path.name)
    scan = pl.scan_csv(app_path, skip_rows=16).select(list(COLS_APP))
    app_df = scan.collect(streaming=True).to_pandas()
    app_df = normalize_colnames(app_df)

    # Other mappings/loaders
    dispo_map = pl.read_excel(
        config["SCRUM_FILE"],
        sheet_name="disposition_mapping",
        # usecols=[
        #     "Disposition Reason",
        #     "consolidated_disposition",
        #     "consolidated_disposition_2",
        #     "is_non_auto_dispo",
        #     "is_candidate_driven_dispo",
        # ],
        #dtype=str,  # version-proof
    ).to_pandas()
    dispo_map = normalize_colnames(dispo_map)

    source_map = pl.read_excel(
        config["SCRUM_FILE"],
        sheet_name="source_mapping",
        columns=["source", "consolidated_channel","channel_sort","internal_external"],
    ).to_pandas()
    source_map = normalize_colnames(source_map)

    parq_path: Path = config["OUTPUT_JOBREQS_PAR"]
    if not parq_path.exists():
        raise FileNotFoundError(f"No jobreq parq found at {parq_path}")
    jobreq_df = pd.read_parquet(parq_path)
    jobreq_df = normalize_colnames(jobreq_df)

    return {
        "app_df": app_df,
        "dispo_map": dispo_map,
        "source_map": source_map,
        "jobreq_df": jobreq_df,
        "app_cutoff": app_cutoff,
        "refresh_date": refresh_date,
    }

#─────────────────────────────────────────────────────────────
# TRANSFORM
#─────────────────────────────────────────────────────────────
def transform(raw: dict, config: dict) -> dict:
    df = raw["app_df"].copy()
    dispo_map = raw["dispo_map"]
    source_map = raw["source_map"]
    jobreq_df = raw["jobreq_df"]
    app_cutoff = raw["app_cutoff"]

    # ── fill missing/blank source early (requested): "zzz_unknown"
    if "source" in df.columns:
        #src = df["source"].astype(str).str.strip()
        #df["source"] = src.mask(src.eq("") | src.eq("nan"), other=np.nan).fillna("zzz_unknown").astype(str)
        df["source"] = df["source"].fillna("zzz_unknown").astype(str)

    # Datetime coercion ────
    for col in ("added_date","job_application_date","offer_accepted_date", "candidate_start_date", "target_hire_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Filter by cutoff date
    if "job_application_date" in df.columns:
        pre = len(df)
        df = df.loc[df["job_application_date"] >= app_cutoff].copy()
        #log.info("Filtered %d→%d rows on application cutoff %s", pre, len(df), app_cutoff.date())

    # Remove Level 9
    if "compensation_grade" in df.columns:
        df = df[df["compensation_grade"] != "Level 9"]

    # Audit originals
    for col in ("last_recruiting_stage", "candidate_recruiting_status", "disposition_reason"):
        if col in df.columns:
            df[f"{col}_orig"] = df[col]

    # Rescind Offer Correction ─────────────────────────────────────────────────────────────
    # Make sure the columns exist before masking
    needed = {
        "hire_transaction_status", "hired",
        "last_recruiting_stage_orig", "candidate_recruiting_status_orig",
        "disposition_reason"
    }
    if needed.issubset(df.columns):
        rescind_mask = (
            df["hire_transaction_status"].eq("Rescinded")
            & df["hired"].isna()
            & df["last_recruiting_stage_orig"].eq("Ready for Hire")
            & df["candidate_recruiting_status_orig"].eq("Offer Accepted")
        )
        df.loc[
            ((df["last_recruiting_stage_orig"].eq("Ready for Hire")) & (df["disposition_reason"].notna()))
            | rescind_mask,
            "last_recruiting_stage",
        ] = "Offer"
        df.loc[rescind_mask, "disposition_reason"] = "Offer Accepted to Rescinded"
    # Rescind Offer Correction ─────────────────────────────────────────────────────────────

    # Stage number and enrichment
    if "last_recruiting_stage" in df.columns:
        df["last_stage_number"] = df["last_recruiting_stage"].map(STAGE_MAP).astype("Int8")

    # disposition mapping
    if "disposition_reason" in df.columns and "disposition_reason" in dispo_map.columns:
        df = df.merge(dispo_map, how="left", on="disposition_reason")

    # jobreq enrichment
    jr_cols = {"job_requisition_id", "function", "sub_function", "rag_target_offer_acceptance_date","complexity"}
    if jr_cols.issubset(jobreq_df.columns):
        df = df.merge(jobreq_df[list(jr_cols)], how="left", on="job_requisition_id")

    # source mapping (after we filled unknowns)
    if "source" in df.columns and "source" in source_map.columns:
        df = df.merge(source_map, how="left", on="source")

    # Recruiter Completed Offer ID (digits only; blank → zzz_blank)
    if "recruiter_completed_offer" in df.columns:
        tmp = df["recruiter_completed_offer"].astype(str).str.replace(r"\D+", "", regex=True)
        df["recruiter_completed_offer_id"] = tmp.replace("", "zzz_blank")

    # On-time Offer vs RAG target (month end inclusive)
    if {"offer_accepted_date", "rag_target_offer_acceptance_date"}.issubset(df.columns):
        df["on_time_offer_accept"] = (
            df["offer_accepted_date"] <= (pd.to_datetime(df["rag_target_offer_acceptance_date"], errors="coerce") + pd.offsets.MonthEnd(0))
        ).map({True: "Yes", False: ""})

    # Final column ordering (safe: keep only those that exist)
    raw_cols_snake = [
        # normalized version of COLS_APP (kept order)
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
    engineered = [
        "last_recruiting_stage_orig",
        "candidate_recruiting_status_orig",
        "disposition_reason_orig",
        "recruiter_completed_offer_id",
        "last_stage_number",
        "consolidated_disposition","consolidated_disposition_2",
        "consolidated_channel","channel_sort","internal_external",
        "is_non_auto_dispo","is_candidate_driven_dispo",
        "function","sub_function","rag_target_offer_acceptance_date",
        "on_time_offer_accept","complexity"
    ]
    final_cols = [c for c in raw_cols_snake + engineered if c in df.columns]
    df = df[final_cols].copy()

    with stage_timer("⏱  Transf Funnel •"):
        funnel_df = funnel_app.run(df,config=config)

    return {
        "apps": df.reset_index(drop=True),
        "funnel": funnel_df.reset_index(drop=True),
    }

#─────────────────────────────────────────────────────────────
# LOAD
#─────────────────────────────────────────────────────────────
def load(dfs: dict, config: dict):

    #apps_pl = pl.from_pandas(dfs["apps"])
    funnel = pl.from_pandas(dfs["funnel"])

    with stage_timer("⏱  Load apps •"):
        #apps_pl.write_csv(
        #    file=config["OUTPUT_APPS_CSV"],
        #    include_header=True,
        #)
        #apps_pl.write_parquet(config["OUTPUT_APPS_PAR"])
        dfs["apps"].to_csv(config["OUTPUT_APPS_CSV"], index=False, date_format="%Y-%m-%d")
        dfs["apps"].to_parquet(config["OUTPUT_APPS_PAR"], index=False, compression="zstd") 

    with stage_timer("⏱  Load funnel •"):
        funnel.write_parquet(config["OUTPUT_FUNNEL_PAR"])

    #log.info("Saved applications data to %s and %s", config["OUTPUT_APPS_CSV"], config["OUTPUT_APPS_PAR"])

#─────────────────────────────────────────────────────────────
# ENTRYPOINT
#─────────────────────────────────────────────────────────────
def run_app_pipeline(config) -> pd.DataFrame:

    """Orchestrate full ETL for applications."""
    log.info("🚀 App ETL started...")

    _t0_run = perf_counter()  # start overall timer

    with stage_timer("⏱  Extract •"):
        ext_df = extract(config)

    with stage_timer("⏱  Transform (overall) •"):
        dfs = transform(ext_df, config)

    with stage_timer("⏱  Load •"):
        load(dfs, config)

    _dt_run = perf_counter() - _t0_run
    log.info("✅ App ETL completed • duration : %.2fs", _dt_run)   
    log.info("────────────────────────────────────")

    return dfs
