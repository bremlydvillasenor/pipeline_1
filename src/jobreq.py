# ─────────────────────────────────────────────────────────────
# src/jobreq.py   ·   Job Requisitions ETL / cleansing
# ─────────────────────────────────────────────────────────────
from __future__ import annotations

import logging
import warnings
from pathlib import Path
from datetime import datetime
from functools import lru_cache
from contextlib import contextmanager              # [NEW]
from time import perf_counter                      # [NEW]
import re

import numpy as np
import pandas as pd
import polars as pl

from src.utils.helpers import (
    latest_file,
    extract_date_from_filename,
    add_date_slices,
)

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

_LOG_FORMAT = "%(asctime)s ▸ %(levelname)-8s ▸ %(name)s ▸ %(message)s"
logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Utilities
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

def _to_snake(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)        # punctuation → space
    s = re.sub(r"\s+", "_", s)            # spaces → _
    s = re.sub(r"_+", "_", s).strip("_")  # collapse underscores
    return s or "col"

def normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + snake_case + de-dupe column names."""
    raw = [_to_snake(c) for c in df.columns]
    counts: dict[str, int] = {}
    uniq: list[str] = []
    for c in raw:
        if c in counts:
            counts[c] += 1
            uniq.append(f"{c}_{counts[c]}")
        else:
            counts[c] = 0
            uniq.append(c)
    out = df.copy()
    out.columns = uniq
    return out

# ────────────── INDIVIDUAL EXTRACTS ──────────────

def _extract_refresh_date(config: dict) -> pd.Timestamp:
    """Most-recent All-Status file date token → refresh_date."""
    path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    return extract_date_from_filename(path)

def _extract_status(config: dict, jobreq_cutoff: pd.Timestamp) -> pd.DataFrame:
    """All-Status sheet → cleansed & filtered status_df (snake_case)."""
    path = latest_file(config["RAW_DATA_ROOT"], config["ALL_STATUS_PATTERN"])
    log.info("%s", path.name)

    # Read with header row offset, then set header from first data row
    df = pl.read_excel(path, has_header=False, read_options={"skip_rows": 13}).to_pandas()
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)

    df = normalize_colnames(df)

    # Select & types
    usecols = [
        "job_requisition_id",
        "target_hire_date",
        "positions_filled_hire_selected",
        "positions_openings_available",
        "mbps_teams",
    ]
    df = df[usecols].copy()

    df["target_hire_date"] = pd.to_datetime(df["target_hire_date"], errors="coerce")
    df["positions_filled_hire_selected"] = pd.to_numeric(
        df["positions_filled_hire_selected"], errors="coerce"
    ).astype("Int16")
    df["positions_openings_available"] = pd.to_numeric(
        df["positions_openings_available"], errors="coerce"
    ).astype("Int16")

    # Window & dedupe (keep most-recent THD per JobReq)
    df = (
        df.loc[df["target_hire_date"].ge(jobreq_cutoff)]
            .sort_values("target_hire_date", ascending=False)
            .drop_duplicates("job_requisition_id", keep="first")
            .reset_index(drop=True)
    )

    # Derived metrics & flags
    df["positions_requested_new"] = (
        df["positions_openings_available"] + df["positions_filled_hire_selected"]
    )
    df["jobreq_status"] = np.select(
        [
            df["positions_requested_new"].eq(0),
            df["positions_openings_available"].gt(0),
        ],
        ["CAN", "OPEN"],
        default="FILLED",
    )

    #log.info("All Status rows=%d · cols=%d", *df.shape)             # [NEW]
    return df

def _extract_scrum(config: dict) -> pd.DataFrame:
    #log.info("Reading scrum file       : %s", config["SCRUM_FILE"].name)
    df = pl.read_excel(
        config["SCRUM_FILE"],
        sheet_name="scrum",
        has_header=True,
    )
    #log.info(df.columns)
    df = df.drop([
            "Hiring_Scrum[# Positions Openings Available]",
            "Hiring_Scrum[# Positions Filled (Hire selected)]",
            "Hiring_Scrum[Positions Requested_New]",
            "Hiring_Scrum[Target Hire Date]",
            ])

    rename_map = {
        col: (m.group(1) if (m := re.search(r"\[(.*?)\]", col)) else col)
        for col in df.columns
    }
    df = df.rename(rename_map).to_pandas()
    return normalize_colnames(df)

def _extract_dmt(config: dict) -> pd.DataFrame:
    #log.info("Reading dmt              : %s", config["SCRUM_FILE"].name)
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
    ).to_pandas()
    return normalize_colnames(df)

def _extract_thd_corr(config: dict) -> pd.DataFrame:
    #log.info("Reading thd_correction   : %s", config["SCRUM_FILE"].name)
    df = pl.read_excel(
        config["SCRUM_FILE"],
        sheet_name="thd_correction",
        columns=["Job Requisition ID", "New Target Hire Date"],
    ).to_pandas()
    return normalize_colnames(df)

# ────────────── BUNDLED EXTRACT ──────────────

def extract_all(config: dict, jobreq_cutoff: pd.Timestamp) -> dict:
    """Return every raw DataFrame needed for transforms (already snake_case)."""
    return {
        "status": _extract_status(config, jobreq_cutoff),
        "scrum": _extract_scrum(config),
        "dmt": _extract_dmt(config),
        "thd_corr": _extract_thd_corr(config),
    }

# ────────────── TRANSFORM HELPERS ──────────────

def _apply_new_thd(status_df: pd.DataFrame, corr_df: pd.DataFrame) -> pd.DataFrame:
    """Override THD when correction table provides a value."""
    if status_df.empty or corr_df.empty:
        return status_df
    mapper = dict(zip(corr_df["job_requisition_id"], corr_df["new_target_hire_date"]))
    updated = status_df["job_requisition_id"].map(mapper).fillna(status_df["target_hire_date"])
    status_df["target_hire_date"] = pd.to_datetime(updated, errors="coerce")
    return status_df

def _add_rag_fields(df: pd.DataFrame, refresh_date: pd.Timestamp) -> pd.DataFrame:
    """Business RAG metrics."""
    # OTS days → target offer acceptance date
    df["rag_target_offer_acceptance_date"] = (
        pd.to_datetime(df["target_hire_date"], errors="coerce")
        - pd.to_timedelta(pd.to_numeric(df["ots"], errors="coerce").fillna(0), unit="D")
    )

    open_mask = df["positions_openings_available"].gt(0)
    days_remaining = (df["rag_target_offer_acceptance_date"] - refresh_date).dt.days
    df["rag_days_remaining_offer"] = np.where(open_mask, days_remaining, np.nan)

    # RAG bands (OPEN only)
    conds = [
        df["positions_openings_available"].eq(0),                 # Filled
        open_mask & df["target_hire_date"].le(refresh_date),      # Backlog (THD passed)
        open_mask & (df["rag_days_remaining_offer"] < 8),         # Red
        open_mask & (df["rag_days_remaining_offer"] < 14),        # Amber
    ]
    df["rag_rating"] = np.select(conds, ["", "Backlog", "Red", "Amber"], default="Green")
    df["rag_rating_sort"] = np.select(conds, ["9", "1", "2", "3"], default="4")

    df["rag_processing_date"] = np.where(
        df["rag_target_offer_acceptance_date"] < refresh_date,
        refresh_date,
        df["rag_target_offer_acceptance_date"],
    ).astype("datetime64[ns]")
    df["rag_processing_month"] = df["rag_processing_date"].dt.to_period("M").astype(str)

    return df

# ────────────── BUNDLED TRANSFORM ──────────────

def transform_all(extracts: dict, refresh_date: pd.Timestamp) -> dict:
    """Return the final jobreq DataFrame plus DMT subset."""
    with stage_timer("⏱  Transform •"):        
        status_df = _apply_new_thd(extracts["status"], extracts["thd_corr"])

        # Keep only jobreqs present in status
        scrum = extracts["scrum"][extracts["scrum"]["job_requisition_id"].isin(status_df["job_requisition_id"])].copy()

        jobreq_df = (
            scrum
            .merge(status_df, on="job_requisition_id", how="left")
            .pipe(_add_rag_fields, refresh_date=refresh_date)
            .pipe(add_date_slices, date_col="target_hire_date", prefix="thd_")
            .assign(
                complexity=lambda d: d["complexity"].fillna("SPEC"),
                is_future=lambda d: np.where(d["target_hire_date"].gt(refresh_date), "Yes", ""),
            )
        )

        # Optimize: convert low-cardinality text to category (handle object + string)
        obj_str = jobreq_df.select_dtypes(include=["object"]).columns.tolist()
        for c in obj_str:
            if jobreq_df[c].nunique(dropna=True) / max(len(jobreq_df), 1) < 0.5:
                jobreq_df[c] = jobreq_df[c].astype("category")

        # DMT rows used for capacity modelling (Is_logged == No)
        dmt = extracts["dmt"].copy()
        if "is_logged" in dmt.columns:
            dmt_open = dmt[dmt["is_logged"]=="No"]

        return {"jobreq": jobreq_df.reset_index(drop=True), "dmt_open": dmt_open}

# ────────────── LOAD ──────────────

def load_outputs(
    dfs: dict,
    refresh_date: pd.Timestamp,
    config: dict,
):
    with stage_timer("⏱  Load •"):
        """Persist outputs and write refresh_date marker."""
        dfs["jobreq"].to_csv(config["OUTPUT_JOBREQS_CSV"], index=False, date_format="%Y-%m-%d")
        dfs["jobreq"].to_parquet(config["OUTPUT_JOBREQS_PAR"], index=False)
        #log.info(
        #    "Saved jobreq CSV & PAR   : %s | %s",
        #    config["OUTPUT_JOBREQS_CSV"].name,
        #    config["OUTPUT_JOBREQS_PAR"].name,
        #)

        # Open-reqs + DMT combined
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
        open_reqs = dfs["jobreq"].loc[
            dfs["jobreq"]["positions_openings_available"].gt(0), [c for c in cols if c in dfs["jobreq"].columns]
        ]

        dmt_cols_present = [c for c in cols if c in dfs["dmt_open"].columns]
        combined = pd.concat([open_reqs, dfs["dmt_open"][dmt_cols_present]], ignore_index=True)
        combined.to_csv(config["OUTPUT_JOBREQS_OPENDMT_CSV"], index=False, date_format="%Y-%m-%d")
        #log.info("Saved Open & DMT CSV     : %s", config["OUTPUT_JOBREQS_OPENDMT_CSV"].name)

        # Refresh marker
        pd.DataFrame({"refresh_date": [refresh_date]}).to_csv(
            Path(config["PROCESSED_DATA_ROOT"]) / "refresh_date.csv", index=False
        )

# ────────────── MISC ──────────────

def _backlog_count(df: pd.DataFrame, refresh_date: pd.Timestamp) -> int:
    mask = df["target_hire_date"].le(refresh_date)
    return int(df.loc[mask, "positions_openings_available"].sum())

# ────────────── MAIN PIPELINE ──────────────

def run_jobreq_pipeline(config: dict) -> tuple[pd.DataFrame, pd.Timestamp]:
    """
    End-to-end Job Requisitions ETL.
    Returns the final jobreq DataFrame.
    """
    _t0_run = perf_counter()                                      # [NEW] start overall timer

    refresh_date = _extract_refresh_date(config)
    log.info("──────────────────────────────")
    log.info("🚀 Jobreq ETL started (refresh date=%s) ", refresh_date.date())

    years_scope = config.get("REQS_YEARS_SCOPE", 1)
    jobreq_cutoff = pd.Timestamp(refresh_date.year - years_scope, 1, 1)
    log.info("Scope THD ≥ %s", jobreq_cutoff.date())

    # ---------- EXTRACT ----------
    with stage_timer("⏱  Extract •"):
        extracts = extract_all(config, jobreq_cutoff)

    # ---------- TRANSFORM --------
    dfs = transform_all(extracts, refresh_date)

    # ---------- LOAD -------------
    load_outputs(dfs, refresh_date, config)

    log.info("Final Stats...") 
    log.info("Backlog positions : %d", _backlog_count(dfs["jobreq"], refresh_date))
    log.info("Jobreqs rows=%d · cols=%d ", dfs["jobreq"].shape[0], dfs["jobreq"].shape[1])
    log.info("DMT open rows=%d", len(dfs["dmt_open"]))

    # overall elapsed time
    _dt_run = perf_counter() - _t0_run
    log.info("✅ Jobreq ETL completed • duration : %.2fs", _dt_run)         
    log.info("──────────────────────────────")

    return dfs["jobreq"], refresh_date
