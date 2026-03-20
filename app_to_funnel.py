# to_funnel.py

from __future__ import annotations
from typing import Iterable, Mapping
import logging
import re
import pandas as pd
import numpy as np

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# -----------------------#
#  Helpers / Conventions #
# -----------------------#

def normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    def to_snake(name: str) -> str:
        s = name.strip().lower()
        s = re.sub(r"[^\w\s]", " ", s)       # punctuation → space
        s = re.sub(r"\s+", "_", s)           # spaces → _
        s = re.sub(r"_+", "_", s).strip("_") # collapse _
        return s or "col"

    # 1) snake_case first
    raw = [to_snake(c) for c in df.columns]

    # 2) make unique (public, version-proof)
    counts: dict[str, int] = {}
    unique = []
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

# canonical field names after normalize_colnames()
REQ_COLS = {
    "candidate_id",
    "added_date","job_application_date",
    "job_requisition_id",
    "recruiting_agency",
    "source","consolidated_channel","internal_external",
    "disposition_reason",
    "candidate_recruiting_status",
    "last_stage_number",
    "on_time_offer_accept",
}

# we keep/expand across these stages only
VALID_STAGES = (1, 2, 3, 4, 6, 8)

STAGE_LABELS = {
    1: "Review",
    2: "Screen",
    3: "Assessment",
    4: "Interview",
    6: "Offer",
    8: "Ready for Hire",
}

# -----------------------#
#   ETL: EXTRACT STEP    #
# -----------------------#

def extract(source: pd.DataFrame | str | Mapping | Iterable) -> pd.DataFrame:
    """
    Accepts a DataFrame or a file path (CSV/Parquet). Returns raw DataFrame.
    """
    if isinstance(source, pd.DataFrame):
        df = source.copy()
    elif isinstance(source, str) and source.lower().endswith(".parquet"):
        df = pd.read_parquet(source)
    elif isinstance(source, str) and source.lower().endswith((".csv", ".txt")):
        df = pd.read_csv(source)
    else:
        # fallback: try DataFrame constructor
        df = pd.DataFrame(source)
    return df


# -----------------------#
#  ETL: TRANSFORM STEP   #
# -----------------------#

def transform(
    df_raw: pd.DataFrame,
    config: dict,
    *,
    drop_review_in_process: bool = True,
) -> pd.DataFrame:
    """
    Transform candidate app rows into funnel rows.

    Rules:
      - Normalize column names to snake_case
      - Coerce stage 5→4 and 7→6
      - Expand to one row per stage REACHED (stages: 1,2,3,4,6,8)
      - Only LAST stage keeps true status/disposition; earlier stages => Status="Passed", disposition=None
      - Optional: drop (stage==1 & status == 'application in process') rows if drop_review_in_process=True
    """
    if df_raw.empty:
        return df_raw.copy()

    # 1) Normalize column names (Pythonic convention)
    df = normalize_colnames(df_raw)

    # 2) Keep only columns we actually use (tolerant if some are missing)
    keep = [c for c in df.columns if c in REQ_COLS]
    missing = sorted(list(REQ_COLS - set(keep)))
    if missing:
        log.debug("Missing columns (ok if not needed downstream): %s", missing)
    df = df[keep].copy()

    # 3) Dtypes: last_stage_number -> Int16, date -> datetime (month start if present)
    if "last_stage_number" in df.columns:
        df["last_stage_number"] = pd.to_numeric(df["last_stage_number"], errors="coerce").astype("Int16")

    if "job_application_date" in df.columns:
        jad = pd.to_datetime(df["job_application_date"], errors="coerce")
        # month-start (mirrors M's Date.StartOfMonth)
        df["job_application_date"] = jad.dt.to_period("M").dt.to_timestamp()

    # Map 5→4 and 7→6 up front so downstream never sees 5/7
    if "last_stage_number" in df.columns:
        df["last_stage_number"] = df["last_stage_number"].replace({5: 4, 7: 6}).astype("Int16")

    # Build per-row stage list (only include VALID_STAGES up to max reached)
    valid_set = set(VALID_STAGES)

    def stage_list(max_stage: pd.Series | int) -> list[int]:
        # filter to <= max and in valid set
        return [s for s in VALID_STAGES if (s <= (max_stage if pd.notna(max_stage) else -1))]

    df["stage_number_list"] = df["last_stage_number"].apply(stage_list)

    # 7) Explode → one row per stage
    long = df.explode("stage_number_list").rename(
        columns={"stage_number_list": "stage_number"}
    ).reset_index(drop=True)

    # rows with no stages (NaNs) will appear; drop them
    long = long[long["stage_number"].notna()].copy()
    long["stage_number"] = long["stage_number"].astype("Int16")

    # 8) Stage label
    long["stage"] = long["stage_number"].map(STAGE_LABELS).astype("category")

    # 9) Status/Disposition assignment: only last stage keeps truth; previous are "Passed"/NA
    if "candidate_recruiting_status" in long.columns:
        is_last = long["last_stage_number"] == long["stage_number"]
        long["candidate_recruiting_status"] = np.where(
            is_last,
            long["candidate_recruiting_status"],
            "Passed",
        ).astype(str)

    if "disposition_reason" in long.columns:
        long["disposition_reason"] = long["disposition_reason"].where(
            long["last_stage_number"] == long["stage_number"], other=pd.NA
        )

    if "on_time_offer_accept" in long.columns:
        long["on_time_offer_accept"] = long["on_time_offer_accept"].where(
            long["last_stage_number"] == long["stage_number"], other=pd.NA
    )    

    # 10) Optional helper flags (cheap & useful)
    if "candidate_recruiting_status" in long.columns:
        status_ci = long["candidate_recruiting_status"].astype(str).str.strip().str.lower()
        in_process = (status_ci == "application in process")
        long["in_process_count"] = in_process.astype("Int8")
        long["completed_count"] = (~in_process).astype("Int8")

    # Other mappings/loaders
    dispo_map = pd.read_excel(
        config["SCRUM_FILE"],
        sheet_name="disposition_mapping",
    )
    dispo_map = normalize_colnames(dispo_map)    

    # disposition mapping
    if "disposition_reason" in df.columns and "disposition_reason" in dispo_map.columns:
        long = long.merge(dispo_map, how="left", on="disposition_reason")

    # 11) Order & final dtypes
    order_cols = [
        "job_requisition_id",
        "candidate_id",
        "added_date","job_application_date",
        "last_stage_number",          # original (mapped) max stage reached
        "stage_number",              # exploded per-stage row
        "stage",                     # label
        "candidate_recruiting_status",
        "recruiting_agency",
        "source","consolidated_channel","internal_external",
        "disposition_reason","is_dispo",
        "consolidated_disposition","consolidated_disposition_2",
        "is_non_auto_dispo","is_candidate_driven_dispo",
        "in_process_count","completed_count",
        "on_time_offer_accept",
    ]
    
    # keep only those that exist
    order_cols = [c for c in order_cols if c in long.columns]
    long = long[order_cols].copy()

    # Cast some low-cardinality text to category (memory-friendly)
    for c in ("stage", 
              "source","consolidated_channel","internal_external",
              "disposition_reason","consolidated_disposition","consolidated_disposition_2",
              "recruiting_agency", "on_time_offer_accept"):
        if c in long.columns:
            long[c] = long[c].astype("category")

    return long

# -----------------------#
#     Orchestrator       #
# -----------------------#

def run(
    source: pd.DataFrame | str | Mapping | Iterable,
    *,
    config: dict,
    drop_review_in_process: bool = False,
) -> pd.DataFrame:
    """
    Full ETL: extract → transform
    """
    raw = extract(source)
    funnel = transform(raw, config,  drop_review_in_process=drop_review_in_process)
    return funnel
