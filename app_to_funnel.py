from __future__ import annotations

import logging
import polars as pl

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# -------------------------#
#  Schema / Conventions    #
# -------------------------#

REQUIRED_COLS = {
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
    "on_time_offer_accept",
}

VALID_STAGES = [1, 2, 3, 4, 6, 8]

STAGE_LABELS = {
    1: "Review",
    2: "Screen",
    3: "Assessment",
    4: "Interview",
    6: "Offer",
    8: "Ready for Hire",
}

_STAGE_MAP = pl.DataFrame(
    {
        "stage_number": pl.Series(list(STAGE_LABELS.keys()), dtype=pl.Int16),
        "stage": pl.Series(list(STAGE_LABELS.values()), dtype=pl.Utf8),
    }
)

APPLICATION_IN_PROCESS = "application in process"


def check_schema(df: pl.DataFrame) -> None:
    """Raise ValueError if any required input column is absent."""
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"Input DataFrame is missing required columns: {sorted(missing)}"
        )


# -------------------------#
#  ETL: TRANSFORM STEP     #
# -------------------------#

def transform(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """
    Transform a Polars DataFrame of candidate application rows into funnel rows.

    Rules
    -----
    - Input must be a Polars DataFrame containing all REQUIRED_COLS.
    - Stage coercions: 5 → 4, 7 → 6.
    - job_application_date is truncated to month-start.
    - Rows where candidate_recruiting_status == 'Application in Process'
      are excluded entirely (not exploded, not part of the output).
    - Remaining rows are expanded to one row per stage reached (1,2,3,4,6,8).
    - Only the last (max) stage row keeps the true status / disposition;
      all earlier stage rows get status='Passed' and null disposition fields.
    - Disposition mapping is left-joined from config['SCRUM_FILE'].
    """
    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"Expected a Polars DataFrame, got {type(df).__name__}")

    if df.is_empty():
        return df

    check_schema(df)

    # Keep only the columns we work with
    df = df.select([c for c in df.columns if c in REQUIRED_COLS])

    # --- Coerce last_stage_number; map 5→4, 7→6 ---
    df = df.with_columns(
        pl.when(pl.col("last_stage_number").cast(pl.Int16, strict=False) == 5)
        .then(pl.lit(4, dtype=pl.Int16))
        .when(pl.col("last_stage_number").cast(pl.Int16, strict=False) == 7)
        .then(pl.lit(6, dtype=pl.Int16))
        .otherwise(pl.col("last_stage_number").cast(pl.Int16, strict=False))
        .alias("last_stage_number")
    )

    # --- Normalize job_application_date to month-start ---
    df = df.with_columns(
        pl.col("job_application_date")
        .cast(pl.Date, strict=False)
        .dt.truncate("1mo")
        .alias("job_application_date")
    )

    # --- Exclude "Application in Process" rows entirely ---
    df = df.filter(
        pl.col("candidate_recruiting_status")
        .str.strip_chars()
        .str.to_lowercase()
        .ne(APPLICATION_IN_PROCESS)
    )

    if df.is_empty():
        return df

    # --- Build per-row list of stages reached ---
    df = df.with_columns(
        pl.col("last_stage_number")
        .map_elements(
            lambda x: [s for s in VALID_STAGES if s <= x] if x is not None else [],
            return_dtype=pl.List(pl.Int16),
        )
        .alias("_stage_list")
    )

    # --- Explode to one row per stage ---
    long = (
        df.explode("_stage_list")
        .rename({"_stage_list": "stage_number"})
        .filter(pl.col("stage_number").is_not_null())
    )

    # --- Add stage label ---
    long = long.join(_STAGE_MAP, on="stage_number", how="left")

    # --- Status / disposition: only last stage row keeps truth ---
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

    # --- Helper flags ---
    status_norm = (
        pl.col("candidate_recruiting_status").str.strip_chars().str.to_lowercase()
    )
    long = long.with_columns(
        pl.when(status_norm == APPLICATION_IN_PROCESS)
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(0, dtype=pl.Int8))
        .alias("in_process_count"),
        pl.when(status_norm != APPLICATION_IN_PROCESS)
        .then(pl.lit(1, dtype=pl.Int8))
        .otherwise(pl.lit(0, dtype=pl.Int8))
        .alias("completed_count"),
    )

    # --- Load and left-join disposition mapping ---
    dispo_map = pl.read_excel(
        config["SCRUM_FILE"], sheet_name="disposition_mapping"
    )
    if "disposition_reason" in dispo_map.columns:
        long = long.join(dispo_map, on="disposition_reason", how="left")

    # --- Final column order (only those present) ---
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


# -------------------------#
#     Orchestrator         #
# -------------------------#

def run(df: pl.DataFrame, *, config: dict) -> pl.DataFrame:
    """Transform a Polars DataFrame of candidate applications into a funnel DataFrame."""
    return transform(df, config)
