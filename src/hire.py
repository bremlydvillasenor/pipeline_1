"""
src/hire.py  ·  Hires ETL

Flow:
  1. Extract fulfillment + INTX Excel files
  2. Build a cleansed incoming LazyFrame
  3. Validate schema
  4. Upsert-merge into master hires database (exception-aware)
  5. Run discrepancy check against job-requisition counts
  6. Persist merged output to Parquet
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from dateutil.relativedelta import relativedelta

from ._utils import (
    latest_file,
    read_excel_with_header,
    ensure_schema,
    write_parquet_atomic,
)

log = logging.getLogger(__name__)

_SEP = "─" * 50

# ═════════════════════════════════════════════
# TYPE ALIAS
# ═════════════════════════════════════════════

PipelineConfig = dict[str, Any]


# ═════════════════════════════════════════════
# COLUMN RENAME MAPS
# ═════════════════════════════════════════════

FULFILLMENT_RENAME_MAP: dict[str, str] = {
    "Job Requisition ID":    "job_requisition_id",
    "Position ID - Proposed": "position_id",
    "Employee ID":           "employee_id",
    "Legal Name":            "employee_name",
    "Business Process Reason": "business_process_reason",
    "Effective Date":        "candidate_start_date",
    "Grade - Proposed":      "grade_proposed",
}

INTX_RENAME_MAP: dict[str, str] = {
    "Candidate ID":                  "candidate_id",
    "Employee ID":                   "employee_id",
    "Added Date":                    "added_date",
    "Recruiting Start Date":         "recruiting_start_date",
    "Job Application Date":          "job_application_date",
    "Ready for Hire Date":           "ready_for_hire_date",
    "Candidate Start Date":          "candidate_start_date",
    "Recruiter Completed Offer":     "recruiter_completed_offer",
    "Recruiter Employee ID":         "recruiter_employee_id",
    "Referred By Employee ID":       "referred_by_employee_id",
    "Source":                        "source",
    "Recruiting Agency":             "recruiting_agency",
    "Date Completed (Offer)":        "offer_accepted_date",
    "Job Requisition Unfreeze Date": "jr_unfreeze_date",
    "Total Number of Days Frozen":   "days_frozen",
}


# ═════════════════════════════════════════════
# SCHEMAS
# ═════════════════════════════════════════════

HIRES_SCHEMA: dict[str, pl.DataType] = {
    "jrpos_id":                     pl.Utf8,
    "jree_id":                      pl.Utf8,
    "eehire_id":                    pl.Utf8,
    "job_requisition_id":           pl.Utf8,
    "employee_id":                  pl.Utf8,
    "candidate_id":                 pl.Utf8,
    "employee_name":                pl.Utf8,
    "position_id":                  pl.Utf8,
    "business_process_reason":      pl.Utf8,
    "recruiting_start_date":        pl.Date,
    "added_date":                   pl.Date,
    "job_application_date":         pl.Date,
    "ready_for_hire_date":          pl.Date,
    "candidate_start_date":         pl.Date,
    "is_hire_on_time_exception":    pl.Utf8,
    "jr_unfreeze_date":             pl.Date,
    "offer_accepted_date":          pl.Date,
    "days_frozen":                  pl.Utf8,
    "rfh_recruiter_id":             pl.Utf8,
    "rfh_recruiter_id_exception":   pl.Utf8,
    "recruiter_completed_offer":    pl.Utf8,
    "source":                       pl.Utf8,
    "source_exception":             pl.Utf8,
    "recruiting_agency":            pl.Utf8,
    "referred_by_employee_id":      pl.Utf8,
    "remarks":                      pl.Utf8,
    "termination_date":             pl.Date,
    "termination_reason":           pl.Utf8,
    "termination_reason_category":  pl.Utf8,
    "termination_tenure_days_bucket": pl.Int32,
}

JOBREQ_SCHEMA: dict[str, pl.DataType] = {
    "job_requisition_id":           pl.Utf8,
    "target_hire_date":             pl.Date,
    "positions_filled_hire_selected": pl.Int32,
}


# ═════════════════════════════════════════════
# SCHEMA CHECK
# ═════════════════════════════════════════════

def _schema_check_lf(lf: pl.LazyFrame, target_schema: dict[str, pl.DataType]) -> None:
    actual = lf.collect_schema()
    missing      = set(target_schema) - set(actual)
    type_mismatch = {
        col: (actual[col], target_schema[col])
        for col in target_schema
        if col in actual and actual[col] != target_schema[col]
    }
    if missing or type_mismatch:
        raise ValueError(
            f"Schema mismatch — missing: {missing}  type errors: {type_mismatch}"
        )


# ═════════════════════════════════════════════
# EXTRACT
# ═════════════════════════════════════════════

def extract_incoming_hires_files(config: PipelineConfig) -> dict[str, pl.DataFrame]:
    raw_root = config["RAW_DATA_ROOT"]

    fulfillment = read_excel_with_header(
        latest_file(raw_root, config["FULFILLMENT_PATTERN"]), skip_rows=7
    )
    intx = read_excel_with_header(
        latest_file(raw_root, config["INTX_HIRES_PATTERN"]), skip_rows=10
    )

    log.info("  source : fulfillment  (%d rows)", fulfillment.height)
    log.info("  source : intx         (%d rows)", intx.height)
    return {"fulfillment": fulfillment, "intx": intx}


# ═════════════════════════════════════════════
# SUB FUNCTIONS
# ═════════════════════════════════════════════

def _select_rename(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    return df.select([
        pl.col(src).alias(dst)
        for src, dst in mapping.items()
        if src in df.columns
    ])


def _make_eehire_id(employee_col: str = "employee_id") -> pl.Expr:
    return (
        pl.col(employee_col).cast(pl.Utf8)
        + pl.lit("_")
        + pl.col("candidate_start_date").dt.strftime("%Y%m")
    ).alias("eehire_id")


def _make_jree_id(employee_col: str = "employee_id") -> pl.Expr:
    return (
        pl.col("job_requisition_id").cast(pl.Utf8)
        + pl.lit("_")
        + pl.col(employee_col).cast(pl.Utf8)
    ).alias("jree_id")


def _parse_date(col: str) -> pl.Expr:
    if col == "offer_accepted_date":
        return pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d", strict=False)
    elif col in ("job_application_date", "ready_for_hire_date"):
        return (
            pl.col(col)
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f", strict=False)
            .dt.date()
        )
    else:
        return (
            pl.col(col).cast(pl.String)
            .str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S", strict=False)
            .fill_null(
                pl.col(col).cast(pl.String)
                .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
            )
        )


# ═════════════════════════════════════════════
# BUILD INCOMING HIRES
# ═════════════════════════════════════════════

def build_incoming_hires_file(
    fulfillment: pl.DataFrame,
    intx: pl.DataFrame,
) -> pl.LazyFrame:
    cutoff = date.today().replace(day=1) - relativedelta(months=3)

    fulfillment_lf = _select_rename(fulfillment, FULFILLMENT_RENAME_MAP).lazy()
    intx_lf        = _select_rename(intx, INTX_RENAME_MAP).lazy()

    fulfillment_lf = (
        fulfillment_lf
        .with_columns([
            _parse_date(c)
            for c in fulfillment_lf.collect_schema().names()
            if c.endswith("_date")
        ])
        .filter(
            (pl.col("candidate_start_date") >= cutoff)
            & (~pl.col("grade_proposed").is_in(["Level 9", "Level 10"]))
        )
        .with_columns(
            pl.col("business_process_reason")
            .cast(pl.Utf8).str.split(" > ").list.first()
            .alias("business_process_reason"),
            (
                pl.col("job_requisition_id").cast(pl.Utf8)
                + pl.lit("_")
                + pl.col("position_id").cast(pl.Utf8)
            ).alias("jrpos_id"),
            _make_eehire_id(),
            _make_jree_id(),
        )
    )

    intx_lf = (
        intx_lf
        .with_columns([
            _parse_date(c)
            for c in intx_lf.collect_schema().names()
            if c.endswith("_date")
        ])
        .filter(pl.col("candidate_start_date") >= cutoff)
        .with_columns(
            pl.coalesce(
                pl.col("recruiter_completed_offer").str.extract(r"\((\d+)\)"),
                pl.col("recruiter_employee_id").cast(pl.Utf8),
            ).alias("rfh_recruiter_id"),
            pl.when(pl.col("jr_unfreeze_date").is_null())
            .then(0)
            .otherwise(pl.col("days_frozen"))
            .alias("days_frozen"),
            pl.coalesce(
                pl.col("offer_accepted_date"),
                pl.col("ready_for_hire_date"),
            ).alias("offer_accepted_date"),
            _make_eehire_id(),
        )
        .drop("candidate_start_date", "employee_id", "recruiter_employee_id", strict=False)
    )

    return (
        fulfillment_lf
        .join(intx_lf, on="eehire_id", how="left")
        .sort("candidate_start_date", "employee_id")
        .unique(subset=["eehire_id"], keep="last")
    )


# ═════════════════════════════════════════════
# UPSERT MERGE (Exception-Aware)
# ═════════════════════════════════════════════

def _upsert_merge(
    master: pl.LazyFrame,
    incoming: pl.LazyFrame,
    key_col: str,
) -> pl.LazyFrame:
    master_schema  = master.collect_schema().names()
    incoming_cols  = list(incoming.collect_schema().names())
    master_col_set = set(master_schema)

    update_cols = [c for c in incoming_cols if c != key_col and c in master_col_set]

    joined = master.join(
        incoming.select(key_col, *update_cols),
        on=key_col, how="left", suffix="_upd",
    )

    coalesce_exprs = []
    for col in update_cols:
        exc_col = f"{col}_exception"
        upd_col = f"{col}_upd"
        if exc_col in master_col_set:
            coalesce_exprs.append(
                pl.when(pl.col(exc_col).is_not_null()).then(pl.col(exc_col))
                .when(pl.col(upd_col).is_not_null()).then(pl.col(upd_col))
                .otherwise(pl.col(col))
                .alias(col)
            )
        else:
            coalesce_exprs.append(
                pl.coalesce(pl.col(upd_col), pl.col(col)).alias(col)
            )

    updated = joined.with_columns(coalesce_exprs).select(master_schema)

    new_rows = incoming.join(master.select(key_col), on=key_col, how="anti")
    new_rows_aligned = new_rows.with_columns([
        pl.lit(None).cast(dtype).alias(col)
        for col, dtype in HIRES_SCHEMA.items()
        if col not in incoming_cols
    ]).select(HIRES_SCHEMA.keys())

    _schema_check_lf(new_rows_aligned, HIRES_SCHEMA)

    return pl.concat([updated, new_rows_aligned], how="vertical")


# ═════════════════════════════════════════════
# DISCREPANCY REPORT
# ═════════════════════════════════════════════

def _generate_discrepancy_report(
    hires_df: pl.DataFrame,
    jobreq_df: pl.DataFrame,
) -> pl.DataFrame:
    hires_agg = (
        hires_df.lazy()
        .group_by("job_requisition_id")
        .agg(pl.len().alias("hires_count"))
    )
    jobreq_sel = (
        jobreq_df.lazy()
        .select(
            "job_requisition_id",
            pl.col("positions_filled_hire_selected")
            .fill_null(0).cast(pl.Int32)
            .alias("jobreq_filled_count"),
        )
        .unique(subset=["job_requisition_id"])
    )
    discrepancies = (
        jobreq_sel
        .join(hires_agg, on="job_requisition_id", how="left")
        .with_columns(pl.col("hires_count").fill_null(0))
        .with_columns(
            (pl.col("jobreq_filled_count") - pl.col("hires_count")).abs()
            .alias("discrepancy")
        )
        .filter(pl.col("discrepancy") != 0)
        .collect()
    )

    if discrepancies.height > 0:
        log.warning("  discrepancies : %d JR count mismatches detected", discrepancies.height)
    else:
        log.info("  discrepancies : none")

    return discrepancies


# ═════════════════════════════════════════════
# MAIN PIPELINE
# ═════════════════════════════════════════════

def run_hire(
    config: PipelineConfig,
    incoming_df: pl.DataFrame | None = None,
    master_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """
    Orchestrate full Hires ETL.

    Returns
    -------
    merged_df : pl.DataFrame – updated master hires database
    """
    log.info(_SEP)
    log.info("[hire] started")

    # Build incoming
    if incoming_df is None:
        extracted   = extract_incoming_hires_files(config)
        incoming_lf = build_incoming_hires_file(extracted["fulfillment"], extracted["intx"])
    else:
        incoming_lf = incoming_df.lazy()

    # Load master / hiresdb
    if master_df is None:
        hiresdb_path = Path(config["OUTPUT_HIRESDB_PAR"])
        if not hiresdb_path.exists():
            raise FileNotFoundError(f"Master hiresdb not found: {hiresdb_path}")
        master_df = pl.read_parquet(hiresdb_path)

    master_df = ensure_schema(master_df, HIRES_SCHEMA, "master")

    # Merge
    merged_df = _upsert_merge(master_df.lazy(), incoming_lf, key_col="jrpos_id").collect()

    # Discrepancy check
    jobreq_df = ensure_schema(
        pl.read_parquet(Path(config["OUTPUT_JOBREQS_PAR"])),
        JOBREQ_SCHEMA, "jobreq",
    )
    _generate_discrepancy_report(merged_df, jobreq_df)

    # Persist
    for dest in (config["OUTPUT_HIRESDB_PAR"], config["OUTPUT_HIRES_PAR"]):
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(merged_df, dest)
        log.info("  output : %s  (%d rows)", dest.name, merged_df.height)

    log.info("[hire] done")
    log.info(_SEP)

    return merged_df
