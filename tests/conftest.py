"""
tests/conftest.py  –  Shared fixtures for the pipeline test suite.

Fixtures are organised into four groups:
  1. Raw in-memory DataFrames  (unit tests, no file I/O)
  2. Config dict               (points everything at tmp_path)
  3. Excel / CSV file builders (integration tests)
  4. Pre-built parquet files   (hiresdb, funnel)
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import openpyxl
import polars as pl
import pytest

# ── schema imports ──────────────────────────────────────────────────────────
from src.jobreq import INPUT_DTYPES, OUTPUT_SCHEMA as JOBREQ_OUTPUT_SCHEMA
from src.hire import HIRES_SCHEMA, FULFILLMENT_RENAME_MAP, INTX_RENAME_MAP
from src.referral import REFERRAL_SCHEMA
from src.app import APP_RENAME_MAP, VALID_GRADES, STAGE_MAP

DATA_DIR = Path(__file__).parent / "data"


# ════════════════════════════════════════════════════════════════════════════
# 1.  RAW IN-MEMORY DATAFRAMES
# ════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def raw_jobreq_rows() -> list[dict]:
    """Rows that simulate the All-Status sheet after header promotion."""
    return [
        # OPEN: openings > 0
        {"job_requisition_id": "JR-001", "target_hire_date": "2025-06-01",
         "positions_filled_hire_selected": "0", "positions_openings_available": "2", "mbps_teams": "Team A",
         "extra_col": "ignored"},
        # FILLED: requested > 0, openings == 0
        {"job_requisition_id": "JR-002", "target_hire_date": "2025-07-15",
         "positions_filled_hire_selected": "1", "positions_openings_available": "0", "mbps_teams": "Team B",
         "extra_col": "ignored"},
        # CAN: filled=0, openings=0  → requested_new == 0
        {"job_requisition_id": "JR-003", "target_hire_date": "2025-08-30",
         "positions_filled_hire_selected": "0", "positions_openings_available": "0", "mbps_teams": "Team C",
         "extra_col": "ignored"},
        # Duplicate JR-001 (older date — should be deduped away)
        {"job_requisition_id": "JR-001", "target_hire_date": "2025-05-01",
         "positions_filled_hire_selected": "1", "positions_openings_available": "1", "mbps_teams": "Team A",
         "extra_col": "ignored"},
        # Below scope cutoff (should be filtered by extract)
        {"job_requisition_id": "JR-OLD", "target_hire_date": "2020-01-01",
         "positions_filled_hire_selected": "0", "positions_openings_available": "1", "mbps_teams": "Team Z",
         "extra_col": "ignored"},
    ]


@pytest.fixture
def jobreq_input_df() -> pl.DataFrame:
    """Typed LazyFrame-ready DataFrame matching INPUT_DTYPES (post-extract state)."""
    return pl.DataFrame({
        "job_requisition_id":             ["JR-001", "JR-002", "JR-003", "JR-001", "JR-004"],
        "target_hire_date":               [date(2025, 6, 1), date(2025, 7, 15), date(2025, 8, 30),
                                           date(2025, 5, 1), date(2025, 9, 10)],
        "positions_filled_hire_selected": [0, 1, 0, 1, 2],
        "positions_openings_available":   [2, 0, 0, 1, 1],
        "mbps_teams":                     ["Team A", "Team B", "Team C", "Team A", "Team A"],
    }).with_columns([
        pl.col("positions_filled_hire_selected").cast(pl.Int16),
        pl.col("positions_openings_available").cast(pl.Int16),
    ])


@pytest.fixture
def master_hires_df() -> pl.DataFrame:
    """Minimal master hiresdb matching HIRES_SCHEMA."""
    base = pl.DataFrame({
        "jrpos_id":            ["JR-001_POS-001", "JR-002_POS-002"],
        "jree_id":             ["JR-001_EMP-200",  "JR-002_EMP-201"],
        "eehire_id":           ["EMP-200_202504",  "EMP-201_202504"],
        "job_requisition_id":  ["JR-001",           "JR-002"],
        "employee_id":         ["EMP-200",          "EMP-201"],
        "candidate_id":        ["CAND-002",         "CAND-008"],
        "employee_name":       ["Bob Jones",        "Helen Troy"],
        "position_id":         ["POS-001",          "POS-002"],
        "business_process_reason": ["Hire",         "Hire"],
        "candidate_start_date": [date(2025, 4, 1), date(2025, 4, 15)],
    })
    # Fill all remaining HIRES_SCHEMA columns with nulls
    for col, dtype in HIRES_SCHEMA.items():
        if col not in base.columns:
            base = base.with_columns(pl.lit(None).cast(dtype).alias(col))
    return base.select(list(HIRES_SCHEMA.keys()))


@pytest.fixture
def sample_apps_df() -> pl.DataFrame:
    """Applications DataFrame with a mix of grades, stages and statuses."""
    return pl.DataFrame({
        "candidate_id":              ["CAND-001", "CAND-002", "CAND-003", "CAND-004", "CAND-005"],
        "candidate_name":            ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "job_requisition_id":        ["JR-001", "JR-001", "JR-002", "JR-002", "JR-001"],
        "job_requisition":           ["SWE I"] * 5,
        "recruiting_instruction":    ["External"] * 5,
        "job_family":                ["Engineering"] * 5,
        "compensation_grade":        ["Level 3", "Level 3", "Level 9", "Level 2", "Level 3"],
        "worker_type_hiring_requirement":     ["Employee"] * 5,
        "worker_sub_type_hiring_requirement": ["Regular"] * 5,
        "target_hire_date":          ["2025-06-01"] * 5,
        "added_date":                ["2025-01-10", "2025-01-12", "2025-02-01",
                                      "2025-01-05", "2025-01-20"],
        "job_application_date":      ["2025-01-10", "2025-01-12", "2025-02-01",
                                      "2025-01-05", "2025-01-20"],
        "offer_accepted_date":       [None, "2025-03-01", None, None, None],
        "candidate_start_date":      [None, "2025-04-01", None, None, None],
        "recruiter_employee_id":     ["R123", "R222", "R333", "R444", "R555"],
        "recruiter_completed_offer": [None, "R222 (22222)", None, None, None],
        "disposition_reason":        [None, None, "Not a Fit", "Withdrew", None],
        "candidate_recruiting_status": ["Application in Process", "Offer Accepted",
                                        "Rejected", "Withdrawn", "Application in Process"],
        "last_recruiting_stage":     ["Interview", "Ready for Hire", "Screen", "Interview", "Review"],
        "hired":                     [None, "Yes", None, None, None],
        "hire_transaction_status":   [None, "Completed", None, None, None],
        "source":                    ["ERP Referral", "Referral", "LinkedIn", "LinkedIn", "Direct"],
        "referred_by_employee_id":   ["EMP-100", "EMP-101", None, None, None],
        "referred_by":               ["Bob Jones", "Jane Doe", None, None, None],
        "recruiting_agency":         [None, None, None, None, None],
        "last_employer":             ["Acme", "Tech Co", None, None, None],
        "school_name":               [None] * 5,
        "is_cancelled":              ["No"] * 5,
        "mbps_teams":                ["Team A", "Team A", "Team B", "Team B", "Team A"],
    })


@pytest.fixture
def sample_prospect_df() -> pl.DataFrame:
    """Prospect rows (pre-funnel) matching REFERRAL_SCHEMA."""
    return pl.DataFrame({
        "candidate_id":                ["CAND-010", "CAND-011", "CAND-012"],
        "job_requisition_id":          ["JR-001",   "JR-002",   "JR-003"],
        "referred_by_employee_id":     ["EMP-100",  "EMP-101",  "EMP-102"],
        "referred_by":                 ["Bob Jones (100)", "Jane Doe (101)", "Carol White (102)"],
        "added_date":                  [date(2025, 1, 15), date(2025, 2, 1), date(2025, 1, 20)],
        "source":                      ["ERP Referral", "ERP Referral", "Referral"],
        "disposition_reason":          [None, None, "Not a Fit"],
        "candidate_recruiting_status": ["Prospect", "Prospect", "Rejected"],
        "last_stage_number":           pl.Series([0, 0, 0], dtype=pl.Int16),
    })


@pytest.fixture
def sample_funnel_df() -> pl.DataFrame:
    """Funnel DataFrame as produced by app.py (ERP/Referral rows only)."""
    return pl.DataFrame({
        "candidate_id":                ["CAND-001", "CAND-001", "CAND-002"],
        "job_requisition_id":          ["JR-001",   "JR-001",   "JR-001"],
        "referred_by_employee_id":     ["EMP-100",  "EMP-100",  "EMP-101"],
        "referred_by":                 ["Bob Jones", "Bob Jones", "Jane Doe"],
        "added_date":                  [date(2025, 1, 10), date(2025, 1, 10), date(2025, 1, 12)],
        "source":                      ["ERP Referral", "ERP Referral", "Referral"],
        "disposition_reason":          [None, None, None],
        "candidate_recruiting_status": ["Passed", "Application in Process", "Offer Accepted"],
        "last_stage_number":           pl.Series([4, 4, 8], dtype=pl.Int16),
        "stage_number":                pl.Series([1, 2, 8], dtype=pl.Int16),
        "stage":                       ["Review", "Screen", "Ready for Hire"],
    })


@pytest.fixture
def sample_headcount_df() -> pl.DataFrame:
    """Headcount DataFrame after extract() — before transform()."""
    return pl.DataFrame({
        "reporting_date": [date(2025, 1, 31)] * 4 + [date(2025, 2, 28)] * 4,
        "location":       ["Manila", "Manila", "Cebu", "Manila"] * 2,
        "function":       ["Engineering", "Engineering", "Finance", "Operations"] * 2,
        "sub_function":   ["Frontend", "Backend", "FP&A", "HR"] * 2,
        "job_grade":      ["3", "4", "2", "5"] * 2,
        "employee_id":    ["EMP-200", "EMP-201", "EMP-202", "EMP-203"] * 2,
    })


@pytest.fixture
def unique_referrals_df() -> pl.DataFrame:
    """Unique referrals as returned by referral.transform()[1]."""
    return pl.DataFrame({
        "headcount_id": ["EMP-100_2025-01", "EMP-101_2025-01", "EMP-102_2025-02"],
        "has_referral": pl.Series([1, 1, 1], dtype=pl.Int8),
    })


# ════════════════════════════════════════════════════════════════════════════
# 2.  CONFIG DICT (points to tmp_path)
# ════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def pipeline_config(tmp_path: Path) -> dict:
    """Full config dict mirroring what _config.py produces, using tmp_path."""
    raw       = tmp_path / "raw"
    processed = tmp_path / "processed"
    historical = tmp_path / "historical"
    logs      = tmp_path / "logs"

    for d in (raw, processed, historical, logs):
        d.mkdir(parents=True)

    return {
        "RAW_DATA_ROOT":        raw,
        "PROCESSED_DATA_ROOT":  processed,
        "HISTORICAL_DATA_ROOT": historical,
        "LOG_DIR":              logs,
        # Patterns
        "ALL_STATUS_PATTERN":   "MBPS Requisition - All Status*.xlsx",
        "FULFILLMENT_PATTERN":  "*Fulfillment*.xlsx",
        "INTX_HIRES_PATTERN":   "Recruiting - Internal and External Hires*.xlsx",
        "DAILY_APP_PATTERN":    "MBPS Daily Application_2*.csv",
        "PROSPECT_PATTERN":     "Referrals Without Job Applications*.xlsx",
        "HEADCOUNT_PATTERN":    "pmd_monthly_headcount.xlsx",
        # Output paths
        "OUTPUT_JOBREQS_PAR":           processed / "jobreqs.parquet",
        "OUTPUT_HIRES_PAR":             processed / "hires.parquet",
        "OUTPUT_HIRESDB_PAR":           historical / "hiresdb.parquet",
        "OUTPUT_APPS_PAR":              processed / "applications.parquet",
        "OUTPUT_APPS_CSV":              processed / "applications.csv",
        "OUTPUT_APPS_OFFER_ACCEPTS_PAR": processed / "applications_offer_accepts.parquet",
        "OUTPUT_FUNNEL_PAR":            processed / "app_funnel.parquet",
        "OUTPUT_REFERRALS_PARQUET":     processed / "referrals.parquet",
        "OUTPUT_MHEADCOUNT_PARQUET":    processed / "monthly_headcount.parquet",
        # Settings
        "REQS_YEARS_SCOPE": 3,
    }


# ════════════════════════════════════════════════════════════════════════════
# 3.  EXCEL / CSV FILE BUILDERS
# ════════════════════════════════════════════════════════════════════════════

def _excel_with_preamble(path: Path, headers: list[str], rows: list[list], preamble_count: int) -> None:
    """Write an Excel file: N preamble rows, then header row, then data rows."""
    wb = openpyxl.Workbook()
    ws = wb.active
    for i in range(preamble_count):
        ws.append([f"Report metadata – row {i + 1}"])
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


@pytest.fixture
def all_status_excel(pipeline_config: dict) -> Path:
    """All-Status Excel in the raw directory (13 preamble rows)."""
    headers = [
        "Job Requisition ID", "Target Hire Date",
        "Positions Filled (Hire Selected)", "Positions Openings Available",
        "MBPS Teams", "Some Extra Column",
    ]
    rows = [
        ["JR-001", "2025-06-01", 0, 2, "Team A", "x"],
        ["JR-002", "2025-07-15", 1, 0, "Team B", "x"],
        ["JR-003", "2025-08-30", 0, 0, "Team C", "x"],
        ["JR-001", "2025-05-01", 1, 1, "Team A", "x"],   # duplicate, older date
        ["JR-004", "2020-01-01", 0, 1, "Team Z", "x"],   # below cutoff
    ]
    path = pipeline_config["RAW_DATA_ROOT"] / "MBPS Requisition - All Status 2025-06-01.xlsx"
    _excel_with_preamble(path, headers, rows, preamble_count=13)
    return path


@pytest.fixture
def fulfillment_excel(pipeline_config: dict) -> Path:
    """Fulfillment Excel (7 preamble rows)."""
    headers = list(FULFILLMENT_RENAME_MAP.keys())
    rows = [
        ["JR-001", "POS-001", "EMP-200", "Bob Jones",   "Hire > External", "2025-04-01", "Level 3"],
        ["JR-002", "POS-002", "EMP-201", "Helen Troy",  "Hire > External", "2025-04-15", "Level 4"],
        ["JR-003", "POS-003", "EMP-202", "Ivan Rex",    "Hire > Internal", "2025-03-15", "Level 9"],  # excluded
    ]
    path = pipeline_config["RAW_DATA_ROOT"] / "MBPS Fulfillment 2025-06-01.xlsx"
    _excel_with_preamble(path, headers, rows, preamble_count=7)
    return path


@pytest.fixture
def intx_excel(pipeline_config: dict) -> Path:
    """INTX Hires Excel (10 preamble rows)."""
    headers = list(INTX_RENAME_MAP.keys())
    rows = [
        ["CAND-002", "EMP-200", "2025-01-12", "2025-01-12", "2025-01-12", "2025-03-01 00:00:00",
         "2025-04-01", "R22222 (22222)", "R22222", "EMP-101", "Referral", "", "2025-03-01", "", ""],
        ["CAND-008", "EMP-201", "2025-02-01", "2025-02-01", "2025-02-01", "2025-03-15 00:00:00",
         "2025-04-15", "R33333 (33333)", "R33333", "",        "LinkedIn",  "", "2025-03-15", "", ""],
    ]
    path = pipeline_config["RAW_DATA_ROOT"] / "Recruiting - Internal and External Hires 2025-06-01.xlsx"
    _excel_with_preamble(path, headers, rows, preamble_count=10)
    return path


@pytest.fixture
def daily_app_csv(pipeline_config: dict) -> Path:
    """Daily Application CSV (16 metadata lines before header)."""
    path = pipeline_config["RAW_DATA_ROOT"] / "MBPS Daily Application_2025-06-01.csv"
    col_names = list(APP_RENAME_MAP.keys())

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for i in range(16):
            writer.writerow([f"Metadata line {i + 1}"])
        writer.writerow(col_names)
        writer.writerow([
            "CAND-001", "Alice Smith", "JR-001", "SWE I", "External", "Engineering",
            "Level 3", "Employee", "Regular", "2025-06-01", "2025-01-10", "2025-01-10",
            "", "", "R12345", "", "", "Application in Process", "Interview",
            "", "", "ERP Referral", "EMP-100", "Bob Jones", "", "Acme Corp", "", "No", "Team A",
        ])
        writer.writerow([
            "CAND-002", "Bob Jones", "JR-001", "SWE I", "External", "Engineering",
            "Level 3", "Employee", "Regular", "2025-06-01", "2025-01-12", "2025-01-12",
            "2025-03-01", "2025-04-01", "R22222", "R22222 (22222)", "", "Offer Accepted",
            "Ready for Hire", "Yes", "Completed", "Referral", "EMP-101", "Jane Doe",
            "", "Tech Co", "", "No", "Team A",
        ])
        writer.writerow([
            "CAND-005", "Eve Green", "JR-001", "SWE I", "External", "Engineering",
            "Level 9", "Employee", "Regular", "2025-06-01", "2025-01-20", "2025-01-20",
            "", "", "R555", "", "", "Application in Process", "Review",
            "", "", "Direct", "", "", "", "", "", "No", "Team A",
        ])
    return path


@pytest.fixture
def prospect_excel(pipeline_config: dict) -> Path:
    """Prospect (Referrals Without Job Applications) Excel."""
    from src.referral import PROSPECT_RENAME_MAP
    headers = list(PROSPECT_RENAME_MAP.keys())
    rows = [
        ["CAND-010", "JR-001", "EMP-100", "Bob Jones (100)",   "2025-01-15", "ERP Referral", "",           "Prospect"],
        ["CAND-011", "JR-002", "EMP-101", "Jane Doe (101)",    "2025-02-01", "ERP Referral", "",           "Prospect"],
        ["CAND-012", "JR-003", "EMP-102", "Carol White (102)", "2025-01-20", "Referral",     "Not a Fit",  "Rejected"],
    ]
    path = pipeline_config["RAW_DATA_ROOT"] / "Referrals Without Job Applications 2025-06-01.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)
    return path


@pytest.fixture
def headcount_excel(pipeline_config: dict) -> Path:
    """Monthly headcount Excel (1 preamble row)."""
    from src.erp import HEADCOUNT_RAW_COLS
    rows = [
        ["2025-01-31 00:00:00", "Manila", "Engineering", "Frontend", "3", "EMP-200"],
        ["2025-01-31 00:00:00", "Manila", "Engineering", "Backend",  "4", "EMP-201"],
        ["2025-02-28 00:00:00", "Cebu",   "Finance",     "FP&A",     "2", "EMP-202"],
        ["2025-02-28 00:00:00", "Manila", "Operations",  "HR",       "5", "EMP-203"],
    ]
    path = pipeline_config["HISTORICAL_DATA_ROOT"] / "pmd_monthly_headcount.xlsx"
    _excel_with_preamble(path, HEADCOUNT_RAW_COLS, rows, preamble_count=1)
    return path


# ════════════════════════════════════════════════════════════════════════════
# 4.  PRE-BUILT PARQUET FILES
# ════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def hiresdb_parquet(pipeline_config: dict, master_hires_df: pl.DataFrame) -> Path:
    """Write master_hires_df as hiresdb.parquet and return the path."""
    path = pipeline_config["OUTPUT_HIRESDB_PAR"]
    master_hires_df.write_parquet(path, compression="zstd")
    return path


@pytest.fixture
def jobreqs_parquet(pipeline_config: dict, jobreq_input_df: pl.DataFrame) -> Path:
    """Write a minimal jobreqs.parquet for downstream stages."""
    from src.jobreq import transform
    df = transform(jobreq_input_df.lazy())
    path = pipeline_config["OUTPUT_JOBREQS_PAR"]
    df.write_parquet(path, compression="zstd")
    return path


@pytest.fixture
def funnel_parquet(pipeline_config: dict, sample_funnel_df: pl.DataFrame) -> Path:
    """Write sample funnel data as app_funnel.parquet."""
    path = pipeline_config["OUTPUT_FUNNEL_PAR"]
    sample_funnel_df.write_parquet(path, compression="zstd")
    return path
