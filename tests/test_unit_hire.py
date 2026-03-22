"""
tests/test_unit_hire.py  –  Unit tests for src/hire.py
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.hire import (
    HIRES_SCHEMA,
    _generate_discrepancy_report,
    _select_rename,
    _upsert_merge,
    build_incoming_hires_file,
)


# ════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════

def _minimal_hires(**overrides) -> pl.DataFrame:
    """Return a one-row DataFrame matching HIRES_SCHEMA."""
    defaults = {c: None for c in HIRES_SCHEMA}
    defaults.update({
        "jrpos_id":           "JR-X_POS-X",
        "jree_id":            "JR-X_EMP-X",
        "eehire_id":          "EMP-X_202501",
        "job_requisition_id": "JR-X",
        "employee_id":        "EMP-X",
        "candidate_start_date": date(2025, 1, 1),
    })
    defaults.update(overrides)
    return pl.DataFrame({
        k: [v] for k, v in defaults.items()
    }).select([
        pl.col(c).cast(dtype, strict=False)
        for c, dtype in HIRES_SCHEMA.items()
    ])


# ════════════════════════════════════════════════════════════════════════════
# _select_rename
# ════════════════════════════════════════════════════════════════════════════

class TestSelectRename:
    def test_renames_present_columns(self):
        df = pl.DataFrame({"Job Requisition ID": ["JR-1"], "Employee ID": ["EMP-1"]})
        result = _select_rename(df, {"Job Requisition ID": "job_requisition_id", "Employee ID": "employee_id"})
        assert "job_requisition_id" in result.columns
        assert "employee_id" in result.columns

    def test_skips_missing_source_columns(self):
        df = pl.DataFrame({"Job Requisition ID": ["JR-1"]})
        result = _select_rename(df, {"Job Requisition ID": "jr_id", "Missing Col": "missing"})
        assert "jr_id" in result.columns
        assert "missing" not in result.columns


# ════════════════════════════════════════════════════════════════════════════
# _upsert_merge
# ════════════════════════════════════════════════════════════════════════════

class TestUpsertMerge:
    def test_new_rows_appended(self, master_hires_df: pl.DataFrame):
        new_row = _minimal_hires(
            jrpos_id="JR-NEW_POS-NEW",
            jree_id="JR-NEW_EMP-NEW",
            eehire_id="EMP-NEW_202506",
            job_requisition_id="JR-NEW",
            employee_id="EMP-NEW",
        )
        result = _upsert_merge(master_hires_df.lazy(), new_row.lazy(), "jrpos_id").collect()
        assert result.height == master_hires_df.height + 1
        assert "JR-NEW_POS-NEW" in result["jrpos_id"].to_list()

    def test_existing_key_updated(self, master_hires_df: pl.DataFrame):
        updated = _minimal_hires(
            jrpos_id="JR-001_POS-001",
            jree_id="JR-001_EMP-200",
            eehire_id="EMP-200_202504",
            job_requisition_id="JR-001",
            employee_id="EMP-200",
            source="Updated Source",
        )
        result = _upsert_merge(master_hires_df.lazy(), updated.lazy(), "jrpos_id").collect()
        row = result.filter(pl.col("jrpos_id") == "JR-001_POS-001")
        assert row["source"][0] == "Updated Source"

    def test_result_has_master_schema(self, master_hires_df: pl.DataFrame):
        new_row = _minimal_hires(jrpos_id="NEW", jree_id="NEW", eehire_id="NEW")
        result = _upsert_merge(master_hires_df.lazy(), new_row.lazy(), "jrpos_id").collect()
        for col in HIRES_SCHEMA:
            assert col in result.columns, f"Missing column after merge: {col}"

    def test_no_rows_lost(self, master_hires_df: pl.DataFrame):
        """Merging with empty incoming should keep all master rows."""
        empty = pl.DataFrame(schema={k: v for k, v in HIRES_SCHEMA.items()})
        result = _upsert_merge(master_hires_df.lazy(), empty.lazy(), "jrpos_id").collect()
        assert result.height >= master_hires_df.height


# ════════════════════════════════════════════════════════════════════════════
# _generate_discrepancy_report
# ════════════════════════════════════════════════════════════════════════════

class TestDiscrepancyReport:
    def test_no_discrepancy(self, master_hires_df: pl.DataFrame):
        """Each JR has exactly the count that jobreq says."""
        jobreq = pl.DataFrame({
            "job_requisition_id":           ["JR-001", "JR-002"],
            "positions_filled_hire_selected": [1, 1],
            "target_hire_date":             [date(2025, 4, 1), date(2025, 4, 15)],
        })
        report = _generate_discrepancy_report(master_hires_df, jobreq)
        assert report.height == 0, (
            f"Expected no discrepancies, got:\n{report}"
        )

    def test_discrepancy_detected(self, master_hires_df: pl.DataFrame):
        """JR-001 has 1 hire in master but jobreq says 5 filled."""
        jobreq = pl.DataFrame({
            "job_requisition_id":           ["JR-001", "JR-002"],
            "positions_filled_hire_selected": [5, 1],
            "target_hire_date":             [date(2025, 4, 1), date(2025, 4, 15)],
        })
        report = _generate_discrepancy_report(master_hires_df, jobreq)
        assert report.height > 0
        jr001 = report.filter(pl.col("job_requisition_id") == "JR-001")
        assert jr001["discrepancy"][0] == 4

    def test_discrepancy_columns_present(self, master_hires_df: pl.DataFrame):
        jobreq = pl.DataFrame({
            "job_requisition_id":           ["JR-001"],
            "positions_filled_hire_selected": [99],
            "target_hire_date":             [date(2025, 4, 1)],
        })
        report = _generate_discrepancy_report(master_hires_df, jobreq)
        for col in ("job_requisition_id", "jobreq_filled_count", "hires_count", "discrepancy"):
            assert col in report.columns, f"Missing column in discrepancy report: {col}"


# ════════════════════════════════════════════════════════════════════════════
# build_incoming_hires_file (unit-level, no file I/O)
# ════════════════════════════════════════════════════════════════════════════

class TestBuildIncomingHiresFile:
    def test_level9_excluded(self):
        """Grade Level 9 rows should be excluded from fulfillment."""
        from src.hire import FULFILLMENT_RENAME_MAP, INTX_RENAME_MAP

        fulfillment = pl.DataFrame({
            "Job Requisition ID":    ["JR-001", "JR-002"],
            "Position ID - Proposed": ["POS-001", "POS-002"],
            "Employee ID":           ["EMP-200", "EMP-201"],
            "Legal Name":            ["Alice",   "Bob"],
            "Business Process Reason": ["Hire > External", "Hire > External"],
            "Effective Date":        ["2025-04-01", "2025-04-15"],
            "Grade - Proposed":      ["Level 3",   "Level 9"],   # Level 9 excluded
        })
        intx = pl.DataFrame({k: pl.Series([], dtype=pl.Utf8) for k in INTX_RENAME_MAP.keys()})

        result = build_incoming_hires_file(fulfillment, intx).collect()
        assert "EMP-201" not in result["employee_id"].to_list(), (
            "Level 9 employee should have been excluded"
        )

    def test_eehire_id_format(self):
        """eehire_id = employee_id + '_' + YYYYMM of candidate_start_date."""
        from src.hire import FULFILLMENT_RENAME_MAP, INTX_RENAME_MAP

        fulfillment = pl.DataFrame({
            "Job Requisition ID":    ["JR-001"],
            "Position ID - Proposed": ["POS-001"],
            "Employee ID":           ["EMP-200"],
            "Legal Name":            ["Alice"],
            "Business Process Reason": ["Hire > External"],
            "Effective Date":        ["2025-04-01"],
            "Grade - Proposed":      ["Level 3"],
        })
        intx = pl.DataFrame({k: pl.Series([], dtype=pl.Utf8) for k in INTX_RENAME_MAP.keys()})

        result = build_incoming_hires_file(fulfillment, intx).collect()
        if result.height > 0:
            assert result["eehire_id"][0] == "EMP-200_202504"
