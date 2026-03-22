"""
tests/test_unit_jobreq.py  –  Unit tests for src/jobreq.py
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.jobreq import OUTPUT_SCHEMA, transform, load


# ════════════════════════════════════════════════════════════════════════════
# transform()
# ════════════════════════════════════════════════════════════════════════════

class TestJobreqTransform:

    def test_output_schema_matches(self, jobreq_input_df: pl.DataFrame):
        result = transform(jobreq_input_df.lazy())
        for col, dtype in OUTPUT_SCHEMA.items():
            assert col in result.columns, f"Missing output column: {col}"
            assert result.schema[col] == dtype, (
                f"Column {col}: expected {dtype}, got {result.schema[col]}"
            )

    def test_deduplication_keeps_latest_date(self, jobreq_input_df: pl.DataFrame):
        """JR-001 appears twice (2025-06-01 and 2025-05-01). Keep 2025-06-01."""
        result = transform(jobreq_input_df.lazy())
        jr001 = result.filter(pl.col("job_requisition_id") == "JR-001")
        assert jr001.height == 1, "JR-001 should appear exactly once after dedup"
        assert jr001["target_hire_date"][0] == date(2025, 6, 1)

    def test_open_status(self, jobreq_input_df: pl.DataFrame):
        """JR-001: openings=2, filled=0 → OPEN."""
        result = transform(jobreq_input_df.lazy())
        status = result.filter(pl.col("job_requisition_id") == "JR-001")["jobreq_status"][0]
        assert status == "OPEN", f"Expected OPEN, got {status}"

    def test_filled_status(self, jobreq_input_df: pl.DataFrame):
        """JR-002: openings=0, filled=1, requested_new=1 → FILLED."""
        result = transform(jobreq_input_df.lazy())
        status = result.filter(pl.col("job_requisition_id") == "JR-002")["jobreq_status"][0]
        assert status == "FILLED", f"Expected FILLED, got {status}"

    def test_can_status(self, jobreq_input_df: pl.DataFrame):
        """JR-003: openings=0, filled=0 → positions_requested_new=0 → CAN."""
        result = transform(jobreq_input_df.lazy())
        status = result.filter(pl.col("job_requisition_id") == "JR-003")["jobreq_status"][0]
        assert status == "CAN", f"Expected CAN, got {status}"

    def test_positions_requested_new_computed(self, jobreq_input_df: pl.DataFrame):
        """JR-004: openings=1, filled=2 → positions_requested_new=3."""
        result = transform(jobreq_input_df.lazy())
        jr004 = result.filter(pl.col("job_requisition_id") == "JR-004")
        assert jr004["positions_requested_new"][0] == 3

    def test_no_duplicate_ids(self, jobreq_input_df: pl.DataFrame):
        result = transform(jobreq_input_df.lazy())
        assert result["job_requisition_id"].n_unique() == result.height, (
            "Duplicate job_requisition_id found after transform"
        )

    def test_status_only_valid_values(self, jobreq_input_df: pl.DataFrame):
        result = transform(jobreq_input_df.lazy())
        invalid = result.filter(~pl.col("jobreq_status").is_in(["OPEN", "FILLED", "CAN"]))
        assert invalid.is_empty(), (
            f"Unexpected jobreq_status values:\n{invalid.select('job_requisition_id', 'jobreq_status')}"
        )

    @pytest.mark.parametrize("openings,filled,expected_status", [
        (3, 0, "OPEN"),     # clear open
        (0, 2, "FILLED"),   # filled, no openings left
        (0, 0, "CAN"),      # cancelled — nothing requested
        (1, 1, "OPEN"),     # still has openings despite some filled
    ])
    def test_status_logic_parametrized(self, openings, filled, expected_status):
        df = pl.DataFrame({
            "job_requisition_id":             ["JR-X"],
            "target_hire_date":               [date(2025, 1, 1)],
            "positions_filled_hire_selected": pl.Series([filled], dtype=pl.Int16),
            "positions_openings_available":   pl.Series([openings], dtype=pl.Int16),
            "mbps_teams":                     ["Team X"],
        })
        result = transform(df.lazy())
        assert result["jobreq_status"][0] == expected_status

    def test_empty_input_returns_empty(self):
        df = pl.DataFrame({
            "job_requisition_id":             pl.Series([], dtype=pl.Utf8),
            "target_hire_date":               pl.Series([], dtype=pl.Date),
            "positions_filled_hire_selected": pl.Series([], dtype=pl.Int16),
            "positions_openings_available":   pl.Series([], dtype=pl.Int16),
            "mbps_teams":                     pl.Series([], dtype=pl.Utf8),
        })
        result = transform(df.lazy())
        assert result.height == 0
        for col in OUTPUT_SCHEMA:
            assert col in result.columns


# ════════════════════════════════════════════════════════════════════════════
# load()
# ════════════════════════════════════════════════════════════════════════════

class TestJobreqLoad:

    def test_parquet_written(self, jobreq_input_df: pl.DataFrame, pipeline_config: dict):
        df = transform(jobreq_input_df.lazy())
        load(df, pipeline_config, date(2025, 6, 1))

        out = pipeline_config["OUTPUT_JOBREQS_PAR"]
        assert Path(out).exists(), "jobreqs.parquet not written"
        read_back = pl.read_parquet(out)
        assert read_back.height == df.height

    def test_refresh_date_csv_written(self, jobreq_input_df: pl.DataFrame, pipeline_config: dict):
        df = transform(jobreq_input_df.lazy())
        refresh = date(2025, 6, 1)
        load(df, pipeline_config, refresh)

        csv_path = Path(pipeline_config["PROCESSED_DATA_ROOT"]) / "refresh_date.csv"
        assert csv_path.exists(), "refresh_date.csv not written"
        content = csv_path.read_text()
        assert "2025-06-01" in content

    def test_parquet_schema_preserved(self, jobreq_input_df: pl.DataFrame, pipeline_config: dict):
        df = transform(jobreq_input_df.lazy())
        load(df, pipeline_config, date(2025, 6, 1))
        read_back = pl.read_parquet(pipeline_config["OUTPUT_JOBREQS_PAR"])
        for col, dtype in OUTPUT_SCHEMA.items():
            assert col in read_back.columns
            assert read_back.schema[col] == dtype


# ════════════════════════════════════════════════════════════════════════════
# extract() via file (integration-lite)
# ════════════════════════════════════════════════════════════════════════════

class TestJobreqExtract:
    def test_extract_filters_old_records(self, all_status_excel: Path, pipeline_config: dict):
        """JR-OLD with 2020 date should be excluded given 3-year scope from 2025."""
        from src.jobreq import extract
        cutoff = date(2022, 1, 1)  # excludes 2020
        lf = extract(pipeline_config, cutoff)
        df = lf.collect()
        assert "JR-OLD" not in df["job_requisition_id"].to_list(), (
            "Record below cutoff should have been filtered"
        )

    def test_extract_returns_lazyframe(self, all_status_excel: Path, pipeline_config: dict):
        from src.jobreq import extract
        result = extract(pipeline_config, date(2020, 1, 1))
        assert isinstance(result, pl.LazyFrame)
