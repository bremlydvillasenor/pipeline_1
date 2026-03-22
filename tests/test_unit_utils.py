"""
tests/test_unit_utils.py  –  Unit tests for src/_utils.py
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src._utils import (
    build_rename_map,
    ensure_schema,
    extract_date_from_filename,
    latest_file,
    to_snake,
    write_parquet_atomic,
)


# ════════════════════════════════════════════════════════════════════════════
# to_snake
# ════════════════════════════════════════════════════════════════════════════

class TestToSnake:
    def test_basic_space(self):
        assert to_snake("Job Requisition ID") == "job_requisition_id"

    def test_special_chars_stripped(self):
        assert to_snake("Positions Filled (Hire Selected)") == "positions_filled_hire_selected"

    def test_already_snake(self):
        assert to_snake("target_hire_date") == "target_hire_date"

    def test_leading_trailing_spaces(self):
        assert to_snake("  Name  ") == "name"

    def test_multiple_spaces_collapsed(self):
        assert to_snake("A  B  C") == "a_b_c"

    def test_empty_string_returns_col(self):
        assert to_snake("") == "col"

    def test_brackets_in_name(self):
        assert to_snake("Masterfile[Reporting Date]") == "masterfile_reporting_date"

    def test_hyphens(self):
        assert to_snake("Sub-function") == "sub_function"

    @pytest.mark.parametrize("raw,expected", [
        ("MBPS Teams",          "mbps_teams"),
        ("Grade - Proposed",    "grade_proposed"),
        ("Is cancelled?",       "is_cancelled"),
        ("Date Completed (Offer)", "date_completed_offer"),
    ])
    def test_parametrized_columns(self, raw, expected):
        assert to_snake(raw) == expected


# ════════════════════════════════════════════════════════════════════════════
# build_rename_map
# ════════════════════════════════════════════════════════════════════════════

class TestBuildRenameMap:
    def test_basic(self):
        cols = ["Job ID", "Hire Date"]
        m = build_rename_map(cols)
        assert m == {"Job ID": "job_id", "Hire Date": "hire_date"}

    def test_duplicate_snake_suffixed(self):
        cols = ["Name", "name", "NAME"]
        m = build_rename_map(cols)
        values = list(m.values())
        assert "name" in values
        assert "name_1" in values
        assert "name_2" in values

    def test_preserves_all_originals(self):
        cols = ["Alpha", "Beta", "Gamma"]
        m = build_rename_map(cols)
        assert set(m.keys()) == {"Alpha", "Beta", "Gamma"}

    def test_empty_list(self):
        assert build_rename_map([]) == {}

    def test_special_chars_columns(self):
        cols = ["Positions Filled (Hire Selected)", "Positions Openings Available"]
        m = build_rename_map(cols)
        assert m["Positions Filled (Hire Selected)"] == "positions_filled_hire_selected"
        assert m["Positions Openings Available"] == "positions_openings_available"


# ════════════════════════════════════════════════════════════════════════════
# extract_date_from_filename
# ════════════════════════════════════════════════════════════════════════════

class TestExtractDateFromFilename:
    def test_standard_filename(self):
        p = Path("MBPS Requisition - All Status 2025-06-01.xlsx")
        assert extract_date_from_filename(p) == date(2025, 6, 1)

    def test_date_in_middle(self):
        p = Path("report_2024-12-31_final.csv")
        assert extract_date_from_filename(p) == date(2024, 12, 31)

    def test_no_date_raises(self):
        with pytest.raises(ValueError, match="No date matching"):
            extract_date_from_filename(Path("nodate_report.xlsx"))

    def test_returns_date_not_datetime(self):
        result = extract_date_from_filename(Path("file_2025-01-15.xlsx"))
        assert isinstance(result, date)
        assert not hasattr(result, "hour")  # datetime.date has no hour attr


# ════════════════════════════════════════════════════════════════════════════
# latest_file
# ════════════════════════════════════════════════════════════════════════════

class TestLatestFile:
    def test_returns_newest(self, tmp_path: Path):
        (tmp_path / "report_2025-01-01.xlsx").touch()
        import time; time.sleep(0.01)
        newer = tmp_path / "report_2025-06-01.xlsx"
        newer.touch()
        result = latest_file(tmp_path, "report_*.xlsx")
        assert result == newer

    def test_no_match_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="No files match"):
            latest_file(tmp_path, "*.xlsx")

    def test_single_file(self, tmp_path: Path):
        f = tmp_path / "only_2025-03-01.csv"
        f.touch()
        assert latest_file(tmp_path, "only_*.csv") == f


# ════════════════════════════════════════════════════════════════════════════
# ensure_schema
# ════════════════════════════════════════════════════════════════════════════

class TestEnsureSchema:
    def test_all_present_casts(self):
        schema = {"id": pl.Utf8, "val": pl.Int32}
        df = pl.DataFrame({"id": ["A"], "val": ["42"]})
        result = ensure_schema(df, schema)
        assert result.schema == {"id": pl.Utf8, "val": pl.Int32}
        assert result["val"][0] == 42

    def test_missing_column_filled_null(self):
        schema = {"id": pl.Utf8, "missing_col": pl.Date}
        df = pl.DataFrame({"id": ["A"]})
        result = ensure_schema(df, schema)
        assert "missing_col" in result.columns
        assert result["missing_col"][0] is None

    def test_column_order_matches_schema(self):
        schema = {"z": pl.Utf8, "a": pl.Utf8, "m": pl.Int16}
        df = pl.DataFrame({"a": ["x"], "m": [1], "z": ["y"]})
        result = ensure_schema(df, schema)
        assert result.columns == ["z", "a", "m"]

    def test_empty_df_returns_schema_df(self):
        schema = {"id": pl.Utf8, "count": pl.Int32}
        df = pl.DataFrame({"id": pl.Series([], dtype=pl.Utf8),
                           "count": pl.Series([], dtype=pl.Int32)})
        result = ensure_schema(df, schema)
        assert result.height == 0
        assert result.schema == schema

    def test_extra_columns_dropped(self):
        schema = {"id": pl.Utf8}
        df = pl.DataFrame({"id": ["A"], "extra": ["B"], "another": [1]})
        result = ensure_schema(df, schema)
        assert result.columns == ["id"]


# ════════════════════════════════════════════════════════════════════════════
# write_parquet_atomic
# ════════════════════════════════════════════════════════════════════════════

class TestWriteParquetAtomic:
    def test_writes_parquet(self, tmp_path: Path):
        df = pl.DataFrame({"a": [1, 2, 3]})
        target = tmp_path / "out.parquet"
        write_parquet_atomic(df, target)
        assert target.exists()
        assert pl.read_parquet(target).equals(df)

    def test_tmp_file_cleaned_up(self, tmp_path: Path):
        df = pl.DataFrame({"a": [1]})
        target = tmp_path / "out.parquet"
        write_parquet_atomic(df, target)
        assert not (tmp_path / "out.parquet.tmp").exists()

    def test_overwrites_existing(self, tmp_path: Path):
        target = tmp_path / "out.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(target)
        new_df = pl.DataFrame({"a": [99, 100]})
        write_parquet_atomic(new_df, target)
        assert pl.read_parquet(target)["a"].to_list() == [99, 100]

    def test_non_parquet_extension_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match=".parquet"):
            write_parquet_atomic(pl.DataFrame({"a": [1]}), tmp_path / "out.csv")
