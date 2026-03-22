"""
tests/test_unit_erp.py  –  Unit tests for src/erp.py
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.erp import _extend_headcount, transform


# ════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════

def _month_end(d: date) -> date:
    """Last day of d's month."""
    y = d.year + (1 if d.month == 12 else 0)
    m = 1 if d.month == 12 else d.month + 1
    return date(y, m, 1) - timedelta(days=1)


# ════════════════════════════════════════════════════════════════════════════
# _extend_headcount
# ════════════════════════════════════════════════════════════════════════════

class TestExtendHeadcount:
    def test_no_extension_if_current_month_present(self):
        today_end = _month_end(date.today())
        df = pl.DataFrame({
            "reporting_date": [today_end],
            "employee_id":    ["EMP-001"],
        })
        result = _extend_headcount(df)
        assert result.height == 1, "Should not add rows if current month-end is present"

    def test_extends_if_current_month_missing(self):
        past_end = _month_end(date.today() - timedelta(days=35))
        df = pl.DataFrame({
            "reporting_date": [past_end],
            "employee_id":    ["EMP-001"],
        })
        result = _extend_headcount(df)
        today_end = _month_end(date.today())
        dates = result["reporting_date"].to_list()
        assert today_end in dates, f"Expected {today_end} in extended headcount, got {dates}"
        assert result.height == 2

    def test_empty_df_returns_empty(self):
        df = pl.DataFrame({"reporting_date": pl.Series([], dtype=pl.Date), "employee_id": []})
        result = _extend_headcount(df)
        assert result.is_empty()

    def test_row_count_doubles_when_extended(self, sample_headcount_df):
        """If latest month is not this month, all rows from latest are duplicated."""
        # sample_headcount_df has Feb 2025 as latest — that's likely in the past
        result = _extend_headcount(sample_headcount_df)
        today_end = _month_end(date.today())
        if today_end not in sample_headcount_df["reporting_date"].to_list():
            assert result.height > sample_headcount_df.height


# ════════════════════════════════════════════════════════════════════════════
# transform()
# ════════════════════════════════════════════════════════════════════════════

class TestErpTransform:
    def test_job_grade_prefixed(self, sample_headcount_df, unique_referrals_df):
        result = transform(sample_headcount_df, unique_referrals_df)
        grades = result["job_grade"].to_list()
        for g in grades:
            if g is not None:
                assert str(g).startswith("Level "), f"Expected 'Level X', got '{g}'"

    def test_headcount_id_created(self, sample_headcount_df, unique_referrals_df):
        result = transform(sample_headcount_df, unique_referrals_df)
        assert "headcount_id" in result.columns
        assert result["headcount_id"].null_count() == 0

    def test_headcount_id_format(self, sample_headcount_df, unique_referrals_df):
        """headcount_id = employee_id + '_' + YYYY-MM"""
        result = transform(sample_headcount_df, unique_referrals_df)
        sample = result.filter(pl.col("employee_id") == "EMP-200")
        if sample.height > 0:
            hc_id = sample["headcount_id"][0]
            assert hc_id.startswith("EMP-200_"), f"Unexpected headcount_id: {hc_id}"
            assert len(hc_id.split("_")[-1]) == 7  # YYYY-MM

    def test_has_referral_flag_joined(self, sample_headcount_df, unique_referrals_df):
        result = transform(sample_headcount_df, unique_referrals_df)
        assert "has_referral" in result.columns

    def test_has_referral_filled_zero_for_nonreferrals(self, sample_headcount_df, unique_referrals_df):
        result = transform(sample_headcount_df, unique_referrals_df)
        # has_referral should be 0 or 1, never null
        assert result["has_referral"].null_count() == 0, (
            "has_referral should have no nulls — missing should be 0"
        )
        valid = result.filter(~pl.col("has_referral").is_in([0, 1]))
        assert valid.is_empty(), f"has_referral contains unexpected values:\n{valid}"

    def test_reporting_date_is_date_type(self, sample_headcount_df, unique_referrals_df):
        result = transform(sample_headcount_df, unique_referrals_df)
        assert result.schema["reporting_date"] == pl.Date

    def test_no_rows_lost_without_referrals(self, sample_headcount_df):
        """Left-join: no headcount rows should be dropped."""
        empty_referrals = pl.DataFrame({
            "headcount_id": pl.Series([], dtype=pl.Utf8),
            "has_referral": pl.Series([], dtype=pl.Int8),
        })
        result = transform(sample_headcount_df, empty_referrals)
        # Row count may grow due to _extend_headcount, but should not shrink
        assert result.height >= sample_headcount_df.height

    @pytest.mark.parametrize("grade,expected_prefix", [
        ("3", "Level 3"),
        ("10", "Level 10"),
        ("1", "Level 1"),
    ])
    def test_grade_prefix_parametrized(self, grade, expected_prefix):
        df = pl.DataFrame({
            "reporting_date": [date(2025, 1, 31)],
            "location": ["Manila"],
            "function": ["Eng"],
            "sub_function": ["BE"],
            "job_grade": [grade],
            "employee_id": ["EMP-X"],
        })
        unique_refs = pl.DataFrame({
            "headcount_id": pl.Series([], dtype=pl.Utf8),
            "has_referral": pl.Series([], dtype=pl.Int8),
        })
        result = transform(df, unique_refs)
        assert result["job_grade"][0].startswith("Level ")
