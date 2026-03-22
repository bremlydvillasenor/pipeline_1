"""
tests/test_unit_referral.py  –  Unit tests for src/referral.py
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.referral import (
    PROSPECT_STAGE_NUMBER,
    REFERRAL_SCHEMA,
    REVIEW_STAGE_NUMBER,
    _apply_common_transforms,
    _parse_date_col,
    transform,
)


# ════════════════════════════════════════════════════════════════════════════
# _parse_date_col
# ════════════════════════════════════════════════════════════════════════════

class TestParseDateCol:
    @pytest.mark.parametrize("value,expected", [
        ("2025-06-01",  date(2025, 6, 1)),
        ("2025/06/01",  date(2025, 6, 1)),
        ("06/01/2025",  date(2025, 6, 1)),
    ])
    def test_formats(self, value, expected):
        df = pl.DataFrame({"d": [value]})
        result = _parse_date_col(df, "d")["d"][0]
        assert result == expected

    def test_null_input(self):
        df = pl.DataFrame({"d": pl.Series([None], dtype=pl.Utf8)})
        result = _parse_date_col(df, "d")["d"][0]
        assert result is None


# ════════════════════════════════════════════════════════════════════════════
# _apply_common_transforms
# ════════════════════════════════════════════════════════════════════════════

class TestApplyCommonTransforms:
    def test_referred_by_strips_employee_id(self, sample_prospect_df: pl.DataFrame):
        result = _apply_common_transforms(sample_prospect_df)
        # "Bob Jones (100)" → "Bob Jones"
        names = result["referred_by"].to_list()
        for name in names:
            assert "(" not in str(name or ""), f"Employee ID not stripped from: {name}"

    def test_schema_columns_present(self, sample_prospect_df: pl.DataFrame):
        result = _apply_common_transforms(sample_prospect_df)
        for col in REFERRAL_SCHEMA:
            assert col in result.columns, f"Missing schema column: {col}"

    def test_null_safe_key_columns(self, sample_prospect_df: pl.DataFrame):
        result = _apply_common_transforms(sample_prospect_df)
        # After transform, key columns should never be null (filled with "")
        assert result["job_requisition_id"].null_count() == 0
        assert result["referred_by_employee_id"].null_count() == 0
        assert result["candidate_id"].null_count() == 0

    def test_dtypes_match_schema(self, sample_prospect_df: pl.DataFrame):
        result = _apply_common_transforms(sample_prospect_df)
        for col, dtype in REFERRAL_SCHEMA.items():
            assert result.schema[col] == dtype, (
                f"{col}: expected {dtype}, got {result.schema[col]}"
            )


# ════════════════════════════════════════════════════════════════════════════
# transform()
# ════════════════════════════════════════════════════════════════════════════

class TestReferralTransform:
    def test_returns_two_dataframes(self, sample_prospect_df, sample_funnel_df):
        result = transform(sample_prospect_df, sample_funnel_df)
        assert isinstance(result, tuple) and len(result) == 2

    def test_erp_id_created(self, sample_prospect_df, sample_funnel_df):
        referrals_df, _ = transform(sample_prospect_df, sample_funnel_df)
        assert "erp_id" in referrals_df.columns

    def test_erp_id_unique(self, sample_prospect_df, sample_funnel_df):
        referrals_df, _ = transform(sample_prospect_df, sample_funnel_df)
        assert referrals_df["erp_id"].n_unique() == referrals_df.height, (
            "erp_id should be unique after deduplication"
        )

    def test_headcount_id_format(self, sample_prospect_df, sample_funnel_df):
        """headcount_id = referred_by_employee_id + '_' + YYYY-MM"""
        referrals_df, _ = transform(sample_prospect_df, sample_funnel_df)
        assert "headcount_id" in referrals_df.columns
        # Spot check: EMP-100 + 2025-01 → EMP-100_2025-01
        sample = referrals_df.filter(pl.col("referred_by_employee_id") == "EMP-100")
        if sample.height > 0:
            assert "EMP-100_" in sample["headcount_id"][0]

    def test_unique_referrals_has_has_referral(self, sample_prospect_df, sample_funnel_df):
        _, unique_df = transform(sample_prospect_df, sample_funnel_df)
        assert "has_referral" in unique_df.columns
        assert all(v == 1 for v in unique_df["has_referral"].to_list())

    def test_unique_referrals_unique_headcount_id(self, sample_prospect_df, sample_funnel_df):
        _, unique_df = transform(sample_prospect_df, sample_funnel_df)
        assert unique_df["headcount_id"].n_unique() == unique_df.height

    def test_prospect_stage_0_present(self, sample_prospect_df, sample_funnel_df):
        """Prospect rows should have last_stage_number == 0 in the output."""
        referrals_df, _ = transform(sample_prospect_df, sample_funnel_df)
        prospect_rows = referrals_df.filter(pl.col("last_stage_number") == PROSPECT_STAGE_NUMBER)
        assert prospect_rows.height > 0, "No prospect (stage 0) rows in output"

    def test_empty_prospect_handled(self, sample_funnel_df):
        """Empty prospect DF should not crash transform."""
        empty_prospect = pl.DataFrame(schema={k: v for k, v in REFERRAL_SCHEMA.items()})
        result, unique = transform(empty_prospect, sample_funnel_df)
        assert isinstance(result, pl.DataFrame)
