"""
tests/test_data_quality.py  –  Data quality checks on pipeline outputs.

These tests validate business rules and referential integrity that go beyond
pure functional correctness.  They are designed to catch regressions in the
ETL logic that produce technically valid but semantically wrong data.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.jobreq import OUTPUT_SCHEMA as JOBREQ_OUTPUT_SCHEMA, transform as jobreq_transform
from src.referral import REFERRAL_SCHEMA
from src._utils import ensure_schema


# ════════════════════════════════════════════════════════════════════════════
# Helpers / reusable validators
# ════════════════════════════════════════════════════════════════════════════

def assert_no_nulls(df: pl.DataFrame, columns: list[str], label: str = "") -> None:
    """Fail if any of *columns* contain null values."""
    prefix = f"[{label}] " if label else ""
    for col in columns:
        if col not in df.columns:
            continue
        n = df[col].null_count()
        assert n == 0, (
            f"{prefix}Column '{col}' has {n} null values (expected 0).\n"
            f"Sample nulls:\n{df.filter(pl.col(col).is_null()).head(5)}"
        )


def assert_values_in_set(df: pl.DataFrame, col: str, valid: set, label: str = "") -> None:
    prefix = f"[{label}] " if label else ""
    invalid = df.filter(~pl.col(col).is_in(list(valid)))
    assert invalid.is_empty(), (
        f"{prefix}Column '{col}' contains unexpected values:\n"
        f"{invalid.select(col).head(10)}"
    )


def assert_no_duplicates(df: pl.DataFrame, key: str | list[str], label: str = "") -> None:
    prefix = f"[{label}] " if label else ""
    keys = [key] if isinstance(key, str) else key
    dups = df.filter(pl.struct(keys).is_duplicated())
    assert dups.is_empty(), (
        f"{prefix}Duplicate key {keys} found:\n{dups.select(keys).head(10)}"
    )


# ════════════════════════════════════════════════════════════════════════════
# DQ — Jobreq output
# ════════════════════════════════════════════════════════════════════════════

class TestJobreqDataQuality:

    @pytest.fixture
    def jobreq_df(self, jobreq_input_df):
        return jobreq_transform(jobreq_input_df.lazy())

    def test_primary_key_unique(self, jobreq_df):
        assert_no_duplicates(jobreq_df, "job_requisition_id", "jobreq")

    def test_required_fields_not_null(self, jobreq_df):
        assert_no_nulls(jobreq_df, ["job_requisition_id", "target_hire_date", "jobreq_status"], "jobreq")

    def test_status_domain(self, jobreq_df):
        assert_values_in_set(jobreq_df, "jobreq_status", {"OPEN", "FILLED", "CAN"}, "jobreq")

    def test_positions_requested_non_negative(self, jobreq_df):
        neg = jobreq_df.filter(pl.col("positions_requested_new") < 0)
        assert neg.is_empty(), (
            f"Negative positions_requested_new:\n{neg.select('job_requisition_id', 'positions_requested_new')}"
        )

    def test_schema_types(self, jobreq_df):
        for col, dtype in JOBREQ_OUTPUT_SCHEMA.items():
            assert jobreq_df.schema.get(col) == dtype, (
                f"Column {col}: expected {dtype}, got {jobreq_df.schema.get(col)}"
            )

    def test_can_status_implies_zero_requested(self, jobreq_df):
        can_rows = jobreq_df.filter(pl.col("jobreq_status") == "CAN")
        if can_rows.height > 0:
            assert (can_rows["positions_requested_new"] == 0).all(), (
                "CAN status rows must have positions_requested_new == 0"
            )

    def test_open_status_implies_positive_openings(self, jobreq_df):
        open_rows = jobreq_df.filter(pl.col("jobreq_status") == "OPEN")
        if open_rows.height > 0:
            assert (open_rows["positions_openings_available"] > 0).all(), (
                "OPEN status rows must have positions_openings_available > 0"
            )

    def test_filled_implies_zero_openings(self, jobreq_df):
        filled = jobreq_df.filter(pl.col("jobreq_status") == "FILLED")
        if filled.height > 0:
            assert (filled["positions_openings_available"] == 0).all(), (
                "FILLED rows must have positions_openings_available == 0"
            )


# ════════════════════════════════════════════════════════════════════════════
# DQ — Application output
# ════════════════════════════════════════════════════════════════════════════

class TestApplicationDataQuality:

    @pytest.fixture
    def apps_result(self, sample_apps_df):
        from src.app import transform
        raw = {"app_lf": sample_apps_df.lazy(), "app_cutoff": date(2020, 1, 1), "run_date": date(2025, 6, 1)}
        return transform(raw, {})

    def test_no_level9_in_output(self, apps_result):
        df = apps_result["apps"].collect()
        from src.app import VALID_GRADES
        if "compensation_grade" in df.columns:
            invalid = df.filter(~pl.col("compensation_grade").is_in(VALID_GRADES))
            assert invalid.is_empty(), (
                f"Invalid grade in applications:\n{invalid.select('candidate_id', 'compensation_grade').head(10)}"
            )

    def test_last_stage_number_in_range(self, apps_result):
        df = apps_result["apps"].collect()
        if "last_stage_number" in df.columns:
            non_null = df.filter(pl.col("last_stage_number").is_not_null())
            out_of_range = non_null.filter(
                (pl.col("last_stage_number") < 1) | (pl.col("last_stage_number") > 8)
            )
            assert out_of_range.is_empty(), (
                f"last_stage_number out of range [1,8]:\n"
                f"{out_of_range.select('candidate_id', 'last_stage_number').head(10)}"
            )

    def test_offer_accepts_all_have_status(self, apps_result):
        oa = apps_result["offer_accepts"].collect()
        assert (oa["candidate_recruiting_status"] == "Offer Accepted").all()

    def test_funnel_stages_valid(self, apps_result):
        from src.app import FUNNEL_VALID_STAGES
        funnel = apps_result["funnel"].collect()
        if funnel.height > 0 and "stage_number" in funnel.columns:
            invalid = funnel.filter(~pl.col("stage_number").is_in(FUNNEL_VALID_STAGES))
            assert invalid.is_empty(), (
                f"Funnel has invalid stage numbers:\n{invalid['stage_number'].to_list()}"
            )

    def test_job_application_date_not_null(self, apps_result):
        df = apps_result["apps"].collect()
        if "job_application_date" in df.columns:
            assert_no_nulls(df, ["job_application_date"], "applications")


# ════════════════════════════════════════════════════════════════════════════
# DQ — Referral output
# ════════════════════════════════════════════════════════════════════════════

class TestReferralDataQuality:

    @pytest.fixture
    def referral_result(self, sample_prospect_df, sample_funnel_df):
        from src.referral import transform
        return transform(sample_prospect_df, sample_funnel_df)

    def test_erp_id_no_nulls(self, referral_result):
        referrals_df, _ = referral_result
        assert_no_nulls(referrals_df, ["erp_id"], "referrals")

    def test_erp_id_unique(self, referral_result):
        referrals_df, _ = referral_result
        assert_no_duplicates(referrals_df, "erp_id", "referrals")

    def test_has_referral_binary(self, referral_result):
        _, unique_df = referral_result
        assert_values_in_set(unique_df, "has_referral", {0, 1}, "unique_referrals")

    def test_headcount_id_unique_in_unique_df(self, referral_result):
        _, unique_df = referral_result
        assert_no_duplicates(unique_df, "headcount_id", "unique_referrals")

    def test_stage_numbers_non_negative(self, referral_result):
        referrals_df, _ = referral_result
        if "last_stage_number" in referrals_df.columns:
            neg = referrals_df.filter(pl.col("last_stage_number") < 0)
            assert neg.is_empty(), "last_stage_number must be >= 0"

    def test_added_date_not_in_future(self, referral_result):
        referrals_df, _ = referral_result
        today = date.today()
        if "added_date" in referrals_df.columns:
            future = referrals_df.filter(pl.col("added_date") > pl.lit(today))
            assert future.is_empty(), (
                f"{future.height} referral records have added_date in the future"
            )


# ════════════════════════════════════════════════════════════════════════════
# DQ — ERP / Headcount output
# ════════════════════════════════════════════════════════════════════════════

class TestErpDataQuality:

    @pytest.fixture
    def erp_result(self, sample_headcount_df, unique_referrals_df):
        from src.erp import transform
        return transform(sample_headcount_df, unique_referrals_df)

    def test_has_referral_no_nulls(self, erp_result):
        assert_no_nulls(erp_result, ["has_referral"], "headcount")

    def test_has_referral_binary(self, erp_result):
        assert_values_in_set(erp_result, "has_referral", {0, 1}, "headcount")

    def test_employee_id_not_null(self, erp_result):
        assert_no_nulls(erp_result, ["employee_id"], "headcount")

    def test_reporting_date_not_null(self, erp_result):
        assert_no_nulls(erp_result, ["reporting_date"], "headcount")

    def test_headcount_id_format(self, erp_result):
        """headcount_id must match pattern: {employee_id}_{YYYY-MM}."""
        import re
        pattern = re.compile(r"^.+_\d{4}-\d{2}$")
        for hc_id in erp_result["headcount_id"].to_list():
            assert pattern.match(str(hc_id)), (
                f"headcount_id '{hc_id}' doesn't match expected format"
            )

    def test_job_grade_format(self, erp_result):
        for grade in erp_result["job_grade"].to_list():
            assert str(grade).startswith("Level "), f"Unexpected job_grade format: {grade}"


# ════════════════════════════════════════════════════════════════════════════
# DQ — ensure_schema utility
# ════════════════════════════════════════════════════════════════════════════

class TestEnsureSchemaQuality:
    def test_cast_failure_becomes_null_not_error(self):
        """Casting 'abc' to Int32 with strict=False should yield null, not raise."""
        schema = {"val": pl.Int32}
        df = pl.DataFrame({"val": ["abc"]})
        result = ensure_schema(df, schema)
        assert result["val"][0] is None

    def test_large_schema_all_columns_present(self):
        from src.hire import HIRES_SCHEMA
        df = pl.DataFrame({"jrpos_id": ["X"]})
        result = ensure_schema(df, HIRES_SCHEMA, "test")
        assert set(result.columns) == set(HIRES_SCHEMA.keys())
