"""
tests/test_integration.py  –  End-to-end pipeline stage tests.

Each stage is tested independently using synthetic input files created by the
conftest fixtures.  Stages run in pipeline order so each test depends only on
what previous fixtures create (no cross-test state).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.jobreq import run_jobreq, OUTPUT_SCHEMA as JOBREQ_OUTPUT_SCHEMA
from src.hire import run_hire, HIRES_SCHEMA
from src.app import run_app
from src.erp import run_erp
from src.referral import run_referral


# ════════════════════════════════════════════════════════════════════════════
# Stage 1 – Jobreq
# ════════════════════════════════════════════════════════════════════════════

class TestJobreqIntegration:
    """Requires: all_status_excel fixture (creates the file in raw dir)."""

    def test_run_returns_df_and_date(self, all_status_excel, pipeline_config):
        result_df, refresh_date = run_jobreq(pipeline_config)
        assert isinstance(result_df, pl.DataFrame)
        assert isinstance(refresh_date, date)

    def test_output_parquet_written(self, all_status_excel, pipeline_config):
        run_jobreq(pipeline_config)
        assert Path(pipeline_config["OUTPUT_JOBREQS_PAR"]).exists()

    def test_refresh_date_csv_written(self, all_status_excel, pipeline_config):
        run_jobreq(pipeline_config)
        csv = Path(pipeline_config["PROCESSED_DATA_ROOT"]) / "refresh_date.csv"
        assert csv.exists()

    def test_output_schema_correct(self, all_status_excel, pipeline_config):
        result_df, _ = run_jobreq(pipeline_config)
        for col, dtype in JOBREQ_OUTPUT_SCHEMA.items():
            assert col in result_df.columns, f"Missing output column: {col}"
            assert result_df.schema[col] == dtype

    def test_old_records_excluded(self, all_status_excel, pipeline_config):
        """JR-OLD (2020) must not appear; scope is 3 years from 2025."""
        result_df, _ = run_jobreq(pipeline_config)
        assert "JR-OLD" not in result_df["job_requisition_id"].to_list()

    def test_dedup_applied(self, all_status_excel, pipeline_config):
        """JR-001 appears twice in source; output should have it once."""
        result_df, _ = run_jobreq(pipeline_config)
        count = result_df.filter(pl.col("job_requisition_id") == "JR-001").height
        assert count == 1

    def test_jobreq_status_values_valid(self, all_status_excel, pipeline_config):
        result_df, _ = run_jobreq(pipeline_config)
        invalid = result_df.filter(~pl.col("jobreq_status").is_in(["OPEN", "FILLED", "CAN"]))
        assert invalid.is_empty(), (
            f"Unexpected jobreq_status values found:\n"
            f"{invalid.select('job_requisition_id', 'jobreq_status')}"
        )

    def test_refresh_date_extracted_from_filename(self, all_status_excel, pipeline_config):
        """Filename contains 2025-06-01 — refresh_date should match."""
        _, refresh_date = run_jobreq(pipeline_config)
        assert refresh_date == date(2025, 6, 1)


# ════════════════════════════════════════════════════════════════════════════
# Stage 2 – Hire
# ════════════════════════════════════════════════════════════════════════════

class TestHireIntegration:
    """Requires: fulfillment_excel, intx_excel, hiresdb_parquet, jobreqs_parquet."""

    @pytest.fixture(autouse=True)
    def _setup(self, fulfillment_excel, intx_excel, hiresdb_parquet, jobreqs_parquet):
        pass

    def test_run_returns_df(self, pipeline_config):
        result = run_hire(pipeline_config)
        assert isinstance(result, pl.DataFrame)

    def test_hiresdb_written(self, pipeline_config):
        run_hire(pipeline_config)
        assert Path(pipeline_config["OUTPUT_HIRESDB_PAR"]).exists()

    def test_hires_written(self, pipeline_config):
        run_hire(pipeline_config)
        assert Path(pipeline_config["OUTPUT_HIRES_PAR"]).exists()

    def test_level9_excluded_from_output(self, pipeline_config):
        """fulfillment_excel includes a Level 9 row — must not be in merged output."""
        result = run_hire(pipeline_config)
        # The Level 9 row (EMP-202) should not appear in the new incoming set
        # (master had 2 rows; after merge it should NOT grow by the Level 9 hire)
        # We check that none of the new rows have grade Level 9
        grades_with_level9 = (
            result
            .filter(pl.col("employee_id") == "EMP-202")
        )
        # EMP-202 is Level 9 in fulfillment — it should have been filtered before merge
        assert grades_with_level9.height == 0, (
            "Level 9 employee (EMP-202) should not be in the hires output"
        )

    def test_output_has_master_schema_columns(self, pipeline_config):
        result = run_hire(pipeline_config)
        for col in HIRES_SCHEMA:
            assert col in result.columns, f"Missing column in hire output: {col}"

    def test_master_rows_preserved(self, pipeline_config, master_hires_df):
        """Original master rows must survive the upsert."""
        result = run_hire(pipeline_config)
        original_ids = master_hires_df["jrpos_id"].to_list()
        output_ids = result["jrpos_id"].to_list()
        for jrpos_id in original_ids:
            assert jrpos_id in output_ids, (
                f"Master record {jrpos_id} lost after upsert merge"
            )


# ════════════════════════════════════════════════════════════════════════════
# Stage 3 – App
# ════════════════════════════════════════════════════════════════════════════

class TestAppIntegration:
    """Requires: daily_app_csv."""

    def test_run_returns_dict(self, daily_app_csv, pipeline_config):
        result = run_app(pipeline_config)
        assert isinstance(result, dict)
        assert "apps" in result and "offer_accepts" in result and "funnel" in result

    def test_applications_parquet_written(self, daily_app_csv, pipeline_config):
        run_app(pipeline_config)
        assert Path(pipeline_config["OUTPUT_APPS_PAR"]).exists()

    def test_applications_csv_written(self, daily_app_csv, pipeline_config):
        run_app(pipeline_config)
        assert Path(pipeline_config["OUTPUT_APPS_CSV"]).exists()

    def test_funnel_parquet_written(self, daily_app_csv, pipeline_config):
        run_app(pipeline_config)
        assert Path(pipeline_config["OUTPUT_FUNNEL_PAR"]).exists()

    def test_offer_accepts_parquet_written(self, daily_app_csv, pipeline_config):
        run_app(pipeline_config)
        assert Path(pipeline_config["OUTPUT_APPS_OFFER_ACCEPTS_PAR"]).exists()

    def test_level9_excluded_from_output(self, daily_app_csv, pipeline_config):
        """CAND-005 in the CSV is Level 9 — must not appear in output."""
        run_app(pipeline_config)
        apps = pl.read_parquet(pipeline_config["OUTPUT_APPS_PAR"])
        assert "CAND-005" not in apps["candidate_id"].to_list(), (
            "Level 9 candidate (CAND-005) should not appear in applications output"
        )

    def test_funnel_stages_subset_of_valid(self, daily_app_csv, pipeline_config):
        """All stage_number values in funnel must be in FUNNEL_VALID_STAGES."""
        from src.app import FUNNEL_VALID_STAGES
        run_app(pipeline_config)
        funnel = pl.read_parquet(pipeline_config["OUTPUT_FUNNEL_PAR"])
        if funnel.height > 0 and "stage_number" in funnel.columns:
            invalid = funnel.filter(~pl.col("stage_number").is_in(FUNNEL_VALID_STAGES))
            assert invalid.is_empty(), (
                f"Invalid stage_number values in funnel:\n{invalid['stage_number'].to_list()}"
            )


# ════════════════════════════════════════════════════════════════════════════
# Stage 4 – Referral
# ════════════════════════════════════════════════════════════════════════════

class TestReferralIntegration:
    """Requires: prospect_excel, funnel_parquet."""

    def test_run_returns_two_dfs(self, prospect_excel, funnel_parquet, pipeline_config):
        referrals_df, unique_df = run_referral(pipeline_config)
        assert isinstance(referrals_df, pl.DataFrame)
        assert isinstance(unique_df, pl.DataFrame)

    def test_referrals_parquet_written(self, prospect_excel, funnel_parquet, pipeline_config):
        run_referral(pipeline_config)
        assert Path(pipeline_config["OUTPUT_REFERRALS_PARQUET"]).exists()

    def test_has_referral_in_unique_df(self, prospect_excel, funnel_parquet, pipeline_config):
        _, unique_df = run_referral(pipeline_config)
        assert "has_referral" in unique_df.columns
        assert all(v == 1 for v in unique_df["has_referral"].to_list())

    def test_no_null_erp_ids(self, prospect_excel, funnel_parquet, pipeline_config):
        referrals_df, _ = run_referral(pipeline_config)
        assert referrals_df["erp_id"].null_count() == 0

    def test_erp_id_unique(self, prospect_excel, funnel_parquet, pipeline_config):
        referrals_df, _ = run_referral(pipeline_config)
        assert referrals_df["erp_id"].n_unique() == referrals_df.height


# ════════════════════════════════════════════════════════════════════════════
# Stage 5 – ERP
# ════════════════════════════════════════════════════════════════════════════

class TestErpIntegration:
    """Requires: headcount_excel, unique_referrals_df."""

    def test_run_returns_df(self, headcount_excel, unique_referrals_df, pipeline_config):
        result = run_erp(pipeline_config, unique_referrals_df)
        assert isinstance(result, pl.DataFrame)

    def test_headcount_parquet_written(self, headcount_excel, unique_referrals_df, pipeline_config):
        run_erp(pipeline_config, unique_referrals_df)
        assert Path(pipeline_config["OUTPUT_MHEADCOUNT_PARQUET"]).exists()

    def test_has_referral_no_nulls(self, headcount_excel, unique_referrals_df, pipeline_config):
        result = run_erp(pipeline_config, unique_referrals_df)
        assert result["has_referral"].null_count() == 0

    def test_job_grade_prefixed(self, headcount_excel, unique_referrals_df, pipeline_config):
        result = run_erp(pipeline_config, unique_referrals_df)
        for grade in result["job_grade"].to_list():
            assert str(grade).startswith("Level "), f"Bad grade: {grade}"

    def test_headcount_id_no_nulls(self, headcount_excel, unique_referrals_df, pipeline_config):
        result = run_erp(pipeline_config, unique_referrals_df)
        assert result["headcount_id"].null_count() == 0
