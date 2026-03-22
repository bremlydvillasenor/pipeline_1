"""
tests/test_unit_app.py  –  Unit tests for src/app.py
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.app import (
    FUNNEL_VALID_STAGES,
    STAGE_MAP,
    VALID_GRADES,
    _apply_rescind_correction,
    _parse_date,
    _transform_funnel,
    transform,
)


# ════════════════════════════════════════════════════════════════════════════
# _parse_date
# ════════════════════════════════════════════════════════════════════════════

class TestParseDate:
    def _apply(self, col_name: str, values: list) -> pl.Series:
        df = pl.DataFrame({col_name: values})
        return df.with_columns(_parse_date(col_name))[col_name]

    def test_iso_format(self):
        result = self._apply("d", ["2025-06-01"])
        assert result[0] == date(2025, 6, 1)

    def test_slash_format(self):
        result = self._apply("d", ["2025/06/01"])
        assert result[0] == date(2025, 6, 1)

    def test_us_format(self):
        result = self._apply("d", ["06/01/2025"])
        assert result[0] == date(2025, 6, 1)

    def test_null_string_returns_null(self):
        result = self._apply("d", pl.Series([None], dtype=pl.Utf8))
        assert result[0] is None

    def test_invalid_string_returns_null(self):
        result = self._apply("d", ["not-a-date"])
        assert result[0] is None


# ════════════════════════════════════════════════════════════════════════════
# _transform_funnel
# ════════════════════════════════════════════════════════════════════════════

class TestTransformFunnel:

    def _make_df(self, stage: int, status: str = "Application in Process",
                 disposition: str = None, candidate_id: str = "C-001") -> pl.DataFrame:
        return pl.DataFrame({
            "candidate_id":                [candidate_id],
            "added_date":                  [date(2025, 1, 1)],
            "job_application_date":        [date(2025, 1, 1)],
            "job_requisition_id":          ["JR-001"],
            "recruiting_agency":           [None],
            "source":                      ["Direct"],
            "disposition_reason":          [disposition],
            "candidate_recruiting_status": [status],
            "last_stage_number":           pl.Series([stage], dtype=pl.Int16),
        })

    def test_in_process_explodes_stages_before_current(self):
        """In-process candidate at stage 4 → stages 1,2,3 only (not 4)."""
        df = self._make_df(stage=4, status="Application in Process")
        result = _transform_funnel(df, {})
        stage_nums = sorted(result["stage_number"].to_list())
        assert 4 not in stage_nums, "In-process: current stage should NOT appear"
        assert 1 in stage_nums and 2 in stage_nums and 3 in stage_nums

    def test_completed_explodes_up_to_and_including_stage(self):
        """Completed candidate at stage 4 → stages 1,2,3,4."""
        df = self._make_df(stage=4, status="Rejected", disposition="Not a Fit")
        result = _transform_funnel(df, {})
        stage_nums = sorted(result["stage_number"].to_list())
        assert 4 in stage_nums
        assert all(s in stage_nums for s in [1, 2, 3, 4])

    def test_stage_5_coerced_to_4(self):
        """Stage 5 (Reference Check) → coerced to 4 (Interview)."""
        df = self._make_df(stage=5, status="Rejected")
        result = _transform_funnel(df, {})
        # After coercion last_stage_number=4, so stages ≤ 4 appear
        assert 5 not in result["stage_number"].to_list()

    def test_only_last_stage_keeps_status(self):
        """All rows except the last stage get status='Passed'."""
        df = self._make_df(stage=6, status="Offer Accepted")
        result = _transform_funnel(df, {})
        last_stage = result.filter(pl.col("stage_number") == pl.col("last_stage_number"))
        earlier = result.filter(pl.col("stage_number") < pl.col("last_stage_number"))
        assert all(s == "Passed" for s in earlier["candidate_recruiting_status"].to_list())
        assert last_stage["candidate_recruiting_status"][0] == "Offer Accepted"

    def test_stage_label_added(self):
        df = self._make_df(stage=2, status="Rejected")
        result = _transform_funnel(df, {})
        labels = result["stage"].to_list()
        assert "Review" in labels
        assert "Screen" in labels

    def test_job_application_date_truncated_to_month(self):
        df = self._make_df(stage=2, status="Rejected")
        result = _transform_funnel(df, {})
        # job_application_date should be 2025-01-01 (already month start)
        assert result["job_application_date"][0] == date(2025, 1, 1)

    def test_empty_df_returns_empty(self):
        from src.app import FUNNEL_REQUIRED_COLS
        schema = {c: pl.Utf8 for c in FUNNEL_REQUIRED_COLS}
        schema["last_stage_number"] = pl.Int16
        schema["added_date"] = pl.Date
        schema["job_application_date"] = pl.Date
        df = pl.DataFrame(schema=schema)
        result = _transform_funnel(df, {})
        assert result.is_empty()

    def test_missing_required_col_raises(self):
        df = pl.DataFrame({"candidate_id": ["C-001"]})
        with pytest.raises(ValueError, match="missing required columns"):
            _transform_funnel(df, {})

    @pytest.mark.parametrize("stage", FUNNEL_VALID_STAGES)
    def test_all_valid_stages_produce_rows(self, stage):
        df = self._make_df(stage=stage, status="Rejected")
        result = _transform_funnel(df, {})
        assert result.height > 0, f"No rows produced for stage {stage}"


# ════════════════════════════════════════════════════════════════════════════
# _apply_rescind_correction
# ════════════════════════════════════════════════════════════════════════════

class TestRescindCorrection:
    def _base_lf(self, **overrides) -> pl.LazyFrame:
        row = {
            "hire_transaction_status":         "Rescinded",
            "hired":                           None,
            "last_recruiting_stage_orig":      "Ready for Hire",
            "candidate_recruiting_status_orig": "Offer Accepted",
            "disposition_reason":              None,
            "last_recruiting_stage":           "Ready for Hire",
        }
        row.update(overrides)
        return pl.DataFrame({k: [v] for k, v in row.items()}).lazy()

    def test_rescind_sets_stage_to_offer(self):
        result = _apply_rescind_correction(self._base_lf()).collect()
        assert result["last_recruiting_stage"][0] == "Offer"

    def test_rescind_sets_disposition(self):
        result = _apply_rescind_correction(self._base_lf()).collect()
        assert result["disposition_reason"][0] == "Offer Accepted to Rescinded"

    def test_non_rescind_unchanged(self):
        lf = self._base_lf(
            hire_transaction_status="Completed",
            hired="Yes",
            last_recruiting_stage="Ready for Hire",
        )
        result = _apply_rescind_correction(lf).collect()
        # Should NOT be corrected since it's not a rescind
        assert result["last_recruiting_stage"][0] == "Ready for Hire"

    def test_missing_columns_passes_through(self):
        """If required columns are absent, lf should be returned unchanged."""
        lf = pl.DataFrame({"some_col": ["x"]}).lazy()
        result = _apply_rescind_correction(lf).collect()
        assert "some_col" in result.columns


# ════════════════════════════════════════════════════════════════════════════
# transform() (full pipeline)
# ════════════════════════════════════════════════════════════════════════════

class TestAppTransform:
    def test_level9_excluded(self, sample_apps_df: pl.DataFrame):
        """CAND-003 has Level 9 — should be filtered out."""
        raw = {"app_lf": sample_apps_df.lazy(), "app_cutoff": date(2020, 1, 1), "run_date": date(2025, 6, 1)}
        result = transform(raw, {})
        ids = result["apps"].collect()["candidate_id"].to_list()
        assert "CAND-003" not in ids, "Level 9 candidate should be excluded"

    def test_valid_grades_kept(self, sample_apps_df: pl.DataFrame):
        raw = {"app_lf": sample_apps_df.lazy(), "app_cutoff": date(2020, 1, 1), "run_date": date(2025, 6, 1)}
        result = transform(raw, {})
        grades = result["apps"].collect()["compensation_grade"].to_list()
        for g in grades:
            assert g in VALID_GRADES, f"Unexpected grade: {g}"

    def test_last_stage_number_mapped(self, sample_apps_df: pl.DataFrame):
        raw = {"app_lf": sample_apps_df.lazy(), "app_cutoff": date(2020, 1, 1), "run_date": date(2025, 6, 1)}
        result = transform(raw, {})
        df = result["apps"].collect()
        assert "last_stage_number" in df.columns
        # "Interview" = 4
        alice = df.filter(pl.col("candidate_id") == "CAND-001")
        assert alice["last_stage_number"][0] == STAGE_MAP["Interview"]

    def test_offer_accepts_slice(self, sample_apps_df: pl.DataFrame):
        """Only CAND-002 has status 'Offer Accepted'."""
        raw = {"app_lf": sample_apps_df.lazy(), "app_cutoff": date(2020, 1, 1), "run_date": date(2025, 6, 1)}
        result = transform(raw, {})
        oa = result["offer_accepts"].collect()
        assert oa.height >= 1
        assert all(s == "Offer Accepted" for s in oa["candidate_recruiting_status"].to_list())

    def test_cutoff_filters_old_records(self, sample_apps_df: pl.DataFrame):
        """Records before cutoff should be removed."""
        far_future_cutoff = date(2030, 1, 1)
        raw = {"app_lf": sample_apps_df.lazy(), "app_cutoff": far_future_cutoff, "run_date": date(2025, 6, 1)}
        result = transform(raw, {})
        df = result["apps"].collect()
        assert df.height == 0, "All records should be filtered by far-future cutoff"

    def test_recruiter_id_extracted(self, sample_apps_df: pl.DataFrame):
        """CAND-002's recruiter_completed_offer 'R222 (22222)' → all digits stripped → '22222222'."""
        raw = {"app_lf": sample_apps_df.lazy(), "app_cutoff": date(2020, 1, 1), "run_date": date(2025, 6, 1)}
        result = transform(raw, {})
        df = result["apps"].collect()
        bob = df.filter(pl.col("candidate_id") == "CAND-002")
        if bob.height > 0 and "recruiter_completed_offer_id" in df.columns:
            assert bob["recruiter_completed_offer_id"][0] == "22222222"
