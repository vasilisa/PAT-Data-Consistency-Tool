"""
tests/test_orchestrator_rollup.py
===================================
Tests for the runner orchestrator:
  - _derive_overall_status precedence rules
  - _build_section* builders produce correctly typed SectionResult
  - run_all_checks returns valid RunResult structure (using mock loader)
"""
from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from dash_app.engine.contracts import RunResult, SectionResult, Status
from dash_app.runner.orchestrator import (
    _build_section1,
    _build_section3,
    _build_section5,
    _derive_overall_status,
    run_all_checks,
)


# ─────────────────────────────────────────────────────────────────────────────
# _derive_overall_status precedence
# ─────────────────────────────────────────────────────────────────────────────

class TestDeriveOverallStatus:

    def test_fail_beats_all(self):
        assert _derive_overall_status([Status.PASS, Status.WARNING, Status.FAIL]) == Status.FAIL

    def test_warning_beats_pass_and_skip(self):
        assert _derive_overall_status([Status.PASS, Status.SKIP, Status.WARNING]) == Status.WARNING

    def test_pass_beats_skip(self):
        assert _derive_overall_status([Status.PASS, Status.SKIP]) == Status.PASS

    def test_all_skip(self):
        assert _derive_overall_status([Status.SKIP, Status.SKIP]) == Status.SKIP

    def test_empty_list(self):
        assert _derive_overall_status([]) == Status.SKIP


# ─────────────────────────────────────────────────────────────────────────────
# Section builder output shapes
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildSection1:

    def test_returns_section_result(self):
        result = _build_section1({"tbl_Test": {"status": "PASS", "missing": []}})
        assert isinstance(result, SectionResult)
        assert result.section_id == "check1"

    def test_fail_propagates(self):
        reg = {"tbl_Test": {"status": "FAIL", "missing": [{"key_col": "Key_Policy", "missing_from": "tbl_DetailedData"}]}}
        result = _build_section1(reg)
        assert result.status == Status.FAIL

    def test_pass_when_all_pass(self):
        reg = {"tbl_A": {"status": "PASS", "missing": []}, "tbl_B": {"status": "PASS", "missing": []}}
        result = _build_section1(reg)
        assert result.status == Status.PASS
        assert len(result.checks) == 2

    def test_empty_dict_returns_skip(self):
        result = _build_section1({})
        assert result.status == Status.SKIP


class TestBuildSection3:

    def test_pass_mapping(self):
        result = _build_section3({"status": "PASS", "total_duplicates": 0})
        assert isinstance(result, SectionResult)
        assert result.status == Status.PASS

    def test_fail_mapping(self):
        dup_df = pd.DataFrame({"State": ["NSW", "NSW"]})
        result = _build_section3({"status": "FAIL", "total_duplicates": 1, "duplicates": dup_df})
        assert result.status == Status.FAIL

    def test_skip_mapping(self):
        result = _build_section3({"status": "SKIP", "reason": "tbl_Key_Mapping not loaded"})
        assert result.status == Status.SKIP


class TestBuildSection5:

    def test_pass_when_all_mapped(self):
        km = {"Key_Modelling": {"status": "PASS", "unmapped_count": 0, "unmapped_premium": 0.0, "premium_pct": 0.0}}
        result = _build_section5(km)
        assert isinstance(result, SectionResult)
        assert result.status == Status.PASS

    def test_warning_when_unmapped_present(self):
        km = {"Key_Modelling": {"status": "WARNING", "unmapped_count": 5, "unmapped_premium": 500.0, "premium_pct": 10.0}}
        result = _build_section5(km)
        assert result.status == Status.WARNING

    def test_skip_when_empty(self):
        result = _build_section5({})
        assert result.status == Status.SKIP


# ─────────────────────────────────────────────────────────────────────────────
# run_all_checks end-to-end with mocked loader
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_datasets():
    dd = pd.DataFrame({
        "Key_Policy":    ["P1", "P2"],
        "State":         ["NSW", "VIC"],
        "Premium":       [100.0, 200.0],
        "AsAt_Month":    [202501, 202501],
        "Key_Modelling": ["M1", "M2"],
    })
    mapping = pd.DataFrame({
        "Key_Policy": ["P1", "P2"],
        "State":      ["NSW", "VIC"],
    })
    rate = pd.DataFrame({
        "Key_Policy": ["P1", "P2"],
        "State":      ["NSW", "VIC"],
        "RateChange": [0.02, -0.01],
    })
    return {"tbl_DetailedData": dd, "tbl_Key_Mapping": mapping, "tbl_RateChange": rate}


class TestRunAllChecks:

    def test_returns_run_result(self, minimal_datasets):
        with (
            patch("dash_app.runner.orchestrator.load_tbl_datasets", return_value=minimal_datasets),
            patch("dash_app.runner.orchestrator.classify_tables") as mock_classify,
        ):
            dd = minimal_datasets["tbl_DetailedData"]
            mapping = minimal_datasets["tbl_Key_Mapping"]
            ref_tables = {"tbl_RateChange": minimal_datasets["tbl_RateChange"]}
            mock_classify.return_value = (dd, mapping, ref_tables)

            result = run_all_checks()

        assert isinstance(result, RunResult)

    def test_result_has_five_sections(self, minimal_datasets):
        with (
            patch("dash_app.runner.orchestrator.load_tbl_datasets", return_value=minimal_datasets),
            patch("dash_app.runner.orchestrator.classify_tables") as mock_classify,
        ):
            dd = minimal_datasets["tbl_DetailedData"]
            mapping = minimal_datasets["tbl_Key_Mapping"]
            ref_tables = {"tbl_RateChange": minimal_datasets["tbl_RateChange"]}
            mock_classify.return_value = (dd, mapping, ref_tables)

            result = run_all_checks()

        assert len(result.sections) == 5

    def test_status_is_valid_enum(self, minimal_datasets):
        with (
            patch("dash_app.runner.orchestrator.load_tbl_datasets", return_value=minimal_datasets),
            patch("dash_app.runner.orchestrator.classify_tables") as mock_classify,
        ):
            dd = minimal_datasets["tbl_DetailedData"]
            mapping = minimal_datasets["tbl_Key_Mapping"]
            ref_tables = {"tbl_RateChange": minimal_datasets["tbl_RateChange"]}
            mock_classify.return_value = (dd, mapping, ref_tables)

            result = run_all_checks()

        assert result.status in (Status.PASS, Status.WARNING, Status.FAIL, Status.SKIP)

    def test_metadata_populated(self, minimal_datasets):
        with (
            patch("dash_app.runner.orchestrator.load_tbl_datasets", return_value=minimal_datasets),
            patch("dash_app.runner.orchestrator.classify_tables") as mock_classify,
        ):
            dd = minimal_datasets["tbl_DetailedData"]
            mapping = minimal_datasets["tbl_Key_Mapping"]
            ref_tables = {"tbl_RateChange": minimal_datasets["tbl_RateChange"]}
            mock_classify.return_value = (dd, mapping, ref_tables)

            result = run_all_checks()

        assert result.metadata.runtime_ms >= 0
        assert result.metadata.started_at_utc
        assert result.metadata.completed_at_utc

    def test_fatal_loader_error_returns_fail(self):
        with patch(
            "dash_app.runner.orchestrator.load_tbl_datasets",
            side_effect=RuntimeError("dataset not found"),
        ):
            result = run_all_checks()

        assert result.status == Status.FAIL
        assert any("Fatal setup error" in e for e in result.errors)
