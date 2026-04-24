"""
tests/test_contracts.py
=======================
Tests for the payload schema (contracts.py):
  - to_payload serialization round-trip
  - empty_run_result shape
  - Status enum values
"""
from __future__ import annotations

import pytest

from dash_app.engine.contracts import (
    CheckResult,
    RunMetadata,
    RunResult,
    SectionResult,
    Status,
    empty_run_result,
    to_payload,
)


class TestStatusEnum:

    def test_all_four_values_exist(self):
        assert Status.PASS.value == "PASS"
        assert Status.FAIL.value == "FAIL"
        assert Status.WARNING.value == "WARNING"
        assert Status.SKIP.value == "SKIP"

    def test_status_is_string_subclass(self):
        # Status values are compared as strings in check functions
        assert Status.PASS == "PASS"
        assert Status.FAIL == "FAIL"


class TestEmptyRunResult:

    def test_returns_run_result_instance(self):
        result = empty_run_result()
        assert isinstance(result, RunResult)

    def test_status_is_skip(self):
        result = empty_run_result()
        assert result.status == Status.SKIP

    def test_no_sections(self):
        result = empty_run_result()
        assert result.sections == []

    def test_no_warnings_or_errors(self):
        result = empty_run_result()
        assert result.warnings == []
        assert result.errors == []


class TestToPayload:

    def _make_run_result(self, status=Status.PASS, summary="ok"):
        now = "2026-04-24T00:00:00+00:00"
        metadata = RunMetadata(
            started_at_utc=now,
            completed_at_utc=now,
            runtime_ms=100,
            dataset_count=2,
            dataset_names=["tbl_DetailedData", "tbl_Key_Mapping"],
        )
        return RunResult(
            status=status,
            summary=summary,
            metadata=metadata,
            sections=[],
            warnings=[],
            errors=[],
        )

    def test_returns_plain_dict(self):
        payload = to_payload(self._make_run_result())
        assert isinstance(payload, dict)

    def test_top_level_keys_present(self):
        payload = to_payload(self._make_run_result())
        for key in ("status", "summary", "metadata", "sections", "warnings", "errors"):
            assert key in payload

    def test_status_is_string_in_payload(self):
        payload = to_payload(self._make_run_result(status=Status.FAIL))
        assert payload["status"] == "FAIL"
        assert isinstance(payload["status"], str)

    def test_metadata_serialized(self):
        payload = to_payload(self._make_run_result())
        md = payload["metadata"]
        assert md["dataset_count"] == 2
        assert md["runtime_ms"] == 100
        assert isinstance(md["dataset_names"], list)

    def test_sections_list(self):
        payload = to_payload(self._make_run_result())
        assert isinstance(payload["sections"], list)

    def test_round_trip_with_sections(self):
        """SectionResult and CheckResult must serialize without error."""
        check = CheckResult(
            check_id="check1.tbl_Test",
            check_label="Key registration — tbl_Test",
            table_name="tbl_Test",
            status=Status.PASS,
            summary="All OK.",
            details={"missing": []},
        )
        section = SectionResult(
            section_id="check1",
            section_label="Check 1",
            status=Status.PASS,
            summary="1 table checked.",
            checks=[check],
        )
        now = "2026-04-24T00:00:00+00:00"
        run_result = RunResult(
            status=Status.PASS,
            summary="All checks passed.",
            metadata=RunMetadata(
                started_at_utc=now, completed_at_utc=now,
                runtime_ms=50, dataset_count=1, dataset_names=["tbl_DetailedData"],
            ),
            sections=[section],
        )
        payload = to_payload(run_result)
        assert len(payload["sections"]) == 1
        assert len(payload["sections"][0]["checks"]) == 1
        assert payload["sections"][0]["checks"][0]["status"] == "PASS"
