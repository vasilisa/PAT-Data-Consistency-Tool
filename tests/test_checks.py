"""
tests/test_checks.py
====================
Unit tests for each check function in dash_app/engine/checks.py.

All tests use small in-memory pandas DataFrames — no file I/O, no Dataiku SDK.
"""
from __future__ import annotations

import pandas as pd
import pytest

from dash_app.engine.checks import (
    check_key_registration,
    check_referential_integrity,
    check_row_uniqueness,
    check_parent_columns,
    check_mapping_uniqueness,
    check_value_ranges,
    check_key_modelling_unmapped,
)


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def dd_df():
    """Minimal tbl_DetailedData with two segments."""
    return pd.DataFrame({
        "Key_Policy":  ["P1", "P2"],
        "State":       ["NSW", "VIC"],
        "Premium":     [100.0, 200.0],
        "AsAt_Month":  [202501, 202501],
    })


@pytest.fixture
def mapping_df():
    return pd.DataFrame({
        "Key_Policy": ["P1", "P2"],
        "State":      ["NSW", "VIC"],
    })


@pytest.fixture
def ref_table():
    return pd.DataFrame({
        "Key_Policy": ["P1", "P2"],
        "State":      ["NSW", "VIC"],
    })


# ─────────────────────────────────────────────────────────────────────────────
# Check 1 — Key Column Registration
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckKeyRegistration:

    def test_pass_when_key_cols_present(self, dd_df, mapping_df, ref_table):
        result = check_key_registration(dd_df, mapping_df, {"tbl_Test": ref_table})
        assert result["tbl_Test"]["status"] == "PASS"
        assert result["tbl_Test"]["missing"] == []

    def test_fail_when_key_col_absent_from_dd(self, mapping_df, ref_table):
        dd_missing = pd.DataFrame({"State": ["NSW"], "Premium": [100.0], "AsAt_Month": [202501]})
        result = check_key_registration(dd_missing, mapping_df, {"tbl_Test": ref_table})
        assert result["tbl_Test"]["status"] == "FAIL"
        missing_from = [m["missing_from"] for m in result["tbl_Test"]["missing"]]
        assert "tbl_DetailedData" in missing_from

    def test_fail_when_key_col_absent_from_mapping(self, dd_df, ref_table):
        mapping_no_key = pd.DataFrame({"State": ["NSW", "VIC"]})
        result = check_key_registration(dd_df, mapping_no_key, {"tbl_Test": ref_table})
        assert result["tbl_Test"]["status"] == "FAIL"
        missing_from = [m["missing_from"] for m in result["tbl_Test"]["missing"]]
        assert "tbl_Key_Mapping" in missing_from

    def test_key_forecast_excluded(self, dd_df, mapping_df):
        ref_with_forecast = pd.DataFrame({
            "Key_Forecast": ["F1"],
            "State":        ["NSW"],
        })
        # Key_Forecast must not appear in missing even if absent from dd_df
        result = check_key_registration(dd_df, mapping_df, {"tbl_Forecast": ref_with_forecast})
        assert result["tbl_Forecast"]["status"] == "PASS"
        assert result["tbl_Forecast"]["missing"] == []

    def test_pass_when_no_mapping_df(self, dd_df, ref_table):
        result = check_key_registration(dd_df, None, {"tbl_Test": ref_table})
        assert result["tbl_Test"]["status"] == "PASS"


# ─────────────────────────────────────────────────────────────────────────────
# Check 2+3 — Referential Integrity
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckReferentialIntegrity:

    def test_pass_when_perfect_match(self, dd_df, ref_table):
        result = check_referential_integrity(dd_df, {"tbl_Test": ref_table})
        assert result["tbl_Test"]["status"] == "PASS"
        assert result["tbl_Test"]["forward"]["total_unmatched"] == 0
        assert result["tbl_Test"]["reverse"]["total_orphaned"] == 0

    def test_fail_on_forward_gap(self, dd_df):
        # ref covers only P1; P2 in DD has no match
        ref_partial = pd.DataFrame({"Key_Policy": ["P1"], "State": ["NSW"]})
        result = check_referential_integrity(dd_df, {"tbl_Test": ref_partial})
        assert result["tbl_Test"]["forward"]["status"] == "FAIL"
        assert result["tbl_Test"]["forward"]["total_unmatched"] == 1

    def test_warning_on_reverse_orphan(self, dd_df):
        # ref has P3 not in DD → orphan
        ref_extra = pd.DataFrame({"Key_Policy": ["P1", "P2", "P3"], "State": ["NSW", "VIC", "QLD"]})
        result = check_referential_integrity(dd_df, {"tbl_Test": ref_extra})
        assert result["tbl_Test"]["reverse"]["status"] == "WARNING"
        assert result["tbl_Test"]["reverse"]["total_orphaned"] == 1

    def test_skip_when_no_common_columns(self, dd_df):
        ref_no_common = pd.DataFrame({"OtherNum": [1.0, 2.0]})
        result = check_referential_integrity(dd_df, {"tbl_Test": ref_no_common})
        assert result["tbl_Test"]["status"] == "SKIP"

    def test_premium_attached_to_forward_unmatched(self, dd_df):
        ref_partial = pd.DataFrame({"Key_Policy": ["P1"], "State": ["NSW"]})
        result = check_referential_integrity(dd_df, {"tbl_Test": ref_partial})
        fwd = result["tbl_Test"]["forward"]
        assert fwd["total_premium"] == pytest.approx(200.0)
        assert fwd["premium_pct"] == pytest.approx(200.0 / 300.0 * 100)


# ─────────────────────────────────────────────────────────────────────────────
# Check 4 — Row Uniqueness
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckRowUniqueness:

    def test_pass_when_unique(self, dd_df, ref_table):
        result = check_row_uniqueness(dd_df, {"tbl_Test": ref_table})
        assert result["tbl_Test"]["status"] == "PASS"
        assert result["tbl_Test"]["duplicate_count"] == 0

    def test_fail_on_duplicate_rows(self, dd_df):
        ref_dup = pd.DataFrame({
            "Key_Policy": ["P1", "P1"],
            "State":      ["NSW", "NSW"],
        })
        result = check_row_uniqueness(dd_df, {"tbl_Test": ref_dup})
        assert result["tbl_Test"]["status"] == "FAIL"
        assert result["tbl_Test"]["duplicate_count"] > 0

    def test_pattern_table_includes_devm(self, dd_df):
        """DevM must be included in uniqueness key for pattern tables."""
        ref_pattern = pd.DataFrame({
            "Key_Pattern":  ["PAT1", "PAT1"],
            "State":        ["NSW", "NSW"],
            "DevM":         [1, 2],  # different DevM → unique rows
        })
        result = check_row_uniqueness(dd_df, {"tbl_Patterns_Attr": ref_pattern})
        assert result["tbl_Patterns_Attr"]["status"] == "PASS"
        assert "DevM" in result["tbl_Patterns_Attr"]["uniqueness_cols"]

    def test_pattern_table_fails_on_devm_duplicate(self, dd_df):
        ref_dup = pd.DataFrame({
            "Key_Pattern":  ["PAT1", "PAT1"],
            "State":        ["NSW", "NSW"],
            "DevM":         [1, 1],  # same DevM → duplicate
        })
        result = check_row_uniqueness(dd_df, {"tbl_Patterns_Attr": ref_dup})
        assert result["tbl_Patterns_Attr"]["status"] == "FAIL"


# ─────────────────────────────────────────────────────────────────────────────
# Check 5 — Parent Columns
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckParentColumns:

    def test_pass_when_key_is_parent_of_context(self, dd_df, ref_table):
        # State is a function of Key_Policy (one-to-one here)
        result = check_parent_columns(dd_df, None, {"tbl_Test": ref_table})
        assert result["tbl_Test"]["status"] == "PASS"
        assert result["tbl_Test"]["failing_cols"] == []

    def test_fail_when_context_not_parent(self):
        """Same key value maps to two different context values → not a parent."""
        dd_not_parent = pd.DataFrame({
            "Key_Policy": ["P1", "P1"],
            "State":      ["NSW", "VIC"],  # P1 → two states
            "Premium":    [100.0, 200.0],
            "AsAt_Month": [202501, 202501],
        })
        ref = pd.DataFrame({"Key_Policy": ["P1", "P1"], "State": ["NSW", "VIC"]})
        result = check_parent_columns(dd_not_parent, None, {"tbl_Test": ref})
        assert result["tbl_Test"]["status"] == "FAIL"
        failing_cols = [f["column"] for f in result["tbl_Test"]["failing_cols"]]
        assert "State" in failing_cols

    def test_empty_ref_table_returns_pass(self, dd_df):
        ref_no_context = pd.DataFrame({"Key_Policy": ["P1"]})
        result = check_parent_columns(dd_df, None, {"tbl_Test": ref_no_context})
        assert result["tbl_Test"]["status"] == "PASS"


# ─────────────────────────────────────────────────────────────────────────────
# Check 6 — Mapping Uniqueness
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckMappingUniqueness:

    def test_pass_on_unique_mapping(self, mapping_df):
        result = check_mapping_uniqueness(mapping_df)
        assert result["status"] == "PASS"
        assert result["total_duplicates"] == 0

    def test_fail_on_duplicate_non_key_rows(self):
        dup_mapping = pd.DataFrame({
            "Key_Policy": ["P1", "P2"],
            "State":      ["NSW", "NSW"],  # same State → duplicate natural key
        })
        result = check_mapping_uniqueness(dup_mapping)
        assert result["status"] == "FAIL"
        assert result["total_duplicates"] > 0

    def test_skip_when_no_mapping_df(self):
        result = check_mapping_uniqueness(None)
        assert result["status"] == "SKIP"

    def test_skip_when_only_key_cols(self):
        key_only = pd.DataFrame({"Key_Policy": ["P1"]})
        result = check_mapping_uniqueness(key_only)
        assert result["status"] == "SKIP"


# ─────────────────────────────────────────────────────────────────────────────
# Check 7 — Value Ranges
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckValueRanges:

    def test_rate_change_pass(self, dd_df):
        ref = {"tbl_RateChange": pd.DataFrame({"Key_Policy": ["P1"], "RateChange": [0.05]})}
        result = check_value_ranges(dd_df, ref)
        assert result["tbl_RateChange.RateChange"]["status"] == "PASS"

    def test_rate_change_fail_when_percentage_scale(self, dd_df):
        """Values entered as percentages (e.g. 5.0 instead of 0.05) → FAIL."""
        vals = [5.0] * 100  # >50% outside [−0.5, +0.5]
        ref = {"tbl_RateChange": pd.DataFrame({"Key_Policy": ["P"] * 100, "RateChange": vals})}
        result = check_value_ranges(dd_df, ref)
        assert result["tbl_RateChange.RateChange"]["status"] == "FAIL"

    def test_asat_month_pass(self, dd_df):
        result = check_value_ranges(dd_df, {})
        assert result["tbl_DetailedData.AsAt_Month"]["status"] == "PASS"

    def test_asat_month_fail_invalid_format(self):
        dd_bad = pd.DataFrame({
            "Key_Policy": ["P1"],
            "Premium":    [100.0],
            "AsAt_Month": [999999],  # invalid YYYYMM (month 99)
        })
        result = check_value_ranges(dd_bad, {})
        assert result["tbl_DetailedData.AsAt_Month"]["status"] == "FAIL"

    def test_asat_month_fail_multiple_distinct_values(self):
        dd_multi = pd.DataFrame({
            "Key_Policy": ["P1", "P2"],
            "Premium":    [100.0, 200.0],
            "AsAt_Month": [202501, 202502],  # two valid but distinct months
        })
        result = check_value_ranges(dd_multi, {})
        assert result["tbl_DetailedData.AsAt_Month"]["status"] == "FAIL"

    def test_devm_fail_when_gap(self, dd_df):
        """Pattern table DevM 1,3 (missing 2) → FAIL."""
        ref = {"tbl_Patterns_Attr": pd.DataFrame({
            "Key_Pattern": ["PAT1"] * 122,
            "DevM":        [1] + list(range(3, 124)),  # gap at 2
        })}
        result = check_value_ranges(dd_df, ref)
        assert result["tbl_Patterns_Attr.DevM"]["status"] == "FAIL"

    def test_devm_pass_when_complete_sequence(self, dd_df):
        ref = {"tbl_Patterns_Attr": pd.DataFrame({
            "Key_Pattern": ["PAT1"] * 120,
            "DevM":        list(range(1, 121)),  # 1..120 complete
        })}
        result = check_value_ranges(dd_df, ref)
        assert result["tbl_Patterns_Attr.DevM"]["status"] == "PASS"

    def test_devm_fail_when_max_below_120(self, dd_df):
        ref = {"tbl_Patterns_Attr": pd.DataFrame({
            "Key_Pattern": ["PAT1"] * 60,
            "DevM":        list(range(1, 61)),  # only up to 60
        })}
        result = check_value_ranges(dd_df, ref)
        assert result["tbl_Patterns_Attr.DevM"]["status"] == "FAIL"


# ─────────────────────────────────────────────────────────────────────────────
# Check 8 — Key_Modelling Coverage
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckKeyModellingUnmapped:

    def test_pass_when_all_mapped(self):
        dd = pd.DataFrame({
            "Key_Modelling": ["M1", "M2"],
            "Premium":       [100.0, 200.0],
        })
        result = check_key_modelling_unmapped(dd)
        assert result["Key_Modelling"]["status"] == "PASS"
        assert result["Key_Modelling"]["unmapped_count"] == 0

    def test_warning_when_nulls_present(self):
        dd = pd.DataFrame({
            "Key_Modelling": ["M1", None],
            "Premium":       [100.0, 50.0],
        })
        result = check_key_modelling_unmapped(dd)
        assert result["Key_Modelling"]["status"] == "WARNING"
        assert result["Key_Modelling"]["unmapped_count"] == 1
        assert result["Key_Modelling"]["unmapped_premium"] == pytest.approx(50.0)

    def test_empty_dict_when_no_premium_col(self):
        dd = pd.DataFrame({"Key_Modelling": ["M1"]})
        result = check_key_modelling_unmapped(dd)
        assert result == {}

    def test_empty_dict_when_dd_is_none(self):
        result = check_key_modelling_unmapped(None)
        assert result == {}

    def test_returns_empty_dict_when_no_key_modelling_col(self):
        dd = pd.DataFrame({"Key_Policy": ["P1"], "Premium": [100.0]})
        result = check_key_modelling_unmapped(dd)
        assert result == {}

    def test_premium_pct_calculated_correctly(self):
        dd = pd.DataFrame({
            "Key_Modelling": ["M1", None, None],
            "Premium":       [100.0, 50.0, 50.0],
        })
        result = check_key_modelling_unmapped(dd)
        assert result["Key_Modelling"]["premium_pct"] == pytest.approx(50.0, abs=0.1)
