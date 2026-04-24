"""
dash_app/engine/checks.py
=========================
All PAT Data Consistency check functions.

All logic is copied verbatim from data_consistency_checks.py (v20260423) with
no semantic changes.  Each function returns a plain dict whose structure is
documented in its docstring; the orchestrator layer converts these dicts into
typed ``CheckResult`` / ``SectionResult`` objects.

This module has no dependency on the Dataiku SDK.
"""
from __future__ import annotations

import pandas as pd

from dash_app.engine.helpers import (
    PATTERN_TABLES,
    STRUCTURAL_COLS,
    find_parent_cols,
    get_context_cols,
    get_display_cols,
    get_join_key,
    get_key_cols,
)


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 1 — KEY COLUMN REGISTRATION
# ─────────────────────────────────────────────────────────────────────────────

def check_key_registration(
    dd_df: pd.DataFrame,
    mapping_df: pd.DataFrame | None,
    ref_tables: dict[str, pd.DataFrame],
) -> dict:
    """
    Every Key_* column in a reference table (except Key_Forecast) must exist in
    both tbl_DetailedData and tbl_Key_Mapping.
    Returns {table_name: {status, missing: [{key_col, missing_from}]}}
    Severity: FAIL.
    """
    dd_cols  = set(dd_df.columns)
    map_cols = set(mapping_df.columns) if mapping_df is not None else set()
    results  = {}

    for tbl, ref_df in ref_tables.items():
        key_cols = [c for c in get_key_cols(ref_df) if c != "Key_Forecast"]
        missing  = []
        for kc in key_cols:
            if kc not in dd_cols:
                missing.append({"key_col": kc, "missing_from": "tbl_DetailedData"})
            if mapping_df is not None and kc not in map_cols:
                missing.append({"key_col": kc, "missing_from": "tbl_Key_Mapping"})
        results[tbl] = {
            "status":  "FAIL" if missing else "PASS",
            "missing": missing,
        }
    return results


# ─────────────────────────────────────────────────────────────────────────────
# CHECKS 2 + 3 — REFERENTIAL INTEGRITY
# ─────────────────────────────────────────────────────────────────────────────

def check_referential_integrity(
    dd_df: pd.DataFrame,
    ref_tables: dict[str, pd.DataFrame],
) -> dict:
    """
    Forward (FAIL):    every unique join key combination in DetailedData has at
                       least one matching row in the reference table.
    Reverse (WARNING): every join key combination in the reference table exists
                       in DetailedData.

    Join key: all common non-numeric columns (get_join_key).
    Premium is summed across all DataTypes for unmatched groups.

    Returns {table_name: {status, join_key, forward: {...}, reverse: {...}}}
    """
    total_premium = dd_df["Premium"].sum() if "Premium" in dd_df.columns else None
    results       = {}

    for tbl, ref_df in ref_tables.items():
        join_key = get_join_key(ref_df, dd_df)
        if not join_key:
            results[tbl] = {
                "status": "SKIP",
                "reason": "No common non-numeric columns with DetailedData",
            }
            continue

        dd_keys  = dd_df[join_key].drop_duplicates()
        ref_keys = ref_df[join_key].drop_duplicates()

        # Forward: DetailedData key combos with no match in ref table
        fwd_merge = dd_keys.merge(ref_keys, on=join_key, how="left", indicator=True)
        unmatched = fwd_merge[fwd_merge["_merge"] == "left_only"].drop(columns="_merge")

        if len(unmatched) > 0 and total_premium is not None:
            prem_by_group = (
                dd_df[join_key + ["Premium"]]
                .merge(unmatched, on=join_key, how="inner")
                .groupby(join_key, dropna=False)["Premium"]
                .sum()
                .reset_index()
                .sort_values("Premium", ascending=False)
            )
            total_unmatched_prem = prem_by_group["Premium"].sum()
            pct  = (total_unmatched_prem / total_premium * 100) if total_premium else None
            top10 = prem_by_group.head(10).copy()
            if pct is not None:
                top10["Premium_%"] = (top10["Premium"] / total_premium * 100).round(1)
        else:
            total_unmatched_prem = None
            pct                  = None
            top10                = unmatched.head(10).copy()

        forward = {
            "status":          "FAIL" if len(unmatched) > 0 else "PASS",
            "total_unmatched": len(unmatched),
            "total_premium":   total_unmatched_prem,
            "premium_pct":     pct,
            "top10":           top10,
        }

        # Reverse: ref table key combos absent from DetailedData
        rev_merge = ref_keys.merge(dd_keys, on=join_key, how="left", indicator=True)
        orphaned  = rev_merge[rev_merge["_merge"] == "left_only"].drop(columns="_merge")

        # For tbl_Forecast: attach premium per combination so the display can
        # show cell-level premium alongside each orphaned key combination.
        forecast_orphaned_prem = None
        forecast_orphaned_df   = None
        if tbl == "tbl_Forecast" and len(orphaned) > 0:
            prem_cols = [c for c in ref_df.columns
                         if c not in join_key
                         and "prem" in c.lower()
                         and pd.api.types.is_numeric_dtype(ref_df[c])]
            if prem_cols:
                prem_col = prem_cols[0]
                prem_by_combo = (
                    ref_df[join_key + [prem_col]]
                    .groupby(join_key, dropna=False)[prem_col].sum().reset_index()
                )
                forecast_orphaned_df   = orphaned.merge(prem_by_combo, on=join_key, how="left")
                forecast_orphaned_prem = float(forecast_orphaned_df[prem_col].sum())

        reverse = {
            "status":                    "WARNING" if len(orphaned) > 0 else "PASS",
            "total_orphaned":            len(orphaned),
            "orphaned":                  orphaned,
            "forecast_orphaned_premium": forecast_orphaned_prem,
            "forecast_orphaned_df":      forecast_orphaned_df,
        }

        results[tbl] = {
            "status":   ("FAIL"    if forward["status"] == "FAIL"
                         else "WARNING" if reverse["status"] == "WARNING"
                         else "PASS"),
            "join_key": join_key,
            "forward":  forward,
            "reverse":  reverse,
        }
    return results


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 4 — ROW UNIQUENESS
# ─────────────────────────────────────────────────────────────────────────────

def check_row_uniqueness(
    dd_df: pd.DataFrame,
    ref_tables: dict[str, pd.DataFrame],
) -> dict:
    """
    The join key combination must be unique in each reference table to ensure
    unambiguous joins to DetailedData (no fan-out / row duplication).
    For pattern tables, DevM is included in the uniqueness key even though it
    is not part of the standard join key.
    Returns {table_name: {status, duplicate_count, duplicates, uniqueness_cols}}
    Severity: FAIL.
    """
    results = {}
    for tbl, ref_df in ref_tables.items():

        if tbl == "tbl_Forecast":
            # Every combination of columns except Key_Modelling must be unique.
            # Key_Modelling is the dimension that varies across forecast scenarios
            # for the same underlying segment — it is intentionally repeated.
            uniq_cols = [c for c in ref_df.columns if c != "Key_Modelling"]
        else:
            uniq_cols = get_join_key(ref_df, dd_df)

            # Year columns (AY / UWY / UY) define row uniqueness within a segment
            # but are numeric so excluded from get_join_key — add them explicitly.
            for year_col in ("AY", "UWY", "UY"):
                if year_col in ref_df.columns and year_col not in uniq_cols:
                    uniq_cols = uniq_cols + [year_col]

            # Pattern tables: DevM defines a distinct row within each pattern segment
            if tbl in PATTERN_TABLES and "DevM" in ref_df.columns and "DevM" not in uniq_cols:
                uniq_cols = uniq_cols + ["DevM"]

        if not uniq_cols:
            results[tbl] = {"status": "SKIP", "reason": "No join key columns identified"}
            continue

        dup_mask = ref_df.duplicated(subset=uniq_cols, keep=False)
        dup_rows = ref_df[dup_mask][uniq_cols].drop_duplicates()

        results[tbl] = {
            "status":          "FAIL" if len(dup_rows) > 0 else "PASS",
            "duplicate_count": len(dup_rows),
            "duplicates":      dup_rows.head(20),
            "uniqueness_cols": uniq_cols,
        }
    return results


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 5 — PARENT COLUMN RULE
# ─────────────────────────────────────────────────────────────────────────────

def check_parent_columns(
    dd_df: pd.DataFrame,
    mapping_df: pd.DataFrame | None,
    ref_tables: dict[str, pd.DataFrame],
) -> dict:
    """
    For each reference table: every non-Key_* column in the join key must be a
    parent of each Key_* join column — i.e. each Key_* value maps to at most one
    value of that column. Verified using find_parent_cols(dd_df, key_col, context_cols).

    Additional check for tbl_Min_Large_Load: every value in a non-Key_* join column
    must exist in the same-named column in tbl_DetailedData (domain membership).

    For tbl_Key_Mapping: parent check uses the mapping table itself as df_map,
    checking all non-Key_* columns against each Key_* column.

    Severity: FAIL.
    """
    results = {}

    for tbl, ref_df in ref_tables.items():

        # ── tbl_Forecast: special parent check ───────────────────────────────
        # Key_Forecast is the correct key column to check against (Key_Modelling
        # is intentionally not a parent — different modelling segments share the
        # same forecast key).  Key_Forecast may be absent from dd_df, so we use
        # ref_df itself as the mapping table.
        if tbl == "tbl_Forecast":
            context_cols = [
                c for c in ref_df.columns
                if not c.startswith("Key_")
                and c not in STRUCTURAL_COLS
                and not pd.api.types.is_numeric_dtype(ref_df[c])
            ]
            failing = []
            if "Key_Forecast" in ref_df.columns and context_cols:
                parents     = find_parent_cols(ref_df, "Key_Forecast", context_cols)
                non_parents = [c for c in context_cols if c not in parents]
                failing     = [{"key_col": "Key_Forecast", "column": c} for c in non_parents]
            results[tbl] = {"status": "FAIL" if failing else "PASS",
                            "failing_cols": failing, "domain_fails": []}
            continue
        # ─────────────────────────────────────────────────────────────────────

        join_key     = get_join_key(ref_df, dd_df)
        key_join     = [c for c in join_key if c.startswith("Key_")]
        context_cols = get_context_cols(ref_df, dd_df)   # non-Key_* display cols

        if not context_cols or not key_join:
            results[tbl] = {"status": "PASS", "failing_cols": [], "domain_fails": []}
            continue

        # Deduplicate dd_df to only the columns needed for this table's parent check.
        # This avoids running groupby on millions of rows — the parent relationship
        # only depends on the unique combinations of key and context values.
        dd_cols_needed = [c for c in [*key_join, *context_cols] if c in dd_df.columns]
        dd_deduped     = dd_df[dd_cols_needed].drop_duplicates()

        failing = []
        for kc in key_join:
            parents     = find_parent_cols(dd_deduped, kc, context_cols)
            non_parents = [c for c in context_cols if c not in parents]
            failing.extend({"key_col": kc, "column": c} for c in non_parents)

        # tbl_Min_Large_Load: domain membership — context column values must
        # be a subset of the values present in the same column in DetailedData
        domain_fails = []
        if tbl == "tbl_Min_Large_Load":
            for col in context_cols:
                bad_vals = sorted(set(ref_df[col].dropna()) - set(dd_df[col].dropna()))
                if bad_vals:
                    domain_fails.append({"column": col, "invalid_values": bad_vals})

        results[tbl] = {
            "status":       "FAIL" if (failing or domain_fails) else "PASS",
            "failing_cols": failing,
            "domain_fails": domain_fails,
        }

    # tbl_Key_Mapping: uniqueness check (Check 6) is sufficient — no parent column check.

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 6 — MAPPING TABLE UNIQUENESS
# ─────────────────────────────────────────────────────────────────────────────

def check_mapping_uniqueness(mapping_df: pd.DataFrame | None) -> dict:
    """
    In tbl_Key_Mapping, the non-Key_* columns form the natural key and must be
    unique — each segmentation combination should appear at most once.
    Returns {status, total_duplicates, duplicates}
    Severity: FAIL.
    """
    if mapping_df is None:
        return {"status": "SKIP", "reason": "tbl_Key_Mapping not loaded"}

    non_key_cols = [c for c in mapping_df.columns if not c.startswith("Key_")]
    if not non_key_cols:
        return {"status": "SKIP", "reason": "No non-Key_* columns in tbl_Key_Mapping"}

    dup_mask = mapping_df.duplicated(subset=non_key_cols, keep=False)
    dup_rows = mapping_df[dup_mask][non_key_cols].drop_duplicates()

    return {
        "status":           "FAIL" if len(dup_rows) > 0 else "PASS",
        "total_duplicates": len(dup_rows),
        "duplicates":       dup_rows.head(20),
    }


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 7 — VALUE RANGE CHECKS
# ─────────────────────────────────────────────────────────────────────────────

def _rate_or_trend_check(
    vals: pd.Series,
    table_name: str,
    col_name: str,
) -> dict | None:
    """
    Shared range check for RateChange and Trend_Value.
    Expected: decimal close to zero (e.g. 0.05 for +5%).
    WARNING if >10% outside [−0.2, +0.2]; FAIL if >50% outside [−0.5, +0.5].
    """
    n = len(vals)
    if n == 0:
        return None
    pct_05 = ((vals < -0.5) | (vals > 0.5)).sum() / n * 100
    pct_02 = ((vals < -0.2) | (vals > 0.2)).sum() / n * 100
    if pct_05 > 50:
        status = "FAIL"
        detail = f"{pct_05:.1f}% outside [−0.5, +0.5] — likely entered as percentages not decimals"
    elif pct_02 > 10:
        status = "WARNING"
        detail = f"{pct_02:.1f}% outside [−0.2, +0.2]"
    else:
        status = "PASS"
        detail = f"{pct_02:.1f}% outside [−0.2, +0.2]"
    return {
        "status": status, "table": table_name, "column": col_name,
        "n": n, "pct_outside_02": round(pct_02, 1), "pct_outside_05": round(pct_05, 1),
        "detail": detail,
    }


def check_value_ranges(
    dd_df: pd.DataFrame,
    ref_tables: dict[str, pd.DataFrame],
) -> dict:
    """
    Range and format checks for specific value columns across tables.
    Returns {check_key: {status, table, column, detail, ...}}

    Checks:
      RateChange   in tbl_RateChange, tbl_RateChange_Pol
      Trend_Value  in tbl_Trend
      DevPct_*     in pattern tables (per column independently)
      AsAt_Month   in tbl_DetailedData
      DevM         in pattern tables (sequence completeness per pattern key)
      MinLoad_*    in tbl_Min_Large_Load (numeric; ≥95% < 3)
    """
    results = {}

    # RateChange
    for tbl in ("tbl_RateChange", "tbl_RateChange_Pol"):
        if tbl in ref_tables and "RateChange" in ref_tables[tbl].columns:
            r = _rate_or_trend_check(ref_tables[tbl]["RateChange"].dropna(), tbl, "RateChange")
            if r:
                results[f"{tbl}.RateChange"] = r

    # Trend_Value
    if "tbl_Trend" in ref_tables and "Trend_Value" in ref_tables["tbl_Trend"].columns:
        r = _rate_or_trend_check(
            ref_tables["tbl_Trend"]["Trend_Value"].dropna(), "tbl_Trend", "Trend_Value"
        )
        if r:
            results["tbl_Trend.Trend_Value"] = r

    # DevPct_* — checked per column per pattern table
    for tbl in PATTERN_TABLES:
        if tbl not in ref_tables:
            continue
        df = ref_tables[tbl]
        for col in [c for c in df.columns if c.startswith("DevPct_")]:
            vals = df[col].dropna()
            n    = len(vals)
            if n == 0:
                continue
            mean_val   = vals.mean()
            pct_over_1 = (vals > 1).sum() / n * 100
            if pct_over_1 > 20:
                status = "FAIL"
                detail = f"{pct_over_1:.1f}% of values > 1 (threshold: >20%)"
            elif mean_val < 0.7 or mean_val > 1.0 or pct_over_1 > 5:
                status = "WARNING"
                detail = f"Mean = {mean_val:.4f}; {pct_over_1:.1f}% of values > 1"
            else:
                status = "PASS"
                detail = f"Mean = {mean_val:.4f}; {pct_over_1:.1f}% of values > 1"
            results[f"{tbl}.{col}"] = {
                "status": status, "table": tbl, "column": col,
                "n": n, "mean": round(mean_val, 4), "pct_above_1": round(pct_over_1, 1),
                "detail": detail,
            }

    # AsAt_Month in tbl_DetailedData
    if dd_df is not None and "AsAt_Month" in dd_df.columns:
        distinct = dd_df["AsAt_Month"].dropna().unique()

        def _valid_yyyymm(v):
            try:
                s = str(int(v))
                return len(s) == 6 and 1 <= int(s[4:]) <= 12
            except Exception:
                return False

        invalid_count = sum(1 for v in distinct if not _valid_yyyymm(v))
        if invalid_count > 0:
            status = "FAIL"
            detail = f"{invalid_count} value(s) not in valid YYYYMM format: {list(distinct[:5])}"
        elif len(distinct) > 1:
            status = "FAIL"
            detail = f"Multiple AsAt_Month values found: {list(distinct)}"
        else:
            status = "PASS"
            detail = f"Single valid value: {distinct[0]}"
        results["tbl_DetailedData.AsAt_Month"] = {
            "status": status, "table": "tbl_DetailedData", "column": "AsAt_Month",
            "distinct_values": list(distinct), "invalid_count": invalid_count, "detail": detail,
        }

    # DevM — sequence completeness per pattern key group
    for tbl in PATTERN_TABLES:
        if tbl not in ref_tables:
            continue
        df = ref_tables[tbl]
        if "DevM" not in df.columns:
            continue
        pattern_key_cols = [c for c in get_key_cols(df) if "Pattern" in c]
        if not pattern_key_cols:
            continue
        group_col = pattern_key_cols[0]

        fails = []
        for key_val, grp in df.groupby(group_col, dropna=False):
            devms = sorted(grp["DevM"].dropna().astype(int).tolist())
            if not devms:
                continue
            expected = set(range(devms[0], devms[-1] + 1))
            gaps     = sorted(expected - set(devms))
            reasons  = []
            if devms[0] != 1:
                reasons.append(f"starts at {devms[0]} (expected 1)")
            if gaps:
                shown = gaps[:5]
                reasons.append(f"gaps at {shown}{'…' if len(gaps) > 5 else ''}")
            if devms[-1] < 120:
                reasons.append(f"max DevM = {devms[-1]} (< 120 required)")
            if reasons:
                fails.append({
                    "pattern_key": key_val,
                    "first_DevM":  devms[0],
                    "last_DevM":   devms[-1],
                    "reasons":     "; ".join(reasons),
                })

        results[f"{tbl}.DevM"] = {
            "status":           "FAIL" if fails else "PASS",
            "table":            tbl,
            "column":           "DevM",
            "failing_patterns": fails[:20],
            "detail":           (f"{len(fails)} pattern key(s) with DevM sequence issues"
                                 if fails else "All DevM sequences valid (1 to ≥120)"),
        }

    # MinLoad_* in tbl_Min_Large_Load
    if "tbl_Min_Large_Load" in ref_tables:
        df = ref_tables["tbl_Min_Large_Load"]
        for col in [c for c in df.columns if c.startswith("MinLoad_")]:
            numeric_vals  = pd.to_numeric(df[col], errors="coerce")
            # NaNs introduced by coercion = non-numeric values
            n_non_numeric = int(numeric_vals.isna().sum() - df[col].isna().sum())
            if n_non_numeric > 0:
                status = "FAIL"
                detail = f"{n_non_numeric} non-numeric value(s) found"
            else:
                valid       = numeric_vals.dropna()
                pct_below_3 = (valid < 3).sum() / len(valid) * 100 if len(valid) > 0 else 100.0
                if pct_below_3 < 95:
                    status = "WARNING"
                    detail = f"Only {pct_below_3:.1f}% of values < 3 (expected ≥95%)"
                else:
                    status = "PASS"
                    detail = f"{pct_below_3:.1f}% of values < 3"
            results[f"tbl_Min_Large_Load.{col}"] = {
                "status": status, "table": "tbl_Min_Large_Load", "column": col,
                "n": len(df), "detail": detail,
            }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CHECK 8 — KEY_MODELLING COVERAGE
# ─────────────────────────────────────────────────────────────────────────────

def check_key_modelling_unmapped(dd_df: pd.DataFrame | None) -> dict:
    """
    For each Key_Modelling* column in tbl_DetailedData, reports how much
    premium corresponds to rows where the value is null or blank — these rows
    are not assigned to any modelling segment and will be excluded from the
    PAT analysis.
    Severity: WARNING if any unmapped premium exists.
    Returns {column_name: {status, unmapped_count, unmapped_premium, premium_pct}}
    """
    if dd_df is None or "Premium" not in dd_df.columns:
        return {}

    total_premium = dd_df["Premium"].sum()
    results = {}

    for col in sorted(c for c in dd_df.columns if c.startswith("Key_Modelling")):
        unmapped_mask = dd_df[col].isna() | (dd_df[col].astype(str).str.strip() == "")
        unmapped_prem = float(dd_df.loc[unmapped_mask, "Premium"].sum())
        pct           = (unmapped_prem / total_premium * 100) if total_premium else 0.0
        results[col]  = {
            "status":           "WARNING" if unmapped_prem > 0 else "PASS",
            "column":           col,
            "unmapped_count":   int(unmapped_mask.sum()),
            "unmapped_premium": unmapped_prem,
            "premium_pct":      round(pct, 1),
        }
    return results
