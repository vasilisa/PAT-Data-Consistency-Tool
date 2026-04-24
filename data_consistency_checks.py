#v20260423 : Initial version 
"""
PAT Data Consistency Tool
=========================
Run from a Dataiku Python notebook in the PAT Data project.

Usage:
    from data_consistency_checks import run_and_display
    run_and_display()

Checks performed:
  1. Key Column Registration     — every Key_* in a ref table exists in DetailedData + Key_Mapping
  2. Forward Referential Integrity — every DetailedData key combo has a match in the ref table (FAIL)
  3. Reverse Referential Integrity — every ref table key combo exists in DetailedData (WARNING)
  4. Row Uniqueness               — no duplicate join key combinations per ref table (FAIL)
  5. Parent Column Rule           — non-Key_* join columns are parents of Key_* columns (FAIL)
  6. Mapping Table Uniqueness     — non-Key_* combinations in tbl_Key_Mapping are unique (FAIL)
  7. Value Range Checks           — RateChange, Trend_Value, DevPct_*, AsAt_Month, DevM, MinLoad_*
  8. Key_Modelling Coverage        — premium with no Key_Modelling assignment (WARNING)

Join key definition (used throughout):
    All columns that appear in BOTH the reference table AND tbl_DetailedData and are non-numeric.
    This covers Key_* segment keys, FullKey segmentation columns, and any other categorical
    columns shared between the two tables — no dependency on Keys_Modelling global variable.
"""

import dataiku
import pandas as pd
import numpy as np
from IPython.display import display, HTML

def find_parent_cols(
    df_map: pd.DataFrame,
    key_col: str,
    col_candidates: list,
) -> list:
    """
    From col_candidates, return those where every value of key_col maps to at
    most one value of that column (many-to-one / parent relationship).
    Columns absent from df_map are silently skipped.
    Copied from PAT_utils.py — included here so this script runs in data projects
    that do not have the PAT Libraries folder on their Python path.
    """
    aligned = []
    for col in col_candidates:
        if col not in df_map.columns:
            continue
        nunique = df_map.groupby(key_col, dropna=False)[col].nunique()
        if (nunique <= 1).all():
            aligned.append(col)
    return aligned


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# All tbl_* datasets in scope per WIKI_InputTables.md.
# Tables absent from the project are silently skipped.
WIKI_TABLES = [
    "tbl_DetailedData",
    "tbl_RateChange",
    "tbl_Trend",
    "tbl_Patterns_Attr",
    "tbl_Patterns_Large",
    "tbl_Patterns_Prem",
    "tbl_Forecast",
    "tbl_Key_Mapping",
    "tbl_RateChange_Pol",
    "tbl_IELR_Attr",
    "tbl_IELR_Large",
    "tbl_ULR_Prior_Attr",
    "tbl_ULR_Prior_Large",
    "tbl_Min_Large_Load",
    "tbl_Weight_HistYears",
]

# Columns excluded from display output and from parent-column checks.
# Year columns (AY/UWY/UY) and DevM are structural dimensions, not segmentation keys.
STRUCTURAL_COLS = {"AY", "UWY", "UY", "DevM"}

# Pattern tables — DevM is part of their natural uniqueness key.
PATTERN_TABLES = {"tbl_Patterns_Attr", "tbl_Patterns_Large", "tbl_Patterns_Prem"}


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

def _get_dd_group_cols(loaded_ref_tables):
    """
    Read tbl_DetailedData schema (no data load) and return the column names
    to use as GROUP BY keys in the pre-aggregated dataset.

    Only includes columns that:
      - appear in at least one loaded reference table (i.e. are actual join
        key or context columns — excludes claim/policy identifiers such as
        Claim_Number, Policy_Number, Inception_Date that would prevent
        meaningful aggregation)
      - are non-numeric in tbl_DetailedData

    AsAt_Month is always included.
    Premium is excluded (it is the aggregation target).
    """
    schema        = dataiku.Dataset("tbl_DetailedData").read_schema()
    NUMERIC_TYPES = {"int", "bigint", "smallint", "tinyint", "double", "float", "decimal"}

    # Union of all columns across every loaded reference table
    ref_cols = set()
    for df in loaded_ref_tables.values():
        ref_cols.update(df.columns)

    cols = []
    for col in schema:
        name  = col["name"]
        ctype = col.get("type", "string").lower()
        if name == "Premium":
            continue
        if name == "AsAt_Month":
            cols.append(name)
            continue
        if name in ref_cols and ctype not in NUMERIC_TYPES:
            cols.append(name)
    return cols


def _ensure_dd_aggregated(project, loaded_ref_tables):
    """
    Ensure tbl_DetailedData_Agg exists and its recipe settings are current.

    Every call:
      - Recomputes the GROUP BY columns from the currently loaded reference
        tables (these can change as reference tables are added or removed)
      - Creates the output dataset and Group recipe if they do not exist
      - Always updates the recipe settings (keys, aggregation) so the recipe
        definition stays in sync with whatever reference tables are present
      - Builds the recipe only when the dataset does not yet exist

    If tbl_DetailedData_Agg already exists, only the settings are refreshed —
    no rebuild is triggered.  If the GROUP BY columns have changed and a
    rebuild is needed, run compute_tbl_DetailedData_Agg manually in the flow.

    Returns True if tbl_DetailedData_Agg is available after the call.
    """
    import time

    existing      = {d["name"] for d in project.list_datasets()}
    agg_exists    = "tbl_DetailedData_Agg" in existing
    group_cols    = _get_dd_group_cols(loaded_ref_tables)

    # ── Create output dataset and recipe if needed ────────────────────────────
    if not agg_exists:
        print("  tbl_DetailedData_Agg not found — creating Group recipe…",
              end=" ", flush=True)

        dd_raw        = project.get_dataset("tbl_DetailedData").get_settings().get_raw()
        project_key   = dataiku.default_project_key()
        output_params = dict(dd_raw["params"])
        output_params["table"] = f"{project_key}_TBL_DETAILEDDATA_AGG"
        for drop_key in ("partitioning", "skipRows", "maxRows", "filterQuery"):
            output_params.pop(drop_key, None)
        project.create_dataset("tbl_DetailedData_Agg", dd_raw["type"], output_params)

    existing_recipes = {r["name"] for r in project.list_recipes()}
    if "compute_tbl_DetailedData_Agg" not in existing_recipes:
        creator = project.new_recipe("grouping")
        creator.set_name("compute_tbl_DetailedData_Agg")
        creator.with_input("tbl_DetailedData")
        creator.with_output("tbl_DetailedData_Agg")
        recipe = creator.create()
    else:
        recipe = project.get_recipe("compute_tbl_DetailedData_Agg")

    # ── Always update recipe settings (group cols may have changed) ───────────
    # DSS uses "keys" (not "groupingKeys") for SQL GROUP BY generation.
    # Aggregation uses boolean flags; "Premium_sum" is renamed to "Premium"
    # via outputColumnNameOverrides.
    settings = recipe.get_settings()
    payload  = settings.get_json_payload()
    payload["keys"]   = [{"column": c} for c in group_cols]
    payload["values"] = [{
        "column": "Premium", "type": "double",
        "sum": True, "avg": False, "min": False, "max": False,
        "count": False, "countDistinct": False, "concat": False, "stddev": False,
    }]
    payload["outputColumnNameOverrides"] = {"Premium_sum": "Premium"}
    payload["globalCount"] = False
    settings.set_json_payload(payload)
    settings.save()

    # ── Build only if the dataset did not previously exist ────────────────────
    if not agg_exists:
        # Drop the Snowflake table if it was left over from a previous failed
        # run.  Without this, Dataiku's CREATE TABLE statement fails with
        # "Object already exists" even though the Dataiku dataset was deleted.
        try:
            catalog   = output_params.get("catalog", "")
            schema_nm = output_params.get("schema", "")
            table_nm  = output_params.get("table", "tbl_DetailedData_Agg")
            conn_name = output_params["connection"]
            full_ref  = (f"{catalog}.{schema_nm}.{table_nm}"
                         if catalog else f"{schema_nm}.{table_nm}")
            dataiku.SQLExecutor2(connection=conn_name).query_to_df(
                f"DROP TABLE IF EXISTS {full_ref}"
            )
        except Exception:
            pass  # non-critical; the build will surface any real SQL error

        print("building…", end=" ", flush=True)
        job_def = project.new_job("NON_RECURSIVE_FORCED_BUILD")
        job_def.with_output("tbl_DetailedData_Agg")
        job = job_def.start()

        while True:
            state = job.get_status().get("baseStatus", {}).get("state", "RUNNING")
            if state in ("DONE", "FAILED", "ABORTED"):
                break
            print(".", end="", flush=True)
            time.sleep(3)

        if state != "DONE":
            raise RuntimeError(
                f"Build job ended with state '{state}'. "
                f"Open compute_tbl_DetailedData_Agg in the Dataiku flow, "
                f"run it manually, then re-run this tool."
            )

        refreshed = {d["name"] for d in project.list_datasets()}
        if "tbl_DetailedData_Agg" not in refreshed:
            raise RuntimeError(
                f"Build completed but tbl_DetailedData_Agg was not found. "
                f"Open compute_tbl_DetailedData_Agg in the Dataiku flow, "
                f"run it manually, then re-run this tool."
            )

        print(" done.", flush=True)

    return True


def load_tbl_datasets():
    """
    Load all in-scope tbl_* datasets present in the Dataiku project.

    tbl_DetailedData is accessed via a pre-aggregated dataset
    (tbl_DetailedData_Agg) produced by a Dataiku Group recipe.  That recipe
    groups tbl_DetailedData by all non-numeric columns and sums Premium; the
    build runs at the storage layer (SQL / Spark), so it is fast even for
    very large tables.  On the first run the recipe is created and built
    automatically; subsequent runs reuse the existing dataset.

    Falls back to a direct load of tbl_DetailedData if the Group recipe
    approach fails for any reason.

    Returns {table_name: DataFrame} for tables that exist and load
    successfully.  The aggregated DetailedData is stored under the key
    "tbl_DetailedData" so all downstream check functions work unchanged.
    """
    project  = dataiku.api_client().get_project(dataiku.default_project_key())
    existing = {d["name"] for d in project.list_datasets()}

    # ── Step 1: load all reference tables first (they are small) ─────────────
    loaded, skipped = {}, []
    ref_names = [n for n in WIKI_TABLES if n != "tbl_DetailedData"]
    for name in ref_names:
        if name not in existing:
            skipped.append(name)
            continue
        try:
            print(f"  Loading {name}…", end=" ", flush=True)
            loaded[name] = dataiku.Dataset(name).get_dataframe()
            print("done.", flush=True)
        except Exception as e:
            print(f"failed: {e}", flush=True)

    # ── Step 2: ensure the pre-aggregated DetailedData exists, then load it ──
    if "tbl_DetailedData" not in existing:
        skipped.append("tbl_DetailedData")
    else:
        _ensure_dd_aggregated(project, loaded)

        print(f"  Loading tbl_DetailedData_Agg…", end=" ", flush=True)
        try:
            loaded["tbl_DetailedData"] = dataiku.Dataset("tbl_DetailedData_Agg").get_dataframe()
            print("done.", flush=True)
        except Exception as e:
            print(f"failed: {e}", flush=True)

    if skipped:
        print(f"  Not in project (skipped): {', '.join(skipped)}", flush=True)
    return loaded


def classify_tables(datasets):
    """
    Split loaded datasets into:
      dd_df      — tbl_DetailedData
      mapping_df — tbl_Key_Mapping
      ref_tables — all other loaded tables  {name: DataFrame}
    """
    dd_df      = datasets.get("tbl_DetailedData")
    mapping_df = datasets.get("tbl_Key_Mapping")
    ref_tables = {k: v for k, v in datasets.items()
                  if k not in ("tbl_DetailedData", "tbl_Key_Mapping")}
    return dd_df, mapping_df, ref_tables


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — COLUMN CLASSIFICATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_join_key(ref_df, dd_df):
    """
    The natural join key between a reference table and tbl_DetailedData:
    all columns that appear in BOTH tables and are non-numeric (categorical).

    This covers Key_* segment keys, FullKey segmentation columns, and any other
    shared categorical columns — no dependency on global variable lists.

    Key_Forecast is excluded: the forecast grain can be finer than the modelling
    grain, so it is not expected to appear in DetailedData.
    """
    dd_cols = set(dd_df.columns)
    return [c for c in ref_df.columns
            if c in dd_cols
            and c != "Key_Forecast"
            and not pd.api.types.is_numeric_dtype(ref_df[c])]


def get_key_cols(df):
    """All Key_* columns in df."""
    return [c for c in df.columns if c.startswith("Key_")]


def get_display_cols(ref_df, dd_df):
    """
    Join key columns minus structural columns (AY/UWY/DevM).
    These are the columns shown in the failure output tables.
    """
    return [c for c in get_join_key(ref_df, dd_df) if c not in STRUCTURAL_COLS]


def get_context_cols(ref_df, dd_df):
    """
    Non-Key_* display columns — the segmentation context columns used
    for parent-column checks and display alongside Key_* columns.
    """
    return [c for c in get_display_cols(ref_df, dd_df) if not c.startswith("Key_")]


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — CHECK FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

# ── Check 1: Key Column Registration ─────────────────────────────────────────

def check_key_registration(dd_df, mapping_df, ref_tables):
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


# ── Checks 2 + 3: Referential Integrity ──────────────────────────────────────

def check_referential_integrity(dd_df, ref_tables):
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


# ── Row Uniqueness ────────────────────────────────────────────────────────────

def check_row_uniqueness(dd_df, ref_tables):
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


# ── Check 4 + 5: Parent Column Rule ──────────────────────────────────────────

def check_parent_columns(dd_df, mapping_df, ref_tables):
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


# ── Check 6: Mapping Table Uniqueness ─────────────────────────────────────────

def check_mapping_uniqueness(mapping_df):
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


# ── Check 7: Value Range Checks ───────────────────────────────────────────────

def _rate_or_trend_check(vals, table_name, col_name):
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


def check_value_ranges(dd_df, ref_tables):
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


# ── Check 8: Key_Modelling Coverage ──────────────────────────────────────────

def check_key_modelling_unmapped(dd_df):
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


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — HTML DISPLAY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_COLOR = {"PASS": "#2e7d32", "WARNING": "#e65100", "FAIL": "#b71c1c", "SKIP": "#616161"}
_STATUS_BG    = {"PASS": "#e8f5e9", "WARNING": "#fff3e0", "FAIL": "#ffebee",  "SKIP": "#f5f5f5"}

_CSS = """
<style>
.dct { font-family: Arial, sans-serif; font-size: 0.88em; color: #212121; }
.dct-h2 {
    font-size: 1.05em; font-weight: bold; margin: 22px 0 8px;
    color: #1a237e; border-bottom: 2px solid #c5cae9; padding-bottom: 4px;
}
.dct-tname { font-family: monospace; font-weight: bold; color: #283593; margin: 10px 0 2px; }
.dct-note  { margin: 3px 0 3px 14px; }
.dct-badge {
    display: inline-block; padding: 1px 7px; border-radius: 3px;
    font-weight: bold; font-size: 0.88em;
}
table.dct-t { border-collapse: collapse; margin: 4px 0 10px; font-size: 0.9em; }
table.dct-t th {
    background: #37474f; color: white; padding: 3px 12px;
    text-align: left; font-weight: normal;
}
table.dct-t td { padding: 2px 12px; border-bottom: 1px solid #eeeeee; }
table.dct-t tr:nth-child(even) td { background: #fafafa; }
</style>
"""

def _badge(status):
    c  = _STATUS_COLOR.get(status, "#000")
    bg = _STATUS_BG.get(status, "#fff")
    return f'<span class="dct-badge" style="color:{c};background:{bg};">{status}</span>'


def _df_to_html(df, max_rows=10):
    """Render a DataFrame as a compact HTML table, capped at max_rows."""
    if df is None or df.empty:
        return "<em style='color:#9e9e9e'>No rows</em>"
    d     = df.head(max_rows)
    heads = "".join(f"<th>{c}</th>" for c in d.columns)
    rows  = "".join(
        f"<tr>{''.join(f'<td>{v}</td>' for v in row)}</tr>"
        for row in d.itertuples(index=False)
    )
    more  = (
        f"<tr><td colspan='{len(d.columns)}' style='color:#9e9e9e;font-style:italic'>"
        f"… {len(df) - max_rows} more rows not shown</td></tr>"
        if len(df) > max_rows else ""
    )
    return (f"<table class='dct-t'><thead><tr>{heads}</tr></thead>"
            f"<tbody>{rows}{more}</tbody></table>")


def _trim_to_display(df, ref_df, dd_df, extra_cols=()):
    """
    Keep only display columns (join key minus structural) plus any extra_cols
    (e.g. Premium, Premium_%) that happen to be present.
    """
    display_col_set = set(get_display_cols(ref_df, dd_df)) | set(extra_cols)
    keep = [c for c in df.columns if c in display_col_set]
    return df[keep] if keep else df


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — DISPLAY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────



def display_banner(status, n_loaded):
    c  = _STATUS_COLOR[status]
    bg = _STATUS_BG[status]
    html = (
        f"{_CSS}<div class='dct'>"
        f"<div style='background:{bg};border:2px solid {c};padding:14px 22px;"
        f"border-radius:4px;margin-bottom:4px;'>"
        f"<span style='font-size:1.5em;font-weight:bold;color:{c};'>"
        f"PAT Data Consistency — {status}</span>"
        f"<span style='color:#616161;margin-left:18px;font-size:0.9em;'>"
        f"{n_loaded} table(s) loaded</span>"
        f"</div></div>"
    )
    display(HTML(html))


def display_check1(reg_results):
    parts = [f"<div class='dct'><div class='dct-h2'>Check 1 — Key Column Registration</div>"]
    fail_tables = {t: r for t, r in reg_results.items() if r["status"] != "PASS"}
    pass_tables = [t for t, r in reg_results.items() if r["status"] == "PASS"]

    for tbl, res in fail_tables.items():
        parts.append(f"<div class='dct-tname'>{tbl} {_badge('FAIL')}</div>")
        rows = "".join(
            f"<tr><td>{m['key_col']}</td><td>{m['missing_from']}</td></tr>"
            for m in res["missing"]
        )
        parts.append(
            f"<table class='dct-t'><thead><tr>"
            f"<th>Key Column</th><th>Missing From</th>"
            f"</tr></thead><tbody>{rows}</tbody></table>"
        )

    if pass_tables:
        parts.append(
            f"<div style='color:#2e7d32;margin:4px 0;'>"
            f"{_badge('PASS')} {len(pass_tables)} table(s): "
            f"{', '.join(pass_tables)}</div>"
        )
    parts.append("</div>")
    display(HTML("".join(parts)))


def display_ri_and_uniqueness(ri_results, uniq_results, parent_results, dd_df, ref_tables):
    parts = [
        f"<div class='dct'>"
        f"<div class='dct-h2'>"
        f"Checks 2–5 — Referential Integrity, Row Uniqueness &amp; Parent Columns"
        f"</div>"
    ]

    # All reference table names (exclude mapping-specific entries)
    all_tables = sorted({
        t for t in list(ri_results) + list(uniq_results) + list(parent_results)
        if "Key_Mapping" not in t
    })

    pass_tables = []
    for tbl in all_tables:
        ri     = ri_results.get(tbl, {})
        uniq   = uniq_results.get(tbl, {})
        parent = parent_results.get(tbl, {})

        statuses   = [ri.get("status","PASS"), uniq.get("status","PASS"), parent.get("status","PASS")]
        tbl_status = "FAIL" if "FAIL" in statuses else ("WARNING" if "WARNING" in statuses else "PASS")

        if tbl_status == "PASS":
            pass_tables.append(tbl)
            continue

        ref_df = ref_tables.get(tbl)
        parts.append(f"<div class='dct-tname'>{tbl} {_badge(tbl_status)}</div>")

        # Forward RI
        fwd = ri.get("forward", {})
        if fwd.get("status") == "FAIL":
            top10 = fwd["top10"].copy()
            if ref_df is not None:
                top10 = _trim_to_display(top10, ref_df, dd_df, extra_cols=("Premium", "Premium_%"))
            summary = f"{fwd['total_unmatched']} unmatched combination(s)"
            if fwd.get("total_premium") is not None:
                summary += (f" — {fwd['total_premium']:,.0f} premium "
                            f"({fwd['premium_pct']:.1f}% of total)")
            n_shown = min(10, len(fwd["top10"]))
            parts.append(
                f"<div class='dct-note'><b>DetailedData → Reference table</b> {_badge('FAIL')} {summary} "
                f"(top {n_shown} by premium shown)</div>"
            )
            parts.append(_df_to_html(top10, max_rows=10))

        # Reverse RI
        rev = ri.get("reverse", {})
        if rev.get("status") == "WARNING":
            # tbl_Forecast: show orphaned rows with cell-level premium attached
            if tbl == "tbl_Forecast" and rev.get("forecast_orphaned_df") is not None:
                orphaned_display = rev["forecast_orphaned_df"]
            else:
                orphaned_display = rev["orphaned"].copy()
                if ref_df is not None:
                    orphaned_display = _trim_to_display(orphaned_display, ref_df, dd_df)
            summary = f"{rev['total_orphaned']} orphaned combination(s) in reference table"
            fp = rev.get("forecast_orphaned_premium")
            if fp is not None:
                summary += f" — {fp:,.0f} forecast premium affected"
            parts.append(
                f"<div class='dct-note'><b>Reference table → DetailedData</b> {_badge('WARNING')} "
                f"{summary}</div>"
            )
            parts.append(_df_to_html(orphaned_display, max_rows=10))

        # Row uniqueness
        if uniq.get("status") == "FAIL":
            dups = uniq["duplicates"].copy()
            if ref_df is not None:
                year_cols = tuple(c for c in ("AY", "UWY", "UY") if c in dups.columns)
                dev_cols  = ("DevM",) if tbl in PATTERN_TABLES else ()
                dups  = _trim_to_display(dups, ref_df, dd_df, extra_cols=year_cols + dev_cols)
            parts.append(
                f"<div class='dct-note'><b>Row uniqueness</b> {_badge('FAIL')} "
                f"{uniq['duplicate_count']} duplicate combination(s) — "
                f"join to DetailedData is ambiguous</div>"
            )
            parts.append(_df_to_html(dups, max_rows=10))

        # Parent columns — text note only (column names, not row-level detail)
        if parent.get("failing_cols"):
            notes = "; ".join(
                f"{f['column']} (vs {f['key_col']})" for f in parent["failing_cols"]
            )
            parts.append(
                f"<div class='dct-note'><b>Parent column</b> {_badge('FAIL')} "
                f"non-parent columns: {notes}</div>"
            )

        # Domain membership (tbl_Min_Large_Load only)
        for df_fail in parent.get("domain_fails", []):
            vals_str = ", ".join(str(v) for v in df_fail["invalid_values"][:10])
            extra    = "…" if len(df_fail["invalid_values"]) > 10 else ""
            parts.append(
                f"<div class='dct-note'><b>Domain membership</b> {_badge('FAIL')} "
                f"{df_fail['column']}: values not in DetailedData: {vals_str}{extra}</div>"
            )

    # PASS summary line
    if pass_tables:
        parts.append(
            f"<div style='color:#2e7d32;margin:8px 0;'>"
            f"{_badge('PASS')} {len(pass_tables)} table(s): "
            f"{', '.join(pass_tables)}</div>"
        )

    parts.append("</div>")
    display(HTML("".join(parts)))


def display_mapping(map_uniq):
    sec_status = map_uniq.get("status", "PASS")

    parts = [
        f"<div class='dct'>"
        f"<div class='dct-h2'>Check 6 — tbl_Key_Mapping {_badge(sec_status)}</div>"
    ]

    u = map_uniq.get("status", "SKIP")
    parts.append(f"<div class='dct-note'><b>Non-Key_* uniqueness</b> {_badge(u)} ")
    if u == "FAIL":
        parts.append(f"— {map_uniq['total_duplicates']} duplicate combination(s)</div>")
        parts.append(_df_to_html(map_uniq.get("duplicates")))
    elif u == "PASS":
        parts.append("— No duplicate combinations.</div>")
    else:
        parts.append(f"— {map_uniq.get('reason', '')}</div>")

    parts.append("</div>")
    display(HTML("".join(parts)))


def display_value_ranges(value_results):
    parts = [
        f"<div class='dct'>"
        f"<div class='dct-h2'>Check 7 — Value Range Checks</div>"
    ]

    if not value_results:
        parts.append("<p style='color:#9e9e9e'>No value range checks performed.</p></div>")
        display(HTML("".join(parts)))
        return

    # Summary table for all non-DevM checks
    summary_rows = "".join(
        f"<tr><td>{r['table']}</td><td>{r['column']}</td>"
        f"<td>{_badge(r['status'])}</td><td>{r['detail']}</td></tr>"
        for r in value_results.values()
        if r["column"] != "DevM"
    )
    if summary_rows:
        parts.append(
            f"<table class='dct-t'><thead><tr>"
            f"<th>Table</th><th>Column</th><th>Status</th><th>Detail</th>"
            f"</tr></thead><tbody>{summary_rows}</tbody></table>"
        )

    # DevM detail — failing patterns only
    for key, res in value_results.items():
        if res["column"] != "DevM" or not res.get("failing_patterns"):
            continue
        parts.append(
            f"<div class='dct-note'><b>{res['table']} — DevM sequence failures "
            f"({len(res['failing_patterns'])} shown)</b></div>"
        )
        rows = "".join(
            f"<tr><td>{fp['pattern_key']}</td><td>{fp['first_DevM']}</td>"
            f"<td>{fp['last_DevM']}</td><td>{fp['reasons']}</td></tr>"
            for fp in res["failing_patterns"]
        )
        parts.append(
            f"<table class='dct-t'><thead><tr>"
            f"<th>Pattern Key</th><th>First DevM</th><th>Last DevM</th><th>Issues</th>"
            f"</tr></thead><tbody>{rows}</tbody></table>"
        )

    parts.append("</div>")
    display(HTML("".join(parts)))


def display_key_modelling(km_results):
    if not km_results:
        return
    sec_status = ("WARNING" if any(r["status"] == "WARNING" for r in km_results.values())
                  else "PASS")
    parts = [
        f"<div class='dct'>"
        f"<div class='dct-h2'>Check 8 — Key_Modelling Coverage {_badge(sec_status)}</div>"
    ]
    rows = "".join(
        f"<tr><td style='font-family:monospace'>{r['column']}</td>"
        f"<td>{_badge(r['status'])}</td>"
        f"<td style='text-align:right'>{r['unmapped_count']:,}</td>"
        f"<td style='text-align:right'>{r['unmapped_premium']:,.0f}</td>"
        f"<td style='text-align:right'>{r['premium_pct']:.1f}%</td></tr>"
        for r in km_results.values()
    )
    parts.append(
        f"<table class='dct-t'><thead><tr>"
        f"<th>Column</th><th>Status</th>"
        f"<th>Unmapped rows</th><th>Unmapped premium</th><th>% of total</th>"
        f"</tr></thead><tbody>{rows}</tbody></table>"
        f"<div class='dct-note' style='color:#616161;font-size:0.85em'>"
        f"Unmapped = null or blank value — these rows will not join to any reference table.</div>"
    )
    parts.append("</div>")
    display(HTML("".join(parts)))


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def _overall_status(reg, ri, uniq, parents, map_uniq, values, km):
    """Derive overall PASS / WARNING / FAIL from all check result dicts."""
    statuses = []
    for d in (reg, ri, uniq, parents):
        statuses.extend(v.get("status", "PASS") for v in d.values())
    statuses.append(map_uniq.get("status", "PASS"))
    statuses.extend(v.get("status", "PASS") for v in values.values())
    statuses.extend(v.get("status", "PASS") for v in km.values())
    if "FAIL"    in statuses: return "FAIL"
    if "WARNING" in statuses: return "WARNING"
    return "PASS"


def run_and_display():
    """
    Run all PAT data consistency checks and display results inline in the notebook.
    Call this function from a Dataiku Python notebook in the PAT Data project.
    """
    print("Loading datasets…", flush=True)
    try:
        datasets = load_tbl_datasets()
    except RuntimeError as e:
        display(HTML(
            f"{_CSS}<div class='dct'>"
            f"<div style='background:#ffebee;border:2px solid #b71c1c;padding:14px 22px;"
            f"border-radius:4px;'>"
            f"<span style='font-size:1.1em;font-weight:bold;color:#b71c1c;'>"
            f"Cannot run — tbl_DetailedData_Agg not ready</span><br><br>"
            f"<span style='color:#424242;'>{e}</span><br><br>"
            f"<b>What to do:</b> Open the Dataiku flow, locate "
            f"<code>compute_tbl_DetailedData_Agg</code>, run it manually, "
            f"then re-run this tool."
            f"</div></div>"
        ))
        return
    print(f"  Loaded {len(datasets)} table(s): {', '.join(datasets)}", flush=True)

    dd_df, mapping_df, ref_tables = classify_tables(datasets)

    if dd_df is None:
        display(HTML(
            "<b style='color:#b71c1c'>tbl_DetailedData not found — cannot run checks.</b>"
        ))
        return

    print("Running checks…", flush=True)
    print("  1/7 Key registration…",     end=" ", flush=True)
    reg_results    = check_key_registration(dd_df, mapping_df, ref_tables)
    print("done.", flush=True)
    print("  2/7 Referential integrity…", end=" ", flush=True)
    ri_results     = check_referential_integrity(dd_df, ref_tables)
    print("done.", flush=True)
    print("  3/7 Row uniqueness…",        end=" ", flush=True)
    uniq_results   = check_row_uniqueness(dd_df, ref_tables)
    print("done.", flush=True)
    print("  4/7 Parent columns…",        end=" ", flush=True)
    parent_results = check_parent_columns(dd_df, mapping_df, ref_tables)
    print("done.", flush=True)
    print("  5/7 Mapping uniqueness…",    end=" ", flush=True)
    map_uniq       = check_mapping_uniqueness(mapping_df)
    print("done.", flush=True)
    print("  6/7 Value ranges…",          end=" ", flush=True)
    value_results  = check_value_ranges(dd_df, ref_tables)
    print("done.", flush=True)
    print("  7/7 Key_Modelling coverage…", end=" ", flush=True)
    km_results     = check_key_modelling_unmapped(dd_df)
    print("done.\n", flush=True)

    overall = _overall_status(
        reg_results, ri_results, uniq_results, parent_results, map_uniq, value_results, km_results
    )

    display_banner(overall, len(datasets))
    display_check1(reg_results)
    display_ri_and_uniqueness(ri_results, uniq_results, parent_results, dd_df, ref_tables)
    display_mapping(map_uniq)
    display_value_ranges(value_results)
    display_key_modelling(km_results)


run_and_display()
