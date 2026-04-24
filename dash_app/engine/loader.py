"""
dash_app/engine/loader.py
=========================
Dataset loading and table classification for the PAT Data Consistency Tool.

All logic is copied verbatim from data_consistency_checks.py (v20260423) with
no semantic changes.  The ``dataiku`` SDK is imported lazily inside each
function that needs it so this module does not raise ImportError in non-Dataiku
environments (e.g. local development).
"""
from __future__ import annotations

from typing import Any

import pandas as pd

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


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_dd_group_cols(loaded_ref_tables: dict[str, pd.DataFrame]) -> list[str]:
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
    import dataiku  # lazy import — only available in Dataiku runtime

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


def _ensure_dd_aggregated(project: Any, loaded_ref_tables: dict[str, pd.DataFrame]) -> bool:
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
    import dataiku  # lazy import — only available in Dataiku runtime
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


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def load_tbl_datasets() -> dict[str, pd.DataFrame]:
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
    import dataiku  # lazy import — only available in Dataiku runtime

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


def classify_tables(
    datasets: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, dict[str, pd.DataFrame]]:
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
