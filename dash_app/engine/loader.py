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
import logging

import pandas as pd

logger = logging.getLogger(__name__)

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

AGG_DATASET_NAME = "tbl_DetailedData_Agg"
AGG_RECIPE_NAME = "compute_tbl_DetailedData_Agg"


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


def _get_agg_output_spec(project: Any, dataiku_module: Any) -> tuple[str, dict[str, Any]]:
    """Return output type/params for the aggregated DetailedData dataset."""
    dd_raw = project.get_dataset("tbl_DetailedData").get_settings().get_raw()
    project_key = dataiku_module.default_project_key()
    output_type = str(dd_raw["type"]).lower()
    output_params = dict(dd_raw["params"])
    output_params["table"] = f"{project_key}_TBL_DETAILEDDATA_AGG"
    for drop_key in ("partitioning", "skipRows", "maxRows", "filterQuery"):
        output_params.pop(drop_key, None)
    return output_type, output_params


def _get_recipe_output_refs(recipe: Any) -> set[str]:
    """Return flattened output dataset refs for a recipe."""
    try:
        return set(recipe.get_settings().get_flat_output_refs())
    except Exception:
        logger.exception("Failed to inspect recipe outputs for %s", AGG_RECIPE_NAME)
        return set()


def _delete_agg_dataset(project: Any) -> None:
    """Delete the agg dataset from Dataiku metadata if it exists."""
    try:
        project.delete_dataset(AGG_DATASET_NAME)
        logger.info("Deleted dataset: %s", AGG_DATASET_NAME)
        return
    except Exception:
        logger.debug("project.delete_dataset unavailable or failed for %s", AGG_DATASET_NAME)

    try:
        project.get_dataset(AGG_DATASET_NAME).delete()
        logger.info("Deleted dataset via dataset handle: %s", AGG_DATASET_NAME)
    except Exception:
        logger.exception("Failed to delete dataset: %s", AGG_DATASET_NAME)
        raise


def _delete_agg_recipe(project: Any) -> None:
    """Delete the agg recipe from Dataiku metadata if it exists."""
    try:
        project.delete_recipe(AGG_RECIPE_NAME)
        logger.info("Deleted recipe: %s", AGG_RECIPE_NAME)
        return
    except Exception:
        logger.debug("project.delete_recipe unavailable or failed for %s", AGG_RECIPE_NAME)

    try:
        project.get_recipe(AGG_RECIPE_NAME).delete()
        logger.info("Deleted recipe via recipe handle: %s", AGG_RECIPE_NAME)
    except Exception:
        logger.exception("Failed to delete recipe: %s", AGG_RECIPE_NAME)
        raise


def _drop_agg_snowflake_table(project: Any, dataiku_module: Any) -> None:
    """Best-effort cleanup of the physical Snowflake table behind the agg dataset."""
    _, output_params = _get_agg_output_spec(project, dataiku_module)

    try:
        catalog = output_params.get("catalog", "")
        schema_nm = output_params.get("schema", "")
        table_nm = output_params.get("table", AGG_DATASET_NAME)
        conn_name = output_params["connection"]
        full_ref = f"{catalog}.{schema_nm}.{table_nm}" if catalog else f"{schema_nm}.{table_nm}"
        dataiku_module.SQLExecutor2(connection=conn_name).query_to_df(
            f"DROP TABLE IF EXISTS {full_ref}"
        )
        logger.info("DetailedData_Agg Snowflake cleanup executed: %s", full_ref)
    except Exception:
        logger.exception("DetailedData_Agg Snowflake cleanup failed")


def _create_agg_recipe(project: Any, output_type: str) -> Any:
    """Create a new grouping recipe with a new agg output dataset."""
    creator = project.new_recipe("grouping")
    creator.set_name(AGG_RECIPE_NAME)
    creator.with_input("tbl_DetailedData")
    creator.with_new_output(AGG_DATASET_NAME, output_type)
    recipe = creator.create()
    logger.info("Created recipe %s with new output %s (%s)", AGG_RECIPE_NAME, AGG_DATASET_NAME, output_type)
    return recipe


def _configure_agg_recipe(recipe: Any, group_cols: list[str]) -> None:
    """Apply current grouping and aggregation settings to the agg recipe."""
    settings = recipe.get_settings()
    payload = settings.get_json_payload()
    payload["keys"] = [{"column": column} for column in group_cols]
    payload["values"] = [{
        "column": "Premium", "type": "double",
        "sum": True, "avg": False, "min": False, "max": False,
        "count": False, "countDistinct": False, "concat": False, "stddev": False,
    }]
    payload["outputColumnNameOverrides"] = {"Premium_sum": "Premium"}
    payload["globalCount"] = False
    settings.set_json_payload(payload)
    settings.save()
    logger.info("DetailedData_Agg recipe settings saved (keys=%d)", len(group_cols))


def _build_agg_recipe(project: Any) -> None:
    """Run the agg recipe and verify the output dataset exists afterward."""
    import time

    logger.info("DetailedData_Agg build starting")
    print("building…", end=" ", flush=True)
    job_def = project.new_job("NON_RECURSIVE_FORCED_BUILD")
    job_def.with_output(AGG_DATASET_NAME)
    job = job_def.start()

    while True:
        state = job.get_status().get("baseStatus", {}).get("state", "RUNNING")
        if state in ("DONE", "FAILED", "ABORTED"):
            break
        print(".", end="", flush=True)
        time.sleep(3)

    logger.info("DetailedData_Agg build finished with state=%s", state)
    if state != "DONE":
        raise RuntimeError(
            f"Build job ended with state '{state}'. "
            f"Open {AGG_RECIPE_NAME} in the Dataiku flow, "
            f"run it manually, then re-run this tool."
        )

    refreshed = {dataset["name"] for dataset in project.list_datasets()}
    if AGG_DATASET_NAME not in refreshed:
        raise RuntimeError(
            f"Build completed but {AGG_DATASET_NAME} was not found. "
            f"Open {AGG_RECIPE_NAME} in the Dataiku flow, "
            f"run it manually, then re-run this tool."
        )

    logger.info("DetailedData_Agg build verified in dataset list")
    print(" done.", flush=True)


def _ensure_dd_aggregated(project: Any, loaded_ref_tables: dict[str, pd.DataFrame]) -> bool:
    """
    Ensure the agg dataset / grouping recipe pair is consistent, then rebuild.

    Supported paths:
      - Missing pair: create a new grouping recipe with a new agg output, then build.
      - Ghost / broken pair: warn, delete stale metadata/data, recreate, then build.
      - Healthy pair: update recipe settings, then rebuild so data matches settings.
    """
    import dataiku  # lazy import — only available in Dataiku runtime

    existing_datasets = {dataset["name"] for dataset in project.list_datasets()}
    existing_recipes = {recipe["name"] for recipe in project.list_recipes()}
    agg_exists = AGG_DATASET_NAME in existing_datasets
    recipe_exists = AGG_RECIPE_NAME in existing_recipes
    group_cols = _get_dd_group_cols(loaded_ref_tables)
    recipe_points_to_agg = False
    recipe = None

    if recipe_exists:
        recipe = project.get_recipe(AGG_RECIPE_NAME)
        recipe_outputs = _get_recipe_output_refs(recipe)
        recipe_points_to_agg = AGG_DATASET_NAME in recipe_outputs
        logger.info(
            "DetailedData_Agg recipe inspect: outputs=%s, points_to_agg=%s",
            sorted(recipe_outputs),
            recipe_points_to_agg,
        )

    logger.info(
        "DetailedData_Agg ensure start: agg_exists=%s, recipe_exists=%s, recipe_points_to_agg=%s, ref_tables_loaded=%d, group_cols=%d",
        agg_exists,
        recipe_exists,
        recipe_points_to_agg,
        len(loaded_ref_tables),
        len(group_cols),
    )
    logger.debug("DetailedData_Agg group columns: %s", group_cols)

    healthy_pair = agg_exists and recipe_exists and recipe_points_to_agg
    logger.info("DetailedData_Agg branch evaluation: healthy_pair=%s", healthy_pair)

    if not healthy_pair:
        output_type, _ = _get_agg_output_spec(project, dataiku)

        if agg_exists and (not recipe_exists or not recipe_points_to_agg):
            logger.warning("DetailedData_Agg branch selected: ghost_dataset_recreate")
            logger.warning(
                "Found _Agg dataset but no group by recipe pointing to it, will proceed by dropping the data and recreating the recipe"
            )
        elif not agg_exists and recipe_exists:
            logger.warning("DetailedData_Agg branch selected: broken_recipe_recreate")
            logger.warning(
                "Found %s recipe but no _Agg dataset, will proceed by recreating the recipe",
                AGG_RECIPE_NAME,
            )
        else:
            logger.info("DetailedData_Agg branch selected: missing_pair_recreate")
            logger.info("DetailedData_Agg missing together with recipe; creating from scratch")

        if recipe_exists:
            _delete_agg_recipe(project)
        if agg_exists:
            _delete_agg_dataset(project)

        _drop_agg_snowflake_table(project, dataiku)

        print(f"  Creating {AGG_RECIPE_NAME}…", end=" ", flush=True)
        recipe = _create_agg_recipe(project, output_type)
        print("done.", flush=True)
    else:
        logger.info("DetailedData_Agg branch selected: healthy_pair_rebuild")
        logger.info("DetailedData_Agg healthy pair found; updating settings and rebuilding")

    _configure_agg_recipe(recipe, group_cols)
    _build_agg_recipe(project)
    logger.info("DetailedData_Agg ensure complete")
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
    very large tables.  Each run first ensures the dataset/recipe pairing is
    healthy, then rebuilds the recipe so the loaded data matches current
    reference-table-driven grouping settings.

    Returns {table_name: DataFrame} for tables that exist and load
    successfully.  The aggregated DetailedData is stored under the key
    "tbl_DetailedData" so all downstream check functions work unchanged.
    """
    import dataiku  # lazy import — only available in Dataiku runtime

    project  = dataiku.api_client().get_project(dataiku.default_project_key())
    existing = {d["name"] for d in project.list_datasets()}
    logger.info("Dataset load start: project has %d dataset(s)", len(existing))

    # ── Step 1: load all reference tables first (they are small) ─────────────
    loaded, skipped = {}, []
    ref_names = [n for n in WIKI_TABLES if n != "tbl_DetailedData"]
    for name in ref_names:
        if name not in existing:
            skipped.append(name)
            logger.info("Dataset missing (skipped): %s", name)
            continue
        try:
            print(f"  Loading {name}…", end=" ", flush=True)
            loaded[name] = dataiku.Dataset(name).get_dataframe()
            print("done.", flush=True)
            logger.info("Loaded dataset: %s (rows=%d, cols=%d)", name, len(loaded[name]), len(loaded[name].columns))
        except Exception as e:
            print(f"failed: {e}", flush=True)
            logger.exception("Failed loading dataset: %s", name)

    # ── Step 2: ensure the pre-aggregated DetailedData exists, then load it ──
    if "tbl_DetailedData" not in existing:
        skipped.append("tbl_DetailedData")
        logger.warning("tbl_DetailedData missing; DetailedData_Agg path skipped")
    else:
        logger.info("Ensuring DetailedData_Agg before loading DetailedData")
        _ensure_dd_aggregated(project, loaded)

        print(f"  Loading tbl_DetailedData_Agg…", end=" ", flush=True)
        try:
            loaded["tbl_DetailedData"] = dataiku.Dataset("tbl_DetailedData_Agg").get_dataframe()
            print("done.", flush=True)
            logger.info(
                "Loaded tbl_DetailedData from tbl_DetailedData_Agg (rows=%d, cols=%d)",
                len(loaded["tbl_DetailedData"]),
                len(loaded["tbl_DetailedData"].columns),
            )
        except Exception as e:
            print(f"failed: {e}", flush=True)
            logger.exception("Failed loading tbl_DetailedData_Agg")

    if skipped:
        print(f"  Not in project (skipped): {', '.join(skipped)}", flush=True)
        logger.info("Datasets skipped: %s", skipped)
    logger.info("Dataset load complete: loaded=%d skipped=%d", len(loaded), len(skipped))
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
