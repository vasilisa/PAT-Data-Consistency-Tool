from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from dash_app.engine.checks import (
    check_key_modelling_unmapped as new_check_key_modelling_unmapped,
    check_key_registration as new_check_key_registration,
    check_mapping_uniqueness as new_check_mapping_uniqueness,
    check_parent_columns as new_check_parent_columns,
    check_referential_integrity as new_check_referential_integrity,
    check_row_uniqueness as new_check_row_uniqueness,
    check_value_ranges as new_check_value_ranges,
)
from dash_app.engine.loader import classify_tables

ROOT = Path(__file__).resolve().parent
MOCK_DIR = ROOT / "mock_data"
SOURCE_FILE = ROOT / "data_consistency_checks.py"

NEEDED_ASSIGN_NAMES = {"STRUCTURAL_COLS", "PATTERN_TABLES"}
NEEDED_FUNC_NAMES = {
    "find_parent_cols",
    "get_join_key",
    "get_key_cols",
    "get_display_cols",
    "get_context_cols",
    "check_key_registration",
    "check_referential_integrity",
    "check_row_uniqueness",
    "check_parent_columns",
    "check_mapping_uniqueness",
    "_rate_or_trend_check",
    "check_value_ranges",
    "check_key_modelling_unmapped",
    "_overall_status",
}


def load_mock_datasets() -> dict[str, pd.DataFrame]:
    datasets: dict[str, pd.DataFrame] = {}
    for csv_file in sorted(MOCK_DIR.glob("tbl_*.csv")):
        datasets[csv_file.stem] = pd.read_csv(csv_file)
    return datasets


def extract_source_checks() -> dict[str, Any]:
    source = SOURCE_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(SOURCE_FILE))

    selected_nodes: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if names & NEEDED_ASSIGN_NAMES:
                selected_nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in NEEDED_FUNC_NAMES:
            selected_nodes.append(node)

    module = ast.Module(body=selected_nodes, type_ignores=[])
    ast.fix_missing_locations(module)

    ns: dict[str, Any] = {"pd": pd, "np": np}
    exec(compile(module, filename=str(SOURCE_FILE), mode="exec"), ns)
    return ns


def run_source(ns: dict[str, Any], datasets: dict[str, pd.DataFrame]) -> dict[str, Any]:
    dd_df, mapping_df, ref_tables = classify_tables(datasets)

    reg = ns["check_key_registration"](dd_df, mapping_df, ref_tables)
    ri = ns["check_referential_integrity"](dd_df, ref_tables)
    uniq = ns["check_row_uniqueness"](dd_df, ref_tables)
    parents = ns["check_parent_columns"](dd_df, mapping_df, ref_tables)
    map_uniq = ns["check_mapping_uniqueness"](mapping_df)
    values = ns["check_value_ranges"](dd_df, ref_tables)
    km = ns["check_key_modelling_unmapped"](dd_df)
    overall = ns["_overall_status"](reg, ri, uniq, parents, map_uniq, values, km)

    return {
        "check1": reg,
        "checks2_3": ri,
        "check4": uniq,
        "check5": parents,
        "check6": map_uniq,
        "check7": values,
        "check8": km,
        "overall": overall,
    }


def run_migrated(datasets: dict[str, pd.DataFrame], ns: dict[str, Any]) -> dict[str, Any]:
    dd_df, mapping_df, ref_tables = classify_tables(datasets)

    reg = new_check_key_registration(dd_df, mapping_df, ref_tables)
    ri = new_check_referential_integrity(dd_df, ref_tables)
    uniq = new_check_row_uniqueness(dd_df, ref_tables)
    parents = new_check_parent_columns(dd_df, mapping_df, ref_tables)
    map_uniq = new_check_mapping_uniqueness(mapping_df)
    values = new_check_value_ranges(dd_df, ref_tables)
    km = new_check_key_modelling_unmapped(dd_df)
    overall = ns["_overall_status"](reg, ri, uniq, parents, map_uniq, values, km)

    return {
        "check1": reg,
        "checks2_3": ri,
        "check4": uniq,
        "check5": parents,
        "check6": map_uniq,
        "check7": values,
        "check8": km,
        "overall": overall,
    }


def _canon(v: Any) -> Any:
    if isinstance(v, pd.DataFrame):
        rows = []
        for row in v.to_dict("records"):
            rows.append({k: _canon(val) for k, val in row.items()})
        return {"__df__": True, "columns": list(v.columns), "rows": rows}
    if isinstance(v, pd.Series):
        return [_canon(x) for x in v.tolist()]
    if isinstance(v, dict):
        return {k: _canon(v[k]) for k in sorted(v.keys())}
    if isinstance(v, list):
        return [_canon(x) for x in v]
    if isinstance(v, tuple):
        return tuple(_canon(x) for x in v)
    if isinstance(v, (np.floating, float)):
        if pd.isna(v):
            return None
        return round(float(v), 10)
    if isinstance(v, (np.integer, int)):
        return int(v)
    if pd.isna(v):
        return None
    return v


def compare_results(left: dict[str, Any], right: dict[str, Any]) -> tuple[bool, str]:
    l = _canon(left)
    r = _canon(right)

    l_json = json.dumps(l, sort_keys=True, ensure_ascii=True)
    r_json = json.dumps(r, sort_keys=True, ensure_ascii=True)
    if l_json == r_json:
        return True, ""
    return False, "canonicalized payload mismatch"


def scenario_missing_optional_tables(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    drop_tables = {"tbl_Trend", "tbl_Weight_HistYears"}
    return {k: v.copy() for k, v in datasets.items() if k not in drop_tables}


def scenario_invalid_asat_month(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    mutated = {k: v.copy() for k, v in datasets.items()}
    dd = mutated["tbl_DetailedData"].copy()
    if "AsAt_Month" in dd.columns and len(dd) >= 2:
        dd.loc[dd.index[0], "AsAt_Month"] = 202513
        dd.loc[dd.index[1], "AsAt_Month"] = 202402
    mutated["tbl_DetailedData"] = dd
    return mutated


def scenario_devm_gap(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    mutated = {k: v.copy() for k, v in datasets.items()}
    tbl = "tbl_Patterns_Attr"
    if tbl in mutated and "DevM" in mutated[tbl].columns:
        df = mutated[tbl].copy()
        candidate = df[df["DevM"].astype(str) == "2"]
        if not candidate.empty:
            df = df.drop(candidate.index[0])
        mutated[tbl] = df
    return mutated


def scenario_mapping_duplicate(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    mutated = {k: v.copy() for k, v in datasets.items()}
    tbl = "tbl_Key_Mapping"
    if tbl in mutated and not mutated[tbl].empty:
        df = mutated[tbl].copy()
        df = pd.concat([df, df.head(1)], ignore_index=True)
        mutated[tbl] = df
    return mutated


def run_scenario(
    name: str,
    transform: Callable[[dict[str, pd.DataFrame]], dict[str, pd.DataFrame]],
    ns: dict[str, Any],
    base: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    ds = transform(base)
    src = run_source(ns, ds)
    new = run_migrated(ds, ns)
    ok, reason = compare_results(src, new)
    return {
        "scenario": name,
        "pass": ok,
        "reason": reason,
        "source_overall": src["overall"],
        "migrated_overall": new["overall"],
    }


def main() -> int:
    base = load_mock_datasets()
    ns = extract_source_checks()

    scenarios: list[tuple[str, Callable[[dict[str, pd.DataFrame]], dict[str, pd.DataFrame]]]] = [
        ("baseline", lambda d: {k: v.copy() for k, v in d.items()}),
        ("missing_optional_tables", scenario_missing_optional_tables),
        ("invalid_asat_month", scenario_invalid_asat_month),
        ("devm_gap", scenario_devm_gap),
        ("mapping_duplicate", scenario_mapping_duplicate),
    ]

    results = [run_scenario(name, fn, ns, base) for name, fn in scenarios]
    all_pass = all(r["pass"] for r in results)

    print(json.dumps({"all_pass": all_pass, "results": results}, indent=2, ensure_ascii=True))
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
