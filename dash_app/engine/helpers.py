"""
dash_app/engine/helpers.py
==========================
Column classification and join-key helpers.

All logic is copied verbatim from data_consistency_checks.py (v20260423) with
no semantic changes.  This module has no dependency on the Dataiku SDK and
can be unit-tested outside of a Dataiku runtime.
"""
from __future__ import annotations

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Columns excluded from display output and from parent-column checks.
# Year columns (AY/UWY/UY) and DevM are structural dimensions, not segmentation keys.
STRUCTURAL_COLS = {"AY", "UWY", "UY", "DevM"}

# Pattern tables — DevM is part of their natural uniqueness key.
PATTERN_TABLES = {"tbl_Patterns_Attr", "tbl_Patterns_Large", "tbl_Patterns_Prem"}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

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


def get_join_key(ref_df: pd.DataFrame, dd_df: pd.DataFrame) -> list:
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


def get_key_cols(df: pd.DataFrame) -> list:
    """All Key_* columns in df."""
    return [c for c in df.columns if c.startswith("Key_")]


def get_display_cols(ref_df: pd.DataFrame, dd_df: pd.DataFrame) -> list:
    """
    Join key columns minus structural columns (AY/UWY/DevM).
    These are the columns shown in the failure output tables.
    """
    return [c for c in get_join_key(ref_df, dd_df) if c not in STRUCTURAL_COLS]


def get_context_cols(ref_df: pd.DataFrame, dd_df: pd.DataFrame) -> list:
    """
    Non-Key_* display columns — the segmentation context columns used
    for parent-column checks and display alongside Key_* columns.
    """
    return [c for c in get_display_cols(ref_df, dd_df) if not c.startswith("Key_")]
