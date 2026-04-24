#v20260414 NK : find_parent_cols added.
#v20260325 NK : aggregate_Date -> OLFactor_CY aggregation rule added. 
#v20260324 NK : aggregate_Date -> PctDev added. 
#               build submap added.                 

import dataiku
import pandas as pd, numpy as np
from dataiku.core.flow import FLOW
import logging
# Module-level logger for all PAT utility functions.
# Calling code can configure log level via logging.basicConfig() or
# Dataiku's built-in log handler — no changes needed here.
logger = logging.getLogger(__name__)

#functions that are commonly used in PAT.  
def PAT_na():
    #set NA value. set here, as this seems to be version sensitive, and to avoid mass correction. 
    return np.nan
#==========================================================
#Join Tables by common fields or specified joining keys 
#==========================================================
def JoinTables(dfMain, dfToJoin, joinKey =[],  joinMethod ='left', suffixRight=""):
    
    if not joinKey: 
        joinKey  = [c for c in dfToJoin.columns if c in dfMain.columns]
    
    print(joinKey)
    if joinKey: 
        dfOut = pd.merge(dfMain, dfToJoin, on = joinKey, how = joinMethod, suffixes=("",suffixRight))
    else: 
        dfOut = dfMain
    return dfOut

def join_tables_by_common_columns(dfMain, dfToJoin, joinKey =[],  joinMethod ='left', suffixRight=""):
    #same as above, keep both until the old fully replaced.
    if not joinKey: 
        joinKey  = [c for c in dfToJoin.columns if c in dfMain.columns]
    
    print(joinKey)
    if joinKey: 
        dfOut = pd.merge(dfMain, dfToJoin, on = joinKey, how = joinMethod, suffixes=("",suffixRight))
    else: 
        dfOut = dfMain
    return dfOut

#==========================================================
#Read global variable to list, and allow some type conversion. 
#==========================================================
def read_GlobalVariable(GVname, dataType=None):
    listGV = dataiku.get_custom_variables()[GVname].split(",")
    listGV = [x.strip() for x in listGV]
    if dataType == 'int': 
        listGV = [int(x.strip()) for x in listGV]
    elif dataType == 'float': 
        listGV = [float(x.strip()) for x in listGV]
    else:
        listGV = [x.strip() for x in listGV]
    return listGV

#==========================================================
# get input and output dataset names for the current recipe
#==========================================================
def get_output_dataset_names():

    project = dataiku.api_client().get_project(dataiku.default_project_key())

    # Get the current recipe name.  recipe name should be the currentActivityID minus the "_NP" suffix.
    current_recipe_name = FLOW["currentActivityId"][:-3]

    recipe = dataiku.api_client().get_project(dataiku.default_project_key()).get_recipe(current_recipe_name)
    recipe_settings = recipe.get_settings()
    #input_datasets = recipe_settings.get_flat_input_refs()
    output_datasets = recipe_settings.get_flat_output_refs()

    return output_datasets 
    #return input_datasets[0] if input_datasets else None

def get_input_dataset_names():

    project = dataiku.api_client().get_project(dataiku.default_project_key())

    # Get the current recipe name.  recipe name should be the currentActivityID minus the "_NP" suffix.
    current_recipe_name = FLOW["currentActivityId"][:-3]

    recipe = dataiku.api_client().get_project(dataiku.default_project_key()).get_recipe(current_recipe_name)
    recipe_settings = recipe.get_settings()
    input_datasets = recipe_settings.get_flat_input_refs()
    #output_datasets = recipe_settings.get_flat_output_refs()

    #return output_datasets 
    return input_datasets
    
#==========================================================
# identify dummy tables, and remove dummy rows. 
#==========================================================
def Dataset_is_InUse(dfIn):
    
    if dfIn.empty: 
        ansewr = False 
    else: 
        valInvalid = ["Dummy", "dummy", "Not in use", "not in use", "Not in Use"]
        for c in [c for c in dfIn.columns if 'Key' in c]:
            dfOut = dfIn[~dfIn[c].isin(valInvalid)]
        if len(dfOut)>0: 
            answer = True
        else: 
            answer = False
            
    return answer

def dataset_is_in_use(dfIn):
    #same as above, keep both until all migrated to new. 
    
    if dfIn.empty: 
        ansewr = False 
    else: 
        valInvalid = ["Dummy", "dummy", "Not in use", "not in use", "Not in Use"]
        for c in [c for c in dfIn.columns if 'Key' in c]:
            dfOut = dfIn[~dfIn[c].isin(valInvalid)]
        if len(dfOut)>0: 
            answer = True
        else: 
            answer = False
            
    return answer
#==========================================================
#  get first matching, to assign key column to use. 
#==========================================================
def get_first_matching_key(df: pd.DataFrame, 
                           key_candidates: list):
    """Return the first key candidate found in DataFrame columns."""
    matched = next((key for key in key_candidates if key in df.columns), None)
    if matched is None:
        raise KeyError(f"None of the key candidates found in DataFrame columns: {key_candidates}")
    return matched

#==========================================================
#  aggregate data
#==========================================================
def aggregate_Data(
                df: pd.DataFrame,
                cols_to_group: list,
                cols_to_keep: list = None,
                col_types_to_sum: list = None,
                col_types_to_avg: list = None,
                col_types_to_recalc: list = None,
) -> pd.DataFrame:    
    
    print("version 20260324")
    #changes recalculate ULR and stuff, rather than doing average which is not correct. 
    
    # ------------------------------------------------------------------ #
    # 1. Defaults
    # ------------------------------------------------------------------ #
    # If DevM is a requested group variable, cast it to integer so it can
    # be used as a groupby key (left joins silently convert int → float).
    # If DevM is not in cols_to_group, fall back to keeping it as a
    # first-value column (original behaviour).
    if "DevM" in cols_to_group and "DevM" in df.columns:
        df["DevM"] = df["DevM"].astype("Int64")

    if not cols_to_keep:
        cols_to_keep = [] if "DevM" in cols_to_group else ["DevM"]
    cols_to_keep = [c for c in cols_to_keep if c in df.columns]

    # Ensure grouping columns do not overlap with kept columns
    cols_to_group = [ c for c in df.columns if c in cols_to_group and c not in cols_to_keep ]
    
    #claims inflation is likely to be done on a higher level. 
    if not col_types_to_avg:
        col_types_to_avg = ["DevPct", "OLFactor"]
        
    if not col_types_to_recalc:
        col_types_to_recalc = [
                            "OLFactor_Premium",
                            "OLFactor_Premium_PY",
                            "OLFactor_CY_Premium",  # premium CY factor — weighted by Ult_OLPY_Premium
                            "OLFactor_CY",          # loss trend CY: weighted by Incd_OLPY; skipped if absent
                            "Ratio",
                            "ULR",
                            "ULR_OL",
                            "IBNR_LR",
                            "IBNR_LR_OL",
                            "DevPct_Premium",
                            "DevPct_Incd",
                            "DevPct_Paid",

                            #'Trend', 'RateChange',  #trend and rate change calculation is somewhat complex.. they need to be induced from OL Factors.
                            #'FXRate', #exclude FX rate. not obvious which weighting factor to use in case of mixed currency.
                            ]
                            
    if not col_types_to_sum:
        col_types_to_sum = ["Incd", "Premium", "Commission", "Paid", "Ult", "IBNR", "Count"]
        
    
    #get claims types that had been used. 
    #clmTypes = ['Attr','Large','CAT','Systemic']    
    
    # ------------------------------------------------------------------ #
    # 2. Ratio recalculation formula dictionary
    #    Each entry defines numerator & denominator for weighted aggregation.
    #    numerator is informational; denominator is used as the weight.
    # ------------------------------------------------------------------ #
    ratio_formulas = {
                    "OLFactor_Premium":    {"numerator": "Ult_OL",          "denominator": "Ult_Premium"},
                    "OLFactor_PY_Premium": {"numerator": "xxxxx",           "denominator": "Ult_Premium"},
                    "OLFactor_CY_Premium": {"numerator": "Ult_OL_Premium",  "denominator": "Ult_OLPY_Premium"},  # weighted by prior-year on-level premium
                    "OLFactor_CY":         {"numerator": "Incd_OL",         "denominator": "Incd_OLPY"},         # weighted by prior-year on-level incurred; skipped if absent
                    "ULR":                 {"numerator": "Ult",             "denominator": "Ult_Premium"},
                    "ULR_OL":              {"numerator": "Ult_OL",          "denominator": "Ult_OL_Premium"},
                    "IBNR_LR":             {"numerator": "IBNR",            "denominator": "Ult_Premium"},
                    "IBNR_LR_OL":          {"numerator": "IBNR",            "denominator": "Ult_OL_Premium"},
                    "Ratio":               {"numerator": "xxx",             "denominator": "Ult_OL_Premium"},
    }
    
    """val_dict = {
                #"DevPct_Premium": {"numerator":"Premium", "denominator":"Ult_Premium"}, 
                #"DevPct_Commission": {"numerator":"Commission", "denominator":"Ult_Commission"},
                #"DevPct_Incd": {"numerator":"Incd", "denominator":"Ult"},
                #"DevPct_Paid": {"numerator":"Paid", "denominator":"Ult"},
                
                "OLFactor_Premium":{"numerator":"Ult_OL", "denominator":"Ult_Premium"}, #only for claim types, prem, commission 
                "OLFactor_PY_Premium":{"numerator":"xxxxx", "denominator":"Ult_Premium"}, #numerator should be OL to PY which is not always available. 
                
                #"OLFactor":{"numerator":"Incd_OL", "denominator":"Incd"}, #only for claim types, weight by Incd as ultimate is often missing in earlier steps. 
                #"OLFactor_PY":{"numerator":"xxxxx", "denominator":"Incd"}, #numerator should be OL to PY which is not always available. 
                "ULR":{"numerator":"Ult", "denominator":"Ult_Premium"}, 
                "ULR_OL":{"numerator":"Ult_OL", "denominator":"Ult_OL_Premium"}, 
                "IBNR_LR":{"numerator":"IBNR", "denominator":"Ult_Premium"},
                "IBNR_LR_OL":{"numerator":"IBNR", "denominator":"Ult_OL_Premium"},
                "Ratio":{"numerator":"xxx", "denominator":"Ult_OL_Premium"} #for expense ratio, commission ratio. use ult 
                }
"""
    # Retain only formulas relevant to the requested recalc types
    ratio_formulas = {k: v for k, v in ratio_formulas.items() if k in col_types_to_recalc}

    # Add denominator/numerator columns to the sum list so they are available post-groupby
    # Use a local copy to avoid mutating the caller's list
    col_types_to_sum_local = list(col_types_to_sum)
    for formula in ratio_formulas.values():
        col_types_to_sum_local += [formula["numerator"], formula["denominator"]]
    col_types_to_sum_local = list(set(col_types_to_sum_local))
    
    # ------------------------------------------------------------------ #
    # 3. Classify columns into aggregation buckets
    #    Priority: recalc > avg > sum  (higher priority excludes from lower)
    # ------------------------------------------------------------------ #
    def matches(col: str, terms: list) -> bool:
        return any(term in col for term in terms)

    cols_to_avg = [ c for c in df.columns
                    if matches(c, col_types_to_avg)
                    and not matches(c, col_types_to_recalc)
                    and c not in cols_to_group
                    and "Key" not in c
    ]

    cols_to_recalc = [  c for c in df.columns
                        if matches(c, col_types_to_recalc)
                        and c not in cols_to_group
                        and "Key" not in c
    ]

    cols_to_sum = [ c for c in df.columns
                    if matches(c, col_types_to_sum_local)
                    and not matches(c, col_types_to_avg)
                    and not matches(c, col_types_to_recalc)
                    and c not in cols_to_group
                    and "Key" not in c
    ]

    logger.info("Columns to sum:     %s", cols_to_sum)
    logger.info("Columns to average: %s", cols_to_avg)
    logger.info("Columns to recalc:  %s", cols_to_recalc)
    
    # ------------------------------------------------------------------ #
    # 4. Summation
    # ------------------------------------------------------------------ #
    df_agg = (
                df.groupby(cols_to_group, dropna=False)[cols_to_sum]
                .sum()
                .reset_index()
            )
    _str_fill = {c: "Unknown" for c in cols_to_group if c in df_agg.columns and df_agg[c].dtype == object}
    if _str_fill:
        df_agg = df_agg.fillna(_str_fill)

    # ------------------------------------------------------------------ #
    # 5. Keep columns (first non-null value per group)
    # ------------------------------------------------------------------ #
    if cols_to_keep:
        df_keep = (
                    df.dropna(subset=cols_to_keep)
                    .groupby(cols_to_group, dropna=False)[cols_to_keep]
                    .first()
                    .reset_index()
        )
        _str_fill = {c: "Unknown" for c in cols_to_group if c in df_keep.columns and df_keep[c].dtype == object}
        if _str_fill:
            df_keep = df_keep.fillna(_str_fill)
        df_agg = pd.merge(df_keep, df_agg, on=cols_to_group, how="right")
    
    # ------------------------------------------------------------------ #
    # 6. Simple average columns
    # ------------------------------------------------------------------ #
    if cols_to_avg:
        df_avg = (
            df.groupby(cols_to_group, dropna=False)[cols_to_avg]
            .mean()
            .reset_index()
        )
        _str_fill = {c: "Unknown" for c in cols_to_group if c in df_avg.columns and df_avg[c].dtype == object}
        if _str_fill:
            df_avg = df_avg.fillna(_str_fill)
        df_agg = pd.merge(df_agg, df_avg, on=cols_to_group, how="left")

    # ------------------------------------------------------------------ #
    # 7. Weighted recalculation of ratio columns
    #
    #    For each ratio key (e.g. "ULR"), find all columns whose name contains
    #    that key but does NOT contain a name part that would indicate a
    #    distinct variant already handled separately (e.g. "ULR_OL" should not
    #    be re-processed under "ULR").
    #
    #    Weighted average: Σ(ratio × weight) / Σ(weight)
    #    Falls back to simple mean when denominator sums to zero.
    # ------------------------------------------------------------------ #

    # These suffixes/prefixes distinguish variant columns that have their own formula entry and should not be matched by a more general key.
    name_parts_to_exclude = ["OL", "IBNR", "PY", "CY", "Premium"]

    for key, formula in ratio_formulas.items():
        candidate_cols = [c for c in cols_to_recalc if key in c]

        for col in candidate_cols:
            # Skip columns where the residual name (after removing the key) contains a distinguishing suffix — those have their own formula.
            residual = col.replace(key, "")
            if any(part in residual for part in name_parts_to_exclude):
                continue                        
            
            # Determine weighting column
            # Premium-denominated ratios always use the same premium column;
            # other ratios substitute the key with the denominator fragment.
            denominator_fragment = formula["denominator"]
            if "Premium" in denominator_fragment:
                weight_col = denominator_fragment
            else:
                weight_col = col.replace(key, denominator_fragment)    
            
            
            if weight_col not in df.columns:
                logger.warning(
                    "Weight column '%s' not found for recalculating '%s'. Skipping.",
                    weight_col, col,
                )
                continue
            
            logger.info("Recalculating '%s' weighted by '%s'.", col, weight_col)
            
            # Exclude rows where the ratio itself is null
            df_valid = df.dropna(subset=[col])

            def _weighted_agg(grp: pd.DataFrame, col: str=col,weight_col: str=weight_col) -> pd.Series:
                total_weight = grp[weight_col].sum()
                weighted_sum = (grp[col] * grp[weight_col]).sum()
                recalc = weighted_sum / total_weight if total_weight != 0 else np.nan
                return pd.Series({"recalc": recalc, "avg": grp[col].mean()})
            
            d_agg = (
                    df_valid.groupby(cols_to_group, dropna=False)
                    .apply(_weighted_agg)
                    .reset_index()
                    #.fillna({c: "Unknown" for c in cols_to_group})
                )
            
            for c in cols_to_group:
                # Only fill "Unknown" for columns with dtype 'object' (string)
                if df_agg[c].dtype == object:
                    df_agg[c].fillna("Unknown", inplace=True)
            
            # Use weighted recalc where available; fall back to simple mean
            # when denominator is zero (recalc is NaN).
            d_agg[col] = np.where(
                                d_agg["recalc"].isna(),
                                d_agg["avg"],       # fallback: zero-weight group
                                d_agg["recalc"],    # preferred: weighted average
            )
            
            df_agg = pd.merge(
                            df_agg,
                            d_agg[[*cols_to_group, col]],
                            on=cols_to_group,
                            how="left",
            )
            
    # ------------------------------------------------------------------ #
    # 8. Derive CY OL factor relative to PY  [BUG FIX]
    #    Original code divided OLFactor_Premium by itself (always = 1).
    #    Corrected to divide current-year factor by prior-year equivalent.
    # ------------------------------------------------------------------ #
    cy_col = "OLFactor_Premium"
    py_col = "OLFactor_PY_Premium"
    if (cy_col in df_agg.columns and py_col in df_agg.columns
            and "OLFactor_CY_Premium" not in cols_to_recalc):
        # Only derive from ratio when not already computed via weighted recalc above
        df_agg["OLFactor_CY_Premium"] = df_agg[cy_col] / df_agg[py_col]

    # ------------------------------------------------------------------ #
    # 9. Derive DevPct_Incd from aggregated sums
    #    DevPct_Incd_{ct} = sum(Incd_OL_{ct}) / sum(Ult_OL_{ct}_IncdCL)
    #    Overrides any averaged DevPct values carried through from step 6,
    #    which would be incorrect after summing across segments.
    #    Falls back to 1 (fully developed) when the ultimate sum is zero.
    # ------------------------------------------------------------------ #
    for col in df_agg.columns:
        if col.startswith("Incd_OL_"):
            ct = col[len("Incd_OL_"):]
            ult_col = f"Ult_OL_{ct}_IncdCL"
            if ult_col in df_agg.columns:
                df_agg[f"DevPct_Incd_{ct}"] = np.where(
                    df_agg[ult_col] != 0,
                    df_agg[col] / df_agg[ult_col],
                    1,
                )

    return df_agg


#=================================================================================================================================    
def build_not_in_use_row(
    columns: list
) -> pd.DataFrame:
    """
    Build a single-row placeholder DataFrame for outputs not in use.

    Column assignment rules (applied in order):
      - Contains ``key_pattern``  → "Not in Use"
      - Matches ``year_col``      → PAT_na()
      - Name == "Comments"        → ""
      - All others                → None

    Parameters
    ----------
    columns     : Ordered list of column names for the output DataFrame.
    year_cols   : Name of the year column (e.g. "AY" or "UWY").
    key_pattern : Substring that identifies key columns (default ``"Key_"``).

    Returns
    -------
    pd.DataFrame with one row.
    """
    year_cols = ['AY', 'UWY', 'UY']
    key_pattern ='Key_'
    
    def _assign(col: str):
        if key_pattern in col:
            return "Not in Use"
        if col == "Comments":
            return ""
        else: 
            return PAT_na()
        
        return None

    return pd.DataFrame({col: [_assign(col)] for col in columns})

#=================================================================================================================================
def build_key_submap(
    df_map: pd.DataFrame,
    key_col: str,
    col_keys: list,
) -> pd.DataFrame:
    """
    Extract the minimal sub-map from a key mapping table that explains the
    variation in ``key_col``.

    Starting from all FullKey columns present in ``df_map``, the function
    iteratively removes any column that is redundant — i.e. the remaining
    columns still uniquely determine ``key_col``.  The result is the smallest
    set of columns needed to look up ``key_col``, together with ``key_col``
    itself, deduplicated.

    Parameters
    ----------
    df_map  : Key mapping table (e.g. synced_Key_Mapping).
    key_col : Name of the target key column (e.g. ``"Key_RateChange"``).
    col_keys: Ordered list of FullKey column names to consider as candidates.

    Returns
    -------
    pd.DataFrame with the minimal explaining columns plus ``key_col``.
    Returns an empty DataFrame with those columns if ``key_col`` is absent.
    """
    available_cols = [c for c in col_keys if c in df_map.columns]

    if key_col not in df_map.columns:
        logger.warning("build_key_submap: '%s' not found in mapping table.", key_col)
        return pd.DataFrame(columns=available_cols + [key_col])

    sub = df_map[available_cols + [key_col]].drop_duplicates()

    # Iteratively remove columns that are not needed to uniquely determine key_col.
    # After each removal restart the loop — order of removal can affect which
    # columns are retained, so we repeat until no further reduction is possible.
    explaining_cols = list(available_cols)
    changed = True
    while changed:
        changed = False
        for col in list(explaining_cols):
            candidate = [c for c in explaining_cols if c != col]
            if not candidate:
                break
            # A column is redundant if the remaining candidate columns still
            # map uniquely to key_col (every group has at most one key value).
            nunique = sub.groupby(candidate, dropna=False)[key_col].nunique()
            if (nunique <= 1).all():
                explaining_cols = candidate
                changed = True
                break  # restart after each removal

    return sub[explaining_cols + [key_col]].drop_duplicates().reset_index(drop=True)


#=================================================================================================================================
def find_parent_cols(
    df_map: pd.DataFrame,
    key_col: str,
    col_candidates: list,
) -> list:
    """
    From col_candidates, return those where every distinct value maps to
    at most one value of key_col (many-to-one relationship).

    A column qualifies if each value of key_col always corresponds to the
    same value of that column — i.e. key_col is a deterministic lookup for
    the candidate column. The candidate column may map to multiple key values
    (one-to-many in the other direction is allowed).

    Parameters
    ----------
    df_map          : Key mapping table (e.g. synced_Key_Mapping).
    key_col         : Name of the target key column (e.g. ``"Key_Forecast"``).
    col_candidates  : List of column names to test (e.g. ``["Country", "SubRegion", "Industry"]``).

    Returns
    -------
    List of column names from col_candidates that pass the many-to-one check.

    Example
    -------
    Key_Model=EL_Conv → EL only (EL_GL), Key_Model=EL_NonConv → EL only  →  EL_GL qualifies.
    Key_Model=EL_Conv → UK and Ireland (Country)  →  Country fails (one key maps to multiple values).
    """
    aligned = []
    for col in col_candidates:
        if col not in df_map.columns:
            continue
        nunique = df_map.groupby(key_col, dropna=False)[col].nunique()
        if (nunique <= 1).all():
            aligned.append(col)
    return aligned


#=================================================================================================================================
def parse_period(period_str: str) -> tuple[list[int], list[int]]:
    """
    Parse an averaging period string into include and exclude year lists.

    Supported formats:
        "2018-2022"              → include 2018–2022, no exclusions
        "2018-2022 ex 2020"      → include 2018–2022, exclude 2020
        "2018-2022 ex 2019-2020" → include 2018–2022, exclude 2019–2020

    Returns (include_years, exclude_years) as integer lists.
    """
    parts         = period_str.split("ex")
    include_range = [int(x.strip()) for x in parts[0].strip().split("-")]
    include_years = list(np.arange(include_range[0], include_range[-1] + 1))

    if len(parts) > 1:
        exclude_range = [int(x.strip()) for x in parts[1].strip().split("-")]
        exclude_years = list(np.arange(exclude_range[0], exclude_range[-1] + 1))
    else:
        exclude_years = []

    return include_years, exclude_years

#=================================================================================================================================    
def period_col_suffix(period_str: str) -> str:
    """Normalise a period string for safe use as a column name suffix."""
    return period_str.replace(" ", "").replace("-", "_")
                
