# PAT Data Consistency Tool

## Overview

The **PAT Data Consistency Tool** is a Dataiku-based validation utility that performs comprehensive data quality checks on PAT (Premium Actuarial Tool) input tables before a PAT build is executed. It verifies internal consistency across multiple reference tables, mapping tables, and detailed data, ensuring that joins are unambiguous and all data relationships are valid.

**Version:** v20260423 (Initial version)  
**Platform:** Dataiku Python notebook in the PAT Data project  
**Status:** On-demand execution (no parameters required)

---

## Quick Start

### Usage

```python
from data_consistency_checks import run_and_display
run_and_display()
```

Simply import the main function and call it from a Dataiku Python notebook. The tool will:
1. Load all relevant `tbl_*` datasets from the project
2. Run 8 consistency checks
3. Display results inline as formatted HTML tables and summaries

---

## Checks Performed

### Check 1: Key Column Registration (FAIL)

**Purpose:** Ensures all segmentation keys are properly registered across tables.

**Rule:** Every `Key_*` column found in a reference table (except `Key_Forecast`) must exist in:
- `tbl_DetailedData`
- `tbl_Key_Mapping`

**Output:** Lists missing key columns and their target tables.

**Exception:** `Key_Forecast` is excluded—forecast grain can be finer than modelling grain.

---

### Check 2: Forward Referential Integrity (FAIL)

**Purpose:** Validates that all detailed data can be joined to reference tables.

**Rule:** Every unique combination of join key values in `tbl_DetailedData` must have at least one matching row in each reference table.

**Output:**
- Top 10 unmatched key combinations (sorted by premium)
- Total unmatched count
- Total unmatched premium ($ and %)

**Severity:** FAIL—indicates data that cannot be looked up.

---

### Check 3: Reverse Referential Integrity (WARNING)

**Purpose:** Detects orphaned reference table entries.

**Rule:** Every join key combination in a reference table should exist in `tbl_DetailedData`.

**Output:**
- Count of orphaned key combinations
- List of orphaned values
- For `tbl_Forecast`: cell-level premium affected

**Severity:** WARNING—extra reference data is technically valid but may indicate setup issues.

---

### Check 4: Row Uniqueness (FAIL)

**Purpose:** Ensures unambiguous joins (no fan-out/row duplication).

**Rule:** The join key combination must be unique within each reference table.

**Special Cases:**
- **Pattern tables** (`tbl_Patterns_Attr`, `tbl_Patterns_Large`, `tbl_Patterns_Prem`): Include `DevM` in uniqueness check
- **Forecast table** (`tbl_Forecast`): All columns except `Key_Modelling` must be unique
- **Year columns** (`AY`, `UWY`, `UY`): Always included even though numeric

**Output:** Lists duplicate key combinations and affected row count.

---

### Check 5: Parent Column Rule (FAIL)

**Purpose:** Validates hierarchical relationships between segmentation columns.

**Rule:** Any non-`Key_*` column in a reference table (that isn't a value column) must be a **parent** of the table's join key. A parent column means each key value maps to at most one value of that column.

**Verification:** Uses `find_parent_cols()` function from `PAT_utils.py`.

**Special Case — Domain Membership:** For `tbl_Min_Large_Load`, non-`Key_*` column values must exist in the same-named column in `tbl_DetailedData`.

**Output:** Lists non-parent columns and their associated key columns.

---

### Check 6: Mapping Table Uniqueness (FAIL)

**Purpose:** Ensures each segmentation combination appears exactly once.

**Rule:** In `tbl_Key_Mapping`, all non-`Key_*` columns together form the natural key. Every combination must be unique.

**Output:** Lists duplicate segmentation combinations and their occurrence count.

**Severity:** FAIL—duplicates break the mapping contract.

---

### Check 7: Value Range Checks (WARNING/FAIL)

Validates specific numeric and categorical columns for reasonable ranges:

#### **RateChange** (in `tbl_RateChange`, `tbl_RateChange_Pol`)
- Expected: Decimal close to zero (e.g., 0.05 for +5%)
- **FAIL** if >50% outside [−0.5, +0.5] (likely entered as percentages, not decimals)
- **WARNING** if >10% outside [−0.2, +0.2]

#### **Trend_Value** (in `tbl_Trend`)
- Same rules as RateChange

#### **DevPct_*** (in pattern tables)
- Expected: Values centered around 1.0 (100%)
- **FAIL** if >20% of values exceed 1.0
- **WARNING** if mean < 0.7 or > 1.0, or >5% exceed 1.0

#### **AsAt_Month** (in `tbl_DetailedData`)
- Expected: Single valid YYYYMM value
- **FAIL** if invalid YYYYMM format or multiple values present

#### **DevM** (in pattern tables)
- Expected: Continuous sequence from 1 to ≥120 per pattern key
- **FAIL** if gaps, non-unit start, or max < 120

#### **MinLoad_*** (in `tbl_Min_Large_Load`)
- Expected: Numeric values; ≥95% should be < 3
- **FAIL** if non-numeric values found
- **WARNING** if <95% below threshold

---

### Check 8: Key_Modelling Coverage (WARNING)

**Purpose:** Identifies premium that will be excluded from analysis.

**Rule:** For each `Key_Modelling*` column, measures premium in rows with null or blank values (unmapped premium).

**Output:**
- Column name
- Count of unmapped rows
- Sum of unmapped premium
- Percentage of total premium

**Severity:** WARNING—unmapped premium rows will not join to any reference table and are excluded from PAT analysis.

---

## Code Structure

### Section 1: Data Loading

#### `_get_dd_group_cols(loaded_ref_tables)`
Determines GROUP BY columns for `tbl_DetailedData` aggregation. Includes:
- Columns that appear in at least one loaded reference table
- Non-numeric columns only
- Always includes `AsAt_Month`
- Excludes `Premium` (aggregation target)

#### `_ensure_dd_aggregated(project, loaded_ref_tables)`
Manages pre-aggregated `tbl_DetailedData_Agg` dataset:
- Creates a Dataiku Group recipe (automatic on first run)
- Groups by non-numeric columns and sums Premium
- Runs aggregation at storage layer (SQL/Spark) for performance
- Only rebuilds if dataset doesn't exist; updates recipe settings on every call

#### `load_tbl_datasets()`
Loads all in-scope `tbl_*` datasets:
- Loads all reference tables first (small tables)
- Ensures aggregated DetailedData exists
- Silently skips missing tables
- Returns `{table_name: DataFrame}`

#### `classify_tables(datasets)`
Splits loaded datasets into three groups:
- `tbl_DetailedData` → `dd_df`
- `tbl_Key_Mapping` → `mapping_df`
- All others → `ref_tables` dict

---

### Section 2: Column Classification Helpers

#### `get_join_key(ref_df, dd_df)`
Identifies natural join columns between reference table and DetailedData:
- Columns present in both tables
- Non-numeric (categorical) only
- Excludes `Key_Forecast`

#### `get_key_cols(df)`
Returns all columns starting with `Key_*`

#### `get_display_cols(ref_df, dd_df)`
Join key columns minus structural columns (AY, UWY, DevM)

#### `get_context_cols(ref_df, dd_df)`
Non-`Key_*` display columns used for parent checks and output context

---

### Section 3: Check Functions

Each check function returns a dictionary with status and detailed results:

- `check_key_registration(dd_df, mapping_df, ref_tables)`
- `check_referential_integrity(dd_df, ref_tables)`
- `check_row_uniqueness(dd_df, ref_tables)`
- `check_parent_columns(dd_df, mapping_df, ref_tables)`
- `check_mapping_uniqueness(mapping_df)`
- `check_value_ranges(dd_df, ref_tables)`
- `check_key_modelling_unmapped(dd_df)`

---

### Section 4: Display Helpers

#### HTML Rendering
- `_badge(status)` — Colored status badges (PASS/WARNING/FAIL/SKIP)
- `_df_to_html(df, max_rows=10)` — Converts DataFrames to compact HTML tables
- `_trim_to_display(df, ref_df, dd_df, extra_cols)` — Keeps only display columns

#### Display Functions
- `display_banner(status, n_loaded)` — Overall result header
- `display_check1(reg_results)` — Key registration results
- `display_ri_and_uniqueness(...)` — Checks 2–5 results
- `display_mapping(map_uniq)` — Mapping uniqueness results
- `display_value_ranges(value_results)` — Value range checks with detail tables
- `display_key_modelling(km_results)` — Modelling coverage results

---

### Section 5: Main Entry Point

#### `_overall_status(reg, ri, uniq, parents, map_uniq, values, km)`
Derives overall status from all check results:
- Returns "FAIL" if any check fails
- Returns "WARNING" if any check has warnings (no fails)
- Returns "PASS" if all checks pass

#### `run_and_display()`
Main orchestration function:
1. Loads datasets via `load_tbl_datasets()`
2. Classifies tables
3. Runs all 8 checks sequentially
4. Computes overall status
5. Displays results inline via HTML

---

## Key Constants

### `WIKI_TABLES`
All in-scope `tbl_*` datasets:
```
tbl_DetailedData, tbl_RateChange, tbl_Trend, tbl_Patterns_Attr,
tbl_Patterns_Large, tbl_Patterns_Prem, tbl_Forecast, tbl_Key_Mapping,
tbl_RateChange_Pol, tbl_IELR_Attr, tbl_IELR_Large, tbl_ULR_Prior_Attr,
tbl_ULR_Prior_Large, tbl_Min_Large_Load, tbl_Weight_HistYears
```

### `STRUCTURAL_COLS`
Excluded from display and parent checks: `{AY, UWY, UY, DevM}`

### `PATTERN_TABLES`
Tables where DevM is part of uniqueness key:
`{tbl_Patterns_Attr, tbl_Patterns_Large, tbl_Patterns_Prem}`

---

## Key Features

### Performance Optimizations

1. **Pre-aggregation of DetailedData**: Uses Dataiku Group recipe at SQL/Spark layer instead of loading full granular table
2. **Selective deduplication**: For parent checks, only deduplicates necessary columns
3. **Efficient joins**: Uses pandas merge operations with indicator columns for set logic
4. **Streaming display**: Results displayed immediately after each check

### Robustness

- **Graceful fallback**: If aggregation fails, can fall back to direct load (with performance warning)
- **Silent table skipping**: Missing tables don't break the tool
- **Null handling**: Properly handles NA/null values in categorical columns
- **Type safety**: Casts numeric columns (e.g., DevM) as needed for grouping

### User-Friendly Output

- Color-coded status badges (green/orange/red)
- Top 10 summary tables with premium context
- Hierarchical display (summary → details)
- Responsive HTML rendering

---

## Typical Workflow

1. **Data Setup**: Prepare PAT input tables in Dataiku
2. **Run Consistency Tool**: Call `run_and_display()` from a Python notebook
3. **Review Results**: Examine HTML output for any FAILs or WARNINGs
4. **Remediate Issues**: Fix data quality issues identified by checks
5. **Re-run Tool**: Confirm all checks pass before proceeding with PAT build

---

## Requirements

- **Dataiku**: API client configured for project access
- **Python**: pandas, numpy for data processing; IPython for display
- **Data**: `tbl_DetailedData` and at least one reference table present

---

## Dependencies

- `dataiku` — Dataiku API
- `pandas` — Data manipulation
- `numpy` — Numeric operations
- `IPython.display` — HTML rendering in notebooks

---

## Notes

- The tool is **read-only**—it does not modify any tables
- It is designed to complement the Data Validation Tool (v2), which checks naming and table/column presence
- Assumes all structural metadata (column types, table names) is correct; focuses on data value validation
- All joins use non-numeric columns (categorical keys) to ensure semantic correctness

---

## Support

For issues or questions:
- Check that all required `tbl_*` datasets exist
- Verify `tbl_DetailedData_Agg` recipe has been built
- Review the technical specification: `DataConsistencyTool_Spec_v1.md`
- Consult `PAT_utils.py` for utility functions
