# Technical Spec: PAT Data Consistency Tool (v1)

**Author:** Namhi Kim
**Date:** 2026-04-15
**Assignee:** Vasilisa
**Status:** Draft
**Delivery:** Dataiku webapp on PAT Data projects

---

## Purpose

An on-demand tool that checks whether the content and structure of PAT input tables are internally consistent before a PAT build is run. It complements the Data Validation Tool (v2), which checks naming and table/column presence. This tool assumes those checks have already passed — it focuses on whether the data values and relationships across tables are correct.

Specifically it checks:
1. Key columns in reference tables are properly registered in DetailedData and the mapping table
2. Key values in DetailedData have matching rows in each reference table (referential integrity)
3. Non-key columns in reference tables are at a valid parent level relative to the joining key
4. The mapping table has no duplicate segmentation combinations
5. Numeric value columns fall within expected ranges

---

## Platform

- Dataiku **webapp** in the PAT Data project
- Triggered on demand via a **"Run"** button — no other inputs required
- Reads datasets from the same Dataiku project only
- Separate webapp from the Data Validation Tool (v2)

---

## Definitions

The following terms are used throughout this spec:

| Term | Meaning |
|---|---|
| **DetailedData** | `tbl_DetailedData` — the core granular dataset |
| **Mapping table** | `tbl_Key_Mapping` — the segmentation cross-reference table |
| **Reference table** | Any `tbl_*` dataset that is neither DetailedData nor the mapping table (e.g. `tbl_RateChange`, `tbl_Trend`, `tbl_Patterns_Attr`, etc.) |
| **Key column** | Any column whose name starts with `Key_` |
| **Value column** | A column in a reference table that holds the actual data being looked up — i.e. a non-`Key_*` column that does **not** appear in DetailedData (e.g. `RateChange`, `Trend_Value`, `DevPct_Incd_Attr`) |
| **Parent column** | A non-`Key_*` column that appears in a reference table AND in DetailedData. It is a valid parent of a key column if each value of that key maps to at most one value of the candidate column — verified using `find_parent_cols` from `PAT_utils.py` |

---

## Check 1 — Key Column Registration

### Rule

For every reference table present in the project, identify all `Key_*` columns it contains. Each of those key columns must be present in both:
- `tbl_DetailedData`
- `tbl_Key_Mapping`

### Exception

`Key_Forecast` is excluded from this check. It does not need to appear in DetailedData or the mapping table because the forecast grain can be finer than the modelling grain.

### Severity

**FAIL** — flag which reference table contains the key and which target table (DetailedData or mapping table) is missing it.

---

## Check 2+3 — Referential Integrity

### Overview

For each reference table, join it against DetailedData on the shared `Key_*` columns and check in both directions.

**Join key:** `Key_*` columns that appear in both the reference table and DetailedData. Do not include non-`Key_*` columns in the join.

**Note:** Parent columns from the reference table may be included alongside the join key for display and filtering context in the output — but they are not part of the join condition.

---

### Forward check — DetailedData → reference table (FAIL)

For each reference table, take every unique combination of join key values present in `tbl_DetailedData`. Check that each combination has at least one matching row in the reference table. Combinations with no match are unmatched rows.

**Output for each reference table with unmatched rows:**

| Output element | Description |
|---|---|
| Top 10 unmatched groups | Unique join key combinations, sorted descending by sum of `Premium` from DetailedData. Include parent columns from the reference table as additional context columns if present. |
| Total unmatched count | Total number of unique unmatched key combinations |
| Total unmatched premium | Sum of `Premium` across all DetailedData rows belonging to unmatched key combinations (sum across all rows regardless of `DataType`) |

**Severity:** FAIL

---

### Reverse check — reference table → DetailedData (WARNING)

For each reference table, check whether every `Key_*` value present in that table also appears in `tbl_DetailedData`. Any key value in the reference table that has no counterpart in DetailedData is flagged.

This is a warning only — extra rows in a reference table are technically valid but may indicate leftover segments or mis-mapping.

**Output:** Count of orphaned key values per reference table, with the values listed.

**Severity:** WARNING

---

## Check 4 — Parent Column Rule

### Rule

Any non-`Key_*` column in a reference table or the mapping table that is not a value column must qualify as a **parent** of the table's joining key column. A column qualifies as a parent if each value of the key column maps to at most one value of that column — verified using `find_parent_cols(df_map, key_col, col_candidates)` from `PAT_utils.py`.

### For reference tables

- Candidate columns = all columns in the reference table that are neither `Key_*` nor value columns (i.e. non-`Key_*` columns that also appear in DetailedData)
- Run `find_parent_cols` using `tbl_Key_Mapping` as `df_map`, the reference table's `Key_*` column as `key_col`, and the candidate columns as `col_candidates`
- Any candidate column that does not qualify as a parent is flagged

### For `tbl_Key_Mapping`

- Candidate columns = all non-`Key_*` columns in the mapping table
- For each `Key_*` column in the mapping table, run `find_parent_cols` using the mapping table itself as `df_map`
- Flag any candidate column that fails the parent check for any `Key_*` column

### Severity

**FAIL**

---

## Check 5 — Mapping Table Uniqueness

### Rule

In `tbl_Key_Mapping`, the non-`Key_*` columns together form the natural key of the table. Every combination of values across those columns must appear at most once. `Key_*` columns are expected to repeat and are excluded from this check.

**Output if duplicates found:** List of duplicate combinations (non-`Key_*` column values), with row counts.

**Severity:** FAIL

---

## Check 6 — Value Range Checks

The following checks apply to specific value columns across tables. Each check reports an overall statistic and the reason for the result — no per-key breakdown is required.

---

### `RateChange` — in `tbl_RateChange` and `tbl_RateChange_Pol`

Expected to be a decimal close to zero (e.g. `0.05` for +5%).

| Condition | Severity |
|---|---|
| >10% of values fall outside [−0.2, +0.2] | WARNING |
| >50% of values fall outside [−0.5, +0.5] | FAIL — likely a formatting issue (e.g. values entered as percentages rather than decimals) |

**Output:** Percentage of values outside each threshold, flagged band.

---

### `Trend_Value` — in `tbl_Trend`

Same rules as `RateChange`.

| Condition | Severity |
|---|---|
| >10% of values fall outside [−0.2, +0.2] | WARNING |
| >50% of values fall outside [−0.5, +0.5] | FAIL — likely a formatting issue |

**Output:** Percentage of values outside each threshold, flagged band.

---

### `DevPct_*` — in `tbl_Patterns_Attr`, `tbl_Patterns_Large`, `tbl_Patterns_Prem`

Development percentages represent the proportion of ultimate losses or premium emerged to date. Most values should be between 0 and 1; values above 1 indicate over-development, which is possible but should be rare.

| Condition | Severity |
|---|---|
| Mean of all `DevPct_*` values is outside [0.7, 1.0] | WARNING |
| Fewer than 95% of `DevPct_*` values are below 1 | WARNING |
| More than 20% of `DevPct_*` values are above 1 | FAIL |

**Output:** Mean value, percentage of values above 1, flagged condition.

Note: Apply checks per `DevPct_*` column independently (e.g. `DevPct_Incd_Attr` and `DevPct_Paid_Attr` are checked separately).

---

### `AsAt_Month` — in `tbl_DetailedData`

The valuation date. Must be in `YYYYMM` format and must be a single consistent value across the entire table.

| Condition | Severity |
|---|---|
| Any value is not a valid YYYYMM integer (6 digits, month 01–12) | FAIL |
| More than one distinct value is present | FAIL |

**Output:** Distinct values found, count of invalid format values if any.

---

### `DevM` — in `tbl_Patterns_Attr`, `tbl_Patterns_Large`, `tbl_Patterns_Prem`

Development months. For each unique `Key_Pattern_*` value, the sequence of `DevM` values must:
- Start at 1
- Be complete with no gaps (consecutive integers)
- Reach at least 120

| Condition | Severity |
|---|---|
| Sequence does not start at 1 | FAIL |
| Gaps exist in the sequence | FAIL |
| Maximum `DevM` is less than 120 | FAIL |

**Output:** Per pattern key — first DevM, last DevM, any gaps identified, count of keys failing each condition.

---

## Webapp Output

### Status banner

Displayed at the top after run completes:

| Banner | Condition |
|---|---|
| **PASS** | All checks clear |
| **WARNING** | One or more warnings, no failures |
| **FAIL** | One or more failures |

### Results

Displayed in five collapsible sections, one per check group:

| Section | Contents |
|---|---|
| **1. Key Registration** | Pass/fail per reference table; list of missing keys and which target table they are absent from |
| **2. Referential Integrity** | Per reference table: forward FAIL rows (top 10 by premium, total count, total premium); reverse WARNING with orphaned key values |
| **3. Parent Column Rule** | Per table: list of columns that fail the parent check, with the key column they were tested against |
| **4. Mapping Table Uniqueness** | Pass/fail; list of duplicate non-`Key_*` combinations if any |
| **5. Value Checks** | Per column: statistic summary, threshold breached, severity |

---

## Open Items

| Item | Note |
|---|---|
| `tbl_Min_Large_Load` column structure | Not yet defined — value range checks cannot be specified until column structure is agreed |
| `find_parent_cols` — column absent from mapping table | If a candidate column is not present in `tbl_Key_Mapping`, the function silently skips it. Confirm whether this should be treated as a FAIL or excluded from the check |
