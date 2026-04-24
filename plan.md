# PAT Data Consistency Tool: Dash Webapp Build Plan

## 0. General instructions
- Create a commit for each succesfully completed task 

## 1. Goal and Scope

Build a Dash webapp in the PAT Data project that runs the data consistency checks defined in `DataConsistencyTool_Spec_v1.md` on demand, using only datasets from the same Dataiku project.

The webapp must:
- expose a single `Run` action with no user inputs
- inspect `tbl_DetailedData`, `tbl_Key_Mapping`, and all other `tbl_*` datasets treated as reference tables
- compute all FAIL and WARNING outcomes required by the spec
- present a top-level PASS/WARNING/FAIL banner
- display results in five collapsible sections matching the spec

Out of scope for the first build:
- replacing the Data Validation Tool
- cross-project dataset reads
- checks for `tbl_Min_Large_Load` until the column structure is defined

## 2. Key Design Decisions

Use a two-layer design:
- `validation engine`: pure Python functions that load datasets, classify tables, run checks, and return structured results
- `Dash UI`: a thin presentation layer that triggers the engine and renders summary banners, tables, and collapsible sections

Keep the validation engine independent from Dash so it can be unit tested and reused in recipes later.

Reuse `find_parent_cols` from `PAT_utils.py` for the parent-column rule. Capture the current ambiguity explicitly: the helper skips candidate columns not present in the mapping table, so the implementation must either:
- treat skipped columns as excluded from the check, or
- wrap the helper with an explicit pre-check and mark those columns as FAIL

This needs to be decided before coding Check 3 because it changes the failure semantics.

## 3. Proposed Webapp Structure

Recommended code layout inside the Dataiku webapp:

- `app.py`: Dash app initialization, layout, callbacks
- `services/data_access.py`: load datasets from the current Dataiku project and normalize column types
- `services/table_registry.py`: identify DetailedData, mapping table, and reference tables
- `checks/key_registration.py`: Check 1
- `checks/referential_integrity.py`: forward and reverse referential checks
- `checks/parent_columns.py`: Check 4 in the spec output grouping
- `checks/mapping_uniqueness.py`: Check 5
- `checks/value_checks.py`: all value range checks
- `models/results.py`: shared result schema and severity aggregation
- `ui/components.py`: banner, summary cards, result tables, collapsible sections
- `tests/`: unit tests for check functions using small pandas fixtures

If the Dataiku Dash webapp needs a single-file entrypoint, keep the same logical structure in helper modules and import them from the main file.

## 4. Result Contract

Define a single normalized result object before building the checks. Each check function should return:
- `check_group`: one of `key_registration`, `referential_integrity`, `parent_column_rule`, `mapping_uniqueness`, `value_checks`
- `table_name`: dataset name if applicable
- `status`: `PASS`, `WARNING`, or `FAIL`
- `summary`: short user-facing description
- `details`: structured payload for display tables and counts
- `metrics`: machine-readable counts and percentages

Define a final run payload:
- run timestamp
- datasets scanned
- overall status derived as FAIL > WARNING > PASS
- grouped check results in UI order

This contract prevents the Dash callback code from embedding business logic.

## 5. Step-by-Step Delivery Plan

### Step 1. Confirm open spec decisions

Before coding, confirm these two items with the spec owner:
- whether a candidate parent column absent from `tbl_Key_Mapping` should be a FAIL or should be excluded, since `find_parent_cols` currently skips it
- whether `tbl_Min_Large_Load` should be ignored entirely in v1 or shown as not checked

Deliverable:
- written decision note added to the implementation README or module docstring

### Step 2. Create the validation engine skeleton

Build the shared plumbing first:
- dataset loader for same-project Dataiku datasets
- table classifier:
	- `tbl_DetailedData` as DetailedData
	- `tbl_Key_Mapping` as mapping table
	- every other `tbl_*` dataset as a reference table
- common helpers to identify `Key_*` columns, value columns, parent candidates, and join keys
- result dataclasses or dictionaries for consistent outputs

Implementation details:
- normalize column names only if required; otherwise preserve source naming
- coerce numeric fields lazily inside each value check rather than globally
- guard against empty datasets and missing required tables with explicit FAIL results

Deliverable:
- engine can scan datasets and return an empty structured result set without UI integration

### Step 3. Implement Check 1: key column registration

For each reference table:
- collect all `Key_*` columns
- exclude `Key_Forecast`
- verify presence in both `tbl_DetailedData` and `tbl_Key_Mapping`
- record which target table is missing each key

Output requirements:
- one result per reference table
- missing keys listed with target table missingness
- FAIL if any required key is absent

Acceptance test:
- a reference table with one missing key in DetailedData and one missing key in mapping returns FAIL and names both gaps

### Step 4. Implement Check 2: referential integrity

Build one shared routine per reference table using only shared `Key_*` columns between the reference table and DetailedData.

Forward check, DetailedData to reference table:
- derive unique join-key combinations from DetailedData
- aggregate DetailedData `Premium` by join key
- left join to unique join-key combinations in the reference table
- flag unmatched combinations
- sort unmatched groups by descending aggregated premium
- keep top 10 groups for display
- compute total unmatched group count and total unmatched premium
- mark FAIL when unmatched groups exist

Reverse check, reference table to DetailedData:
- compare distinct key values in the reference table against DetailedData
- list orphaned values per key column or per join signature, depending on what is most readable
- mark WARNING when orphaned values exist

Output requirements:
- single section per reference table with forward FAIL content and reverse WARNING content
- include parent columns only as display context, never as join conditions

Acceptance test:
- a table with one unmatched DetailedData key group and two orphaned reference values returns FAIL overall for the table slice and preserves the reverse warning details

### Step 5. Implement Check 3: parent column rule

For reference tables:
- identify candidate columns as non-`Key_*` columns that also exist in DetailedData
- exclude value columns, meaning non-`Key_*` columns that do not appear in DetailedData
- determine the relevant key column or key columns to test
- call `find_parent_cols(tbl_Key_Mapping, key_col, col_candidates)`
- flag candidates that do not qualify

For `tbl_Key_Mapping`:
- use all non-`Key_*` columns as candidates
- run the parent check for each `Key_*` column in the mapping table
- flag candidate columns that fail for any tested key column

Implementation note:
- if a reference table has multiple key columns, treat each tested key independently and report failures by `(table, key_col, candidate_col)`

Acceptance test:
- one candidate column that maps to multiple values for a single key is reported as FAIL with the offending key column named

### Step 6. Implement Check 4: mapping table uniqueness

Using `tbl_Key_Mapping`:
- define natural key columns as all non-`Key_*` columns
- group by those columns and count rows
- extract combinations with count greater than 1
- display duplicate combinations and counts
- mark FAIL if any duplicates exist

Acceptance test:
- duplicated segmentation combinations are returned with row counts and FAIL severity

### Step 7. Implement Check 5: value range checks

Implement these as separate validators registered in one dispatcher.

`RateChange`:
- apply to `tbl_RateChange` and `tbl_RateChange_Pol`
- compute percentage outside `[-0.2, 0.2]`
- compute percentage outside `[-0.5, 0.5]`
- WARNING if more than 10% outside the narrower band
- FAIL if more than 50% outside the wider band

`Trend_Value`:
- apply the same logic to `tbl_Trend`

`DevPct_*`:
- in `tbl_Patterns_Attr`, `tbl_Patterns_Large`, `tbl_Patterns_Prem`
- detect all columns beginning with `DevPct_`
- compute mean and percentage above 1 per column
- WARNING if mean outside `[0.7, 1.0]`
- WARNING if fewer than 95% of values are below 1
- FAIL if more than 20% of values are above 1

`AsAt_Month`:
- validate `tbl_DetailedData.AsAt_Month`
- confirm all values are valid `YYYYMM` integers with months `01` to `12`
- confirm exactly one distinct value is present
- FAIL on any invalid format or multiple distinct values

`DevM` sequence:
- in the three pattern tables
- detect the pattern key column such as `Key_Pattern_*`
- for each distinct key value, verify:
	- minimum `DevM` equals 1
	- sequence has no gaps
	- maximum `DevM` is at least 120
- return failing keys with first `DevM`, last `DevM`, and identified gaps

Acceptance tests:
- percentage-style `RateChange` data triggers FAIL
- mixed `AsAt_Month` values trigger FAIL
- incomplete `DevM` sequence triggers FAIL with reported gaps

### Step 8. Add overall severity aggregation and summaries

Once all checks return structured results:
- aggregate to overall PASS/WARNING/FAIL
- compute per-section status badges
- count total failures and warnings
- create short summary strings for the banner and section headers

Deliverable:
- a complete run result can be serialized to JSON and consumed by Dash

### Step 9. Build the Dash layout

Create the webapp UI with:
- header with tool name and short purpose text
- single `Run` button
- status banner that updates after execution
- five collapsible sections in this order:
	- Key Registration
	- Referential Integrity
	- Parent Column Rule
	- Mapping Table Uniqueness
	- Value Checks
- within each section, render compact summaries first and expandable detail tables second

Recommended UX details:
- show a loading spinner while checks run
- display dataset scan timestamp and project key
- use color semantics consistently: red for FAIL, amber for WARNING, green for PASS
- truncate very long value lists in the UI and provide full text in expandable details

Deliverable:
- static Dash layout wired to mock result data

### Step 10. Wire callbacks and execution flow

Implement a single callback triggered by the `Run` button that:
- loads datasets
- executes the validation engine in sequence
- captures exceptions per check group where possible
- returns a full result payload to the UI

Implementation details:
- fail fast only for missing `tbl_DetailedData` or `tbl_Key_Mapping`
- for individual missing reference tables expected by value checks, report them within the value check section rather than crashing the app
- log run start, end, runtime, and per-check errors for debugging in Dataiku logs

Deliverable:
- end-to-end webapp run against project datasets

### Step 11. Add tests and fixture datasets

Build pandas fixture-based unit tests for the engine:
- one fixture per check group with minimal positive and negative examples
- one integrated smoke test covering overall severity rollup
- one regression test for the `find_parent_cols` absent-column behavior once the decision is confirmed

If Dash UI tests are too heavy for v1, keep them minimal:
- callback smoke test with mocked engine response
- snapshot or structural check that all five sections render

Deliverable:
- repeatable local validation for core check logic

### Step 12. Prepare deployment and handover

Before release:
- document required dataset names and assumptions
- document known exclusions and open items
- validate performance on realistic project-sized datasets
- confirm the webapp is separate from the Data Validation Tool
- provide a short user guide for running and reading results

Release checklist:
- no uncaught exceptions on empty or partially missing optional tables
- every spec rule maps to a visible output in the UI
- FAIL and WARNING thresholds match the spec exactly

## 6. Mapping From Spec to Implementation

| Spec area | Implementation module | Primary UI section |
|---|---|---|
| Key column registration | `checks/key_registration.py` | `1. Key Registration` |
| Referential integrity forward and reverse | `checks/referential_integrity.py` | `2. Referential Integrity` |
| Parent column rule | `checks/parent_columns.py` | `3. Parent Column Rule` |
| Mapping uniqueness | `checks/mapping_uniqueness.py` | `4. Mapping Table Uniqueness` |
| Value range checks | `checks/value_checks.py` | `5. Value Checks` |

## 7. Acceptance Criteria for v1

The Dash webapp is ready for v1 when:
- clicking `Run` completes a full scan without requiring user inputs
- the app reads datasets only from the current Dataiku project
- all five result sections render even when some sections are fully PASS
- every spec-defined FAIL and WARNING condition is implemented
- the status banner correctly rolls up section results
- top 10 unmatched groups are sorted by descending DetailedData premium
- reverse referential warnings list orphaned values by reference table
- duplicate mapping combinations are displayed with counts
- all required value checks are implemented except explicitly deferred `tbl_Min_Large_Load`
- unit tests cover the main happy path and failure path for each check group

## 8. Recommended Build Order

Use this implementation sequence to reduce rework:
1. Confirm open spec decisions.
2. Build the result schema and dataset registry.
3. Implement and test Check 1.
4. Implement and test referential integrity.
5. Implement and test parent-column validation.
6. Implement and test mapping uniqueness.
7. Implement and test value checks.
8. Add overall aggregation.
9. Build the Dash UI with mocked data.
10. Wire the live callback.
11. Run integrated tests and deploy in Dataiku.

## 9. Risks and Mitigations

- Ambiguous parent-column behavior: resolve before implementation to avoid changing FAIL counts later.
- Large dataset performance: use distinct keys and pre-aggregated joins rather than row-level joins where possible.
- Tables with multiple key columns: define result formatting early so the UI stays understandable.
- Missing optional tables for value checks: report gracefully in results instead of failing the whole run.
- Dash callback complexity: keep business logic outside callbacks and return preformatted result objects.

## 10. First Coding Sprint Recommendation

For the first sprint, aim to complete:
- project scaffold for the Dash webapp
- dataset registry and result schema
- Check 1
- referential integrity checks
- a minimal UI showing banner plus two sections

That delivers the highest-value path first and de-risks the join logic early.