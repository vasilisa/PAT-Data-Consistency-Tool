# PAT Data Consistency Webapp (Concise)

## What This Webapp Does

The Dash webapp runs the PAT data consistency checks against live Dataiku `tbl_*` datasets and returns:

- Overall status: `PASS`, `WARNING`, `FAIL`, or `SKIP`
- Sectioned results for checks 1 to 8
- Warnings/errors when any check degrades but execution continues

No user inputs are required. Users click **Run Checks** and review the rendered sections.

## Current Module Map

- `dash_app/app.py`: app bootstrap + callbacks
- `dash_app/ui/layout.py`: static page structure and placeholders
- `dash_app/ui/components.py`: render banner and sections from payload
- `dash_app/engine/contracts.py`: typed payload (`RunResult`, `SectionResult`, `CheckResult`, `Status`)
- `dash_app/engine/helpers.py`: pure column/key helper logic
- `dash_app/engine/checks.py`: check implementations (business rules)
- `dash_app/engine/loader_v2.py`: dataset loading + `tbl_DetailedData_Agg` lifecycle
- `dash_app/runner/orchestrator.py`: run flow, degradation handling, status rollup

## End-to-End Run Flow

1. UI callback triggers `run_all_checks()`.
2. `loader_v2.load_tbl_datasets()` loads reference tables and prepares aggregated DetailedData.
3. `classify_tables()` splits into `dd_df`, `mapping_df`, and `ref_tables`.
4. Orchestrator executes checks with timed wrappers.
5. If a check errors, section degrades to safe payload and run continues.
6. Results are serialized to `dcc.Store` and rendered by UI components.

## Latest Functional Changes

### 2026-05-12 Parity Update

- Check 4 parity: `tbl_Trend` uniqueness uses all columns except `Trend_Value`.
- Check 8 parity: unmapped `Key_Modelling*` premium breakdown by shared mapping identifier columns.
- UI parity: Check 8 breakdown table rendered (including `% of total`).
- UI table UX: result tables are scrollable (`overflowY` + `overflowX`) and no longer capped to 10 rows by default.

### 2026-05-15 Loader Ghost-Dataset Fix

- Scenario fixed: `tbl_DetailedData_Agg` exists but `compute_tbl_DetailedData_Agg` recipe is missing.
- Previous failure mode:
  - attempted `with_new_output(...)` for an already-existing dataset,
  - fallback attempted `project.create_dataset(...)`,
  - led to Dataiku errors in some environments.
- Current behavior:
  - when dataset exists and recipe is missing, create recipe with `with_output(...)` bound to existing dataset.

## Loader v2 Decision Summary

`_ensure_dd_aggregated(...)` now behaves as follows:

- `recipe missing + agg exists`:
  - create recipe bound to existing agg dataset (`with_output`)
- `recipe missing + agg missing`:
  - create recipe + output dataset (`with_new_output`, with fallback path)
- `recipe exists but agg missing` or `recipe does not point to agg`:
  - clean broken pair metadata, recreate recipe/output, rebuild
- healthy pair:
  - update settings, sync schema, rebuild

In all branches, loader then:

1. applies grouping settings
2. syncs output schema explicitly
3. forces build and verifies completion

## Robustness Guarantees

- Fatal setup guard: returns `FAIL` run result if loading/classification crashes.
- Section-level degradation: one failing check does not abort the entire run.
- Explicit schema sync before build: prevents empty-output schema issues in DSS/Snowflake flows.
- Optional table tolerance: missing optional `tbl_*` tables are skipped.

## Snowflake Connection Handling

The Snowflake connection is managed by the loader logic in `loader_v2.py`:

- Connection details (`connection`, `catalog`, `schema`) are extracted from the Dataiku dataset settings (`dd_raw['params']`).
- The loader enforces these parameters when creating or binding the `tbl_DetailedData_Agg` dataset and its recipe.
- If the primary connection fails, fallback parameters are used to ensure the dataset is created on a known-good Snowflake connection, database, and schema.
- Drift detection logic checks if the dataset metadata points to the wrong database or schema and triggers recreation if needed.
- The loader uses Dataiku's `with_new_output` or `with_output` to bind the recipe to the correct Snowflake dataset, and explicitly syncs the schema before building.
- Physical Snowflake table cleanup is handled with `SQLExecutor2` if required.

This ensures robust, automated management of the Snowflake connection and dataset lifecycle, with fallback and recovery for common failure scenarios.

## Verification Status (Current)

- Unit tests: passing (`tests/`)
- Loader ghost branch regression test: added and passing (`tests/test_loader_schema.py`)
- Parity harness: passing with `all_pass: true` (`parity_verify.py`)
