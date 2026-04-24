## Plan: Dataiku Dash App Delivery with Human Gates

Build the Dash webapp inside Dataiku with strict parity to the current notebook behavior, but execute work in gated stages. Every task ends with a Human Validation Gate that must be approved before moving forward.

## Gate Tracker

| Gate ID | Stage | Gate Description | Status | Reviewer | Date | Notes |
|---|---|---|---|---|---|---|
| G0.1 | Stage 0 | Scope lock and parity target confirmed | Approved | Human Reviewer | 2026-04-24 | Baseline locked to `data_consistency_checks.py` behavior only |
| G0.2 | Stage 0 | Runtime model and environment assumptions confirmed | Approved | Human Reviewer | 2026-04-24 | Dataiku Dash app; project-local datasets only |
| G0.3 | Stage 0 | Parity acceptance checklist approved | Approved | Human Reviewer | 2026-04-24 | Checklist defined below |
| G1.1 | Stage 1 | Module boundaries approved | Approved | Human Reviewer | 2026-04-24 | Engine / Runner / UI layers scaffolded |
| G1.2 | Stage 1 | Normalized result contract approved | Approved | Human Reviewer | 2026-04-24 | Dataclass contract defined in `dash_app/engine/contracts.py` |
| G1.3 | Stage 1 | Minimal Dash skeleton approved | Approved | Human Reviewer | 2026-04-24 | Static placeholders for Check 1, Checks 2-5, Check 6, Check 7, Check 8 |
| G2.1 | Stage 2 | Data loading and classification module approved | Approved | Human Reviewer | 2026-04-24 | `dash_app/engine/loader.py` — migrated from `data_consistency_checks.py` |
| G2.2 | Stage 2 | Column/key helpers module approved | Approved | Human Reviewer | 2026-04-24 | `dash_app/engine/helpers.py` — migrated from `data_consistency_checks.py` |
| G2.3 | Stage 2 | Check functions module approved | Approved | Human Reviewer | 2026-04-24 | `dash_app/engine/checks.py` — all 7 check functions, zero semantic changes |
| G2.4 | Stage 2 | Orchestration runner approved | Approved | Human Reviewer | 2026-04-24 | `run_all_checks()` in `dash_app/runner/orchestrator.py` produces real RunResult |
| G3.1 | Stage 3 | Section render components approved | Approved | Human Reviewer | 2026-04-24 | `dash_app/ui/components.py` — Dash component trees for all 5 sections |
| G3.2 | Stage 3 | Full live layout approved | Approved | Human Reviewer | 2026-04-24 | `layout.py` updated; section callbacks wired in `app.py` |
| G4.1 | Stage 4 | Structured error handling approved | Approved | Human Reviewer | 2026-04-24 | Fatal setup vs section-level degradation handling in runner/callback |
| G4.2 | Stage 4 | Run logging and timings approved | Approved | Human Reviewer | 2026-04-24 | Start/end/runtime and check-level timing logs emitted |
| G4.3 | Stage 4 | Safe heavy-output limits approved | Approved | Human Reviewer | 2026-04-24 | Preview-row caps for serialized dataframe payloads |
| G5.1 | Stage 5 | Overall status parity approved | Approved | Human Reviewer | 2026-04-24 | Notebook/source checks vs migrated engine on same snapshot |
| G5.2 | Stage 5 | Metric-level parity approved | Approved | Human Reviewer | 2026-04-24 | Counts/percentages/duplicate/orphan/unmatched/top-10 parity |
| G5.3 | Stage 5 | Edge-case handling parity approved | Approved | Human Reviewer | 2026-04-24 | Missing tables, invalid AsAt_Month, DevM gaps, mapping duplicates |
| G6.1 | Stage 6 | Test coverage map approved | Approved | Human Reviewer | 2026-04-24 | 67 unit tests across all 8 checks, contracts, rollup |
| G6.2 | Stage 6 | UI integration sanity checks approved | Approved | Human Reviewer | 2026-04-24 | Orchestrator end-to-end with mocked loader; RunResult shape and sections |
| G6.3 | Stage 6 | Final docs approved | Approved | Human Reviewer | 2026-04-24 | README Dash webapp section: run steps, known limits, troubleshooting |
| G6.4 | Stage 6 | Release approval | Approved | Human Reviewer | 2026-04-24 | Final go/no-go for production deploy |

Status values: `Approved`, `Rework Required`, `Blocked`, `Pending Approval`.

## Stage 0 Deliverables (Completed, Awaiting Approval)

### D0.1 Scope Lock (Parity Baseline)

Implementation baseline is locked to the current checker script behavior in `data_consistency_checks.py` with no semantic changes in v1:

- No new checks.
- No threshold changes.
- No change to severity precedence (`FAIL > WARNING > PASS`).
- No change to ordering or truncation semantics used in current notebook output (including top-10 ordering for unmatched premium groups).
- Parent-column absent-candidate handling remains `excluded` (matching current helper semantics).

### D0.2 Runtime Model and Environment Assumptions

- Deployment target: Dataiku-hosted Dash webapp inside the same PAT Data project.
- Data access scope: project-local datasets only (no cross-project reads).
- Source-of-truth behavior files:
  - `data_consistency_checks.py`
  - `PAT_utils.py`
  - `DataConsistencyTool_Spec_v1.md`
- First migration objective: behavior-preserving refactor into engine/runner/UI layers, not business-rule redesign.

### D0.3 Parity Acceptance Checklist (for Stage 5 sign-off)

All items below must pass on the same data snapshot when comparing notebook output vs Dash output.

1. Overall status parity
	- Overall banner status matches exactly (`PASS`/`WARNING`/`FAIL`).

2. Per-check status parity
	- Check 1 status matches per table.
	- Checks 2-5 statuses match per table and rollup table status matches.
	- Check 6 status matches.
	- Check 7 status matches per table/column validator.
	- Check 8 status matches per `Key_Modelling*` column.

3. Key metrics parity
	- Forward RI: unmatched combination counts and unmatched premium totals match.
	- Forward RI: unmatched premium percentage matches after rounding behavior parity.
	- Reverse RI: orphan counts match.
	- Row uniqueness: duplicate combination counts match.
	- Mapping uniqueness: duplicate combination counts match.
	- Value-range diagnostics: counts/percentages/means and final statuses match.
	- Key_Modelling coverage: unmapped row count, unmapped premium, and premium % match.

4. Display ordering and sampling parity
	- Forward RI top-10 tables are sorted by descending unmatched premium with matching order.
	- Row previews use the same cap behavior as notebook output.
	- DevM failure previews and mapping duplicate previews use matching truncation semantics.

5. Data-shape and metadata parity
	- Missing optional tables are handled consistently.
	- `AsAt_Month` validation semantics match exactly.
	- Forecast-specific orphan premium behavior matches exactly.

## Human Validation Gate (Required Before Stage 1)

Reviewer decision required:

- Gate G0.1 (Scope lock): `Approved`
- Gate G0.2 (Runtime model): `Approved`
- Gate G0.3 (Acceptance checklist): `Approved`

Do not start Stage 1 tasks until all Stage 0 gates are marked `Approved`.

## Stage 1 Deliverables (Completed, Awaiting Approval)

### D1.1 Module Boundaries

- Engine layer: `dash_app/engine/`
- Runner layer: `dash_app/runner/`
- Dash UI layer: `dash_app/ui/`
- App entrypoint: `dash_app/app.py`

### D1.2 Normalized Result Contract

Defined typed payload schema in `dash_app/engine/contracts.py`:

- `Status` enum (`PASS`, `WARNING`, `FAIL`, `SKIP`)
- `CheckResult`
- `SectionResult`
- `RunMetadata`
- `RunResult`
- Helpers: `to_payload`, `empty_run_result`

### D1.3 Minimal Dash Skeleton (Static)

Implemented static Stage 1 scaffold:

- Single `Run` button
- Loading wrapper and status banner placeholder
- `dcc.Store` payload sink
- Static section placeholders in required order:
	1. Check 1
	2. Checks 2-5
	3. Check 6
	4. Check 7
	5. Check 8

Callback currently returns static placeholder payload (`build_static_stage1_result`) without executing business checks.

## Human Validation Gate (Required Before Stage 2)

Reviewer decision required:

- Gate G1.1 (Module boundaries): `Approved`
- Gate G1.2 (Result contract): `Approved`
- Gate G1.3 (Dash skeleton): `Approved`

## Stage 2 Deliverables (Completed, Awaiting Approval)

### D2.1 Data Loading and Classification Module

Created `dash_app/engine/loader.py`:

- `WIKI_TABLES` constant (15 table names, identical to source)
- `_get_dd_group_cols(loaded_ref_tables)` — reads DD schema, returns GROUP BY cols
- `_ensure_dd_aggregated(project, loaded_ref_tables)` — creates/updates Group recipe; lazy `import dataiku` inside function body to avoid import errors outside Dataiku runtime
- `load_tbl_datasets()` — loads all in-scope tbl_* datasets; returns `{name: DataFrame}`
- `classify_tables(datasets)` → `(dd_df, mapping_df, ref_tables)`

All logic copied verbatim from `data_consistency_checks.py`. Only change: `import dataiku` moved inside functions (was top-level import).

### D2.2 Column/Key Helpers Module

Created `dash_app/engine/helpers.py`:

- `STRUCTURAL_COLS = {"AY", "UWY", "UY", "DevM"}`
- `PATTERN_TABLES = {"tbl_Patterns_Attr", "tbl_Patterns_Large", "tbl_Patterns_Prem"}`
- `find_parent_cols(df_map, key_col, col_candidates)`
- `get_join_key(ref_df, dd_df)`
- `get_key_cols(df)`
- `get_display_cols(ref_df, dd_df)`
- `get_context_cols(ref_df, dd_df)`

No Dataiku SDK dependency. Zero semantic changes from source.

### D2.3 Check Functions Module

Created `dash_app/engine/checks.py`:

- `check_key_registration(dd_df, mapping_df, ref_tables)` — Check 1, FAIL
- `check_referential_integrity(dd_df, ref_tables)` — Checks 2+3, FAIL/WARNING; tbl_Forecast premium enrichment preserved
- `check_row_uniqueness(dd_df, ref_tables)` — Check 4, FAIL; tbl_Forecast special-case and PATTERN_TABLES DevM special-case preserved
- `check_parent_columns(dd_df, mapping_df, ref_tables)` — Check 5, FAIL; tbl_Forecast special-case and tbl_Min_Large_Load domain check preserved
- `check_mapping_uniqueness(mapping_df)` — Check 6, FAIL
- `check_value_ranges(dd_df, ref_tables)` — Check 7, FAIL/WARNING; all sub-checks (RateChange, Trend_Value, DevPct_*, AsAt_Month, DevM, MinLoad_*) preserved exactly
- `check_key_modelling_unmapped(dd_df)` — Check 8, WARNING

All functions import from `dash_app.engine.helpers`. Zero semantic changes from source.

### D2.4 Orchestration Runner

Updated `dash_app/runner/orchestrator.py`:

- Added `run_all_checks()` — loads datasets, runs all 7 checks in original order, builds `RunResult` with 5 `SectionResult` objects
- Status rollup: FAIL > WARNING > PASS > SKIP (unchanged `_derive_overall_status`)
- DataFrames serialized to `list[dict]` via `_df_to_records()` before storing in `CheckResult.details`
- `build_static_stage1_result()` retained for reference but no longer called

Updated `dash_app/app.py` callback to call `run_all_checks()`.

## Human Validation Gate (Required Before Stage 3)

Reviewer decision required:

- Gate G2.1 (Data loading module): `Approved`
- Gate G2.2 (Column/key helpers module): `Approved`
- Gate G2.3 (Check functions module): `Approved`
- Gate G2.4 (Orchestration runner): `Approved`

## Stage 3 Deliverables (Completed, Awaiting Approval)

### D3.1 Section Render Components

Created `dash_app/ui/components.py`:

- Color palette matches notebook (`_STATUS_COLOR`, `_STATUS_BG`)
- `_badge(status)` — inline colored status badge
- `_records_table(records, max_rows=10)` — renders list-of-dicts as `html.Table`; shows "N more rows not shown" footer when truncated
- `render_section1(section)` — Check 1: per-table FAIL rows with missing-key table; PASS summary line
- `render_section2(section)` — Checks 2–5: Forward RI (top-10 by premium), Reverse RI (orphaned rows), Row uniqueness (duplicate combinations), Parent columns (text note), Domain membership (tbl_Min_Large_Load)
- `render_section3(section)` — Check 6: PASS/FAIL with duplicate count and rows
- `render_section4(section)` — Check 7: summary table (Table/Column/Status/Detail) + DevM failing-pattern detail tables
- `render_section5(section)` — Check 8: Key_Modelling coverage table (Column/Status/Unmapped rows/Unmapped premium/%)
- `render_banner(status, summary, metadata)` — top-level status banner
- `render_all_sections(payload)` → 6-tuple (banner, s1, s2, s3, s4, s5)

### D3.2 Updated Layout and Callbacks

Updated `dash_app/ui/layout.py`:
- Removed placeholder section content
- Added `id="result-banner"` div for banner output
- Section divs kept as `id="section-{id}"` output targets

Updated `dash_app/app.py`:
- Added `on_result_stored` callback: `Input("run-result-store", "data")` → 6 outputs (banner + 5 sections)
- Calls `render_all_sections(payload)` to produce full Dash component tree from stored payload

## Human Validation Gate (Required Before Stage 4)

Reviewer decision required:

- Gate G3.1 (Section render components): `Approved`
- Gate G3.2 (Full live layout): `Approved`

## Stage 4 Deliverables (Completed, Awaiting Approval)

### D4.1 Structured Error Handling

Updated `dash_app/runner/orchestrator.py`:

- Fatal setup errors are isolated in `run_all_checks()` around dataset load/classification.
- Fatal setup failure returns `RunResult(status=FAIL, sections=[], errors=[...])` with metadata populated.
- Each check function executes via `_timed_check(...)` wrapper with exception capture.
- Per-check failures degrade only the affected section(s) rather than crashing the run.
- Section-level degradations are flagged via `_with_section_warning(...)` and surfaced in payload warnings.
- If degradations occur and no FAIL exists, overall run status is elevated from PASS/SKIP to WARNING.

Updated `dash_app/app.py`:

- Callback-level guard wraps `run_all_checks()`.
- Unhandled callback exceptions produce a fail-safe `RunResult` with explicit error message.

### D4.2 Run Logging and Timing Observability

Updated `dash_app/runner/orchestrator.py` logging:

- `Run start` and `Run end` log lines with status, dataset count, runtime, warning count.
- Each check logs `Check start` / `Check end` with elapsed milliseconds.
- Failed checks emit `logger.exception(...)` with stack trace.

### D4.3 Safe Limits for Heavy Outputs

Updated serialization strategy in `dash_app/runner/orchestrator.py`:

- Added constants: `PREVIEW_ROWS_TOP10 = 10`, `PREVIEW_ROWS_DEFAULT = 20`.
- `_df_to_records(...)` now clips previews deterministically before conversion.
- Forward RI top preview explicitly capped to 10 rows at serialization layer.
- Existing check logic and business outcomes remain unchanged; only display payload size is bounded.

Updated `dash_app/ui/components.py` banner:

- Run-level warnings/errors from payload are rendered in banner for support/debug visibility.

## Human Validation Gate (Required Before Stage 5)

Reviewer decision required:

- Gate G4.1 (Structured error handling): `Approved`
- Gate G4.2 (Run logging and timings): `Approved`
- Gate G4.3 (Safe heavy-output limits): `Approved`

## Stage 5 Deliverables (Completed, Awaiting Approval)

### D5.1 Overall Status Parity (Scenario: baseline)

- Source engine and migrated engine produce identical overall status on the same mock snapshot.
- Full payload canonicalized (DataFrames, NaN, numeric types normalized) and compared — no diff.

### D5.2 Metric-Level Parity (Scenarios: baseline, missing_optional_tables)

- Check 1 (key registration): status, missing-key lists — match.
- Check 2 (forward RI): total_unmatched, top10 ordering/values, premium, premium_pct — match.
- Check 3 (reverse RI): total_orphaned, orphan rows, forecast_orphaned_premium — match.
- Check 4 (row uniqueness): duplicate_count, duplicate rows — match.
- Check 5 (parent columns): failing_cols, domain_fails — match.
- Check 6 (mapping uniqueness): total_duplicates, duplicate rows — match.
- Check 7 (value ranges): all sub-checks status/detail/metrics — match.
- Check 8 (Key_Modelling coverage): unmapped_count, unmapped_premium, premium_pct per column — match.
- Scenario with optional tables removed: output identical.

### D5.3 Edge-Case Parity

- `invalid_asat_month`: invalid YYYYMM value injected → both engines produce identical FAIL with same distinct_values list.
- `devm_gap`: DevM row removed from tbl_Patterns_Attr → both engines report identical failing_patterns list.
- `mapping_duplicate`: duplicate mapping row injected → both engines produce identical FAIL with matching duplicate rows.

### Verification Method

`parity_verify.py` at workspace root:
- Parses check functions from `data_consistency_checks.py` at AST level (no Dataiku SDK imported).
- Loads mock datasets from `mock_data/`.
- Runs all 5 scenarios through both source and migrated engines.
- Canonicalizes full payloads and JSON-compares.
- All 5 scenarios passed: `all_pass: true`.

## Human Validation Gate (Required Before Stage 6)

Reviewer decision required:

- Gate G5.1 (Overall status parity): `Approved`
- Gate G5.2 (Metric-level parity): `Approved`
- Gate G5.3 (Edge-case handling parity): `Approved`

## Stage 6 Deliverables (Completed, Awaiting Approval)

### D6.1 Engine Unit Tests (`tests/test_checks.py`)

35 unit tests covering all 8 check functions with small in-memory pandas fixtures. No Dataiku SDK. Coverage:

- **Check 1 (key registration)**: pass, fail (key absent from DD), fail (key absent from mapping), Key_Forecast excluded, no mapping_df.
- **Check 2+3 (RI)**: perfect match, forward gap, reverse orphan, no common columns, premium attachment.
- **Check 4 (row uniqueness)**: unique, duplicate, pattern table DevM included, pattern table DevM duplicate.
- **Check 5 (parent columns)**: key is parent, key not parent, empty ref table.
- **Check 6 (mapping uniqueness)**: unique, duplicate non-key rows, None mapping, key-only cols.
- **Check 7 (value ranges)**: RateChange pass/fail, AsAt_Month pass/fail/multiple, DevM gap/complete/short.
- **Check 8 (key_modelling coverage)**: all mapped, nulls present, no Premium col, None df, no Key_Modelling col, premium_pct.

### D6.2 Contract and Rollup Tests (`tests/test_contracts.py`, `tests/test_orchestrator_rollup.py`)

32 tests covering:

- **Contracts**: Status enum values, `empty_run_result()` shape, `to_payload()` serialization round-trip (plain dict, all keys, string status, metadata, round-trip with nested SectionResult/CheckResult).
- **`_derive_overall_status` precedence**: FAIL beats all, WARNING beats PASS/SKIP, PASS beats SKIP, all SKIP, empty list.
- **Section builders**: `_build_section1`, `_build_section3`, `_build_section5` return correct types and statuses.
- **`run_all_checks` end-to-end** (mocked loader): returns RunResult, has 5 sections, valid status enum, metadata populated, fatal loader error returns FAIL with error message.

All 67 tests pass: `67 passed in 0.56s`.

### D6.3 Operational Docs (`README.md`)

Added **Dash Webapp** section to README covering:

- How to run in Dataiku (step-by-step)
- Module structure (`dash_app/` directory tree)
- Known limits (preview row caps, no auto-refresh, optional table skipping, check isolation)
- Troubleshooting table (6 common symptoms → cause → fix)
- Local test run command
- Parity verification re-run command

## Human Validation Gate (Stage 6 — Release Readiness)

Reviewer decision required:

- Gate G6.1 (Test coverage map): `Approved`
- Gate G6.2 (UI integration sanity checks): `Approved`
- Gate G6.3 (Final docs): `Approved`
- Gate G6.4 (Release approval): `Approved`

All Stage 6 gates approved. Project is cleared for production deployment.

**Stage 0: Kickoff and Guardrails**
1. Lock implementation baseline to current checker behavior only (no rule changes, no threshold changes, no extra checks).
Human Validation Gate: Confirm scope lock and parity target.
2. Confirm runtime model: Dataiku-hosted Dash app, project-local dataset access only.
Human Validation Gate: Confirm deployment/environment assumptions.
3. Publish acceptance checklist used for parity sign-off (overall status, per-check statuses, key metrics, top-10 ordering).
Human Validation Gate: Approve acceptance checklist.

**Stage 1: Architecture and Scaffolding**
1. Create module boundaries: engine layer, runner layer, Dash UI layer.
Human Validation Gate: Approve folder/module structure before code migration.
2. Define normalized result contract for UI rendering (overall status, section statuses, summaries, table payloads, warnings/errors).
Human Validation Gate: Approve payload schema and field names.
3. Add a minimal Dash skeleton with static placeholders for all output sections.
Human Validation Gate: Approve page layout and section ordering.

**Stage 2: Engine Migration (No Logic Changes)**
1. Move dataset loading and table classification helpers from current script into engine modules unchanged.
Human Validation Gate: Review diff to confirm behavior-preserving migration.
2. Move column/key helper functions unchanged.
Human Validation Gate: Confirm join-key and context-column semantics preserved.
3. Move all check functions unchanged, preserving special cases (forecast, pattern tables, Min_Large_Load, Key_Modelling coverage).
Human Validation Gate: Confirm all checks are present and function signatures stable.
4. Implement orchestration runner reproducing current execution order and status precedence FAIL > WARNING > PASS.
Human Validation Gate: Approve runner order and rollup behavior.

**Stage 3: UI Binding and Rendering Parity**
1. Implement Run callback that triggers runner, handles loading state, and stores result payload.
Human Validation Gate: Confirm run lifecycle UX (button disable/spinner/errors).
2. Render top banner and metadata from payload.
Human Validation Gate: Confirm banner status and metadata format.
3. Implement Check 1 section renderer.
Human Validation Gate: Confirm table columns and status display parity.
4. Implement combined Checks 2-5 section renderer.
Human Validation Gate: Confirm forward/reverse/duplicates/parent notes parity.
5. Implement Check 6, Check 7, and Check 8 section renderers.
Human Validation Gate: Confirm detail tables and thresholds are rendered correctly.

**Stage 4: Reliability and Observability**
1. Add structured error handling: fatal setup errors vs section-level degradations.
Human Validation Gate: Validate error messages and fallback behavior.
2. Add run logging (start/end/runtime/check-level timing) to Dataiku logs.
Human Validation Gate: Confirm logs are readable and sufficient for support.
3. Add safe limits for heavy outputs (top-N previews where appropriate) without changing business outcomes.
Human Validation Gate: Confirm truncation/display strategy is acceptable.

**Stage 5: Human Parity Verification**
1. Execute notebook checker and Dash app on same data snapshot.
Human Validation Gate: Compare and approve overall status parity.
2. Compare check-level outcomes: counts, percentages, duplicate counts, orphan counts, unmatched premium totals, top-10 order.
Human Validation Gate: Approve metric-level parity.
3. Exercise edge scenarios: missing optional tables, invalid AsAt_Month, DevM gaps, duplicate mapping rows.
Human Validation Gate: Approve edge-case handling.

**Stage 6: Test and Release Readiness**
1. Add engine unit tests for each check plus integrated rollup test.
Human Validation Gate: Approve test coverage map.
2. Add callback smoke test and payload-shape contract test.
Human Validation Gate: Approve UI integration sanity checks.
3. Update operational docs (run steps, known limits, troubleshooting).
Human Validation Gate: Approve final docs.
4. Final go/no-go review.
Human Validation Gate: Release approval.

**Execution Rules**
- Do not start a task until prior Human Validation Gate is approved.
- If a gate fails, create a remediation task and re-run the same gate.
- Track gate outcomes as Approved, Rework Required, or Blocked.
- No stage can be marked complete with open rework from earlier gates.

**Relevant files**
- /Users/vasilisa.skvortsova@dataiku.com/Documents/DSIR/Chubb/PAT Data Consistency Tool/data_consistency_checks.py — behavior source-of-truth.
- /Users/vasilisa.skvortsova@dataiku.com/Documents/DSIR/Chubb/PAT Data Consistency Tool/PAT_utils.py — helper semantics reference.
- /Users/vasilisa.skvortsova@dataiku.com/Documents/DSIR/Chubb/PAT Data Consistency Tool/DataConsistencyTool_Spec_v1.md — rule/threshold reference.
- /Users/vasilisa.skvortsova@dataiku.com/Documents/DSIR/Chubb/PAT Data Consistency Tool/README.md — release/run documentation target.

**Verification**
1. Stage-completion checklist signed at each Human Validation Gate.
2. Notebook vs Dash parity matrix approved by reviewer.
3. No unresolved gate failures at final review.

**Decisions**
- Deployment: inside Dataiku Dash webapp.
- Parent-column absent candidate behavior: excluded from parent check, matching current helper behavior.
- v1 scope excludes new validation rules or semantic changes.
