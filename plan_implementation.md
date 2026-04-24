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
| G3.1 | Stage 3 | Section render components approved | Pending Approval |  |  | `dash_app/ui/components.py` — Dash component trees for all 5 sections |
| G3.2 | Stage 3 | Full live layout approved | Pending Approval |  |  | `layout.py` updated; section callbacks wired in `app.py` |

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

- Gate G3.1 (Section render components): `Pending Approval`
- Gate G3.2 (Full live layout): `Pending Approval`

Do not start Stage 4 tasks until all Stage 3 gates are marked `Approved`.

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
