# PAT Data Consistency Tool Wiki

## Purpose

This wiki explains:

1. Each major part of the Dash app.
2. The two loader versions:
   - Original/previous loader (saved backup).
   - Current loader with consistency-first recreate/rebuild logic.


## App Architecture

### 1) Entry Point

- File: dash_app/app.py
- Responsibility:
  - Creates Dash app instance.
  - Wires callbacks.
  - Connects UI to run-result payload.

Main callback flow:

1. User clicks Run.
2. `run_all_checks()` is called.
3. Result is serialized into `dcc.Store`.
4. Renderer callback transforms payload into UI sections.


### 2) Layout Layer

- File: dash_app/ui/layout.py
- Responsibility:
  - Defines static page frame (title, Run button, placeholders).
  - Declares output containers for each result section.

Key containers:

- `run-result-store`
- `result-banner`
- `section-check1`
- `section-checks2_5`
- `section-check6`
- `section-check7`
- `section-check8`


### 3) UI Component Rendering

- File: dash_app/ui/components.py
- Responsibility:
  - Convert payload data into Dash HTML components.
  - Render sections for checks 1 to 8.
  - Render status banner and warning/error notes.

Notes:

- Section UI is rendered as replacement content per callback output.
- Duplicate-ID inner wrappers were removed to prevent repeated UI stacking.


### 4) Contracts Layer

- File: dash_app/engine/contracts.py
- Responsibility:
  - Defines canonical payload dataclasses.
  - Defines status enum (`PASS`, `WARNING`, `FAIL`, `SKIP`).
  - Provides serialization helpers for Dash store transport.


### 5) Helper Layer

- File: dash_app/engine/helpers.py
- Responsibility:
  - Pure helper functions for key/join/context column detection.
  - No Dataiku SDK dependency.


### 6) Checks Layer

- File: dash_app/engine/checks.py
- Responsibility:
  - Implements all data consistency checks.
  - Returns plain dictionaries consumed by orchestrator.


### 7) Loader Layer

- File: dash_app/engine/loader.py
- Responsibility:
  - Reads Dataiku datasets.
  - Ensures `tbl_DetailedData_Agg` and its grouping recipe are valid.
  - Loads aggregate data as logical `tbl_DetailedData`.


### 8) Orchestration Layer

- File: dash_app/runner/orchestrator.py
- Responsibility:
  - Executes checks in order.
  - Builds section-level and run-level summaries.
  - Applies status rollup.
  - Adds warnings/errors and metadata.


## Loader Version 1 (Backup)

- File: dash_app/engine/loader_backup.py

Behavior summary:

1. If `tbl_DetailedData_Agg` does not exist, create dataset first.
2. Ensure recipe exists (`compute_tbl_DetailedData_Agg`).
3. Always update recipe settings.
4. Build only when agg dataset was missing.
5. If agg exists already, settings are updated but no rebuild is run.

Main downside:

- In Dataiku, creating/linking recipe and existing output dataset can create flow consistency problems and manual rebuild requirements.
- Existing data may become stale if grouping settings changed but rebuild is skipped.


## Loader Version 2 (Current)

- File: dash_app/engine/loader.py

Core goal:

- Treat agg dataset + recipe as a single consistency unit.
- Recreate broken pairs.
- Rebuild healthy pairs after settings updates.

Constants:

- `AGG_DATASET_NAME = "tbl_DetailedData_Agg"`
- `AGG_RECIPE_NAME = "compute_tbl_DetailedData_Agg"`

### New helper functions

- `_get_agg_output_spec(...)`
- `_get_recipe_output_refs(...)`
- `_delete_agg_dataset(...)`
- `_delete_agg_recipe(...)`
- `_drop_agg_snowflake_table(...)`
- `_create_agg_recipe(...)`
- `_configure_agg_recipe(...)`
- `_build_agg_recipe(...)`


## Current Loader Decision Tree

Inside `_ensure_dd_aggregated(...)`:

Inputs inspected:

- `agg_exists`
- `recipe_exists`
- `recipe_points_to_agg`

Healthy pair condition:

- `healthy_pair = agg_exists and recipe_exists and recipe_points_to_agg`

### Branch A: `healthy_pair_rebuild`

Condition:

- Dataset exists.
- Recipe exists.
- Recipe outputs to agg dataset.

Action:

1. Update recipe settings.
2. Rebuild recipe.


### Branch B: `ghost_dataset_recreate`

Condition:

- Agg dataset exists, but recipe missing or recipe does not point to agg dataset.

Action:

1. Warn in logs.
2. Delete stale recipe if present.
3. Delete stale agg dataset.
4. Drop Snowflake agg table (`DROP TABLE IF EXISTS ...`).
5. Recreate recipe with `with_new_output(...)`.
6. Apply settings.
7. Build recipe.


### Branch C: `broken_recipe_recreate`

Condition:

- Recipe exists, but agg dataset is missing.

Action:

1. Warn in logs.
2. Delete stale recipe.
3. Drop Snowflake agg table.
4. Recreate recipe with `with_new_output(...)`.
5. Apply settings.
6. Build recipe.


### Branch D: `missing_pair_recreate`

Condition:

- Neither agg dataset nor recipe exists.

Action:

1. Drop Snowflake agg table (safe cleanup).
2. Create recipe + new output dataset in one step.
3. Apply settings.
4. Build recipe.


## Why This Version Is Safer in Dataiku

1. Avoids partial/broken recipe-dataset linkage states.
2. Avoids stale agg data after settings changes.
3. Handles ghost metadata explicitly.
4. Uses one consistent recreation path when pairing is invalid.


## Logging You Can Track

Key log markers:

- `DetailedData_Agg ensure start: ...`
- `DetailedData_Agg branch evaluation: healthy_pair=...`
- `DetailedData_Agg branch selected: healthy_pair_rebuild`
- `DetailedData_Agg branch selected: ghost_dataset_recreate`
- `DetailedData_Agg branch selected: broken_recipe_recreate`
- `DetailedData_Agg branch selected: missing_pair_recreate`
- `DetailedData_Agg build finished with state=...`


## File Map

- dash_app/app.py
- dash_app/ui/layout.py
- dash_app/ui/components.py
- dash_app/engine/contracts.py
- dash_app/engine/helpers.py
- dash_app/engine/checks.py
- dash_app/engine/loader.py
- dash_app/engine/loader_backup.py
- dash_app/runner/orchestrator.py


## Notes

- `loader_backup.py` is a snapshot of the previous logic for reference and rollback.
- The active loader is `loader.py`.
