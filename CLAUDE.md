# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Kedro 1.0 project that processes and analyzes Eastern States 100 mile foot race data (2016–2025). It ingests split times from multiple sources with varying formats, normalizes them into a unified long-format dataset, and will eventually feed ML pace-prediction models.

Years not run: 2018, 2020, 2024.

## Core Commands

```bash
# Install dependencies
uv sync

# Run everything (data_processing → feature_engineering → reporting)
uv run kedro run

# Run a single pipeline
uv run kedro run --pipeline data_processing
uv run kedro run --pipeline feature_engineering
uv run kedro run --pipeline reporting

# Data-quality regression tests (read pipeline outputs — run the pipeline first).
# Test deps live in the `dev` extra: `uv sync --extra dev` once, or `uv run pytest`
# silently falls back to a system pytest without the project installed.
uv run pytest tests/

# Run specific nodes by name
uv run kedro run --nodes wide_to_long__es_splits_2021_2025
uv run kedro run --nodes enrich__es_splits_2021_2025,plot__pace_chart

# Interactive development (catalog, context, session available in scope)
uv run kedro jupyter lab
uv run kedro ipython

# Visualize pipeline DAG and dataset previews
uv run kedro viz

# Lint / format
ruff check .
ruff format .

# Browse MLflow experiment tracking (runs logged automatically by kedro run)
uv run mlflow ui --backend-store-uri sqlite:///mlflow.db
```

## Architecture

### Kedro Conventions

- **Pipeline discovery**: `find_pipelines()` in `pipeline_registry.py` auto-discovers all pipeline modules. The `__default__` pipeline is the union of all registered pipelines.
- **Config loader**: `OmegaConfigLoader` with a custom `polars` resolver — use `${polars:Int64}` in `catalog.yml` to reference Polars dtypes directly.
- **Environments**: base config in `conf/base/`, local overrides (credentials, etc.) in `conf/local/` (gitignored).

### Data Layer Conventions (`conf/base/catalog.yml`)

| Layer | Path | Purpose |
|---|---|---|
| `raw` | `data/01_raw/` | Source files, never modified |
| `intermediate` | `data/02_intermediate/` | Node outputs, regenerable |
| `feature` | `data/04_feature/` | Imputed splits and interval-pace features |
| `models` | `data/06_models/` | Pickled stoppage model + validation metrics |
| `reporting` | `data/08_reporting/` | Plotly charts, exported artifacts |

Intermediate datasets that need Kedro Viz table previews use `PolarsPreviewCSVDataset` (defined in `src/.../datasets/polars_preview_csv_dataset.py`) instead of bare `polars.CSVDataset`. Raw datasets load with `infer_schema_length: 0` plus `schema_overrides` when column types need to be pinned.

### Custom Dataset Classes (`src/.../datasets/`)

- **`PolarsPreviewCSVDataset`**: extends `polars.CSVDataset` with a `preview()` method so Kedro Viz renders a data table. Use this for any intermediate CSV that should be inspectable in Viz.
- **`PolarsExcelDataset`**: read-only wrapper around `polars.read_excel` (via fastexcel). Used for `.xlsx` source files.

### Data Processing Pipeline (`pipelines/data_processing/`)

Runs in strict node order:

```
es_splits_2021_2025 (raw CSV)
    → wide_to_long__es_splits_2021_2025   # unpivots wide→long, one row per runner×AS
    → es_splits_2021_2025_long (intermediate)
    → enrich__es_splits_2021_2025          # joins meta/AS info/finish times, computes elapsed
    → es_splits_2021_2025_processed (intermediate)
    → [+ process__2016_2017_splits → combine__splits → es_splits_all]
    → plot__pace_chart                     # Plotly scatter: distance vs elapsed, year toggle
    → es_pace_chart (reporting, JSON)
```

**Key logic in `wide_to_long__es_splits_2021_2025`:**

- Unpivots `asNN_arr_tod` / `asNN_dep_tod` / `asNN_arr_rank` column groups. The
  arr/dep/rank joins use **only `(year, bib_number, as_index)`** as keys — the
  runner-level columns (`MaxTime`, `OverallRank`, …) are entirely null for
  2022/2023/2025 in the raw file, and Polars joins never match null keys
  (a past bug silently dropped every departure time for those years).
- Keeps rows with an arrival **or** a departure — 2025 recorded mostly
  departures (2,679 dep cells vs 1,219 arr cells).

**Key logic in `enrich__es_splits_2021_2025`:**

- Renames `bib_number` → `bib` and casts `year` to `Int64` (CSV loads everything as str with `infer_schema_length: 0`).
- Sanitizes garbage TOD cells (literal `"DNF"`, typos like `8::27`) to null in both arr/dep columns, then drops rows with neither time.
- Joins `es_race_meta` (race start date/time per year), `es_asinfo_historical` (AS names, distances, finish flag), and `es_finish_historical` (official finish times and runner demographics).
- TOD → elapsed conversion uses **seconds-since-midnight** as the intermediate unit. The base datetime for each point is `race_date midnight + seconds + day_offset` — **not** race start time — to avoid a 5-hour offset error.
- Midnight rollover detection runs on `coalesce(arr, dep)` per row (arrivals can be null): cumulative crossing count per runner (sorted by `as_index`). Backward jumps ≤ 1 h are timing inversions, not crossings. The first crossing is genuine; a second apparent crossing while already on day+1 is an AM/PM data-entry error corrected by +12 h on that row only — unless that would exceed the 36 h cutoff (then the previous row was the outlier and the raw value stands).
- Same-row departure-before-arrival is classified by jump size: ≤1 h inversion (kept), 1–6 h garbage (nulled), 6–18 h AM/PM (+12 h), >18 h genuine midnight crossing (+1 day).
- Runner-level fields (`MaxAS`, `FinishRank`, `OverallRank`, `MaxTime`) are computed via `group_by` aggregation and joined back; max elapsed uses `max_horizontal(check_in, check_out)` so departure-only rows count.
- Per-AS `as_rank` is computed by sorting on `as_check_in__tod__datetime` within `(year, as_index)`, nulls last.

**`as_check_in__elapsed__min` / `as_check_out__elapsed__min` store decimal hours** (e.g., 1.75 = 1 h 45 min), matching the existing 2016-2017 data convention despite the "min" suffix. Catalog entries pin these columns to Float64 via `schema_overrides` because leading rows can be all-null (String inference).

### Feature Engineering Pipeline (`pipelines/feature_engineering/`)

Consumes `es_splits_2021_2025_processed` (2021–2025 only; 2016-2017 has no check-out data at all):

```
train__stoppage_model    # HistGradientBoostingRegressor for minutes spent in an AS
    → es_stoppage_model (pickle) + es_stoppage_model_metrics (JSON: MAE vs naive baseline)
impute__missing_times    # fills missing check-in/check-out elapsed hours
    → es_splits_2021_2025_imputed (04_feature)
features__interval_pace  # interval pace, overall pace, pace ratio per runner×AS
    → es_interval_features (04_feature)
```

### Reporting Pipeline (`pipelines/reporting/`)

`build__as_dashboard` aggregates `es_interval_features` per year (KPIs, arrival
windows, observed stoppage by cohort, leg pace ratios, hourly flow, DNF drop
points) and injects the JSON into `template.html` (sits next to `nodes.py`) →
`es_as_dashboard` = `data/08_reporting/es_as_dashboard.html`, a fully
self-contained page (inline CSS/JS, no external deps) intended to be dropped
onto the race website. Design notes: charts are hand-built SVG following the
dataviz skill (validated palette, light+dark via CSS custom properties, per-mark
tooltips, table-view toggle per card). Stoppage medians are suppressed where
observed check-in/out pairs cover <30% of a station's visits (2025's sparse
check-ins leave a biased subset); the flow heatmap color scale caps at the 95th
percentile of non-zero cells.

- Stoppage target/predictions are in **minutes**; elapsed columns remain decimal hours.
- Trains on rows with both times observed (~7k, mostly 2021-2023), validated by year-holdout (params in `conf/base/parameters_feature_engineering.yml`).
- **MLflow tracking** (`kedro-mlflow`, config in `conf/base/mlflow.yml`): every `kedro run` involving this pipeline creates a run in the `eastern_states_pace_predict` experiment (SQLite backend `mlflow.db`, gitignored). All `stoppage_model.*` params are auto-logged (flattened); the `es_stoppage_model_metrics_tracked` output logs MAE/baseline/sample-size metrics with prefix `stoppage.`; the model pickle and metrics JSON are attached as run artifacts while remaining on disk in `data/06_models/` for downstream nodes.
- Imputed check-ins are clamped to `[previous station's time, departure]` for monotonicity; flags: `check_in_imputed`, `check_out_imputed`, `stoppage_imputed`.
- `as_interval_pace` (min/mile) runs from the previous station's departure (race start = elapsed 0 for the first leg) to this station's arrival; `spans_missing_as` flags intervals across unrecorded stations. `as_interval_pace_ratio` = interval pace / overall pace (1.0 = at overall pace; runner medians sit ~0.92 because moving pace excludes stoppage). DNF status: `is_finisher` / `FinishRank == "DNF"`.

### 2016-2017 vs 2021-2025 Data Differences

| Aspect | 2016-2017 | 2021-2025 |
|---|---|---|
| Source format | Long (one row per runner×AS) | Wide (one row per runner, 17 AS columns) |
| Elapsed times | Pre-computed in source | Computed in pipeline |
| Check-out times | Absent | Partial |
| Demographics | Absent | Joined from finish times |
| Finish station | AS_18 row | Identified via `flag_finish` in as_info |

The raw 2016-2017 file (`es100_2016-2017.csv`) already has all elapsed/datetime columns. It does not need the unpivot step.

### Known Issues

- ~278 raw TOD cells contain garbage (literal `"DNF"` where a runner dropped, typos like `8::27` / `12"17`) — nulled during sanitization in `enrich_2021_2025_splits`.
- ~24 station visits have the departure clocked minutes before the arrival (raw timing inversions ≤ 1 h). They pass through as-is; negative stoppages are excluded from model training and non-positive intervals get null pace.
- Some bib numbers have trailing asterisks (e.g., `"545*"`) — these are stripped before casting to `Int64`.
- 2025's finish-line split row is absent from the raw wide file, so finishers' last recorded station is before the finish; no synthetic finish interval is created (future work).
- Orphan `.pq` files under `data/02_intermediate/` and `data/04_feature/` are leftovers from a pre-Kedro experimental workflow — not referenced by the catalog.
