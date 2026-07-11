# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Kedro 1.0 project that processes and analyzes Eastern States 100 mile foot race data (2016–2025). It ingests split times from multiple sources with varying formats, normalizes them into a unified long-format dataset, and will eventually feed ML pace-prediction models.

Years not run: 2018, 2020, 2024.

## Core Commands

```bash
# Install dependencies
uv sync

# Run the working pipeline (use --pipeline flag — bare `kedro run` fails
# because the feature_engineering pipeline has an unresolved input)
uv run kedro run --pipeline data_processing

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
| `reporting` | `data/08_reporting/` | Plotly charts, exported artifacts |

Intermediate datasets that need Kedro Viz table previews use `PolarsPreviewCSVDataset` (defined in `src/.../datasets/polars_preview_csv_dataset.py`) instead of bare `polars.CSVDataset`. Raw datasets load with `infer_schema_length: 0` plus `schema_overrides` when column types need to be pinned.

### Custom Dataset Classes (`src/.../datasets/`)

- **`PolarsPreviewCSVDataset`**: extends `polars.CSVDataset` with a `preview()` method so Kedro Viz renders a data table. Use this for any intermediate CSV that should be inspectable in Viz.
- **`PolarsExcelDataset`**: read-only wrapper around `polars.read_excel` (via fastexcel). Used for `.xlsx` source files.

### Data Processing Pipeline (`pipelines/data_processing/`)

This is the only fully wired pipeline. It runs in strict node order:

```
es_splits_2021_2025 (raw CSV)
    → wide_to_long__es_splits_2021_2025   # unpivots wide→long, one row per runner×AS
    → es_splits_2021_2025_long (intermediate)
    → enrich__es_splits_2021_2025          # joins meta/AS info/finish times, computes elapsed
    → es_splits_2021_2025_processed (intermediate)
    → plot__pace_chart                     # Plotly scatter: distance vs elapsed, year toggle
    → es_pace_chart (reporting, JSON)
```

**Key logic in `enrich__es_splits_2021_2025`:**

- Renames `bib_number` → `bib` and casts `year` to `Int64` (CSV loads everything as str with `infer_schema_length: 0`).
- Joins `es_race_meta` (race start date/time per year), `es_asinfo_historical` (AS names, distances, finish flag), and `es_finish_historical` (official finish times and runner demographics).
- TOD → elapsed conversion uses **seconds-since-midnight** as the intermediate unit. The base datetime for each point is `race_date midnight + seconds + day_offset` — **not** race start time — to avoid a 5-hour offset error.
- Midnight rollover detection: cumulative crossing count per runner (sorted by `as_index`). The first crossing is genuine; a second apparent crossing while already on day+1 is treated as an AM/PM data-entry error and corrected by adding 12 h to that row's TOD seconds only (not subsequent rows).
- Runner-level fields (`MaxAS`, `FinishRank`, `OverallRank`, `MaxTime`) are computed via `group_by` aggregation and joined back.
- Per-AS `as_rank` is computed by sorting on `as_check_in__tod__datetime` within `(year, as_index)`.

**`as_check_in__elapsed__min` stores decimal hours** (e.g., 1.75 = 1 h 45 min), matching the existing 2016-2017 data convention despite the "min" suffix.

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

- `uv run kedro run` (bare) fails because `feature_engineering` pipeline references `es_splits_with_finish` which is not in the catalog. Always use `--pipeline data_processing` until that pipeline is fixed.
- Five rows in the 2021-2025 wide data have literal `"DNF"` in `as_check_in__tod` (not null) and are filtered out at the start of `enrich_2021_2025_splits`.
- Some bib numbers have trailing asterisks (e.g., `"545*"`) — these are stripped before casting to `Int64`.
