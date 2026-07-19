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
uv run kedro run --pipeline course           # parse 2026 GPX + build station crosswalk
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
| `reporting` | `data/08_reporting/` | Plotly charts, HTML dashboard, static blog figures (PNG/SVG) |

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

### Course Pipeline (`pipelines/course/`)

Parses the 2026 race GPX (`data/01_raw/Eastern States 100 - Full Course.gpx`, a
Footpath export: one `<rte>` + AS `<wpt>`s) and builds the historical→2026
station crosswalk. Uses stdlib `xml.etree` (no gpxpy dependency).

```
parse__course_gpx        # rtept list → cumulative miles (haversine); snap each wpt to nearest vertex
    → es_course_route (02_intermediate: seq, lat, lon, ele_m, cum_mi)
    → es_course_stations (02_intermediate: station_id 0–16, name, lat/lon, route_seq, cum_mi, scaled_mi)
map__historical_stations # match every (year, as_index) to a 2026 station by distance
    → es_station_xwalk (02_intermediate: …, station_2026, station_2026_name, station_mi_2026, delta_mi)
```

- GPX miles are scaled by `official_finish_mi / gpx_total_mi` (params in
  `conf/base/parameters_course.yml`) so `scaled_mi` matches the official 103.1;
  raw lat/lon are kept unscaled for the map.
- **Never join on `as_index` across eras** — AS numbering is renumbered
  (2023/25 drop a station, Blackwell is AS_12 vs AS_13 earlier). The crosswalk
  matches on scaled distance-from-start only, nulling matches beyond
  `max_delta_mi` (4.0). Many-to-one is expected (Algerine + Long Branch →
  Cedar Run). `map_historical_stations` also takes `es_splits_all` and unions in
  split-only `(year, as_index)` keys via anti-join, so the 2016/17 finish row
  (`AS_18`, absent from as_info) still maps to the 2026 Finish.

### Feature Engineering Pipeline (`pipelines/feature_engineering/`)

The model **trains** on `es_splits_2021_2025_processed` (only 2021-2023 have
observed check-in/out pairs to learn from), but imputation and every downstream
feature run on **all years** via `es_splits_all`:

```
train__stoppage_model    # HistGradientBoostingRegressor for minutes spent in an AS
    → es_stoppage_model (pickle) + es_stoppage_model_metrics (JSON: MAE vs naive baseline)
impute__missing_times    # fills missing check-in/check-out elapsed hours (es_splits_all in)
    → es_splits_imputed (04_feature, all years)
features__interval_pace  # interval pace, overall pace, pace ratio per runner×AS
    → es_interval_features (04_feature, all years)
features__interval_ratio # leg-pace-vs-final-overall-pace, finishers only
    → es_interval_ratio (04_feature)
```

**All years incl. 2016-2017 flow through the feature pipeline.** 2016-2017 has no
recorded check-outs at all, so `impute__missing_times` predicts the stoppage at
every one of their stations with the trained model and sets `check-out = arrival
+ predicted stoppage`. That imputed departure is what lets `features__interval_pace`
compute a **moving** interval pace (previous departure → this arrival) for those
years. Consequence: the per-year dashboard cards now cover 2016-2025, but the
observed-stoppage card is suppressed for 2016-2017 (0% observed coverage — every
stop is predicted, so the coverage guard hides it, honestly).

**`features__interval_ratio`** (inputs `es_interval_features`, `es_station_xwalk`)
is the shared artifact behind both the static blog figure and the dashboard
planner card:

- Finishers only (`FinishRank != "DNF"`, `finish_elapsed_hrs` non-null), rows with
  a non-null `as_interval_pace_ratio` and `as_dist_from_start > 0`.
- `interval_ratio = as_interval_pace_ratio` = the moving pace on the leg *into*
  the station ÷ the runner's final overall pace. Below 1.0 = that leg ran faster
  than their whole-race average; above 1.0 = a slower/harder leg. **The ×60
  hours→minutes factor cancels** (numerator and denominator both carry it), so
  the ratio is unit-safe — no decimal-hours trap here. Field-wide median ≈ 0.93
  (moving legs are a touch faster than overall pace, which includes stoppage);
  that median is the sentinel test.
- `finish_hr_block = floor(finish_elapsed_hrs + 0.5)` (block = [h−0.5, h+0.5)),
  left-joined to `es_station_xwalk` for `station_2026` / `station_mi_2026`.

### Reporting Pipeline (`pipelines/reporting/`)

`build__as_dashboard` aggregates `es_interval_features` per year (KPIs, arrival
windows, observed stoppage by cohort, leg pace ratios, hourly flow, DNF drop
points) plus three **year-independent, all-years** planner cards, and injects the
JSON into `template.html` (sits next to `nodes.py`) → `es_as_dashboard` =
`data/08_reporting/es_as_dashboard.html`. The page is **fully self-contained
(inline CSS/JS, no external scripts/styles/fonts/tiles) and works offline** —
including the course-map card, which is now a hand-built inline SVG projection of
the route (equirectangular, `cos(lat)` corrected) rather than Leaflet/OSM tiles.
This matters because the dashboard doubles as a race-day crew tool with no cell
service. Design notes: charts are hand-built SVG following the dataviz skill. **The dashboard mirrors the project matplotlib theme
(`mpl_theme.py`): light-only** (no dark mode), blue-gray panel `#e6e8ef`, dotted
grid, uppercase bold titles + ochre `#a87b1e` subtitles, and **Geist Mono
embedded as a base64 `woff2` `@font-face`** in `template.html` (Latin subset,
~31 KB, keeps the page self-contained). Chart palettes are remapped to the theme
greens and **validated with `scripts/validate_palette.js`** against the panel:
the cohort scatter uses the slate-green ordinal ramp, the flow heatmap a
7-step green sequential ramp, and the per-year leg-speed chart is **single-hue
with a baseline encoding direction** (green + ochre fails red-green CVD as a
diverging pair, so bar direction + opacity carry faster/slower instead). The
per-year leg chart shows **absolute mph** (bars vs the year's median-leg-speed
line — deliberately the honest, compressed view); the **planner** scatter shows
the **mph speed ratio** (see below), rendered as a **beeswarm** (points fanned by
local density per station, not a vertical strip). Per-mark
tooltips + table-view toggle per card. Stoppage medians are suppressed where
observed check-in/out pairs cover <30% of a station's visits (all of 2016-2017
and 2025's sparse check-ins leave a biased subset); the flow heatmap color scale
caps at the 95th percentile of non-zero cells.

- Inputs beyond `es_interval_features`: `es_interval_ratio`, `es_splits_all`,
  `es_course_route`, `es_course_stations`, `es_station_xwalk`, `params:reporting`.
- Top-level payload keys `planner` and `course` (helpers `_planner_payload` /
  `_course_payload`) drive the planner scatter (leg **speed** relative to final
  overall pace, station selector + goal-finish trend line), the arrival-time
  distribution (half-hour bins + goal-cohort p25–p75 band), and the inline-SVG
  course map (highlights the leg into the selected station). Per-year cards now
  cover 2016–2025; planner cards pool all years.
- **Speed metrics** (`es_interval_ratio.interval_ratio` / `as_interval_pace_ratio`
  stay a **pace ratio** on disk — leg pace ÷ final pace, <1 = faster — inverted in
  the reporting layer **per-row before aggregating**, since median(1/x) ≠
  1/median(x)):
  - **Per-year leg card** (`_year_payload`, `renderLegs`): **absolute mph** via
    `_leg_mph_expr` (60 ÷ leg pace). `legs[].mph` = per-station median; year-level
    `median_leg_speed` is the reference line. Bars diverge from that line (above =
    faster than the year's typical leg). No ratio here — mph is the honest,
    compressed view the field actually runs.
  - **Planner scatter + blog figure** (`_planner_payload`, `plot_blog_interval_ratio`):
    the **mph speed ratio** = final pace ÷ leg pace = `1/pace_ratio` via
    `_speed_ratio_expr` (**>1 = faster**), axis "Speed Relative To Final (higher
    means faster segment)". `planner.avg` / `planner.trend` means are in these
    ratio units (the pacing planner reads them back as speed).
- **Race-day pacing planner** (`#card-pacing`, `renderPacing`/`recomputePacing`
  in `template.html`, no Python — pure client-side over `DATA.planner`): a crew
  member enters a goal finish; the plan distributes it across legs by each leg's
  typical speed (`trend`/`avg` speed ratios, leg time ∝ distance ÷ speed,
  normalized so the finish equals the goal) and shows a per-station predicted
  arrival + the goal-cohort p25–p75 "typical range" (`arrivals.cohort`). Typing an
  actual arrival re-projects in place: the furthest actual anchors a pace factor
  `f = actual/model`, rescaling every downstream ETA and the finish. Fully
  offline, no new pipeline outputs.
- Size control (`conf/base/parameters_reporting.yml`): the scatter is
  stratified-sampled to `max_scatter_points` (aggregates always from full data);
  the route is downsampled to `max_route_points` keeping station vertices, coords
  rounded to `coord_decimals`. Page is ~245 KB (well under the 1 MB test bound);
  the existing `</`→`<\/` JSON escaping still applies.
- **Static blog figures**: `plot__blog_interval_ratio` (input
  `es_interval_ratio`) renders the leg-**speed**-ratio scatter with `mpl_theme`
  → `es_blog_figures` (PNG) + `es_blog_figures_svg` (SVG) under
  `data/08_reporting/blog_figures/`. `MatplotlibWriter` saves through a buffer so
  it can't infer format from the filename — each format needs its **own catalog
  entry with an explicit `format:` save_arg** and the node returns a tuple of two
  `{filename: fig}` dicts (a single dict with both extensions silently wrote two
  identical PNGs).

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
| Check-out times | Absent (all imputed via the stoppage model) | Partial |
| Demographics | Absent for DNFs, present for finishers | Joined from finish times |
| Finish station | AS_18 row | Identified via `flag_finish` in as_info |

The raw 2016-2017 file (`es100_2016-2017.csv`) already has all elapsed/datetime columns. It does not need the unpivot step.

### Known Issues

- ~278 raw TOD cells contain garbage (literal `"DNF"` where a runner dropped, typos like `8::27` / `12"17`) — nulled during sanitization in `enrich_2021_2025_splits`.
- ~24 station visits have the departure clocked minutes before the arrival (raw timing inversions ≤ 1 h). They pass through as-is; negative stoppages are excluded from model training and non-positive intervals get null pace.
- Some bib numbers have trailing asterisks (e.g., `"545*"`) — these are stripped before casting to `Int64`.
- 2025's finish-line split row is absent from the raw wide file, so finishers' last recorded station is before the finish; no synthetic finish interval is created (future work).
- Orphan `.pq` files under `data/02_intermediate/` and `data/04_feature/` are leftovers from a pre-Kedro experimental workflow — not referenced by the catalog.
