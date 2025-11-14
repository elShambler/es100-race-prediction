# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Kedro 1.0 project analyzing Eastern States 100 mile foot race data (2016-2023+). The project processes historical split times and finish data from multiple years with varying data formats and quality.

## Core Commands

### Environment Setup
```bash
# Install dependencies using uv (preferred)
uv sync

# Or using pip
pip install -r requirements.txt
```

### Running Pipelines
```bash
# Run all pipelines
kedro run

# Run specific pipeline
kedro run --pipeline data_processing

# Run specific node
kedro run --node preprocess_ultralive_node
```

### Testing
```bash
# Run tests with coverage
pytest

# Note: As of this writing, no tests directory exists yet
```

### Linting and Formatting
```bash
# Format code (ruff is configured in pyproject.toml)
ruff format .

# Check linting
ruff check .
```

### Jupyter/Interactive Development
```bash
# Start Jupyter notebook (provides catalog, context, pipelines, session in scope)
kedro jupyter notebook

# Start JupyterLab
kedro jupyter lab

# Start IPython session
kedro ipython
```

### Visualization
```bash
# Launch Kedro Viz to visualize pipelines
kedro viz
```

## Architecture

### Kedro Framework Structure

This project follows Kedro's standard structure with custom configurations:

- **Pipeline Registry** (`src/eastern_states_pace_predict/pipeline_registry.py`): Uses `find_pipelines()` to auto-discover all pipeline modules. The `__default__` pipeline is the sum of all pipelines.

- **Settings** (`src/eastern_states_pace_predict/settings.py`):
  - Uses `OmegaConfigLoader` with custom Polars resolver
  - Registers `polars` resolver to use Polars datatypes in config files: `${polars:Int64}`
  - Base environment: `base`, Default run environment: `local`

- **Data Catalog** (`conf/base/catalog.yml`):
  - Primarily uses Polars datasets (`polars.CSVDataset`, `polars.LazyPolarsDataset`)
  - Raw data in `data/01_raw/`, processed in `data/02_intermediate/`
  - Keep credentials in `conf/local/credentials.yml` (gitignored)

### Data Processing Pipeline

Located in `src/eastern_states_pace_predict/pipelines/data_processing/`:

**Key Node**: `preprocess_20162017_data` (nodes.py:165)
- Processes UltraLive.net scraped data (2016-2017) which only has time-in values
- Adds race dates based on year
- Converts time-of-day strings to datetime objects

**Helper Functions**:
- `add_race_date()`: Maps year to actual race date (e.g., 2016 → 2016-08-13)
- `add_check_in_out__tod()`: Converts time-of-day strings to datetime using race_date
- `convert_elapsed_tod()`: Converts elapsed time format (HH:MM:SS) to datetime from 05:00 start

### Data Handling Philosophy

The project uses **Polars** as the primary dataframe library (not Pandas) for better performance. When working with data:
- Prefer `polars.LazyPolarsDataset` for large datasets
- Use `try_parse_dates: True` in catalog load_args for automatic date parsing
- Race start time is assumed to be 05:00 on race day

### Historical Data Context

Different years have different data formats:
- **2016-2017**: UltraLive.net scrapes (time-in only, no checkout times)
- **2021+**: Standard format with full split data (check-in and check-out times)
- **2022-2023**: Excel files with 'rollup' sheets

See README.md for complete race history including years not run (2018, 2020, 2024).

## Configuration Notes

- Python 3.9+ required
- Uses `uv` for modern Python package management (note `uv.lock` file)
- Kedro telemetry enabled (project_id in pyproject.toml)
- Coverage threshold set to 0 (no enforcement)
- Line length: 88 characters
- Ruff ignores E501 (line-too-long) since formatter handles it

## Data Catalog Conventions

When adding new datasets:
1. Place raw data in `data/01_raw/`
2. Define in `conf/base/catalog.yml` using Polars datasets
3. Use descriptive names like `es_splits_YEAR` or `es_processed_YEARRANGE`
4. Intermediate/processed data goes to `data/02_intermediate/`

## Key Dependencies

- **kedro[jupyter]==1.0**: Core framework
- **polars>=1.30.0**: Primary data processing (not pandas)
- **duckdb>=1.3.0**: SQL analytics engine
- **scikit-learn~=1.5.1**: ML models
- **fastexcel>=0.14.0**: Fast Excel reading
- **kedro-viz**: Pipeline visualization
