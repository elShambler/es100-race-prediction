import logging
import re
from datetime import datetime, timedelta

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

# Constants for 2025 data processing
_RACE_START_TIME = "05:00"
_RACE_START_TIME_PARSED = datetime.strptime(_RACE_START_TIME, "%H:%M").time()
_MISSING_TIME_MARKER = "__:__"

logger = logging.getLogger(__name__)


def add_race_date(df: pl.DataFrame, year_col: str = "year") -> pl.DataFrame:
    """
    Add race_date column based on the year

    Args:
        df: Input dataframe
        year_col: name of the year column
            - Expected value "year"

    Returns:
        Dataframe with race_date column added

    Raises:
        ValueError: If year column doesn't exist
            or contains in valid value
    """

    # Check if column exists:
    if year_col not in df.columns:
        raise ValueError(f'Column "{year_col}" not found in dataframe.')

    # Check for valid year values
    unique_years = df[year_col].unique().drop_nulls().sort()
    logger.info(f"Found the following years in data: {unique_years.to_list()}")

    # Check for null values in year column
    null_count = df[year_col].null_count()
    if null_count > 0:
        logger.warning(
            f"""Found {null_count} null values
            in '{year_col}'. These will be discarded."""
        )

    # Add race date based on year
    result_df = df.with_columns(
        pl.when(pl.col(year_col) == 2016)
        .then(pl.lit("2016-08-13"))
        .when(pl.col(year_col) == 2017)
        .then(pl.lit("2017-08-12"))
        .when(pl.col(year_col) == 2019)
        .then(pl.lit("2019-08-10"))
        .when(pl.col(year_col) == 2021)
        .then(pl.lit("2021-08-14"))
        .when(pl.col(year_col) == 2022)
        .then(pl.lit("2022-08-13"))
        .when(pl.col(year_col) == 2023)
        .then(pl.lit("2023-08-12"))
        .when(pl.col(year_col) == 2025)
        .then(pl.lit("2025-08-09"))
        .otherwise(None)
        .alias("race_date")
    )

    # Log results
    race_date_stats = result_df["race_date"].value_counts().sort("race_date")
    logger.info(f"Race date distribution:\n{race_date_stats}")

    return result_df


def add_check_in_out__tod(
    df: pl.DataFrame,
    as_checkpoint_col: str,
    date_col: str = "race_date",
) -> pl.DataFrame:
    """
    Converts the time-of-day with correct date and time,
    based on the previously added race_date.
    This function can be applied for both:
        - as_check_in__tod
        - as_check_out__tod

    Args:
        df: DataFrame containing all split information
        as_checkpoint_col: Specify which column (check in or check out)
        date_col: expected value -race_date

    Returns:
        Dataframe with processed time of day into a datetime object
    """

    # TODO: Determine if the time object contains date too (error handling required)
    # First extract the time component of the time of day
    result_df = df.with_columns(
        [pl.col(as_checkpoint_col).alias(f"{as_checkpoint_col}_time")]
    )

    # This converts the time-of-day value into datetime
    result_df = result_df.with_columns(
        [
            (pl.col(date_col) + " " + pl.col(f"{as_checkpoint_col}_time"))
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            .alias(f"{as_checkpoint_col}_datetime")
        ]
    )

    return result_df


def convert_elapsed_tod(
    df: pl.DataFrame,
    as_checkpoint_col: str,
    date_col: str = "race_date",
) -> pl.DataFrame:
    """
    Converts the elapsed time of a split value into datetime
    based on the start date of the race and a 05:00 start.

    This function can be applied for both:
    - as_check_in__elapsed
    - as_check_out__elapsed

    Args:
    - df: DataFrame containing all split information
    - as_checkpoint_col: specify if checking in or out
    - date_col: contains the date of the race

    Returns:
    - Dataframe with elapsed time turned into datetime since the start
    """
    # Determine aid station check type (in or out)
    if "check_in" in as_checkpoint_col:
        as_tod = "as_check_in__tod"
        as_elap = "as_check_in__elapsed"
        as_dt = "as_check_in__tod_datetime"
    else:
        as_tod = "as_check_out__tod"
        as_elap = "as_check_out__elapsed"
        as_dt = "as_check_out__tod_datetime"

    # Convert the missing values to datetime as well
    # result_df = df.filter(
    #     pl.col(as_dt).is_null() and pl.col(as_elap).is_not_null()
    #     ).with_columns(
    #         pl.col(date_col).str.strptime(pl.Datetime)
    # )
    result_df = df.with_columns(
        pl.when(pl.col(as_dt).is_null())
        .then(
            pl.col(date_col).str.strptime(pl.Datetime)
            + pl.duration(
                hours=pl.col(as_elap).str.split(":").list.get(0).cast(pl.Int32),
                minutes=pl.col(as_elap).str.split(":").list.get(1).cast(pl.Int32),
                seconds=pl.col(as_elap).str.split(":").list.get(2).cast(pl.Float32),
            )
        )
        .alias(as_dt)
    )

    return result_df


def preprocess_20162017_data(df: pl.DataFrame):
    """
    2016-2017 data scraped from UltraLive and contains only time-in
    This functions normalizes the data to be in the format required
    and does the following:
    - Adds the start date (`add_race_date`)
    - Converts aid_station time-of-day into time fields ('preprocess_convert_time_to_numeric')

    Args:
        df: raw dataframe from ultralive results (2016-2017)

    Returns: Preprocessed dataframe ready to combine with other years
    """

    logger.info(
        f"Starting preprocessing of {df.shape[0]} rows and {df.shape[1]} features."
    )

    try:
        # Data validation
        if df.is_empty():
            raise ValueError("Input dataframe is empty")

        # Step 1: Add race date based on year
        logger.info("Adding race date column...")
        df = add_race_date(df, year_col="year")

        # Log successful modification
        logger.info(
            f"Successfully preprocessed data. Final shape: {df.shape[0]} rows and {df.shape[1]} columns"
        )

        return df

    except Exception as e:
        logger.error(f"Error during preprocessing: {str(e)}")
        raise


def process_2021_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    2021 data was part of the new initiative to take in data more methodically
    from each aid station, in collaboration with the ESTEA. This data was
    originally compiled by a group analyzing the raw data, and cleaned previously.
    As such, the data load is in the final state.
    This is the basis for all subsequent data loads.

    Args:
        df: Raw dataframe from 2021 splits

    Returns:
        Preprocessed DataFrame with race_date added
    """
    try:
        # Data validation
        if df.is_empty():
            raise ValueError("Input dataframe is empty")

        logger.info("Loading in 2021 data")

        df = add_race_date(df, year_col="year")

        logger.info(
            f"Successfully preprocessed data. Final shape: {df.shape[0]} rows and {df.shape[1]} columns"
        )

        return df

    except Exception as e:
        logger.error(f"Error during preprocessing: {str(e)}")
        raise


def _parse_aid_station_columns(columns: list[str]) -> list[dict]:
    """
    Parse 2025 CSV column headers to extract aid station info.

    Full headers look like "Ramsey RD: AS-1 - In". Bare "Out" columns
    (which Polars may rename to "Out_duplicated_N") are associated with
    the preceding parsed column via a last_parsed memory variable.
    """
    parsed_columns = []
    last_parsed = None

    for col in columns:
        if col in ["Bib", "Name"]:
            continue

        # Match bare Out columns (Polars deduplicates to Out_duplicated_N)
        if re.fullmatch(r"Out(_duplicated_\d+)?", col.strip()):
            if last_parsed is None:
                logger.warning(f"Bare 'Out' column '{col}' has no preceding column to associate with; skipping")
                continue
            parsed_columns.append({
                'original_col': col,
                'as_name': last_parsed['as_name'],
                'as_number': last_parsed['as_number'],
                'direction': 'Out',
                'as_index': last_parsed['as_index'],
            })
            last_parsed = None
            continue

        try:
            if ":" not in col:
                logger.warning(f"Skipping column with unexpected format: {col}")
                last_parsed = None
                continue

            name_part, rest = col.split(":", 1)
            as_name = name_part.strip()

            if " - " not in rest:
                logger.warning(f"Skipping column with unexpected format: {col}")
                last_parsed = None
                continue

            as_part, direction = rest.split(" - ", 1)
            as_part = as_part.strip()
            direction = direction.strip()

            if as_part.startswith("AS-"):
                as_number = as_part.replace("AS-", "")
                as_index = f"AS{as_number}"
            elif "Start" in as_part:
                as_number = "Start"
                as_index = "START"
            elif "Finish" in as_part:
                as_number = "Finish"
                as_index = "FINISH"
            else:
                as_number = as_part
                as_index = as_part.upper().replace(" ", "_")

            entry = {
                'original_col': col,
                'as_name': as_name,
                'as_number': as_number,
                'direction': direction,
                'as_index': as_index,
            }
            parsed_columns.append(entry)
            last_parsed = entry

        except Exception as e:
            logger.warning(f"Error parsing column '{col}': {e}")
            last_parsed = None
            continue

    in_count = sum(1 for c in parsed_columns if c['direction'] == 'In')
    out_count = sum(1 for c in parsed_columns if c['direction'] == 'Out')
    if in_count != out_count:
        logger.warning(f"Column imbalance: {in_count} In vs {out_count} Out")
    else:
        logger.info(f"Column balance check passed: {in_count} In and {out_count} Out columns")

    return parsed_columns


def _normalize_time_string(time_str: str) -> str | None:
    """Normalize a raw time string to HH:MM:SS, returning None if missing/invalid."""
    if not time_str or time_str == _MISSING_TIME_MARKER:
        return None
    time_str = time_str.strip()
    try:
        parts = time_str.split(":")
        if len(parts) == 2:
            return f"{parts[0].zfill(2)}:{parts[1]}:00"
        elif len(parts) == 3:
            return f"{parts[0].zfill(2)}:{parts[1]}:{parts[2]}"
        return None
    except Exception as e:
        logger.warning(f"Error normalizing time string '{time_str}': {e}")
        return None


def _parse_time_to_datetime(time_str: str, race_date: str) -> datetime | None:
    """
    Convert a HH:MM:SS time string and race date to a datetime.
    Times before 05:00 are assumed to be post-midnight (next day).
    """
    if not time_str:
        return None
    try:
        dt = datetime.strptime(f"{race_date} {time_str}", "%Y-%m-%d %H:%M:%S")
        time_only = datetime.strptime(time_str, "%H:%M:%S").time()
        if time_only < _RACE_START_TIME_PARSED:
            dt += timedelta(days=1)
        return dt
    except Exception as e:
        logger.warning(f"Error parsing time to datetime '{time_str}': {e}")
        return None


def _calculate_elapsed_minutes(checkpoint_time: datetime, race_start: datetime) -> float | None:
    """Return elapsed minutes from race start to checkpoint, or None if either is missing."""
    if not checkpoint_time or not race_start:
        return None
    try:
        return (checkpoint_time - race_start).total_seconds() / 60.0
    except Exception as e:
        logger.warning(f"Error calculating elapsed time: {e}")
        return None


def _reshape_wide_to_long(
    df_wide: pl.DataFrame,
    parsed_columns: list[dict],
    as_metadata: pl.DataFrame,
    race_date: str,
    race_year: int,
) -> pl.DataFrame:
    """Reshape wide-format 2025 data to long format matching the 2016-2017 structure."""
    logger.info(f"Reshaping {df_wide.shape[0]} rows from wide to long format")

    race_start_dt = datetime.strptime(f"{race_date} {_RACE_START_TIME}", "%Y-%m-%d %H:%M")

    # Group parsed columns by aid station index
    aid_stations: dict[str, dict] = {}
    for col_info in parsed_columns:
        key = col_info['as_index']
        if key not in aid_stations:
            aid_stations[key] = {'in': None, 'out': None, 'name': col_info['as_name']}
        if col_info['direction'] == 'In':
            aid_stations[key]['in'] = col_info['original_col']
        elif col_info['direction'] == 'Out':
            aid_stations[key]['out'] = col_info['original_col']

    logger.info(f"Found {len(aid_stations)} unique aid stations")

    all_rows = []
    for row_idx, runner_row in enumerate(df_wide.iter_rows(named=True)):
        bib = runner_row.get('Bib')
        name = runner_row.get('Name', '')

        for as_index, as_cols in aid_stations.items():
            if as_index == 'START':
                continue

            in_col = as_cols.get('in')
            out_col = as_cols.get('out')
            in_time_raw = runner_row.get(in_col, _MISSING_TIME_MARKER) if in_col else _MISSING_TIME_MARKER
            out_time_raw = runner_row.get(out_col, _MISSING_TIME_MARKER) if out_col else _MISSING_TIME_MARKER

            in_time_str = _normalize_time_string(in_time_raw)
            out_time_str = _normalize_time_string(out_time_raw)

            if not in_time_str and not out_time_str:
                continue

            in_datetime = _parse_time_to_datetime(in_time_str, race_date) if in_time_str else None
            out_datetime = _parse_time_to_datetime(out_time_str, race_date) if out_time_str else None

            in_elapsed_min = _calculate_elapsed_minutes(in_datetime, race_start_dt)
            out_elapsed_min = _calculate_elapsed_minutes(out_datetime, race_start_dt)

            all_rows.append({
                'year': race_year,
                'bib': bib,
                'name': name,
                'gender': None,
                'age': None,
                'as_index': as_index,
                'as_name': as_cols['name'],
                'as_check_in__tod': in_time_str,
                'as_check_out__tod': out_time_str,
                'as_check_in__elapsed': None,
                'as_check_out__elapsed': None,
                'race_datetime': race_start_dt.strftime("%m/%d/%Y %H:%M"),
                'as_check_in__tod__datetime': in_datetime.strftime("%m/%d/%Y %H:%M") if in_datetime else None,
                'as_check_in__elapsed__min': in_elapsed_min,
                'as_check_out__tod__datetime': out_datetime.strftime("%m/%d/%Y %H:%M") if out_datetime else None,
                'as_check_out__elapsed__min': out_elapsed_min,
                'as_dist_from_start': None,
                'as_dist_incr': None,
            })

        if (row_idx + 1) % 50 == 0:
            logger.info(f"Processed {row_idx + 1} runners...")

    logger.info(f"Created {len(all_rows)} total checkpoint records")
    df_long = pl.DataFrame(all_rows)

    # Join aid station distances from metadata
    as_meta_clean = as_metadata.select([
        pl.col('as_index').str.to_uppercase().alias('as_index'),
        pl.col('as_cum_dist').alias('as_dist_from_start'),
        pl.col('as_dist').alias('as_dist_incr'),
    ])
    df_long = df_long.join(as_meta_clean, on='as_index', how='left', suffix='_meta')
    if 'as_dist_from_start_meta' in df_long.columns:
        df_long = df_long.drop(['as_dist_from_start', 'as_dist_incr']).rename({
            'as_dist_from_start_meta': 'as_dist_from_start',
            'as_dist_incr_meta': 'as_dist_incr',
        })

    logger.info(f"Final long format shape: {df_long.shape}")
    return df_long


def process_2025_data(df: pl.DataFrame, as_metadata: pl.DataFrame) -> pl.DataFrame:
    """
    Process 2025 Eastern States 100 data from wide format to long format.

    The 2025 data has one row per runner with aid station columns, predominantly
    with check-out times. Converts to long format matching other years' structure.

    Args:
        df: Raw wide-format DataFrame from ES100_2025_splits.csv
        as_metadata: Aid station metadata with distances (es_asinfo_historical)

    Returns:
        Long-format DataFrame matching the 2016-2017 processed structure
    """
    race_date = "2025-08-09"
    race_year = 2025

    logger.info(f"Starting preprocessing of 2025 data: {df.shape[0]} rows, {df.shape[1]} columns")

    try:
        if df.is_empty():
            raise ValueError("Input dataframe is empty")

        parsed_cols = _parse_aid_station_columns(df.columns)
        df_long = _reshape_wide_to_long(df, parsed_cols, as_metadata, race_date, race_year)

        logger.info(f"Successfully processed 2025 data. Final shape: {df_long.shape[0]} rows, {df_long.shape[1]} columns")
        return df_long

    except Exception as e:
        logger.error(f"Error during 2025 preprocessing: {str(e)}")
        raise


def flag_negative_elapsed_times(
    df: pl.DataFrame, elapsed_col: str = "as_check_in__elapsed__min"
) -> pl.DataFrame:
    """
    Flag runners with negative elapsed times by identifying bib and year.
    Adds a 'has_timing_error' column to mark flagged runners.

    Args:
        df: Input dataframe with timing data
        elapsed_col: Name of the elapsed time column to check

    Returns:
        Dataframe with 'has_timing_error' boolean column and
        'flagged_bibs' column containing set of flagged (bib, year) pairs
    """
    logger.info(f"Checking for negative values in column: {elapsed_col}")

    # Check if column exists
    if elapsed_col not in df.collect_schema().names():
        raise ValueError(f'Column "{elapsed_col}" not found in dataframe.')

    # Find runners with any negative elapsed times
    flagged_runners = (
        df.filter(pl.col(elapsed_col) < 0).select(["bib", "year"]).unique()
    )
    flagged_collected = flagged_runners.collect()
    num_flagged = flagged_collected.shape[0]

    if num_flagged > 0:
        logger.warning(f"Found {num_flagged} runners with negative elapsed times:")
        for row in flagged_collected.iter_rows(named=True):
            logger.warning(f"  - Bib: {row['bib']}, Year: {row['year']}")
    else:
        logger.info("No negative elapsed times found.")

    # Exclude flagged runners from the output
    filtered_df = df.join(flagged_runners, on=["bib", "year"], how="anti")
    logger.info(f"Removed {num_flagged} runners with timing errors")

    return filtered_df


def visualize_elapsed_times_by_runner(
    df: pl.DataFrame,
    elapsed_col: str = "as_check_in__elapsed__min",
    index_col: str = "as_dist_from_start",
) -> go.Figure:
    """
    Create line charts showing elapsed times by runner, with separate subplots per year.
    Flagged runners (with timing errors) are shown in red, others in grey.

    Args:
        df: Input dataframe with timing data and flags
        elapsed_col: Name of elapsed time column for y-axis
        index_col: Name of index column for x-axis (aid station index)

    Returns:
        Plotly Figure object with subplots (one per year)
    """
    logger.info("Creating elapsed time visualization...")

    # Check required columns exist
    required_cols = [elapsed_col, index_col, "bib", "year"]
    missing_cols = [
        col for col in required_cols if col not in df.collect_schema().names()
    ]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Get unique years and sort
    years = df.select("year").collect().unique().to_series()
    num_years = len(years)

    logger.info(f"Creating {num_years} subplots for years: {years}")

    # Create subplots - one per year
    fig = make_subplots(
        rows=num_years,
        cols=1,
        subplot_titles=[f"Year {year}" for year in years],
        vertical_spacing=0.1,
    )

    # Process each year
    for idx, year in enumerate(years, start=1):
        year_data = df.filter(pl.col("year") == year)

        # Get unique runners for this year
        runners = list(
            year_data.select("bib").unique().sort(by="bib").collect().to_series()
        )

        logger.info(f"Year {year}: Processing {len(runners)} runners")

        # Plot each runner
        for bib in runners:
            runner_data = year_data.filter(pl.col("bib") == bib).sort(index_col)

            # Convert to pandas for plotly (plotly doesn't support polars directly)
            runner_pd = (
                runner_data.select([index_col, elapsed_col]).collect().to_pandas()
            )

            # Add trace
            fig.add_trace(
                go.Scatter(
                    x=runner_pd[index_col],
                    y=runner_pd[elapsed_col],
                    mode="lines+markers",
                    hovertemplate=f"Bib: {bib}<br>AS Index: %{{x}}<br>Elapsed: %{{y:.2f}} min<extra></extra>",
                ),
                row=idx,
                col=1,
            )

        # Update axes for this subplot
        fig.update_xaxes(title_text="Aid Station Index", row=idx, col=1)
        fig.update_yaxes(title_text="Elapsed Time (min)", row=idx, col=1)

    # Update overall layout
    fig.update_layout(
        height=400 * num_years,  # Scale height with number of years
        title_text="Runner Elapsed Times by Aid Station (Flagged Runners in Red)",
        showlegend=True,
        hovermode="closest",
    )

    logger.info("Visualization created successfully")

    return fig
