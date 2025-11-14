import logging

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)

# Load in our main datasets defined in catalog.yml


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
            in '{year_col}'. These will be discarede"""
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
                hours=pl.col(as_elap).str.split(
                    ":").list.get(0).cast(pl.Int32),
                minutes=pl.col(as_elap).str.split(
                    ":").list.get(1).cast(pl.Int32),
                seconds=pl.col(as_elap).str.split(
                    ":").list.get(2).cast(pl.Float32),
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
    if elapsed_col not in df.columns:
        raise ValueError(f'Column "{elapsed_col}" not found in dataframe.')

    # Find runners with any negative elapsed times
    flagged_runners = (
        df.filter(pl.col(elapsed_col) < 0).select(["bib", "year"]).unique()
    )

    # Log findings
    num_flagged = len(flagged_runners)
    if num_flagged > 0:
        logger.warning(
            f"Found {num_flagged} runners with negative elapsed times:")
        for row in flagged_runners.iter_rows(named=True):
            logger.warning(f"  - Bib: {row['bib']}, Year: {row['year']}")
    else:
        logger.info("No negative elapsed times found.")

    # Create a filtered dataframe to propogate
    filtered_df = df.join(flagged_runners, on=["bib", "year"], how="anti")

    # Create a separate dataframe with the missing values
    flagged_df = df.join(flagged_runners, on=["bib", "year"], how="inner")

    # Log summary
    total_flagged_rows = flagged_df.height
    logger.info(
        f"Flagged {total_flagged_rows} total rows across {num_flagged} runners")

    return filtered_df


def visualize_elapsed_times_by_runner(
    df: pl.DataFrame,
    elapsed_col: str = "as_check_in__elapsed__min",
    index_col: str = "as_index",
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
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Get unique years and sort
    years = df["year"].unique().sort().to_list()
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
        runners = year_data["bib"].unique().sort().to_list()

        logger.info(f"Year {year}: Processing {len(runners)} runners")

        # Plot each runner
        for bib in runners:
            runner_data = year_data.filter(
                pl.col("bib") == bib).sort(index_col)

            # Convert to pandas for plotly (plotly doesn't support polars directly)
            runner_pd = runner_data.select(
                [index_col, elapsed_col]).to_pandas()

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
