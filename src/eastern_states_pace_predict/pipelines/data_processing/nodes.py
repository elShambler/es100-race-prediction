import pandas as pd
import polars as pl
from datetime import datetime as dt
import logging

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
            f"Found {null_count} null values in '{year_col}'. These will be discarede"
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
