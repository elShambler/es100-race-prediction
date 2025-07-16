import pandas as pd
import polars as pl
from datetime import datetime as dt
import logging

logger = logging.getLogger(__name__)

# Load in our main datasets defined in catalog.yml

def add_race_date(
    df: pl.DataFrame,
    year_col: str="year"
) -> pl.DataFrame:
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
        logger.warning(f"Found {null_count} null values in '{year_col}'. These will be discarede")

    # Add race data column
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
        .otherwise(None)
        .alias("race_date")
    )

    # Log results
    race_date_stats = result_df["race_date"].value_counts().sort("race_date")
    logger.info(f"Race date distribution:\n{race_date_stats}")

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
    
    logger.info(f"Starting preprocessing of {df.shape[0]} rows and {df.shape[1]} features.")

    try:
        # Data validation
        if df.is_empty():
            raise ValueError("Input dataframe is empty")

        # Step 1: Add race date based on year
        logger.info("Adding race date column...")
        df = add_race_date(df, year_col="year")
        
        # Log successful modification
        logger.info(f"Successfully preprocessed data. Final shape: {df.shape[0]} rows and {df.shape[1]} columns")



        return df

    except Exception as e:
        logger.error(f"Error during preprocessing: {str(e)}")
        raise

