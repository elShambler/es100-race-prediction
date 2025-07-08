import pandas as pd
import polars as pl
from datetime import datetime as dt

# Load in our main datasets defined in catalog.yml

def preprocess_convert_time_to_numberic(split_time):
    """
    Expected input format: %H:%M:%S
    """

def preprocess_ultralive_data(es_splits_ultralive: pd.DataFrame):
    """
    2016-2017 data scraped from UltraLive and contains only time-in
    This functions normalizes the data to be in the format required
    """

    # Years 2016-2017 start time:
    dt_format = "%Y-%m-%d %H:%M:%S"
    start_time_2016 = dt.strptime("2016-08-13 05:00:00", dt_format)
    start_time_2017 = dt.strptime("2017-08-12 05:00:00", dt_format)

    # Replace missing Time or Day information based on start date
    es_splits_ultralive['as_check_in__tod'] = (
        es_splits_ultralive
        .loc[
            es_splits_ultralive['as_check_in__tod'].isna(),
            ['year', 'as_check_in__elapsed']
            ]
        .apply(
            lambda x: x[1] + start_time_2016 if x[0] == 2016 else x[1] + start_time_2017,
            axis=1
            )
    )
    es_splits_ultralive['as_check_in__elapsed'].nu

def convert_datetime_string_to_minutes_explicit(df: pl.DataFrame, datetime_column: str) -> pl.DataFrame:
    """
    Convert datetime strings to minutes using explicit parsing.
    """
    return df.with_columns([
        (
            pl.col(datetime_column).str.split(" ").list.get(1).str.split(":").list.get(0).cast(pl.Int64) * 60 +  # hours to minutes
            pl.col(datetime_column).str.split(" ").list.get(1).str.split(":").list.get(1).cast(pl.Int64) +       # minutes
            pl.col(datetime_column).str.split(" ").list.get(1).str.split(":").list.get(2).cast(pl.Int64) / 60    # seconds to minutes
        ).alias(f"{datetime_column}_minutes")
    ])

