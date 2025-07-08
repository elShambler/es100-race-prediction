import polars as pl
from datetime import datetime, timedelta

def process_check_in_times(df: pl.DataFrame) -> pl.DataFrame:
    """
    Process check-in times with proper date assignment and day adjustment.
    
    Args:
        df: DataFrame with columns 'year', 'bib', 'as_check_in__tod', 'as_check_in__elapsed'
    
    Returns:
        DataFrame with processed datetime column
    """
    
    # First, let's create the base datetime by combining the date with the time string
    result = df.with_columns([
        # Create base date based on year
        pl.when(pl.col("year") == 2016)
        .then(pl.lit("2016-08-13"))
        .when(pl.col("year") == 2017)
        .then(pl.lit("2017-08-12"))
        .otherwise(pl.lit("2016-08-13"))  # default fallback
        .alias("base_date"),
        
        # Extract time component from the datetime string
        pl.col("as_check_in__tod")
        .str.split(" ")
        .list.get(1)  # Get the time part (HH:MM:SS)
        .alias("time_component")
    ])
    
    # Combine base date with time component
    result = result.with_columns([
        (pl.col("base_date") + " " + pl.col("time_component"))
        .str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S")
        .alias("check_in_datetime")
    ])
    
    # Sort by year, bib, and datetime to ensure proper ordering
    result = result.sort(["year", "bib", "check_in_datetime"])
    
    # Add day adjustment logic
    result = result.with_columns([
        pl.col("check_in_datetime").alias("original_datetime")
    ])
    
    # Group by year and bib to process each person's entries
    result = result.with_columns([
        # Calculate time difference from previous entry for same person
        pl.col("check_in_datetime")
        .over(["year", "bib"])
        .shift(1)
        .alias("prev_datetime")
    ])
    
    # Calculate time difference and adjust for day rollover
    result = result.with_columns([
        # Calculate time difference in seconds
        (pl.col("check_in_datetime") - pl.col("prev_datetime"))
        .dt.seconds()
        .alias("time_diff_seconds")
    ])
    
    # Apply day adjustment logic
    result = result.with_columns([
        pl.when(
            (pl.col("time_diff_seconds") < 0) &  # Time went backwards (indicating day rollover)
            (pl.col("prev_datetime").is_not_null())  # Not the first entry
        )
        .then(pl.col("check_in_datetime") + pl.duration(days=1))
        .otherwise(pl.col("check_in_datetime"))
        .alias("adjusted_datetime")
    ])
    
    # Clean up intermediate columns
    result = result.drop([
        "base_date", 
        "time_component", 
        "check_in_datetime", 
        "prev_datetime", 
        "time_diff_seconds"
    ])
    
    return result