import polars as pl
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

df = pl.read_excel("data/01_raw/ES2016-2017.xlsx",
    sheet_name="summary", schema_overrides={
    "as_check_in__tod": pl.String,
    "as_check_in__elapsed": pl.String,
    "as_check_out__tod": pl.String,
    "as_check_out__elapsed": pl.String,
})



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
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
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
        .dt.total_seconds()
        .alias("time_diff_seconds")
    ])
    
    # Debug: Check time_diff_seconds column for a random bib
    # Filter out null values and get a random bib
    non_null_data = result.filter(pl.col("time_diff_seconds").is_not_null())
    
    if len(non_null_data) > 0:
        # Get a random bib from the data
        random_bib = non_null_data.select("bib").unique().sample(1).item()
        
        # Filter data for this specific bib
        bib_data = non_null_data.filter(pl.col("bib") == random_bib)
        
        # Create the plot
        plt.figure(figsize=(12, 6))
        plt.plot(bib_data.select("as_index").to_series(), 
                bib_data.select("time_diff_seconds").to_series(), 
                'o-', linewidth=2, markersize=6)
        plt.title(f'Time Difference Between Check-ins for Bib {random_bib}')
        plt.xlabel('Check-in Time')
        plt.ylabel('Time Difference (seconds)')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
        
        # Also print some statistics
        print(f"Debug info for bib {random_bib}:")
        print(f"Number of check-ins: {len(bib_data)}")
        print(f"Time differences: {bib_data.select('time_diff_seconds').to_series().to_list()}")
    
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