"""
Experimental test file for preprocessing 2025 Eastern States 100 race data.

This script develops and tests the transformation logic for converting the 2025
raw split data from wide format to long format, matching the structure of
2016-2017 processed data.

Author: Development test file
Date: 2025
"""

import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
RACE_DATE_2025 = "2025-08-09"
RACE_START_TIME = "05:00"
RACE_YEAR = 2025
MISSING_TIME_MARKER = "__:__"

# File paths
RAW_DATA_PATH = Path("data/01_raw/ES100_2025_splits.csv")
AS_INFO_PATH = Path("data/01_raw/ES100_2016-2017_asinfo.csv")
OUTPUT_PATH = Path("data/02_intermediate/es_2025_processed_test.csv")

def load_aid_station_metadata() -> pl.DataFrame:
    """
    Load aid station metadata including names, indices, and distances.

    Returns:
        DataFrame with columns: as_index, as_name, as_cum_dist, as_dist

    Raises:
        FileNotFoundError: If aid station info file doesn't exist
    """
    logger.info(f"Loading aid station metadata from {AS_INFO_PATH}")

    if not AS_INFO_PATH.exists():
        raise FileNotFoundError(f"Aid station info file not found: {AS_INFO_PATH}")

    as_info = pl.read_csv(
        AS_INFO_PATH,
        separator=",",
        encoding="utf8-lossy"  # Handle BOM if present
    )
    logger.info(f"Loaded {len(as_info)} aid stations")
    return as_info


def parse_aid_station_columns(columns: list[str]) -> list[dict]:
    """
    Parse the 2025 CSV column headers to extract aid station information.

    The 2025 format has columns like:
    - "Ramsey RD: AS-1 - In"
    - "Ramsey RD: AS-1 - Out"
    - "Start: Start - Out"
    - "Finish: Finish - In"

    Args:
        columns: List of column names from the CSV

    Returns:
        List of dicts with parsed aid station info:
        {
            'original_col': str,     # Original column name
            'as_name': str,          # Aid station name
            'as_number': str,        # Aid station number (e.g., "1", "Finish")
            'direction': str,        # "In" or "Out"
            'as_index': str          # Standardized index (e.g., "AS1", "FINISH")
        }
    """
    logger.info("Parsing aid station column headers")

    parsed_columns = []
    last_parsed = None  # Memory: tracks the last successfully parsed column

    for col in columns:
        # Skip non-aid-station columns
        if col in ["Bib", "Name"]:
            continue

        # Handle bare "Out" columns that lack a full header but follow their "In" partner.
        # Polars renames duplicate column names to "Out_duplicated_N", so we match both forms.
        if re.fullmatch(r"Out(_duplicated_\d+)?", col.strip()):
            if last_parsed is None:
                logger.warning(f"Found bare 'Out' column with no preceding parsed column to associate it with; skipping")
                continue
            parsed_columns.append({
                'original_col': col,
                'as_name': last_parsed['as_name'],
                'as_number': last_parsed['as_number'],
                'direction': 'Out',
                'as_index': last_parsed['as_index']
            })
            last_parsed = None  # Reset: each "In" should only absorb one following "Out"
            continue

        # Parse format: "AS_NAME: AS-X - Direction"
        try:
            # Split by colon to separate name from AS number
            if ":" not in col:
                logger.warning(f"Skipping column with unexpected format: {col}")
                last_parsed = None
                continue

            name_part, rest = col.split(":", 1)
            as_name = name_part.strip()

            # Split the rest by " - " to get AS number and direction
            if " - " not in rest:
                logger.warning(f"Skipping column with unexpected format: {col}")
                last_parsed = None
                continue

            as_part, direction = rest.split(" - ", 1)
            as_part = as_part.strip()
            direction = direction.strip()

            # Extract AS number (e.g., "AS-1" -> "1", "Start" -> "Start")
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
                'as_index': as_index
            }
            parsed_columns.append(entry)
            last_parsed = entry

        except Exception as e:
            logger.warning(f"Error parsing column '{col}': {e}")
            last_parsed = None
            continue

    # Validate that In and Out counts are balanced
    in_count = sum(1 for c in parsed_columns if c['direction'] == 'In')
    out_count = sum(1 for c in parsed_columns if c['direction'] == 'Out')
    if in_count != out_count:
        logger.warning(
            f"Mismatch between 'In' and 'Out' columns: {in_count} In vs {out_count} Out. "
            "Some aid stations may be missing a paired column."
        )
    else:
        logger.info(f"Column balance check passed: {in_count} In and {out_count} Out columns")

    logger.info(f"Successfully parsed {len(parsed_columns)} aid station columns")
    return parsed_columns


def normalize_time_string(time_str: str) -> str | None:
    """
    Normalize time strings from the 2025 format to HH:MM:SS format.

    Handles various input formats:
    - "6:24" -> "06:24:00"
    - "18:53" -> "18:53:00"
    - "__:__" -> None
    - "" -> None

    Args:
        time_str: Raw time string from CSV

    Returns:
        Normalized time string in HH:MM:SS format, or None if missing/invalid
    """
    if not time_str or time_str == MISSING_TIME_MARKER:
        return None

    time_str = time_str.strip()

    try:
        # Split by colon
        parts = time_str.split(":")

        if len(parts) == 2:
            hours, minutes = parts
            # Pad hours to 2 digits
            hours = hours.zfill(2)
            return f"{hours}:{minutes}:00"
        elif len(parts) == 3:
            hours, minutes, seconds = parts
            hours = hours.zfill(2)
            return f"{hours}:{minutes}:{seconds}"
        else:
            # Turning off logging due to the high frequency of unformatted text
            #logger.warning(f"Unexpected time format: {time_str}")
            return None

    except Exception as e:
        logger.warning(f"Error normalizing time string '{time_str}': {e}")
        return None


def parse_time_to_datetime(time_str: str, race_date: str) -> datetime | None:
    """
    Convert a time string and race date to a datetime object.

    Handles day rollover for times after midnight (e.g., 1:30 AM the next day).

    Args:
        time_str: Time string in HH:MM:SS format
        race_date: Race date in YYYY-MM-DD format

    Returns:
        datetime object, or None if input is invalid
    """
    if not time_str:
        return None

    try:
        # Combine date and time
        dt_str = f"{race_date} {time_str}"
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

        # Handle day rollover: if time is before 5:00 AM, it's the next day
        # Race starts at 5:00 AM, so times like 1:30 mean 1:30 AM the next day
        time_only = datetime.strptime(time_str, "%H:%M:%S").time()
        start_time = datetime.strptime(RACE_START_TIME, "%H:%M").time()

        if time_only < start_time:
            # Add one day
            dt = dt + timedelta(days=1)

        return dt

    except Exception as e:
        logger.warning(f"Error parsing time to datetime '{time_str}': {e}")
        return None


def calculate_elapsed_minutes(checkpoint_time: datetime, race_start: datetime) -> float | None:
    """
    Calculate elapsed time in minutes from race start to checkpoint.

    Args:
        checkpoint_time: Datetime of checkpoint arrival
        race_start: Datetime of race start

    Returns:
        Elapsed time in minutes (as float), or None if invalid
    """
    if not checkpoint_time or not race_start:
        return None

    try:
        elapsed = checkpoint_time - race_start
        return elapsed.total_seconds() / 60.0
    except Exception as e:
        logger.warning(f"Error calculating elapsed time: {e}")
        return None


def reshape_wide_to_long(
    df_wide: pl.DataFrame,
    parsed_columns: list[dict],
    as_metadata: pl.DataFrame
) -> pl.DataFrame:
    """
    Reshape the wide format 2025 data to long format matching 2016-2017 structure.

    Transforms one row per runner with multiple AS columns into multiple rows
    per runner (one per aid station checkpoint).

    Args:
        df_wide: Wide format DataFrame from CSV
        parsed_columns: Parsed aid station column information
        as_metadata: Aid station metadata (distances, names, etc.)

    Returns:
        Long format DataFrame with columns matching 2016-2017 output
    """
    logger.info("Reshaping data from wide to long format")
    logger.info(f"Input shape: {df_wide.shape}")

    # Prepare race start datetime
    race_start_dt = datetime.strptime(f"{RACE_DATE_2025} {RACE_START_TIME}", "%Y-%m-%d %H:%M")

    # Build list to collect all rows
    all_rows = []

    # Group parsed columns by aid station
    # We need to pair In/Out columns for each aid station
    aid_stations = {}
    for col_info in parsed_columns:
        as_key = col_info['as_index']
        if as_key not in aid_stations:
            aid_stations[as_key] = {'in': None, 'out': None, 'name': col_info['as_name']}

        if col_info['direction'] == 'In':
            aid_stations[as_key]['in'] = col_info['original_col']
        elif col_info['direction'] == 'Out':
            aid_stations[as_key]['out'] = col_info['original_col']

    logger.info(f"Found {len(aid_stations)} unique aid stations")

    # Iterate through each runner (row)
    for row_idx, runner_row in enumerate(df_wide.iter_rows(named=True)):
        bib = runner_row.get('Bib')
        name = runner_row.get('Name', '')

        # Iterate through each aid station
        for as_index, as_cols in aid_stations.items():
            # Skip START (only has Out)
            if as_index == 'START':
                continue

            # Get In and Out times
            in_col = as_cols.get('in')
            out_col = as_cols.get('out')

            in_time_raw = runner_row.get(in_col, MISSING_TIME_MARKER) if in_col else MISSING_TIME_MARKER
            out_time_raw = runner_row.get(out_col, MISSING_TIME_MARKER) if out_col else MISSING_TIME_MARKER

            # Normalize time strings
            in_time_str = normalize_time_string(in_time_raw)
            out_time_str = normalize_time_string(out_time_raw)

            # Skip if both times are missing
            if not in_time_str and not out_time_str:
                continue

            # Parse to datetime
            in_datetime = parse_time_to_datetime(in_time_str, RACE_DATE_2025) if in_time_str else None
            out_datetime = parse_time_to_datetime(out_time_str, RACE_DATE_2025) if out_time_str else None

            # Calculate elapsed times in minutes
            in_elapsed_min = calculate_elapsed_minutes(in_datetime, race_start_dt)
            out_elapsed_min = calculate_elapsed_minutes(out_datetime, race_start_dt)

            # Create row matching 2016-2017 structure
            row_data = {
                'year': RACE_YEAR,
                'bib': bib,
                'name': name,
                'gender': None,  # Not available in 2025 data
                'age': None,     # Not available in 2025 data
                'as_index': as_index,
                'as_name': as_cols['name'],
                'as_check_in__tod': in_time_str,
                'as_check_out__tod': out_time_str,
                'as_check_in__elapsed': None,  # Could calculate HH:MM:SS format if needed
                'as_check_out__elapsed': None,
                'race_datetime': race_start_dt.strftime("%m/%d/%Y %H:%M"),
                'as_check_in__tod__datetime': in_datetime.strftime("%m/%d/%Y %H:%M") if in_datetime else None,
                'as_check_in__elapsed__min': in_elapsed_min,
                'as_check_out__tod__datetime': out_datetime.strftime("%m/%d/%Y %H:%M") if out_datetime else None,
                'as_check_out__elapsed__min': out_elapsed_min,
                'as_dist_from_start': None,  # Will join from metadata
                'as_dist_incr': None,        # Will join from metadata
            }

            all_rows.append(row_data)

        # Log progress every 50 runners
        if (row_idx + 1) % 50 == 0:
            logger.info(f"Processed {row_idx + 1} runners...")

    logger.info(f"Created {len(all_rows)} total checkpoint records")

    # Convert to DataFrame
    df_long = pl.DataFrame(all_rows)

    # Join with aid station metadata to add distances
    # First, prepare metadata with matching column names
    as_meta_clean = as_metadata.select([
        pl.col('as_index').str.to_uppercase().alias('as_index'),
        pl.col('as_cum_dist').alias('as_dist_from_start'),
        pl.col('as_dist').alias('as_dist_incr')
    ])

    # Perform left join
    df_long = df_long.join(
        as_meta_clean,
        on='as_index',
        how='left',
        suffix='_meta'
    )

    # If join created duplicate columns, select the metadata versions
    if 'as_dist_from_start_meta' in df_long.columns:
        df_long = df_long.drop(['as_dist_from_start', 'as_dist_incr'])
        df_long = df_long.rename({
            'as_dist_from_start_meta': 'as_dist_from_start',
            'as_dist_incr_meta': 'as_dist_incr'
        })

    logger.info(f"Final long format shape: {df_long.shape}")

    return df_long


def validate_output(df: pl.DataFrame) -> dict:
    """
    Validate the processed output and generate summary statistics.

    Args:
        df: Processed DataFrame in long format

    Returns:
        Dictionary with validation results and statistics
    """
    logger.info("Validating processed data")

    validation = {
        'total_rows': len(df),
        'unique_bibs': df['bib'].n_unique(),
        'unique_aid_stations': df['as_index'].n_unique(),
        'missing_check_in_times': df['as_check_in__tod'].null_count(),
        'missing_check_out_times': df['as_check_out__tod'].null_count(),
        'missing_check_in_datetimes': df['as_check_in__tod__datetime'].null_count(),
        'missing_check_out_datetimes': df['as_check_out__tod__datetime'].null_count(),
        'missing_distances': df['as_dist_from_start'].null_count(),
        'negative_elapsed_times': 0,
        'aid_station_distribution': {}
    }

    # Check for negative elapsed times
    if 'as_check_in__elapsed__min' in df.columns:
        neg_count = df.filter(
            pl.col('as_check_in__elapsed__min').is_not_null() &
            (pl.col('as_check_in__elapsed__min') < 0)
        ).shape[0]
        validation['negative_elapsed_times'] = neg_count

    # Get aid station distribution
    as_dist = df.group_by('as_index').agg(pl.count().alias('count')).sort('as_index')
    validation['aid_station_distribution'] = dict(
        zip(as_dist['as_index'].to_list(), as_dist['count'].to_list())
    )

    return validation


def main():
    """
    Main execution function for testing 2025 data preprocessing.
    """
    logger.info("=" * 80)
    logger.info("Starting 2025 Eastern States 100 Data Preprocessing Test")
    logger.info("=" * 80)

    try:
        # Step 1: Load aid station metadata
        as_metadata = load_aid_station_metadata()
        logger.info(f"Aid stations: {as_metadata['as_index'].to_list()}")

        # Step 2: Load raw 2025 data
        logger.info(f"Loading raw data from {RAW_DATA_PATH}")
        df_raw = pl.read_csv(RAW_DATA_PATH)
        logger.info(f"Raw data shape: {df_raw.shape}")
        logger.info(f"Columns: {df_raw.columns[:5]}... (showing first 5)")

        # Step 3: Parse column headers
        parsed_cols = parse_aid_station_columns(df_raw.columns)
        logger.info(f"Sample parsed columns: {parsed_cols[:3]}")

        # Step 4: Reshape data
        df_processed = reshape_wide_to_long(df_raw, parsed_cols, as_metadata)

        # Step 5: Validate output
        validation_results = validate_output(df_processed)
        logger.info("=" * 80)
        logger.info("VALIDATION RESULTS")
        logger.info("=" * 80)
        for key, value in validation_results.items():
            if key != 'aid_station_distribution':
                logger.info(f"{key}: {value}")

        logger.info("\nAid Station Distribution:")
        for as_idx, count in validation_results['aid_station_distribution'].items():
            logger.info(f"  {as_idx}: {count} records")

        # Step 6: Display sample output
        logger.info("=" * 80)
        logger.info("SAMPLE OUTPUT (first 10 rows)")
        logger.info("=" * 80)
        print(df_processed.head(10))

        # Step 7: Save output
        logger.info(f"\nSaving processed data to {OUTPUT_PATH}")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_processed.write_csv(OUTPUT_PATH)
        logger.info(f"Successfully saved {len(df_processed)} rows")

        logger.info("=" * 80)
        logger.info("Processing completed successfully!")
        logger.info("=" * 80)

        return df_processed

    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    df_result = main()
