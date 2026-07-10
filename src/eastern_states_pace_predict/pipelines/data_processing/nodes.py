import logging
import re
from datetime import datetime, timedelta

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)


def process_2021_2025_splits(splits: pl.DataFrame) -> pl.DataFrame:
    """Load in 2021-2025 split data. Once loaded, reshape the data:
    one row per runner to long (one row per runner×AS).

    Inputs: es_splits2021_2025 (Polars DF)
    Outputs: es_splits_2021_2025_long (Polars DF)"""

    runner_cols = [
        "year",
        "bib_number",
        "OriginalOrder",
        "MaxTime",
        "OverallRank",
        "MaxAS",
        "FinishRank",
    ]

    def _unpivot_metric(df: pl.DataFrame, suffix: str, out_col: str) -> pl.DataFrame:
        cols = [c for c in df.columns if c.endswith(suffix)]
        return (
            df.select(runner_cols + cols)
            .unpivot(
                on=cols, index=runner_cols, variable_name="col", value_name=out_col
            )
            .with_columns(
                pl.concat_str(
                    [pl.lit("AS_"), pl.col("col").str.extract(r"^as(\d+)_")]
                ).alias("as_index")
            )
            .drop("col")
        )

    arr_tod = _unpivot_metric(
        splits.drop("as01_arr_rank2"), "_arr_tod", "as_check_in__tod"
    )
    dep_tod = _unpivot_metric(splits, "_dep_tod", "as_check_out__tod")
    arr_rank = _unpivot_metric(
        splits.drop("as01_arr_rank2").select(
            [c for c in splits.columns if not c.endswith("_rank2")]
        ),
        "_arr_rank",
        "arr_rank",
    )

    join_keys = runner_cols + ["as_index"]
    return (
        arr_tod.join(dep_tod, on=join_keys, how="left")
        .join(arr_rank, on=join_keys, how="left")
        .filter(pl.col("as_check_in__tod").is_not_null())
        .sort(["bib_number", "as_index"])
    )


def enrich_2021_2025_splits(
    long_df: pl.DataFrame,
    race_meta: pl.DataFrame,
    as_info: pl.DataFrame,
    finish_times: pl.DataFrame,
) -> pl.DataFrame:
    """Enrich the 2021-2025 long splits with elapsed times, AS distances,
    finish demographics, runner-level metadata, and per-AS rankings.

    Inputs: es_splits_2021_2025_long, es_race_meta, es_asinfo_historical,
            es_finish_historical
    Outputs: es_splits_2021_2025_processed
    """
    RACE_START_S = 5 * 3600  # 05:00 in seconds since midnight

    # a. Rename bib_number → bib; cast year to Int64 (CSV loads it as str).
    # Filter rows where as_check_in__tod is not a valid HH:MM time — the
    # upstream wide-to-long node filters nulls but not garbage string values.
    df = (
        long_df.rename({"bib_number": "bib"})
        .with_columns(pl.col("year").cast(pl.Int64))
        .filter(pl.col("as_check_in__tod").str.contains(r"^\d{2}:\d{2}$"))
    )

    # b. Join race_meta to get race_date and race_datetime
    meta = race_meta.select(
        pl.col("race_year").cast(pl.Int64).alias("year"),
        pl.col("race_date"),
        pl.col("race_time_start"),
    )
    df = df.join(meta, on="year", how="left").with_columns(
        pl.concat_str([pl.col("race_date"), pl.lit(" "), pl.col("race_time_start")])
        .str.to_datetime("%Y-%m-%d %H:%M")
        .alias("race_start_datetime"),
        pl.concat_str([pl.col("race_date"), pl.lit(" "), pl.col("race_time_start")])
        .alias("race_datetime"),
    )

    # c. Join as_info for station names, distances, and finish flag
    ai = as_info.select(
        pl.col("year").cast(pl.Int64),
        pl.col("as_index"),
        pl.col("as_name"),
        pl.col("dist_from_start").alias("as_dist_from_start"),
        pl.col("as_dist").alias("as_dist_incr"),
        pl.col("flag_finish").cast(pl.Boolean),
    )
    df = df.join(ai, on=["year", "as_index"], how="left")

    # d. TOD → elapsed conversion with midnight rollover detection
    def _tod_to_seconds(col: str) -> pl.Expr:
        """Convert HH:MM string column to seconds since midnight."""
        parts = pl.col(col).str.splitn(":", 2)
        return (
            parts.struct.field("field_0").cast(pl.Int64) * 3600
            + parts.struct.field("field_1").cast(pl.Int64) * 60
        )

    def _elapsed_hours(dt_col: str) -> pl.Expr:
        """Decimal hours elapsed since race_start_datetime."""
        return (
            (pl.col(dt_col) - pl.col("race_start_datetime")).dt.total_seconds() / 3600
        )

    def _elapsed_hhmmss(dt_col: str) -> pl.Expr:
        """HH:MM:SS string for total elapsed time."""
        total_s = (
            pl.col(dt_col) - pl.col("race_start_datetime")
        ).dt.total_seconds()
        hours = (total_s // 3600).cast(pl.Int64)
        minutes = ((total_s % 3600) // 60).cast(pl.Int64)
        seconds = (total_s % 60).cast(pl.Int64)
        return pl.concat_str(
            [
                hours.cast(pl.String).str.zfill(2),
                pl.lit(":"),
                minutes.cast(pl.String).str.zfill(2),
                pl.lit(":"),
                seconds.cast(pl.String).str.zfill(2),
            ]
        )

    # Arrival: compute seconds, detect midnight crossings, build datetime
    df = df.sort(["year", "bib", "as_index"]).with_columns(
        _tod_to_seconds("as_check_in__tod").alias("_arr_s")
    ).with_columns(
        pl.col("_arr_s")
        .shift(1, fill_value=RACE_START_S)
        .over(["year", "bib"])
        .alias("_prev_arr_s")
    ).with_columns(
        (pl.col("_arr_s") < pl.col("_prev_arr_s"))
        .cast(pl.Int32)
        .cum_sum()
        .over(["year", "bib"])
        .alias("_arr_day_offset")
    ).with_columns(
        (
            pl.col("race_start_datetime")
            + pl.duration(seconds=pl.col("_arr_s"))
            + pl.duration(days=pl.col("_arr_day_offset"))
        ).alias("as_check_in__tod__datetime")
    ).with_columns(
        _elapsed_hhmmss("as_check_in__tod__datetime").alias("as_check_in__elapsed"),
        _elapsed_hours("as_check_in__tod__datetime").alias("as_check_in__elapsed__min"),
    )

    # Departure: seed from arrival datetime of same row (departure >= arrival)
    df = df.with_columns(
        pl.when(pl.col("as_check_out__tod").is_not_null())
        .then(_tod_to_seconds("as_check_out__tod"))
        .otherwise(None)
        .alias("_dep_s")
    ).with_columns(
        # Use arrival seconds as the "previous" reference for departure
        pl.when(pl.col("_dep_s").is_not_null())
        .then(
            pl.when(pl.col("_dep_s") < pl.col("_arr_s"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
        )
        .otherwise(None)
        .alias("_dep_day_extra")
    ).with_columns(
        pl.when(pl.col("_dep_s").is_not_null())
        .then(
            pl.col("race_start_datetime")
            + pl.duration(seconds=pl.col("_dep_s"))
            + pl.duration(days=pl.col("_arr_day_offset") + pl.col("_dep_day_extra"))
        )
        .otherwise(None)
        .alias("_dep_dt")
    ).with_columns(
        pl.when(pl.col("_dep_dt").is_not_null())
        .then(_elapsed_hhmmss("_dep_dt"))
        .otherwise(None)
        .alias("as_check_out__elapsed"),
        pl.when(pl.col("_dep_dt").is_not_null())
        .then(_elapsed_hours("_dep_dt"))
        .otherwise(None)
        .alias("as_check_out__elapsed__min"),
    ).drop(["_arr_s", "_prev_arr_s", "_arr_day_offset", "_dep_s", "_dep_day_extra", "_dep_dt"])

    # e. Join finish_times for demographics and official results
    ft = (
        finish_times.filter(pl.col("race_year").is_in([2021, 2022, 2023, 2025]))
        .select(
            pl.col("race_year").cast(pl.Int64).alias("year"),
            pl.col("bib").cast(pl.Int64),
            pl.col("name"),
            pl.col("gender"),
            pl.col("age"),
            pl.col("city"),
            pl.col("official_rank"),
            pl.col("finish_time"),
            pl.col("finish_elapsed_hrs"),
            pl.col("finish_elapsed_mins"),
        )
    )
    df = df.with_columns(
        pl.col("bib").str.replace(r"\*", "").cast(pl.Int64)
    ).join(
        ft, on=["year", "bib"], how="left"
    )

    # f. Compute runner-level metadata
    # Extract numeric part of as_index (e.g., "AS_07" → 7)
    df = df.with_columns(
        pl.col("as_index").str.extract(r"(\d+)$").cast(pl.Int32).alias("_as_num")
    )

    per_runner = (
        df.group_by(["year", "bib"])
        .agg(
            pl.col("flag_finish").any().alias("has_finish"),
            pl.col("_as_num").max().alias("_max_as_num"),
            pl.col("as_dist_from_start").max().alias("_max_dist"),
            pl.col("as_check_in__elapsed__min").max().alias("_max_elapsed"),
            pl.col("official_rank").first().alias("_official_rank"),
            pl.col("finish_elapsed_hrs").first().alias("_finish_hrs"),
        )
        .with_columns(
            # FinishRank: official rank for finishers, "DNF" otherwise
            pl.when(pl.col("has_finish"))
            .then(pl.col("_official_rank").cast(pl.String))
            .otherwise(pl.lit("DNF"))
            .alias("FinishRank"),
            # MaxAS
            pl.when(pl.col("has_finish"))
            .then(pl.lit("FINISH"))
            .otherwise(
                pl.concat_str(
                    [
                        pl.lit("AS_"),
                        pl.col("_max_as_num").cast(pl.String).str.zfill(2),
                    ]
                )
            )
            .alias("MaxAS"),
            # MaxTime in decimal hours
            pl.when(pl.col("has_finish"))
            .then(pl.col("_finish_hrs"))
            .otherwise(pl.col("_max_elapsed"))
            .alias("MaxTime"),
        )
    )

    # OverallRank: rank by max_dist DESC then MaxTime ASC within each year
    per_runner = per_runner.with_columns(
        pl.struct(
            (-pl.col("_max_dist")).alias("neg_dist"),
            pl.col("MaxTime").alias("time"),
        )
        .rank(method="ordinal")
        .over("year")
        .alias("OverallRank")
    )

    runner_meta = per_runner.select(
        ["year", "bib", "FinishRank", "MaxAS", "MaxTime", "OverallRank"]
    )

    # Drop the placeholder columns from the wide source and join computed ones
    df = (
        df.drop(["FinishRank", "MaxAS", "MaxTime", "OverallRank", "_as_num"])
        .join(runner_meta, on=["year", "bib"], how="left")
    )

    # g. Compute per-AS arrival rank (single column), replacing source arr_rank
    df = (
        df.sort(["year", "as_index", "as_check_in__tod__datetime", "bib"])
        .with_columns(
            (pl.int_range(pl.len()).over(["year", "as_index"]) + 1).alias("as_rank")
        )
        .drop("arr_rank")
    )

    # h. Drop internal columns
    df = df.drop(["flag_finish", "race_time_start", "race_date", "race_start_datetime"])

    # i. Final column order
    ordered_cols = [
        "year", "bib", "name", "gender", "age", "city",
        "as_index", "as_name",
        "as_check_in__tod", "as_check_out__tod",
        "as_check_in__elapsed", "as_check_out__elapsed",
        "race_datetime", "as_check_in__tod__datetime",
        "as_check_in__elapsed__min",
        "as_dist_from_start", "as_dist_incr",
        "MaxAS", "FinishRank", "OverallRank", "MaxTime",
        "as_rank",
        "official_rank", "finish_time", "finish_elapsed_hrs", "finish_elapsed_mins",
        "OriginalOrder",
    ]
    # Only include columns that exist (guards against schema drift)
    final_cols = [c for c in ordered_cols if c in df.columns]
    return df.select(final_cols)
