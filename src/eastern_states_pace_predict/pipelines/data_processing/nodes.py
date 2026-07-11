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

    # Arrival: compute seconds, detect midnight crossings, build datetime.
    #
    # Base datetime is midnight of race_date (NOT 05:00). Adding seconds-since-
    # midnight to midnight gives the correct TOD datetime; subtracting
    # race_start_datetime then yields the true elapsed time.
    #
    # The race has a hard 36-hour cutoff, so only one genuine midnight crossing
    # is possible. A second apparent backward jump while already on day+1 is an
    # AM/PM data-entry error — correct that specific row by adding 12 h.
    df = df.sort(["year", "bib", "as_index"]).with_columns(
        _tod_to_seconds("as_check_in__tod").alias("_arr_s"),
        pl.col("race_start_datetime").dt.truncate("1d").alias("_race_midnight"),
    ).with_columns(
        pl.col("_arr_s")
        .shift(1, fill_value=RACE_START_S)
        .over(["year", "bib"])
        .alias("_prev_arr_s")
    ).with_columns(
        (pl.col("_arr_s") < pl.col("_prev_arr_s"))
        .cast(pl.Int32)
        .alias("_is_arr_cross")
    ).with_columns(
        pl.col("_is_arr_cross")
        .cum_sum()
        .over(["year", "bib"])
        .alias("_cross_cum")
    ).with_columns(
        # Cap day offset at 1 — only one midnight crossing possible in a 36h race
        pl.col("_cross_cum").clip(upper_bound=1).alias("_arr_day_offset"),
        # Correct only the row where the second+ crossing was detected, not all
        # subsequent rows. Rows with _is_arr_cross=0 pass through unchanged even
        # if _cross_cum is already >= 2.
        pl.when(
            (pl.col("_is_arr_cross") == 1) & (pl.col("_cross_cum") >= 2)
        )
        .then(pl.col("_arr_s") + 12 * 3600)
        .otherwise(pl.col("_arr_s"))
        .alias("_arr_s_corr"),
    ).with_columns(
        (
            pl.col("_race_midnight")
            + pl.duration(seconds=pl.col("_arr_s_corr"))
            + pl.duration(days=pl.col("_arr_day_offset"))
        ).alias("as_check_in__tod__datetime")
    ).with_columns(
        _elapsed_hhmmss("as_check_in__tod__datetime").alias("as_check_in__elapsed"),
        _elapsed_hours("as_check_in__tod__datetime").alias("as_check_in__elapsed__min"),
    )

    # Departure: compare against the corrected arrival seconds for the same row.
    # Same AM/PM rule: if already on day+1 and departure appears before corrected
    # arrival, add 12 h instead of another day.
    df = df.with_columns(
        pl.when(pl.col("as_check_out__tod").is_not_null())
        .then(_tod_to_seconds("as_check_out__tod"))
        .otherwise(None)
        .alias("_dep_s")
    ).with_columns(
        pl.when(pl.col("_dep_s").is_not_null())
        .then(
            pl.when(pl.col("_dep_s") < pl.col("_arr_s_corr"))
            .then(
                pl.when(pl.col("_arr_day_offset") >= 1)
                .then(pl.col("_dep_s") + 12 * 3600)  # AM/PM error: add 12h, no extra day
                .otherwise(pl.col("_dep_s"))           # genuine midnight crossing: handled below
            )
            .otherwise(pl.col("_dep_s"))
        )
        .otherwise(None)
        .alias("_dep_s_corr"),
        # Extra day only when still on day 0 and departure genuinely crossed midnight
        pl.when(pl.col("_dep_s").is_not_null())
        .then(
            pl.when(
                (pl.col("_dep_s") < pl.col("_arr_s_corr"))
                & (pl.col("_arr_day_offset") < 1)
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
        )
        .otherwise(None)
        .alias("_dep_day_extra"),
    ).with_columns(
        pl.when(pl.col("_dep_s_corr").is_not_null())
        .then(
            pl.col("_race_midnight")
            + pl.duration(seconds=pl.col("_dep_s_corr"))
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
    ).drop([
        "_arr_s", "_prev_arr_s", "_is_arr_cross", "_cross_cum",
        "_arr_s_corr", "_arr_day_offset", "_race_midnight",
        "_dep_s", "_dep_s_corr", "_dep_day_extra", "_dep_dt",
    ])

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
            # official_rank from finish_times is the authoritative finish indicator.
            # flag_finish from as_info is unreliable: in some years (e.g., 2025) the
            # finish checkpoint row is absent from the split data entirely, so
            # flag_finish.any() would return False for every finisher in that year.
            pl.col("official_rank").is_not_null().any().alias("has_finish"),
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


def plot_pace_chart(df: pl.DataFrame) -> go.Figure:
    """Scatter-line chart of elapsed time vs distance for each runner.

    Inputs: es_splits_2021_2025_processed
    Outputs: es_pace_chart (plotly.JSONDataset)

    A year-selector button strip is embedded in the figure so only one year's
    runners are visible at a time. Defaults to the earliest available year.
    """
    years = sorted(df["year"].unique().to_list())
    default_year = years[0]

    fig = go.Figure()
    trace_years: list[int] = []

    for year in years:
        year_df = df.filter(pl.col("year") == year).sort(
            ["bib", "as_dist_from_start"]
        )
        for bib in sorted(year_df["bib"].unique().to_list()):
            runner = year_df.filter(pl.col("bib") == bib)
            first_name = runner["name"].drop_nulls().first()
            bib_str = str(int(bib))
            label = bib_str if first_name is None else f"{bib_str} – {first_name}"
            hover_rows = runner.select(
                ["as_index", "as_name", "as_check_in__tod",
                 "as_check_in__elapsed", "as_check_in__tod__datetime"]
            ).rows()
            fig.add_trace(
                go.Scatter(
                    x=runner["as_dist_from_start"].to_list(),
                    y=runner["as_check_in__elapsed__min"].to_list(),
                    mode="lines+markers",
                    name=label,
                    visible=(year == default_year),
                    line=dict(width=1.5),
                    marker=dict(size=4),
                    opacity=0.75,
                    customdata=hover_rows,
                    hovertemplate=(
                        "<b>Bib %{fullData.name}</b><br>"
                        "AS: %{customdata[0]} – %{customdata[1]}<br>"
                        "TOD: %{customdata[2]}<br>"
                        "Elapsed: %{customdata[3]}<br>"
                        "Datetime: %{customdata[4]}<br>"
                        "Distance: %{x:.1f} mi<br>"
                        "<extra></extra>"
                    ),
                )
            )
            trace_years.append(year)

    buttons = [
        dict(
            label=str(y),
            method="update",
            args=[
                {"visible": [t == y for t in trace_years]},
                {"title": f"ES100 Pace Chart — {y}"},
            ],
        )
        for y in years
    ]

    fig.update_layout(
        title=f"ES100 Pace Chart — {default_year}",
        xaxis_title="Distance from Start (miles)",
        yaxis_title="Elapsed Time (hours)",
        showlegend=False,
        height=650,
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                buttons=buttons,
                active=0,
                x=0.0,
                xanchor="left",
                y=1.12,
                yanchor="top",
            )
        ],
    )
    return fig


def process_2016_2017_splits(
    raw: pl.DataFrame,
    as_info: pl.DataFrame,
    finish_times: pl.DataFrame,
) -> pl.DataFrame:
    """Enrich the pre-computed 2016-2017 long splits to match the 2021-2025 schema.

    Inputs: es_splits_historical_2016-17, es_asinfo_historical, es_finish_historical
    Outputs: es_splits_2016_2017_processed
    """
    # a. Clean and cast. Drop columns that may or may not exist depending on the
    # source file version (time_check/time_adjustment were removed in a later cut;
    # the trailing empty column '' comes from a trailing comma in the CSV).
    _cols_to_drop = [c for c in ["time_check", "time_adjustment", "name", "gender", "age", ""] if c in raw.columns]
    df = (
        raw.filter(pl.col("year").is_not_null())
        .drop(_cols_to_drop)
        .with_columns(
            pl.col("year").cast(pl.Int64),
            pl.col("bib").cast(pl.Int64),
            pl.col("as_check_in__elapsed__min").cast(pl.Float64),
            pl.col("as_dist_from_start").cast(pl.Float64),
            pl.col("as_dist_incr").cast(pl.Float64),
        )
    )

    # b. Join as_info to get flag_finish
    ai = as_info.select(
        pl.col("year").cast(pl.Int64),
        pl.col("as_index"),
        pl.col("flag_finish").cast(pl.Boolean),
    )
    df = df.join(ai, on=["year", "as_index"], how="left")

    # c. Join finish_times for demographics and official results
    ft = (
        finish_times.filter(pl.col("race_year").is_in([2016, 2017]))
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
    df = df.join(ft, on=["year", "bib"], how="left")

    # d. Compute runner-level metadata
    df = df.with_columns(
        pl.col("as_index").str.extract(r"(\d+)$").cast(pl.Int32).alias("_as_num")
    )

    per_runner = (
        df.group_by(["year", "bib"])
        .agg(
            # official_rank from finish_times is the authoritative finish indicator.
            # flag_finish from as_info is unreliable for 2016-2017 because some runners
            # bypassed the AS_17 checkpoint (marked as finish in as_info) and only have
            # an AS_18 row, causing flag_finish.any() to return False for true finishers.
            pl.col("official_rank").is_not_null().any().alias("has_finish"),
            pl.col("_as_num").max().alias("_max_as_num"),
            pl.col("as_dist_from_start").max().alias("_max_dist"),
            pl.col("as_check_in__elapsed__min").max().alias("_max_elapsed"),
            pl.col("official_rank").first().alias("_official_rank"),
            pl.col("finish_elapsed_hrs").first().alias("_finish_hrs"),
        )
        .with_columns(
            pl.when(pl.col("has_finish"))
            .then(pl.col("_official_rank").cast(pl.String))
            .otherwise(pl.lit("DNF"))
            .alias("FinishRank"),
            pl.when(pl.col("has_finish"))
            .then(pl.lit("FINISH"))
            .otherwise(
                pl.concat_str([
                    pl.lit("AS_"),
                    pl.col("_max_as_num").cast(pl.String).str.zfill(2),
                ])
            )
            .alias("MaxAS"),
            pl.when(pl.col("has_finish"))
            .then(pl.col("_finish_hrs"))
            .otherwise(pl.col("_max_elapsed"))
            .alias("MaxTime"),
        )
    )

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
    df = df.drop("_as_num").join(runner_meta, on=["year", "bib"], how="left")

    # e. Compute as_rank from elapsed time (TOD is absent in 2016-2017)
    df = (
        df.sort(["year", "as_index", "as_check_in__elapsed__min", "bib"])
        .with_columns(
            (pl.int_range(pl.len()).over(["year", "as_index"]) + 1).alias("as_rank")
        )
    )

    # f. Normalize datetime strings to ISO format
    df = df.with_columns(
        pl.col("as_check_in__tod__datetime")
        .str.to_datetime("%m/%d/%y %H:%M", strict=False)
        .cast(pl.String)
        .alias("as_check_in__tod__datetime"),
        pl.col("race_datetime")
        .str.to_datetime("%m/%d/%y %H:%M", strict=False)
        .dt.strftime("%Y-%m-%d %H:%M")
        .alias("race_datetime"),
    )

    # g. Add null OriginalOrder (no equivalent in 2016-2017)
    df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("OriginalOrder"))

    # h. Drop flag_finish and select final column order
    df = df.drop("flag_finish")
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
    final_cols = [c for c in ordered_cols if c in df.columns]
    return df.select(final_cols)


def combine_splits(
    df_1617: pl.DataFrame,
    df_2125: pl.DataFrame,
) -> pl.DataFrame:
    """Stack 2016-2017 and 2021-2025 processed splits into a single dataset.

    Inputs: es_splits_2016_2017_processed, es_splits_2021_2025_processed
    Outputs: es_splits_all
    """
    float_cols = ["as_check_in__elapsed__min", "as_dist_from_start",
                  "as_dist_incr", "finish_elapsed_hrs", "finish_elapsed_mins"]
    int_cols = ["year", "bib", "OverallRank", "as_rank", "official_rank"]

    def _normalise_types(df: pl.DataFrame) -> pl.DataFrame:
        exprs = []
        for col in float_cols:
            if col in df.columns:
                exprs.append(pl.col(col).cast(pl.Float64, strict=False))
        for col in int_cols:
            if col in df.columns:
                exprs.append(pl.col(col).cast(pl.Int64, strict=False))
        return df.with_columns(exprs) if exprs else df

    combined = pl.concat(
        [_normalise_types(df_1617), _normalise_types(df_2125)],
        how="diagonal_relaxed",
    )
    return combined.sort(["year", "bib", "as_index"])
