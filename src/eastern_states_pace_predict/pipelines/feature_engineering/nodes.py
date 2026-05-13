import logging

import plotly.graph_objects as go
import polars as pl

logger = logging.getLogger(__name__)


def _fix_elapsed_violations(
    group: pl.DataFrame,
    elapsed_col: str,
    flag_col: str,
) -> pl.DataFrame:
    """
    Fix monotonicity violations in one elapsed time column for a single runner.

    Builds a greedy monotonically increasing anchor set from the valid values,
    then linearly interpolates any violated point between its surrounding
    anchors. Points with no anchor on either side are left unchanged (null or
    original value) for model-based estimation downstream.
    """
    group = group.sort("as_dist_from_start", nulls_last=True)

    distances = group["as_dist_from_start"].to_list()
    elapsed = group[elapsed_col].to_list()
    n = len(elapsed)

    fixed = list(elapsed)
    was_interpolated = [False] * n

    # Greedy forward pass: collect indices that form a monotonically increasing
    # subsequence — these become the interpolation anchors.
    clean_indices: list[int] = []
    for i in range(n):
        if fixed[i] is None or distances[i] is None:
            continue
        if not clean_indices or fixed[i] > fixed[clean_indices[-1]]:
            clean_indices.append(i)

    if len(clean_indices) < 2:
        return group.with_columns(pl.lit(False).cast(pl.Boolean).alias(flag_col))

    clean_set = set(clean_indices)

    for i in range(n):
        if fixed[i] is None or distances[i] is None or i in clean_set:
            continue

        prev_anchor = next((j for j in reversed(clean_indices) if j < i), None)
        next_anchor = next((j for j in clean_indices if j > i), None)

        if prev_anchor is not None and next_anchor is not None:
            d0, d1 = distances[prev_anchor], distances[next_anchor]
            t0, t1 = fixed[prev_anchor], fixed[next_anchor]
            d = distances[i]
            if d1 > d0:
                fixed[i] = t0 + (t1 - t0) * (d - d0) / (d1 - d0)
                was_interpolated[i] = True

    return group.with_columns(
        pl.Series(elapsed_col, fixed),
        pl.Series(flag_col, was_interpolated),
    )


def fix_timing_violations(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect and correct monotonicity violations in runner elapsed times.

    For each runner (bib + year), check-in and check-out elapsed times must
    increase monotonically with distance from start. Violations are corrected
    by linear interpolation between the nearest valid neighboring aid stations.
    Points with no valid neighbor on both sides are left null for model-based
    estimation in a later step.

    Two flag columns are added:
      - check_in_interpolated  (Boolean): check-in time was corrected
      - check_out_interpolated (Boolean): check-out time was corrected

    Finish-only rows (no aid station data) are passed through unchanged.

    Args:
        df: Combined split + finish data (es_splits_with_finish)

    Returns:
        DataFrame with corrected elapsed times and interpolation flag columns
    """
    if hasattr(df, "collect"):
        df = df.collect()

    splits = df.filter(pl.col("as_index").is_not_null())
    finish_only = df.filter(pl.col("as_index").is_null())

    logger.info(
        f"Checking timing violations across {splits.select(['bib', 'year']).unique().shape[0]} runners"
    )

    splits = pl.concat([
        _fix_elapsed_violations(g, "as_check_in__elapsed__min", "check_in_interpolated")
        for g in splits.partition_by(["bib", "year"])
    ])
    splits = pl.concat([
        _fix_elapsed_violations(g, "as_check_out__elapsed__min", "check_out_interpolated")
        for g in splits.partition_by(["bib", "year"])
    ])

    n_in = int(splits["check_in_interpolated"].sum())
    n_out = int(splits["check_out_interpolated"].sum())
    logger.info(
        f"Interpolated {n_in} check-in violation(s) and {n_out} check-out violation(s)"
    )

    result = pl.concat([splits, finish_only], how="diagonal_relaxed")
    logger.info(f"Output shape: {result.shape[0]} rows, {result.shape[1]} columns")
    return result


_YEAR_COLORS = {
    2016: "#1f77b4",  # blue
    2017: "#ff7f0e",  # orange
    2019: "#9467bd",  # purple
    2021: "#8c564b",  # brown
    2022: "#e377c2",  # pink
    2023: "#7f7f7f",  # grey
    2025: "#2ca02c",  # green
}


def visualize_runner_timing(df: pl.DataFrame) -> go.Figure:
    """
    Line chart of check-in elapsed time (hrs) vs distance from start.
    One line per runner, colored by year. Intended for Kedro Viz output.

    Args:
        df: Timing-corrected split + finish data (es_timing_corrected)

    Returns:
        Plotly Figure with one trace per runner and a year-color legend
    """
    if hasattr(df, "collect"):
        df = df.collect()

    plot_df = df.filter(
        pl.col("as_index").is_not_null()
        & pl.col("as_check_in__elapsed__min").is_not_null()
        & pl.col("as_dist_from_start").is_not_null()
    )

    years = sorted(plot_df["year"].drop_nulls().unique().to_list())
    logger.info(f"Plotting {plot_df.select(['bib','year']).unique().shape[0]} runners across years: {years}")

    fig = go.Figure()

    # One dummy trace per year to drive the legend
    for year in years:
        color = _YEAR_COLORS.get(year, "#333333")
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode="lines",
            name=str(year),
            line=dict(color=color, width=2),
            showlegend=True,
        ))

    # One trace per runner
    for group in plot_df.sort("as_dist_from_start").partition_by(["bib", "year"]):
        bib = group["bib"][0]
        year = group["year"][0]
        color = _YEAR_COLORS.get(year, "#333333")

        fig.add_trace(go.Scatter(
            x=group["as_dist_from_start"].to_list(),
            y=(group["as_check_in__elapsed__min"] / 60).to_list(),
            mode="lines",
            line=dict(color=color, width=1),
            opacity=0.45,
            showlegend=False,
            hovertemplate=(
                f"Bib: {bib} | Year: {year}<br>"
                "Distance: %{x:.1f} mi<br>"
                "Elapsed: %{y:.2f} hrs<extra></extra>"
            ),
        ))

    fig.update_layout(
        title="Runner Elapsed Time by Distance from Start",
        xaxis_title="Distance from Start (mi)",
        yaxis_title="Elapsed Time (hrs)",
        height=650,
        hovermode="closest",
        legend_title="Year",
    )

    logger.info("Runner timing visualization created")
    return fig


def detect_segment_pace_issues(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    Layer 1 — Segment pace check.

    For each runner, compute the pace (min/mile) between consecutive check-in
    times sorted by distance. Two flag values are added per row:
      - segment_pace_min_per_mile: pace from the previous station to this one
        (null for the first station and any station with a missing check-in)
      - pace_flag: True when pace is outside [floor, ceiling], indicating a
        physically implausible value or a likely day-offset carry-over error

    Finish-only rows (no as_index) are passed through unchanged.

    Args:
        df: Timing-corrected data (es_timing_corrected)
        params: outlier_detection config from parameters_feature_engineering.yml
    """
    if hasattr(df, "collect"):
        df = df.collect()

    floor = params["pace_floor_min_per_mile"]
    ceiling = params["pace_ceiling_min_per_mile"]

    splits = df.filter(pl.col("as_index").is_not_null())
    finish_only = df.filter(pl.col("as_index").is_null())

    def _compute_pace(group: pl.DataFrame) -> pl.DataFrame:
        group = group.sort("as_dist_from_start", nulls_last=True)
        distances = group["as_dist_from_start"].to_list()
        elapsed = group["as_check_in__elapsed__min"].to_list()
        n = len(elapsed)

        paces: list[float | None] = [None] * n
        flags: list[bool] = [False] * n
        prev = None

        for i in range(n):
            if elapsed[i] is None or distances[i] is None:
                continue
            if prev is not None:
                d_delta = distances[i] - distances[prev]
                t_delta = elapsed[i] - elapsed[prev]
                if d_delta > 0:
                    pace = t_delta / d_delta
                    paces[i] = pace
                    flags[i] = pace < floor or pace > ceiling
            prev = i

        return group.with_columns(
            pl.Series("segment_pace_min_per_mile", paces),
            pl.Series("pace_flag", flags),
        )

    splits = pl.concat([_compute_pace(g) for g in splits.partition_by(["bib", "year"])])

    n_flagged = int(splits["pace_flag"].sum())
    flagged_runners = (
        splits.filter(pl.col("pace_flag"))
        .select(["year", "bib", "as_index", "as_dist_from_start", "segment_pace_min_per_mile"])
        .sort(["year", "bib", "as_dist_from_start"])
    )
    logger.info(f"Pace check: {n_flagged} flagged checkpoint(s)")
    if n_flagged > 0:
        logger.warning(f"Flagged segments:\n{flagged_runners}")

    return pl.concat([splits, finish_only], how="diagonal_relaxed")


def detect_rank_based_outliers(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    Layer 2 — Rank-based outlier detection.

    For each (year, as_index) pair with enough runners who have both a
    check-in time and a finish time, fits a simple linear model:

        check_in_elapsed_min ~ finish_elapsed_mins

    Runners whose residual exceeds rank_outlier_std_threshold standard
    deviations are flagged. Two columns are added:
      - rank_residual_min: signed residual in minutes (null if no model exists
        for that station/year, or runner has no finish time)
      - rank_outlier_flag: True when |residual| > threshold * residual std dev

    Finish-only rows and runners without finish times are passed through with
    null residuals and flag=False.

    Args:
        df: Pace-checked data (es_pace_checked)
        params: outlier_detection config from parameters_feature_engineering.yml
    """
    import numpy as np

    if hasattr(df, "collect"):
        df = df.collect()

    min_samples = params["rank_model_min_samples"]
    std_threshold = params["rank_outlier_std_threshold"]

    splits = df.filter(pl.col("as_index").is_not_null()).with_columns(
        pl.lit(None).cast(pl.Float64).alias("rank_residual_min"),
        pl.lit(False).alias("rank_outlier_flag"),
    )
    finish_only = df.filter(pl.col("as_index").is_null())

    # Fit one linear model per (year, as_index) using runners with both times
    models: dict[tuple, tuple] = {}
    for group in (
        splits.filter(
            pl.col("as_check_in__elapsed__min").is_not_null()
            & pl.col("finish_elapsed_mins").is_not_null()
        ).partition_by(["year", "as_index"])
    ):
        if group.shape[0] < min_samples:
            continue
        key = (group["year"][0], group["as_index"][0])
        x = np.array(group["finish_elapsed_mins"].to_list(), dtype=float)
        y = np.array(group["as_check_in__elapsed__min"].to_list(), dtype=float)
        slope, intercept = np.polyfit(x, y, 1)
        residuals = y - (slope * x + intercept)
        std = residuals.std()
        if std > 0:
            models[key] = (slope, intercept, std)

    logger.info(f"Rank model: fitted {len(models)} station-year model(s)")

    # Apply models station by station
    processed: list[pl.DataFrame] = []
    for group in splits.partition_by(["year", "as_index"]):
        key = (group["year"][0], group["as_index"][0])
        model = models.get(key)
        if model is None:
            processed.append(group)
            continue

        slope, intercept, std = model
        finish_times = group["finish_elapsed_mins"].to_list()
        check_in_times = group["as_check_in__elapsed__min"].to_list()

        residuals: list[float | None] = []
        flags: list[bool] = []
        for ft, ct in zip(finish_times, check_in_times):
            if ft is None or ct is None:
                residuals.append(None)
                flags.append(False)
            else:
                r = ct - (slope * ft + intercept)
                residuals.append(r)
                flags.append(abs(r) > std_threshold * std)

        processed.append(group.with_columns(
            pl.Series("rank_residual_min", residuals, dtype=pl.Float64),
            pl.Series("rank_outlier_flag", flags, dtype=pl.Boolean),
        ))

    splits = pl.concat(processed, how="diagonal_relaxed")

    n_flagged = int(splits["rank_outlier_flag"].sum())
    logger.info(f"Rank model: {n_flagged} flagged checkpoint(s)")
    if n_flagged > 0:
        logger.warning(
            splits.filter(pl.col("rank_outlier_flag"))
            .select(["year", "bib", "name", "as_index", "as_check_in__elapsed__min", "rank_residual_min"])
            .sort(["year", "bib"])
        )

    return pl.concat([splits, finish_only], how="diagonal_relaxed")


def flag_cutoff_violations(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    Layer 3 — Race cutoff flag.

    Adds an exceeds_cutoff Boolean column that is True when either the
    check-in or check-out elapsed time exceeds the race cutoff. This is the
    hardest bound — values above it are physically impossible given the rules.

    Args:
        df: Rank-checked data (es_rank_checked)
        params: outlier_detection config from parameters_feature_engineering.yml
    """
    if hasattr(df, "collect"):
        df = df.collect()

    cutoff_min = params["race_cutoff_hours"] * 60

    result = df.with_columns(
        (
            (pl.col("as_check_in__elapsed__min") > cutoff_min)
            | (pl.col("as_check_out__elapsed__min") > cutoff_min)
        ).fill_null(False).alias("exceeds_cutoff")
    )

    n_exceeded = int(result["exceeds_cutoff"].sum())
    if n_exceeded > 0:
        over = (
            result.filter(pl.col("exceeds_cutoff"))
            .select(["year", "bib", "name", "as_index",
                     "as_check_in__elapsed__min", "as_check_out__elapsed__min"])
            .unique(subset=["year", "bib", "as_index"])
            .sort(["year", "bib"])
        )
        logger.warning(f"Cutoff flag: {n_exceeded} checkpoint(s) exceed {params['race_cutoff_hours']} hrs:\n{over}")
    else:
        logger.info(f"Cutoff flag: no checkpoints exceed the {params['race_cutoff_hours']}-hour cutoff")

    return result


def build_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build model features from outlier-flagged split + finish data.

    Input:  es_outliers_flagged  (data/04_feature)
    Output: es_features          (data/04_feature)
    """
    if hasattr(df, "collect"):
        df = df.collect()

    logger.info(f"Building features from {df.shape[0]} rows, {df.shape[1]} columns")

    # TODO: add feature engineering steps here

    return df
