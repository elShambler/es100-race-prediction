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


def build_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build model features from timing-corrected split + finish data.

    Input:  es_timing_corrected  (data/04_feature)
    Output: es_features          (data/04_feature)
    """
    if hasattr(df, "collect"):
        df = df.collect()

    logger.info(f"Building features from {df.shape[0]} rows, {df.shape[1]} columns")

    # TODO: add feature engineering steps here

    return df
