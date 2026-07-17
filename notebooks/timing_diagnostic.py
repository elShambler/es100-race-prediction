import marimo

__generated_with = "0.15.0"
app = marimo.App(width="wide")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import plotly.graph_objects as go
    from pathlib import Path

    return Path, go, mo, pl


@app.cell
def _(mo):
    mo.md(
        """
        # Timing Diagnostic

        Developer tool for inspecting elapsed time monotonicity across aid stations.
        Pick a dataset and year(s), then verify every runner's line climbs left-to-right.
        **Red lines** indicate runners with at least one quality flag in the selected dataset.
        """
    )
    return


@app.cell
def _(Path):
    _root = Path(__file__).parent.parent
    _search_dirs = [
        _root / "data" / "02_intermediate",
        _root / "data" / "04_feature",
    ]

    file_options: dict[str, str] = {}
    for _d in _search_dirs:
        if _d.exists():
            for _f in sorted(_d.glob("*.pq")):
                file_options[f"{_d.name}/{_f.stem}"] = str(_f)

    return (file_options,)


@app.cell
def _(file_options, mo):
    dataset_picker = mo.ui.dropdown(
        options=file_options,
        label="Dataset",
        value=list(file_options.keys())[0] if file_options else None,
    )
    dataset_picker
    return (dataset_picker,)


@app.cell
def _(dataset_picker, pl):
    df_raw = (
        pl.read_parquet(dataset_picker.value)
        if dataset_picker.value
        else pl.DataFrame()
    )
    return (df_raw,)


@app.cell
def _(df_raw, mo):
    _years = (
        sorted(df_raw["year"].drop_nulls().unique().to_list())
        if "year" in df_raw.columns
        else []
    )

    year_picker = mo.ui.multiselect(
        options={str(y): y for y in _years},
        value=[str(y) for y in _years],
        label="Year(s)",
    )

    elapsed_picker = mo.ui.radio(
        options={
            "Check-In": "as_check_in__elapsed__min",
            "Check-Out": "as_check_out__elapsed__min",
        },
        value="Check-In",
        label="Elapsed type",
        inline=True,
    )

    mo.hstack([year_picker, elapsed_picker], gap=2, align="end")
    return elapsed_picker, year_picker


@app.cell
def _(df_raw, elapsed_picker, mo, pl, year_picker):
    elapsed_col = elapsed_picker.value

    # Guard: dataset must have the required columns
    _required = {"as_dist_from_start", "as_index", elapsed_col, "bib", "year"}
    _missing = _required - set(df_raw.columns)
    if _missing:
        mo.stop(
            True,
            mo.callout(
                mo.md(f"**Dataset is missing columns:** `{sorted(_missing)}`"),
                kind="warn",
            ),
        )

    _selected_years = [int(y) for y in (year_picker.value or [])]
    if not _selected_years:
        mo.stop(True, mo.callout(mo.md("**Select at least one year.**"), kind="warn"))

    plot_df = df_raw.filter(
        pl.col("year").is_in(_selected_years)
        & pl.col("as_index").is_not_null()
        & pl.col("as_dist_from_start").is_not_null()
        & pl.col(elapsed_col).is_not_null()
    ).sort("as_dist_from_start")

    # Identify flagged runners (any quality flag column that exists)
    _FLAG_COLS = [
        c
        for c in [
            "pace_flag",
            "rank_outlier_flag",
            "exceeds_cutoff",
            "check_in_interpolated",
            "check_out_interpolated",
        ]
        if c in plot_df.columns
    ]

    flagged_keys: set[tuple] = set()
    if _FLAG_COLS:
        _any_flag = pl.fold(
            acc=pl.lit(False),
            function=lambda a, b: a | b,
            exprs=[pl.col(c).fill_null(False) for c in _FLAG_COLS],
        )
        flagged_keys = {
            (r["bib"], r["year"])
            for r in plot_df.filter(_any_flag)
            .select(["bib", "year"])
            .unique()
            .iter_rows(named=True)
        }

    return elapsed_col, flagged_keys, plot_df


@app.cell
def _(elapsed_col, flagged_keys, go, plot_df):
    _YEAR_COLORS = {
        2016: "#1f77b4",
        2017: "#ff7f0e",
        2019: "#9467bd",
        2021: "#8c564b",
        2022: "#e377c2",
        2023: "#7f7f7f",
        2025: "#2ca02c",
    }

    _selected_years = sorted(plot_df["year"].drop_nulls().unique().to_list())

    fig = go.Figure()

    # Dummy legend traces (one per year)
    for _year in _selected_years:
        _c = _YEAR_COLORS.get(_year, "#333333")
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="lines",
                name=str(_year),
                line=dict(color=_c, width=2),
                showlegend=True,
            )
        )

    # Per-runner traces
    for _grp in plot_df.partition_by(["bib", "year"], maintain_order=True):
        _bib = _grp["bib"][0]
        _yr = _grp["year"][0]
        _is_flagged = (_bib, _yr) in flagged_keys
        _color = "red" if _is_flagged else _YEAR_COLORS.get(_yr, "#333333")

        fig.add_trace(
            go.Scatter(
                x=_grp["as_dist_from_start"].to_list(),
                y=(_grp[elapsed_col] / 60).to_list(),
                mode="lines+markers" if _is_flagged else "lines",
                line=dict(color=_color, width=2 if _is_flagged else 1),
                opacity=0.9 if _is_flagged else 0.4,
                showlegend=False,
                hovertemplate=(
                    f"Bib: {_bib} | Year: {_yr}<br>"
                    "Distance: %{x:.1f} mi<br>"
                    "Elapsed: %{y:.2f} hrs<extra></extra>"
                ),
            )
        )

    _label = "Check-In" if "check_in" in elapsed_col else "Check-Out"
    fig.update_layout(
        title=f"Elapsed Time vs Distance from Start — {_label}",
        xaxis_title="Distance from Start (mi)",
        yaxis_title="Elapsed Time (hrs)",
        height=660,
        hovermode="closest",
        legend_title="Year",
    )

    fig
    return (fig,)


@app.cell
def _(df_raw, mo, plot_df):
    _n_runners = plot_df.select(["bib", "year"]).unique().shape[0]
    _n_rows = plot_df.shape[0]

    _FLAG_COLS = [
        c
        for c in [
            "pace_flag",
            "rank_outlier_flag",
            "exceeds_cutoff",
            "check_in_interpolated",
            "check_out_interpolated",
        ]
        if c in df_raw.columns and c in plot_df.columns
    ]

    _flag_parts = [
        f"**{c}**: {int(plot_df[c].fill_null(False).sum())}" for c in _FLAG_COLS
    ]
    _flag_str = ("  ·  " + "  ·  ".join(_flag_parts)) if _flag_parts else ""

    mo.callout(
        mo.md(f"**{_n_runners} runners** · **{_n_rows} checkpoints**{_flag_str}"),
        kind="info",
    )
    return


if __name__ == "__main__":
    app.run()
