import marimo

__generated_with = "0.15.0"
app = marimo.App(width="wide")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from pathlib import Path

    return Path, mo, pl


@app.cell
def _(mo):
    mo.md(
        """
    # ES100 Splits Explorer

    Interactive table over `es_splits_all` — all years, all runners, all aid stations.
    Use the filters below to narrow the view. Click any column header to sort.
    """
    )
    return


@app.cell
def _(Path, pl):
    _root = Path(__file__).parent.parent
    df_all = pl.read_csv(_root / "data" / "02_intermediate" / "es_splits_all.csv")
    return (df_all,)


@app.cell
def _(df_all, mo):
    _years = sorted(df_all["year"].drop_nulls().unique().cast(int).to_list())

    year_picker = mo.ui.multiselect(
        options={str(y): y for y in _years},
        value=[str(y) for y in _years],
        label="Year(s)",
    )

    status_picker = mo.ui.radio(
        options={"All": "all", "Finishers": "finish", "DNF": "dnf"},
        value="All",
        label="Finish status",
        inline=True,
    )

    name_search = mo.ui.text(placeholder="Search name…", label="Name")

    mo.hstack([year_picker, status_picker, name_search], gap=3, align="end")
    return name_search, status_picker, year_picker


@app.cell
def _(df_all, mo):
    _all_cols = df_all.columns
    _default = [
        "year",
        "bib",
        "name",
        "gender",
        "age",
        "as_index",
        "as_name",
        "as_check_in__tod",
        "as_check_in__elapsed__min",
        "as_dist_from_start",
        "MaxAS",
        "FinishRank",
        "OverallRank",
        "as_rank",
    ]

    col_picker = mo.ui.multiselect(
        options=_all_cols,
        value=[c for c in _default if c in _all_cols],
        label="Columns to show",
    )
    col_picker
    return (col_picker,)


@app.cell
def _(col_picker, df_all, mo, name_search, pl, status_picker, year_picker):
    _selected_years = [int(y) for y in (year_picker.value or [])]
    if not _selected_years:
        mo.stop(True, mo.callout(mo.md("**Select at least one year.**"), kind="warn"))

    filtered = df_all.filter(pl.col("year").cast(int).is_in(_selected_years))

    if status_picker.value == "finish":
        filtered = filtered.filter(pl.col("FinishRank") != "DNF")
    elif status_picker.value == "dnf":
        filtered = filtered.filter(pl.col("FinishRank") == "DNF")

    if name_search.value.strip():
        filtered = filtered.filter(
            pl.col("name")
            .str.to_lowercase()
            .str.contains(name_search.value.strip().lower())
        )

    _cols = col_picker.value or df_all.columns
    filtered = filtered.select([c for c in _cols if c in filtered.columns])

    _runners = (
        df_all.filter(pl.col("year").cast(int).is_in(_selected_years))
        .select(["bib", "year"])
        .unique()
        .shape[0]
    )

    mo.callout(
        mo.md(
            f"**{filtered.shape[0]:,} rows** · "
            f"**{filtered.select(['bib']).n_unique() if 'bib' in filtered.columns else '—'} runners** · "
            f"**{_runners} runners total in selected year(s)**"
        ),
        kind="info",
    )
    return (filtered,)


@app.cell
def _(filtered, mo):
    mo.ui.table(filtered, pagination=True, page_size=25)
    return


if __name__ == "__main__":
    app.run()
