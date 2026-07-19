"""
Tests for the course pipeline (GPX parse + station crosswalk), the shared
cumulative-ratio feature, the static blog figure, and the planner/course
additions to the dashboard payload.

Like test_data_quality.py, these read the pipeline's persisted outputs, so run
the pipelines first:

    uv run kedro run
    uv run pytest tests/test_course_and_planner.py -v
"""

import json
import re
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).parent.parent
ROUTE = ROOT / "data/02_intermediate/es_course_route.csv"
STATIONS = ROOT / "data/02_intermediate/es_course_stations.csv"
XWALK = ROOT / "data/02_intermediate/es_station_xwalk.csv"
RATIO = ROOT / "data/04_feature/es_interval_ratio.csv"
ALL_SPLITS = ROOT / "data/02_intermediate/es_splits_all.csv"
DASHBOARD = ROOT / "data/08_reporting/es_as_dashboard.html"
BLOG_DIR = ROOT / "data/08_reporting/blog_figures"

# Official course length used to scale GPX miles (parameters_course.yml).
OFFICIAL_FINISH_MI = 103.1
MAX_DELTA_MI = 4.0


@pytest.fixture(scope="module")
def route() -> pl.DataFrame:
    return pl.read_csv(ROUTE)


@pytest.fixture(scope="module")
def stations() -> pl.DataFrame:
    return pl.read_csv(STATIONS)


@pytest.fixture(scope="module")
def xwalk() -> pl.DataFrame:
    return pl.read_csv(XWALK)


@pytest.fixture(scope="module")
def ratio() -> pl.DataFrame:
    return pl.read_csv(RATIO)


@pytest.fixture(scope="module")
def splits() -> pl.DataFrame:
    return pl.read_csv(
        ALL_SPLITS, schema_overrides={"as_check_out__elapsed__min": pl.Float64}
    )


# ---------------------------------------------------------------------------
# Course route
# ---------------------------------------------------------------------------


def test_route_cum_miles_monotonic(route):
    cm = route.sort("seq")["cum_mi"].to_list()
    assert all(b >= a for a, b in zip(cm, cm[1:])), "cumulative miles must not decrease"


def test_route_total_length_reasonable(route):
    total = route["cum_mi"].max()
    assert 95 <= total <= 110, f"GPX route length {total:.1f} mi outside expected band"


# ---------------------------------------------------------------------------
# Stations
# ---------------------------------------------------------------------------


def test_seventeen_stations(stations):
    # Start (0) + 15 aid stations + Finish (16).
    assert stations.height == 17


def test_station_miles_strictly_increasing(stations):
    mi = stations.sort("station_id")["scaled_mi"].to_list()
    assert all(
        b > a for a, b in zip(mi, mi[1:])
    ), "station miles must strictly increase"


def test_finish_station_at_official_length(stations):
    finish = stations.filter(pl.col("station_id") == 16)["scaled_mi"][0]
    assert finish == pytest.approx(OFFICIAL_FINISH_MI, abs=0.05)


# ---------------------------------------------------------------------------
# Crosswalk — the AS-numbering regression trap
# ---------------------------------------------------------------------------


def test_every_split_station_is_mapped(xwalk, splits):
    """Every (year, as_index) that appears in the splits must have a mapped row."""
    split_keys = splits.select(["year", "as_index"]).unique()
    mapped = split_keys.join(xwalk, on=["year", "as_index"], how="inner")
    assert (
        mapped.height == split_keys.height
    ), "some split stations have no crosswalk row"
    assert mapped["station_2026"].null_count() == 0, "some stations mapped to null"


def test_blackwell_maps_to_blackwell_every_year(xwalk):
    """Blackwell is AS_13 in early years, AS_12 later — matched by distance, not index.

    Guards the renumbering trap: whatever its as_index, every year's Blackwell
    row must land on the 2026 Blackwell station.
    """
    black = xwalk.filter(pl.col("as_name").str.to_lowercase().str.contains("blackwell"))
    assert black.height >= 5, "expected a Blackwell row in most years"
    names = black["station_2026_name"].str.to_lowercase().unique().to_list()
    assert names == ["blackwell"], f"Blackwell mapped to {names}"


def test_all_deltas_within_tolerance(xwalk):
    assert (
        xwalk["delta_mi"].max() <= MAX_DELTA_MI
    ), "a station matched beyond max_delta_mi"


# ---------------------------------------------------------------------------
# Interval (leg) ratio — leg pace ÷ final overall pace
# ---------------------------------------------------------------------------


def test_ratio_has_six_years_no_dnf(ratio):
    years = set(ratio["year"].unique().to_list())
    assert years == {2016, 2017, 2021, 2022, 2023, 2025}, f"years: {years}"
    # Finishers only — no DNF rows leaked in.
    assert (
        "FinishRank" not in ratio.columns or "DNF" not in ratio["FinishRank"].to_list()
    )


def test_ratio_values_sane(ratio):
    frac = (
        ratio.filter(
            (pl.col("interval_ratio") > 0.3) & (pl.col("interval_ratio") < 3.0)
        ).height
        / ratio.height
    )
    assert frac >= 0.99, f"only {frac:.3f} of interval ratios in (0.3, 3.0)"


def test_interval_ratio_centered(ratio):
    """The interval ratio is unit-safe by construction (the ×60 hours→minutes
    factor cancels between leg pace and overall pace), so the field-wide median
    must sit just below 1.0 — moving legs are slightly faster than overall pace,
    which includes aid-station time. A unit or definition slip would push this
    far off (e.g. ~60× or ~0.017×).
    """
    med = ratio["interval_ratio"].median()
    assert 0.75 <= med <= 1.05, f"median interval ratio {med:.3f} outside sane band"


def test_final_pace_magnitude(ratio):
    """Sanity on the denominator: a 20–36 h finish over ~103 mi is ~11.6–21
    min/mile overall — a coarse guard that the pace units are minutes/mile.
    """
    fp = ratio["final_pace_min_per_mi"].median()
    assert 10 <= fp <= 24, f"median final pace {fp:.1f} min/mi out of range"


# ---------------------------------------------------------------------------
# Dashboard payload + static figures
# ---------------------------------------------------------------------------


def _payload(html: str) -> dict:
    m = re.search(r"const DATA\s*=\s*", html)
    start = m.end()
    assert html[start] == "{"
    depth, i = 0, start
    while i < len(html):
        c = html[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                break
        i += 1
    return json.loads(html[start : i + 1].replace("<\\/", "</"))


def test_dashboard_has_planner_and_course():
    html = DASHBOARD.read_text(encoding="utf-8")
    d = _payload(html)
    assert "planner" in d and "course" in d
    assert len(d["planner"]["stations"]) == 16, "expected 16 selectable stations"
    assert d["course"]["total_mi"] == pytest.approx(OFFICIAL_FINISH_MI, abs=0.1)
    # station routeIdx values must index into the (downsampled) route array
    n = len(d["course"]["route"])
    assert all(0 <= st[5] < n for st in d["course"]["stations"])
    assert d["course"]["stations"][-1][5] == n - 1, "finish must index route end"


def test_dashboard_is_fully_offline():
    """No external scripts/styles/fonts/tiles — the page must work with no
    network (crew uses it at the race). The SVG course map replaced Leaflet."""
    html = DASHBOARD.read_text(encoding="utf-8")
    assert "unpkg.com" not in html and "leaflet" not in html.lower()
    assert "tile.openstreetmap" not in html
    assert 'id="card-map"' in html and "buildMapProjection" in html


def test_dashboard_has_pacing_planner():
    """The offline race-day pacing card and its goal input must be present."""
    html = DASHBOARD.read_text(encoding="utf-8")
    assert 'id="card-pacing"' in html and 'id="paceGoal"' in html
    assert "renderPacing" in html


def test_planner_avg_is_speed_ratio(ratio):
    """The reporting payload flips the pace ratio to a speed ratio (>1 = faster),
    so the per-station average means sit just above 1.0 — the mirror of the
    pace-ratio dataset median (~0.93)."""
    html = DASHBOARD.read_text(encoding="utf-8")
    d = _payload(html)
    means = sorted(v[0] for v in d["planner"]["avg"].values())
    med = means[len(means) // 2]
    assert 1.0 <= med <= 1.25, f"planner avg speed-ratio median {med:.3f} not >1"


def test_dashboard_json_is_escaped():
    html = DASHBOARD.read_text(encoding="utf-8")
    blob_start = html.index("const DATA =")
    blob_end = html.index("</script>", blob_start)
    blob = html[blob_start:blob_end]
    assert (
        "</" not in blob
    ), "unescaped </ inside the embedded JSON would break the script"


def test_dashboard_size_bounded():
    assert DASHBOARD.stat().st_size < 1_000_000, "dashboard HTML exceeded 1 MB"


def test_blog_figures_exist():
    png = BLOG_DIR / "interval_ratio_scatter.png"
    svg = BLOG_DIR / "interval_ratio_scatter.svg"
    assert png.exists() and png.stat().st_size > 10_000, "PNG missing or trivial"
    assert svg.exists() and svg.stat().st_size > 10_000, "SVG missing or trivial"
    assert (
        svg.read_text(encoding="utf-8", errors="ignore").lstrip()[:200].find("svg")
        != -1
    ), "SVG file does not look like SVG"
