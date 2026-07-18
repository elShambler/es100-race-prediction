import json
import logging
from pathlib import Path

import matplotlib
import polars as pl

matplotlib.use("Agg")  # kedro runs headless; never open a GUI window
import matplotlib.pyplot as plt

from eastern_states_pace_predict import mpl_theme

logger = logging.getLogger(__name__)

RACE_START_HR = 5  # 05:00 start, all years
MIN_GROUP_N = 5  # suppress aggregate cells with fewer runners than this
MINUTES_PER_HOUR = 60
# Stoppage medians are only shown when observed check-in/out pairs cover at
# least this share of a station's visits (guards against the biased subset
# left by sparse 2025 check-in recording).
STOPPAGE_COVERAGE_MIN = 0.3


def _fmt_tod(elapsed_hrs: float) -> str:
    """Clock time for an elapsed-hours value; '+1' marks the second day."""
    total = RACE_START_HR + elapsed_hrs
    day, tod = divmod(total, 24)
    h = int(tod)
    m = int(round((tod - h) * MINUTES_PER_HOUR))
    if m == MINUTES_PER_HOUR:
        h, m = h + 1, 0
    suffix = " +1" if day >= 1 else ""
    return f"{h:02d}:{m:02d}{suffix}"


def _quantiles(s: pl.Series, qs: tuple[float, ...]) -> list[float | None]:
    s = s.drop_nulls()
    if s.len() < MIN_GROUP_N:
        return [None] * len(qs)
    return [round(s.quantile(q, interpolation="linear"), 3) for q in qs]


def _year_payload(df: pl.DataFrame) -> dict:
    """All chart aggregates for one year's interval features."""
    runners = df.unique(subset=["bib"])
    n_start = runners.height
    n_finish = runners.filter(pl.col("is_finisher")).height
    finish_hrs = (
        runners.filter(pl.col("is_finisher"))["overall_pace_min_per_mi"] * 103.1 / 60
    )

    stations = (
        df.unique(subset=["as_index"])
        .sort("as_index")
        .select(["as_index", "as_name", "as_num", "as_dist_from_start"])
    )

    # Arrival windows: p10/p25/p50/p75/p90 of check-in elapsed hours per station
    # (imputed 2025 check-ins included — they carry the arrival estimate).
    arrivals = []
    # Stoppage: observed (non-imputed) medians, DNF cohort vs finishers.
    stoppage = []
    # Leg difficulty: median pace ratio per leg, clean legs only.
    legs = []
    # Attrition: furthest station reached by DNF runners.
    dnf_by_max = (
        runners.filter(~pl.col("is_finisher"))
        .group_by("MaxAS")
        .len()
        .rename({"MaxAS": "as_index", "len": "n"})
    )
    attrition = []
    # Flow heatmap: arrivals per station per hour of race time.
    heat_rows = []

    for st in stations.iter_rows(named=True):
        sdf = df.filter(pl.col("as_index") == st["as_index"])
        label = f"{st['as_name']}"
        dist = st["as_dist_from_start"]

        q = _quantiles(sdf["as_check_in__elapsed__min"], (0.1, 0.25, 0.5, 0.75, 0.9))
        arrivals.append(
            {
                "as": st["as_index"],
                "name": label,
                "dist": dist,
                "q10": q[0],
                "q25": q[1],
                "q50": q[2],
                "q75": q[3],
                "q90": q[4],
                "tod50": _fmt_tod(q[2]) if q[2] is not None else None,
                "tod10": _fmt_tod(q[0]) if q[0] is not None else None,
                "tod90": _fmt_tod(q[4]) if q[4] is not None else None,
                "n": sdf["as_check_in__elapsed__min"].drop_nulls().len(),
            }
        )

        # Observed pairs only — and only when they cover enough of the
        # station's visits. Years with sparse check-in recording (2025) leave
        # a biased subset at some stations: the few runners whose arrival got
        # recorded are often exactly the ones who stopped long.
        observed = sdf.filter(~pl.col("stoppage_imputed"))
        coverage = observed.height / sdf.height if sdf.height else 0.0
        fin = observed.filter(pl.col("is_finisher"))["as_stoppage_time_min"]
        dnf = observed.filter(~pl.col("is_finisher"))["as_stoppage_time_min"]
        reliable = coverage >= STOPPAGE_COVERAGE_MIN
        stoppage.append(
            {
                "as": st["as_index"],
                "name": label,
                "dist": dist,
                "finisher": _quantiles(fin, (0.5,))[0] if reliable else None,
                "dnf": _quantiles(dnf, (0.5,))[0] if reliable else None,
                "n_finisher": fin.drop_nulls().len(),
                "n_dnf": dnf.drop_nulls().len(),
            }
        )

        clean = sdf.filter(~pl.col("spans_missing_as"))
        ratio = _quantiles(clean["as_interval_pace_ratio"], (0.5,))[0]
        pace = _quantiles(clean["as_interval_pace"], (0.5,))[0]
        legs.append(
            {
                "as": st["as_index"],
                "name": label,
                "dist": dist,
                "leg_mi": round(clean["interval_dist_mi"].drop_nulls().median() or 0, 1)
                if clean.height
                else None,
                "ratio": ratio,
                "pace": pace,
                "n": clean["as_interval_pace_ratio"].drop_nulls().len(),
            }
        )

        n_dnf_here = dnf_by_max.filter(pl.col("as_index") == st["as_index"])
        attrition.append(
            {
                "as": st["as_index"],
                "name": label,
                "dist": dist,
                "n": int(n_dnf_here["n"][0]) if n_dnf_here.height else 0,
            }
        )

        counts = (
            sdf.with_columns(
                pl.col("as_check_in__elapsed__min").floor().cast(pl.Int32).alias("_hr")
            )
            .group_by("_hr")
            .len()
        )
        by_hr = dict(zip(counts["_hr"].to_list(), counts["len"].to_list()))
        heat_rows.append([int(by_hr.get(h, 0)) for h in range(36)])

    med_stop = df.filter(~pl.col("stoppage_imputed"))["as_stoppage_time_min"]

    return {
        "kpis": {
            "starters": n_start,
            "finishers": n_finish,
            "finish_rate": round(n_finish / n_start, 3) if n_start else None,
            "median_finish_hrs": round(finish_hrs.median(), 2)
            if finish_hrs.len()
            else None,
            "median_stoppage_min": round(med_stop.drop_nulls().median(), 1)
            if med_stop.drop_nulls().len()
            else None,
        },
        "stations": stations.rows(named=True),
        "arrivals": arrivals,
        "stoppage": stoppage,
        "legs": legs,
        "attrition": attrition,
        "heat": {"hours": list(range(36)), "rows": heat_rows},
    }


HALF_HOURS_IN_RACE = 72  # 36 h of race time in half-hour arrival bins


def _planner_payload(
    ratio: pl.DataFrame,
    splits: pl.DataFrame,
    xwalk: pl.DataFrame,
    stations: pl.DataFrame,
    params: dict,
) -> dict:
    """Year-independent planner aggregates for the scatter + arrival cards.

    All years pooled, finishers only for the pace-ratio scatter/trend; the
    arrival histogram uses every recorded arrival (DNFs included) so the
    distribution reflects who actually passed through.
    """
    fhr_min = params["finish_hr_min"]
    fhr_max = params["finish_hr_max"]

    # Selectable stations = the 2026 aid stations, Start excluded (nothing runs
    # into it). [id, name, scaled mile].
    selectable = stations.filter(pl.col("station_id") > 0).sort("station_id")
    station_rows = [
        [int(r["station_id"]), r["name"], round(r["scaled_mi"], 1)]
        for r in selectable.iter_rows(named=True)
    ]

    # Scatter points: one per finisher × station, mapped to a 2026 station.
    # Stratified-sample down to max_scatter_points, keeping cohorts balanced.
    pts_df = ratio.filter(pl.col("station_2026").is_not_null())
    cap = params["max_scatter_points"]
    if pts_df.height > cap:
        frac = cap / pts_df.height
        pts_df = (
            pts_df.with_columns(pl.col("finish_hr_block").alias("_blk"))
            .filter(
                pl.int_range(pl.len()).shuffle(seed=17).over("_blk")
                < (pl.len().over("_blk") * frac).ceil()
            )
            .drop("_blk")
        )
    points = [
        [
            round(r["as_dist_from_start"], 2),
            round(r["cum_ratio"], 3),
            int(r["station_2026"]),
            int(r["finish_hr_block"]),
        ]
        for r in pts_df.iter_rows(named=True)
    ]

    # Per-station average ratio (full data, not the sample): {sid: [mean, n]}.
    avg = {}
    avg_df = (
        ratio.filter(pl.col("station_2026").is_not_null())
        .group_by("station_2026")
        .agg(pl.col("cum_ratio").mean().alias("m"), pl.len().alias("n"))
    )
    for r in avg_df.iter_rows(named=True):
        avg[str(int(r["station_2026"]))] = [round(r["m"], 3), int(r["n"])]

    # Cohort trend: mean ratio per (finish-hour block, station); cells with
    # fewer than MIN_GROUP_N runners are dropped so a lone runner can't define a
    # "trend". {fhr: [[sid, mean, n], ...]}.
    trend = {}
    trend_df = (
        ratio.filter(
            pl.col("station_2026").is_not_null()
            & (pl.col("finish_hr_block") >= fhr_min)
            & (pl.col("finish_hr_block") <= fhr_max)
        )
        .group_by("finish_hr_block", "station_2026", "station_mi_2026")
        .agg(pl.col("cum_ratio").mean().alias("m"), pl.len().alias("n"))
        .filter(pl.col("n") >= MIN_GROUP_N)
        .sort("station_mi_2026")
    )
    for r in trend_df.iter_rows(named=True):
        trend.setdefault(str(int(r["finish_hr_block"])), []).append(
            [int(r["station_2026"]), round(r["m"], 3), int(r["n"])]
        )

    # Arrival distributions: every recorded arrival, mapped to a 2026 station via
    # the crosswalk. Elapsed hours live in as_check_in__elapsed__min (decimal
    # hours despite the name); bin into half-hours over 0–36 h.
    arr = (
        splits.select(["year", "as_index", "as_check_in__elapsed__min", "FinishRank"])
        .join(
            xwalk.select(["year", "as_index", "station_2026"]),
            on=["year", "as_index"],
            how="inner",
        )
        .filter(
            pl.col("station_2026").is_not_null()
            & pl.col("as_check_in__elapsed__min").is_not_null()
        )
        .with_columns(pl.col("as_check_in__elapsed__min").alias("hrs"))
    )
    bins: dict[str, list[int]] = {}
    bin_df = (
        arr.with_columns(
            (pl.col("hrs") * 2)
            .floor()
            .cast(pl.Int32)
            .clip(0, HALF_HOURS_IN_RACE - 1)
            .alias("b")
        )
        .group_by("station_2026", "b")
        .len()
    )
    for r in bin_df.iter_rows(named=True):
        sid = str(int(r["station_2026"]))
        bins.setdefault(sid, [0] * HALF_HOURS_IN_RACE)[int(r["b"])] = int(r["len"])

    # Cohort arrival window: p25/p50/p75 arrival hour at each station for the
    # finishers in each finish-hour block (from the finishers-only ratio frame,
    # which already carries elapsed_hrs + finish_hr_block per station).
    cohort: dict[str, dict[str, list]] = {}
    for r in (
        ratio.filter(
            pl.col("station_2026").is_not_null()
            & (pl.col("finish_hr_block") >= fhr_min)
            & (pl.col("finish_hr_block") <= fhr_max)
        )
        .group_by("station_2026", "finish_hr_block")
        .agg(
            pl.col("elapsed_hrs").quantile(0.25, "linear").alias("p25"),
            pl.col("elapsed_hrs").quantile(0.5, "linear").alias("p50"),
            pl.col("elapsed_hrs").quantile(0.75, "linear").alias("p75"),
            pl.len().alias("n"),
        )
        .iter_rows(named=True)
    ):
        if r["n"] < MIN_GROUP_N:
            continue
        sid = str(int(r["station_2026"]))
        cohort.setdefault(sid, {})[str(int(r["finish_hr_block"]))] = [
            round(r["p25"], 2),
            round(r["p50"], 2),
            round(r["p75"], 2),
            int(r["n"]),
        ]

    return {
        "stations": station_rows,
        "fhr_min": fhr_min,
        "fhr_max": fhr_max,
        "points": points,
        "avg": avg,
        "trend": trend,
        "arrivals": {"bins": bins, "cohort": cohort},
    }


def _course_payload(route: pl.DataFrame, stations: pl.DataFrame, params: dict) -> dict:
    """Downsampled 2026 route + station markers for the Leaflet map card.

    Route is thinned to max_route_points while always keeping the station
    vertices, so each station's routeIdx indexes cleanly into the kept array.
    """
    dec = params["coord_decimals"]
    cap = params["max_route_points"]
    n = route.height
    keep_seqs = set(stations["route_seq"].to_list())
    step = max(1, (n + cap - 1) // cap)
    kept = [
        r
        for i, r in enumerate(route.sort("seq").iter_rows(named=True))
        if i % step == 0 or r["seq"] in keep_seqs
    ]
    seq_to_idx = {r["seq"]: i for i, r in enumerate(kept)}
    route_pts = [[round(r["lat"], dec), round(r["lon"], dec)] for r in kept]

    station_rows = []
    for r in stations.sort("station_id").iter_rows(named=True):
        station_rows.append(
            [
                int(r["station_id"]),
                r["name"],
                round(r["lat"], dec),
                round(r["lon"], dec),
                round(r["scaled_mi"], 1),
                seq_to_idx[r["route_seq"]],
            ]
        )
    total_mi = round(
        stations.filter(pl.col("station_id") == pl.col("station_id").max())[
            "scaled_mi"
        ][0],
        1,
    )
    return {"route": route_pts, "stations": station_rows, "total_mi": total_mi}


def build_as_dashboard(
    features: pl.DataFrame,
    ratio: pl.DataFrame,
    splits: pl.DataFrame,
    route: pl.DataFrame,
    stations: pl.DataFrame,
    xwalk: pl.DataFrame,
    params: dict,
) -> str:
    """Render the aid-station dashboard as a self-contained HTML page.

    Aggregates es_interval_features per year and injects the JSON into the
    HTML/CSS/JS template that lives next to this module. Everything except the
    Leaflet map card is self-contained; the map pulls OpenStreetMap tiles over
    the network at view time.

    Inputs: es_interval_features, es_cumulative_ratio, es_splits_all,
        es_course_route, es_course_stations, es_station_xwalk, params:reporting
    Outputs: es_as_dashboard (text HTML, data/08_reporting)
    """
    payload = {
        "years": sorted(features["year"].unique().to_list(), reverse=True),
        "generated_note": "Eastern States 100 — split data 2016–2025",
        "by_year": {
            str(y): _year_payload(features.filter(pl.col("year") == y))
            for y in sorted(features["year"].unique().to_list())
        },
        "planner": _planner_payload(ratio, splits, xwalk, stations, params),
        "course": _course_payload(route, stations, params),
    }

    template = (Path(__file__).parent / "template.html").read_text(encoding="utf-8")
    marker = "/*__DATA__*/null"
    if marker not in template:
        raise ValueError("dashboard template is missing the /*__DATA__*/ marker")
    # </script> inside a JSON string would end the script block early.
    blob = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    html = template.replace(marker, blob)
    logger.info(
        "Dashboard built: %d years, %d bytes", len(payload["by_year"]), len(html)
    )
    return html


# Finish-time cohorts for the blog scatter: ordered buckets take an ordinal
# one-hue ramp (slate-green, light -> dark = faster -> slower finish),
# validated with the dataviz palette checks against the theme panel.
COHORTS = [
    ("under 26 h", 0, 26, "#8aa89b"),
    ("26–30 h", 26, 30, "#5f8272"),
    ("30–34 h", 30, 34, "#3d5a50"),
    ("34 h and over", 34, 99, "#233a31"),
]


def plot_blog_cumulative_ratio(ratio: pl.DataFrame) -> dict:
    """Blog-ready scatter: cumulative pace vs final pace over the course.

    One point per finisher × aid station, all years; below 1.0 = ahead of the
    runner's eventual overall pace. Per-cohort median lines use the 2026
    station mile marks so all years share an x position.

    Inputs: es_cumulative_ratio
    Outputs: es_blog_figures (PNG), es_blog_figures_svg (SVG)
    """
    mpl_theme.apply()
    fig, ax = plt.subplots(figsize=(12, 7))

    for label, lo, hi, color in COHORTS:
        cohort = ratio.filter(
            (pl.col("finish_elapsed_hrs") >= lo) & (pl.col("finish_elapsed_hrs") < hi)
        )
        ax.scatter(
            cohort["as_dist_from_start"],
            cohort["cum_ratio"],
            s=7,
            color=color,
            alpha=0.25,
            linewidths=0,
            label=None,
        )
        medians = (
            cohort.filter(pl.col("station_2026").is_not_null())
            .group_by("station_2026", "station_mi_2026")
            .agg(pl.col("cum_ratio").median().alias("med"), pl.len().alias("n"))
            .filter(pl.col("n") >= MIN_GROUP_N)
            .sort("station_mi_2026")
        )
        ax.plot(
            medians["station_mi_2026"],
            medians["med"],
            color=color,
            linewidth=2,
            label=label,
            solid_capstyle="round",
        )

    ax.axhline(
        1.0, color=mpl_theme.COLORS["range"], linewidth=1.2, linestyle=(0, (4, 3))
    )
    ax.text(
        1.0,
        1.004,
        "1.0 = your final overall pace",
        fontsize=9,
        color=mpl_theme.COLORS["tick"],
        va="bottom",
    )

    lo_y = max(0.55, ratio["cum_ratio"].quantile(0.005) - 0.02)
    hi_y = min(1.35, ratio["cum_ratio"].quantile(0.995) + 0.02)
    ax.set_xlim(0, 106)
    ax.set_ylim(lo_y, hi_y)
    ax.legend(title=None, loc="lower right", markerscale=1.5)
    mpl_theme.set_title(
        ax,
        "Everyone banks time early",
        "Cumulative pace relative to final overall pace — finishers, 2016–2025",
    )
    mpl_theme.set_labels(ax, "Distance from start [mi]", "Cumulative ÷ final pace")

    return (
        {"cumulative_ratio_scatter.png": fig},
        {"cumulative_ratio_scatter.svg": fig},
    )
