import json
import logging
from pathlib import Path

import polars as pl

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
                "q10": q[0], "q25": q[1], "q50": q[2], "q75": q[3], "q90": q[4],
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


def build_as_dashboard(features: pl.DataFrame) -> str:
    """Render the aid-station dashboard as a self-contained HTML page.

    Aggregates es_interval_features per year and injects the JSON into the
    HTML/CSS/JS template that lives next to this module. The output has no
    external dependencies, so it can be dropped onto the race website as-is.

    Inputs: es_interval_features
    Outputs: es_as_dashboard (text HTML, data/08_reporting)
    """
    payload = {
        "years": sorted(features["year"].unique().to_list(), reverse=True),
        "generated_note": "Eastern States 100 — split data 2021–2025",
        "by_year": {
            str(y): _year_payload(features.filter(pl.col("year") == y))
            for y in sorted(features["year"].unique().to_list())
        },
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
