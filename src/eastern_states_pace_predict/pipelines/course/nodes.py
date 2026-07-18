import logging
import math
import xml.etree.ElementTree as ET

import polars as pl

logger = logging.getLogger(__name__)

GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}
EARTH_RADIUS_MI = 3958.7613

# The 2026 GPX keeps the traditional AS numbering (AS12 is skipped entirely),
# and its two unnamed waypoints sit at the 2023+ Tomb Flats / Cedar Run
# locations — assumed names until the source file labels them.
WPT_NAME_FIXUPS = {
    "AS10": "AS10 - Tomb Flats",
    "AS11": "AS11 - Cedar Run",
}


def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_MI * math.asin(math.sqrt(a))


def _display_name(gpx_name: str) -> str:
    """Human name for a waypoint: 'AS4 - Browns Run' -> 'Browns Run'."""
    fixed = WPT_NAME_FIXUPS.get(gpx_name, gpx_name)
    _, _, tail = fixed.partition(" - ")
    return tail or fixed


def parse_course_gpx(gpx_text: str, params: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse the 2026 course GPX into a route table and a station table.

    The route keeps full resolution (downsampling is a presentation concern).
    Each waypoint is snapped to the nearest route vertex (~35 m spacing), and
    `scaled_mi` restates its mile mark on the official-distance scale so it
    lines up with the historical `dist_from_start` axis.

    Inputs: es_course_gpx (raw XML text), params:course
    Outputs: es_course_route, es_course_stations
    """
    root = ET.fromstring(gpx_text)

    pts = [
        (
            float(p.get("lat")),
            float(p.get("lon")),
            float(p.findtext("gpx:ele", default="nan", namespaces=GPX_NS)),
        )
        for p in root.findall(".//gpx:rte/gpx:rtept", GPX_NS)
    ]
    cum = [0.0]
    for (lat1, lon1, _), (lat2, lon2, _) in zip(pts, pts[1:]):
        cum.append(cum[-1] + _haversine_mi(lat1, lon1, lat2, lon2))
    total_mi = cum[-1]
    if not 95 <= total_mi <= 110:
        logger.warning("GPX route length %.1f mi outside expected [95, 110]", total_mi)

    route = pl.DataFrame(
        {
            "seq": range(len(pts)),
            "lat": [p[0] for p in pts],
            "lon": [p[1] for p in pts],
            "ele_m": [p[2] for p in pts],
            "cum_mi": [round(c, 3) for c in cum],
        }
    )

    scale = params["official_finish_mi"] / total_mi
    stations = []
    for i, w in enumerate(root.findall("gpx:wpt", GPX_NS)):
        lat, lon = float(w.get("lat")), float(w.get("lon"))
        gpx_name = w.findtext("gpx:name", default=f"wpt_{i}", namespaces=GPX_NS)
        seq = min(
            range(len(pts)),
            key=lambda j: _haversine_mi(lat, lon, pts[j][0], pts[j][1]),
        )
        stations.append(
            {
                "station_id": i,
                "name": _display_name(gpx_name),
                "gpx_name": gpx_name,
                "lat": lat,
                "lon": lon,
                "route_seq": seq,
                "cum_mi": round(cum[seq], 2),
                "scaled_mi": round(cum[seq] * scale, 2),
            }
        )
    stations_df = pl.DataFrame(stations)

    miles = stations_df["cum_mi"].to_list()
    if any(b <= a for a, b in zip(miles, miles[1:])):
        logger.warning("Snapped station miles are not strictly increasing: %s", miles)
    logger.info(
        "Course parsed: %d route points, %.1f mi (scale %.4f), %d stations",
        len(pts), total_mi, scale, stations_df.height,
    )
    return route, stations_df


def map_historical_stations(
    stations: pl.DataFrame,
    asinfo: pl.DataFrame,
    splits: pl.DataFrame,
    params: dict,
) -> pl.DataFrame:
    """Crosswalk each historical (year, as_index) to its 2026 station.

    Matching is on distance-from-start only — never on `as_index`, which was
    renumbered in 2023 (Blackwell is AS_13 through 2022 but AS_12 after).
    Many-to-one matches are expected (pre-2023 Algerine and Long Branch both
    fall nearest to Cedar Run). A historical station further than
    `max_delta_mi` from every 2026 station is left unmapped (null).

    Keys come from asinfo plus any (year, as_index) that only exists in the
    splits — the 2016-17 finish is AS_18 there but AS_17 in asinfo.

    Inputs: es_course_stations, es_asinfo_historical, es_splits_all, params:course
    Outputs: es_station_xwalk
    """
    max_delta = params["station_match"]["max_delta_mi"]
    # The raw file ends with a blank line that loads as an all-null record.
    asinfo = asinfo.filter(pl.col("year").is_not_null())

    keys = asinfo.select("year", "as_index", "as_name", "dist_from_start")
    split_keys = (
        splits.select("year", "as_index", "as_name", "as_dist_from_start")
        .rename({"as_dist_from_start": "dist_from_start"})
        .filter(pl.col("dist_from_start").is_not_null())
        .unique(subset=["year", "as_index"])
        .join(keys, on=["year", "as_index"], how="anti")
    )
    keys = pl.concat([keys, split_keys])

    candidates = stations.filter(pl.col("station_id") > 0).select(
        pl.col("station_id").alias("station_2026"),
        pl.col("name").alias("station_2026_name"),
        pl.col("scaled_mi").alias("station_mi_2026"),
    )
    xwalk = (
        keys
        .join(candidates, how="cross")
        .with_columns(
            (pl.col("dist_from_start") - pl.col("station_mi_2026"))
            .abs()
            .round(2)
            .alias("delta_mi")
        )
        .sort("delta_mi")
        .group_by(["year", "as_index"], maintain_order=False)
        .first()
        .with_columns(
            [
                pl.when(pl.col("delta_mi") <= max_delta)
                .then(pl.col(c))
                .otherwise(None)
                .alias(c)
                for c in ("station_2026", "station_2026_name", "station_mi_2026", "delta_mi")
            ]
        )
        .sort(["year", "dist_from_start"])
    )
    unmapped = xwalk.filter(pl.col("station_2026").is_null()).height
    if unmapped:
        logger.warning("%d historical stations left unmapped (> %.1f mi away)", unmapped, max_delta)
    return xwalk
