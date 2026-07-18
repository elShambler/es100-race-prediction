from kedro.pipeline import Pipeline, node, pipeline

from .nodes import map_historical_stations, parse_course_gpx


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=parse_course_gpx,
                inputs=["es_course_gpx", "params:course"],
                outputs=["es_course_route", "es_course_stations"],
                name="parse__course_gpx",
            ),
            node(
                func=map_historical_stations,
                inputs=[
                    "es_course_stations",
                    "es_asinfo_historical",
                    "es_splits_all",
                    "params:course",
                ],
                outputs="es_station_xwalk",
                name="map__historical_stations",
            ),
        ]
    )
