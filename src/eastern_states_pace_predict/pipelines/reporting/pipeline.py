from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_as_dashboard, build_pacing_planner, plot_blog_interval_ratio


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_as_dashboard,
                inputs=[
                    "es_interval_features",
                    "es_interval_ratio",
                    "es_splits_all",
                    "es_course_route",
                    "es_course_stations",
                    "es_station_xwalk",
                    "params:reporting",
                ],
                outputs="es_as_dashboard",
                name="build__as_dashboard",
            ),
            node(
                func=build_pacing_planner,
                inputs=[
                    "es_interval_ratio",
                    "es_splits_all",
                    "es_station_xwalk",
                    "es_course_stations",
                    "params:reporting",
                ],
                outputs="es_pacing_planner",
                name="build__pacing_planner",
            ),
            node(
                func=plot_blog_interval_ratio,
                inputs="es_interval_ratio",
                outputs=["es_blog_figures", "es_blog_figures_svg"],
                name="plot__blog_interval_ratio",
            ),
        ]
    )
