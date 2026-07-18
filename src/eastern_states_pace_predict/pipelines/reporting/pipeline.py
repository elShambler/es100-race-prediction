from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_as_dashboard


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_as_dashboard,
                inputs="es_interval_features",
                outputs="es_as_dashboard",
                name="build__as_dashboard",
            ),
        ]
    )
