from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_features


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_features,
                inputs="es_splits_with_finish",
                outputs="es_features",
                name="build_features_node",
            ),
        ]
    )
