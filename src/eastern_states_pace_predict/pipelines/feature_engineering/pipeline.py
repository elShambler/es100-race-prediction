from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_features, fix_timing_violations, visualize_runner_timing


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=fix_timing_violations,
                inputs="es_splits_with_finish",
                outputs="es_timing_corrected",
                name="fix_timing_violations_node",
            ),
            node(
                func=visualize_runner_timing,
                inputs="es_timing_corrected",
                outputs="es_runner_timing_plot",
                name="visualize_runner_timing_node",
            ),
            node(
                func=build_features,
                inputs="es_timing_corrected",
                outputs="es_features",
                name="build_features_node",
            ),
        ]
    )
