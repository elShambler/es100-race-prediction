from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    preprocess_20162017_data,
    flag_negative_elapsed_times,
    visualize_elapsed_times_by_runner,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_20162017_data,
                inputs="es_splits_20167",
                outputs="es_processed_20162017",
                name="preprocess_ultralive_node",
            ),
            node(
                func=flag_negative_elapsed_times,
                inputs="es_splits_20167",
                outputs="es_splits_20167_filtered",
                name="flag_timing_errors_node",
            ),
            node(
                func=visualize_elapsed_times_by_runner,
                inputs="es_splits_20167_filtered",
                outputs="es_timing_validation_plot",
                name="visualize_timing_errors_node",
            ),
        ]
    )
