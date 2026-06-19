from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    preprocess_20162017_data,
    process_2025_data,
    combine_processed_data,
    join_finish_times,
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
                func=process_2025_data,
                inputs=[
                    "es_splits_2025",
                    "es_asinfo_historical",
                    "params:race_start_time",
                    "params:missing_time_marker",
                ],
                outputs="es_processed_2025",
                name="preprocess_2025_node",
            ),
            node(
                func=combine_processed_data,
                inputs=["es_processed_20162017", "es_processed_2025"],
                outputs="es_processed_combined",
                name="combine_processed_data_node",
            ),
            node(
                func=join_finish_times,
                inputs=["es_processed_combined", "es_finish_times"],
                outputs="es_splits_with_finish",
                name="join_finish_times_node",
            ),
            node(
                func=flag_negative_elapsed_times,
                inputs="es_processed_20162017",
                outputs="es_splits_20167_filtered",
                name="flag_timing_errors_node",
            ),
            node(
                func=visualize_elapsed_times_by_runner,
                inputs="es_processed_combined",
                outputs="es_timing_validation_plot",
                name="visualize_timing_errors_node",
            ),
        ]
    )
