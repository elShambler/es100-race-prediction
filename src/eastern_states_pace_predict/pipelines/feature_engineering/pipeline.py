from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    build_features,
    detect_rank_based_outliers,
    detect_segment_pace_issues,
    flag_cutoff_violations,
    fix_timing_violations,
    visualize_runner_timing,
)


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
                func=detect_segment_pace_issues,
                inputs=["es_timing_corrected", "params:outlier_detection"],
                outputs="es_pace_checked",
                name="detect_segment_pace_issues_node",
            ),
            node(
                func=detect_rank_based_outliers,
                inputs=["es_pace_checked", "params:outlier_detection"],
                outputs="es_rank_checked",
                name="detect_rank_based_outliers_node",
            ),
            node(
                func=flag_cutoff_violations,
                inputs=["es_rank_checked", "params:outlier_detection"],
                outputs="es_outliers_flagged",
                name="flag_cutoff_violations_node",
            ),
            node(
                func=build_features,
                inputs="es_outliers_flagged",
                outputs="es_features",
                name="build_features_node",
            ),
        ]
    )
