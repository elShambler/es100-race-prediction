from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    compute_cumulative_ratio,
    compute_interval_features,
    impute_missing_times,
    train_stoppage_model,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=train_stoppage_model,
                inputs=["es_splits_2021_2025_processed", "params:stoppage_model"],
                outputs=[
                    "es_stoppage_model",
                    "es_stoppage_model_metrics",
                    "es_stoppage_model_metrics_tracked",
                ],
                name="train__stoppage_model",
            ),
            node(
                func=impute_missing_times,
                inputs=[
                    "es_splits_2021_2025_processed",
                    "es_stoppage_model",
                    "params:stoppage_model",
                ],
                outputs="es_splits_2021_2025_imputed",
                name="impute__missing_times",
            ),
            node(
                func=compute_interval_features,
                inputs="es_splits_2021_2025_imputed",
                outputs="es_interval_features",
                name="features__interval_pace",
            ),
            node(
                func=compute_cumulative_ratio,
                inputs=["es_splits_all", "es_asinfo_historical", "es_station_xwalk"],
                outputs="es_cumulative_ratio",
                name="features__cumulative_ratio",
            ),
        ]
    )
