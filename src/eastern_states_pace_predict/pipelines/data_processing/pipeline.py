from kedro.pipeline import Pipeline, node, pipeline
from .nodes import enrich_2021_2025_splits, process_2021_2025_splits


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=process_2021_2025_splits,
                inputs="es_splits_2021_2025",
                outputs="es_splits_2021_2025_long",
                name="wide_to_long__es_splits_2021_2025",
            ),
            node(
                func=enrich_2021_2025_splits,
                inputs=[
                    "es_splits_2021_2025_long",
                    "es_race_meta",
                    "es_asinfo_historical",
                    "es_finish_historical",
                ],
                outputs="es_splits_2021_2025_processed",
                name="enrich__es_splits_2021_2025",
            ),
        ]
    )
