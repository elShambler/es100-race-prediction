from kedro.pipeline import Pipeline, node, pipeline
from .nodes import process_2021_2025_splits


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=process_2021_2025_splits,
                inputs="es_splits_2021_2025",
                outputs="es_splits_2021_2025_long",
                name="wide_to_long__es_splits_2021_2025",
            ),
        ]
    )
