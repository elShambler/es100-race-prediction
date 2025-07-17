from kedro.pipeline import Pipeline, node, pipeline

from .nodes import preprocess_20162017_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_20162017_data,
                inputs="es_splits_20167",
                outputs="es_processed_20162017",
                name="preprocess_ultralive_node",
            ),
        ]
    )
