from kedro.pipeline import Pipeline, node, pipeline
from .nodes import preprocess_20162017_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_20162017_data,
                inputs="es_splits_ultralive",
                outputs="preprocessed_ultralive",
                name="preprocess_ultralive_node",
            ),
        ]
    )
