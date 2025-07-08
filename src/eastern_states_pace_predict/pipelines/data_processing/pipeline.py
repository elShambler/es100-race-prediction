from kedro.pipeline import Pipeline, node, pipeline

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_ultralive_data,
                inputs="es_splits_ultralive",
                outputs="preprocessed_ultralive",
                name="preprocess_ultralive_node",
            ),
        ]
    )
