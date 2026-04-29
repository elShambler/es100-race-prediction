import logging

import polars as pl

logger = logging.getLogger(__name__)


def build_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build model features from the combined split + finish dataset.

    Input:  es_splits_with_finish  (data/02_intermediate)
    Output: es_features            (data/04_feature)
    """
    if hasattr(df, "collect"):
        df = df.collect()

    logger.info(f"Building features from {df.shape[0]} rows, {df.shape[1]} columns")

    # TODO: add feature engineering steps here

    return df
