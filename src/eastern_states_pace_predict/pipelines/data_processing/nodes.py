import logging
import re
from datetime import datetime, timedelta

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)


def process_2021_2025_splits(splits: pl.DataFrame) -> pl.DataFrame:
    """Step 1: reshape wide splits (one row per runner) to long (one row per runner×AS)."""
    runner_cols = [
        "year",
        "bib_number",
        "OriginalOrder",
        "MaxTime",
        "OverallRank",
        "MaxAS",
        "FinishRank",
    ]

    def _unpivot_metric(df: pl.DataFrame, suffix: str, out_col: str) -> pl.DataFrame:
        cols = [c for c in df.columns if c.endswith(suffix)]
        return (
            df.select(runner_cols + cols)
            .unpivot(
                on=cols, index=runner_cols, variable_name="col", value_name=out_col
            )
            .with_columns(
                pl.concat_str(
                    [pl.lit("AS_"), pl.col("col").str.extract(r"^as(\d+)_")]
                ).alias("as_index")
            )
            .drop("col")
        )

    arr_tod = _unpivot_metric(
        splits.drop("as01_arr_rank2"), "_arr_tod", "as_check_in__tod"
    )
    dep_tod = _unpivot_metric(splits, "_dep_tod", "as_check_out__tod")
    arr_rank = _unpivot_metric(
        splits.drop("as01_arr_rank2").select(
            [c for c in splits.columns if not c.endswith("_rank2")]
        ),
        "_arr_rank",
        "arr_rank",
    )

    join_keys = runner_cols + ["as_index"]
    return (
        arr_tod.join(dep_tod, on=join_keys, how="left")
        .join(arr_rank, on=join_keys, how="left")
        .filter(pl.col("as_check_in__tod").is_not_null())
        .sort(["bib_number", "as_index"])
    )
