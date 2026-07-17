import logging

import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

# Full course distance in miles (Little Pine Creek SP finish, all years).
FINISH_DIST_MI = 103.1
# Rows at/after this distance are the finish — there is no departure there.
FINISH_CUTOFF_MI = 103.0


def _stoppage_features(splits: pl.DataFrame) -> pl.DataFrame:
    """Add the model feature columns and the stoppage target to the splits.

    All time columns named *__elapsed__min hold decimal HOURS (legacy naming);
    the stoppage target and predictions are expressed in minutes.
    """
    df = splits.with_columns(
        pl.col("as_index").str.extract(r"(\d+)$").cast(pl.Int32).alias("as_num"),
        (pl.col("FinishRank") != "DNF").cast(pl.Int8).alias("is_finisher"),
        pl.col("age").cast(pl.Float64, strict=False).alias("age"),
        pl.when(pl.col("gender").str.to_uppercase().str.starts_with("M"))
        .then(0.0)
        .when(pl.col("gender").str.to_uppercase().str.starts_with("F"))
        .then(1.0)
        .otherwise(None)
        .alias("gender_code"),
        pl.coalesce("as_check_in__elapsed__min", "as_check_out__elapsed__min").alias(
            "elapsed_hrs"
        ),
        (pl.col("OverallRank") / pl.col("OverallRank").max().over("year")).alias(
            "overall_rank_pct"
        ),
    )

    # Runner-level overall pace: finishers over the full course, DNFs over the
    # distance they actually covered (MaxTime already holds the right hours
    # for both cases).
    per_runner = df.group_by(["year", "bib"]).agg(
        pl.col("as_dist_from_start").max().alias("_max_dist"),
    )
    df = (
        df.join(per_runner, on=["year", "bib"], how="left")
        .with_columns(
            pl.when(pl.col("is_finisher") == 1)
            .then(pl.col("MaxTime") * 60 / FINISH_DIST_MI)
            .otherwise(pl.col("MaxTime") * 60 / pl.col("_max_dist"))
            .alias("overall_pace_min_per_mi")
        )
        .drop("_max_dist")
    )

    return df.with_columns(
        (
            (pl.col("as_check_out__elapsed__min") - pl.col("as_check_in__elapsed__min"))
            * 60
        ).alias("as_stoppage_time_min")
    )


def _to_matrix(df: pl.DataFrame, features: list[str]):
    """Feature matrix as float numpy array; polars nulls become NaN, which
    HistGradientBoostingRegressor handles natively."""
    return df.select([pl.col(f).cast(pl.Float64) for f in features]).to_numpy()


def train_stoppage_model(
    splits: pl.DataFrame, params: dict
) -> tuple[HistGradientBoostingRegressor, dict, dict]:
    """Train a regressor for time spent inside an aid station (minutes).

    Trains on rows where both check-in and check-out are observed (mostly
    2021-2023), validates against a naive per-station median baseline, then
    refits on all available rows.

    The hyperparameters, feature list, and validation config are logged to the
    MLflow run automatically by the kedro-mlflow hook (all `params:` inputs);
    the third output carries the numeric metrics to the run's metrics panel.

    Inputs: es_splits_2021_2025_processed, params:stoppage_model
    Outputs: es_stoppage_model, es_stoppage_model_metrics,
             es_stoppage_model_metrics_tracked
    """
    features = params["features"]
    fdf = _stoppage_features(splits)

    train_df = fdf.filter(
        pl.col("as_stoppage_time_min").is_not_null()
        & (pl.col("as_stoppage_time_min") >= 0)
        & (pl.col("as_dist_from_start") < FINISH_CUTOFF_MI)
    )

    val_cfg = params["validation"]
    if val_cfg["strategy"] == "year_holdout":
        holdout = val_cfg["holdout_year"]
        fit_df = train_df.filter(pl.col("year") != holdout)
        val_df = train_df.filter(pl.col("year") == holdout)
    else:
        idx_train, idx_val = train_test_split(
            range(train_df.height),
            test_size=val_cfg["test_size"],
            random_state=val_cfg["random_state"],
        )
        fit_df = train_df[list(idx_train)]
        val_df = train_df[list(idx_val)]

    model = HistGradientBoostingRegressor(**params["model"])
    model.fit(
        _to_matrix(fit_df, features),
        fit_df["as_stoppage_time_min"].to_numpy(),
    )
    val_pred = model.predict(_to_matrix(val_df, features))

    # Naive baseline: median stoppage per aid station from the fit split.
    global_median = fit_df["as_stoppage_time_min"].median()
    medians = fit_df.group_by("as_num").agg(
        pl.col("as_stoppage_time_min").median().alias("_naive")
    )
    naive_pred = (
        val_df.join(medians, on="as_num", how="left")
        .with_columns(pl.col("_naive").fill_null(global_median))["_naive"]
        .to_numpy()
    )

    y_val = val_df["as_stoppage_time_min"].to_numpy()
    metrics = {
        "strategy": val_cfg["strategy"],
        "holdout_year": val_cfg.get("holdout_year"),
        "n_fit": fit_df.height,
        "n_val": val_df.height,
        "mae_model_min": float(mean_absolute_error(y_val, val_pred)),
        "mae_naive_median_by_as_min": float(mean_absolute_error(y_val, naive_pred)),
        "median_stoppage_min": float(global_median),
    }
    logger.info("Stoppage model validation: %s", metrics)

    # Refit on everything so the shipped model uses all observed stoppages.
    model.fit(
        _to_matrix(train_df, features),
        train_df["as_stoppage_time_min"].to_numpy(),
    )

    # Numeric metrics in the {name: {value, step}} shape that
    # MlflowMetricsHistoryDataset logs to the run's metrics panel. Validation
    # config (strategy, holdout year) is a parameter, not a metric.
    metrics_tracked = {
        k: {"value": float(v), "step": 0}
        for k, v in metrics.items()
        if isinstance(v, int | float) and k != "holdout_year"
    }
    return model, metrics, metrics_tracked


def impute_missing_times(
    splits: pl.DataFrame,
    model: HistGradientBoostingRegressor,
    params: dict,
) -> pl.DataFrame:
    """Fill missing check-in/check-out elapsed hours using predicted stoppage.

    Rows with only a departure (most of 2025) get check-in = departure minus
    predicted stoppage; rows with only an arrival (non-finish stations) get
    check-out = arrival plus predicted stoppage. Imputed check-ins are clamped
    between the previous station's time and the departure so per-runner
    elapsed times stay monotonic.

    Inputs: es_splits_2021_2025_processed, es_stoppage_model,
            params:stoppage_model
    Outputs: es_splits_2021_2025_imputed
    """
    features = params["features"]
    pred_cfg = params["prediction"]
    fdf = _stoppage_features(splits).sort(["year", "bib", "as_index"])

    needs_check_in = (
        pl.col("as_check_in__elapsed__min").is_null()
        & pl.col("as_check_out__elapsed__min").is_not_null()
    )
    needs_check_out = (
        pl.col("as_check_out__elapsed__min").is_null()
        & pl.col("as_check_in__elapsed__min").is_not_null()
        & (pl.col("as_dist_from_start") < FINISH_CUTOFF_MI)
    )
    fdf = fdf.with_columns(
        needs_check_in.alias("check_in_imputed"),
        needs_check_out.alias("check_out_imputed"),
    )

    needs = fdf.filter(pl.col("check_in_imputed") | pl.col("check_out_imputed"))
    pred = model.predict(_to_matrix(needs, features)).clip(
        pred_cfg["min_stoppage_min"], pred_cfg["max_stoppage_min"]
    )
    needs = needs.select(["year", "bib", "as_index"]).with_columns(
        pl.Series("_pred_stoppage_min", pred)
    )

    df = (
        fdf.join(needs, on=["year", "bib", "as_index"], how="left")
        .with_columns(
            pl.coalesce("as_check_in__elapsed__min", "as_check_out__elapsed__min")
            .shift(1)
            .over(["year", "bib"])
            .alias("_prev_elapsed")
        )
        .with_columns(
            # check-in = departure − predicted stoppage, clamped to
            # [previous station's time (or race start), departure]
            pl.when(pl.col("check_in_imputed"))
            .then(
                (
                    pl.col("as_check_out__elapsed__min")
                    - pl.col("_pred_stoppage_min") / 60
                ).clip(
                    lower_bound=pl.col("_prev_elapsed").fill_null(0.0),
                    upper_bound=pl.col("as_check_out__elapsed__min"),
                )
            )
            .otherwise(pl.col("as_check_in__elapsed__min"))
            .alias("as_check_in__elapsed__min"),
            # check-out = arrival + predicted stoppage
            pl.when(pl.col("check_out_imputed"))
            .then(
                pl.col("as_check_in__elapsed__min") + pl.col("_pred_stoppage_min") / 60
            )
            .otherwise(pl.col("as_check_out__elapsed__min"))
            .alias("as_check_out__elapsed__min"),
        )
        .with_columns(
            # Unified stoppage: observed where both times were recorded,
            # predicted where one side was imputed, null at the finish.
            pl.when(pl.col("check_in_imputed") | pl.col("check_out_imputed"))
            .then(pl.col("_pred_stoppage_min"))
            .otherwise(pl.col("as_stoppage_time_min"))
            .alias("as_stoppage_time_min"),
            (pl.col("check_in_imputed") | pl.col("check_out_imputed")).alias(
                "stoppage_imputed"
            ),
        )
        .drop(["_pred_stoppage_min", "_prev_elapsed"])
    )

    n_in = df["check_in_imputed"].sum()
    n_out = df["check_out_imputed"].sum()
    logger.info("Imputed %d check-in and %d check-out times", n_in, n_out)
    return df


def compute_interval_features(imputed: pl.DataFrame) -> pl.DataFrame:
    """Per runner × aid-station interval pace features.

    Interval pace (min/mile) runs from the previous station's departure (race
    start for the first station) to this station's arrival. Overall pace uses
    the official finish time for finishers and the furthest-point elapsed time
    for DNFs. The ratio interval/overall is 1.0 at overall pace, >1 slower.

    Inputs: es_splits_2021_2025_imputed
    Outputs: es_interval_features
    """
    df = imputed.sort(["year", "bib", "as_index"]).with_columns(
        pl.coalesce("as_check_out__elapsed__min", "as_check_in__elapsed__min")
        .shift(1)
        .fill_null(0.0)
        .over(["year", "bib"])
        .alias("_prev_out_hrs"),
        pl.col("as_dist_from_start")
        .shift(1)
        .fill_null(0.0)
        .over(["year", "bib"])
        .alias("_prev_dist"),
        pl.col("as_num")
        .shift(1)
        .fill_null(0)
        .over(["year", "bib"])
        .alias("_prev_as_num"),
    )

    df = df.with_columns(
        (pl.col("as_dist_from_start") - pl.col("_prev_dist")).alias("interval_dist_mi"),
        ((pl.col("as_check_in__elapsed__min") - pl.col("_prev_out_hrs")) * 60).alias(
            "interval_time_min"
        ),
        ((pl.col("as_num") - pl.col("_prev_as_num")) > 1).alias("spans_missing_as"),
    )

    n_bad = df.filter(
        (pl.col("interval_time_min") <= 0) | (pl.col("interval_dist_mi") <= 0)
    ).height
    if n_bad:
        logger.warning(
            "%d intervals have non-positive time or distance; pace set to null",
            n_bad,
        )

    df = df.with_columns(
        pl.when((pl.col("interval_dist_mi") > 0) & (pl.col("interval_time_min") > 0))
        .then(pl.col("interval_time_min") / pl.col("interval_dist_mi"))
        .otherwise(None)
        .alias("as_interval_pace")
    )

    # Overall pace per runner: official finish time over the full course for
    # finishers; furthest-point elapsed over distance covered for DNFs.
    per_runner = (
        df.group_by(["year", "bib"])
        .agg(
            (pl.col("is_finisher").first() == 1).alias("_fin"),
            pl.max_horizontal("as_check_in__elapsed__min", "as_check_out__elapsed__min")
            .max()
            .alias("_max_elapsed"),
            pl.col("as_dist_from_start").max().alias("_max_dist"),
            pl.col("finish_elapsed_hrs").first().alias("_finish_hrs"),
        )
        .with_columns(
            pl.when(pl.col("_fin"))
            .then(pl.col("_finish_hrs") * 60 / FINISH_DIST_MI)
            .otherwise(pl.col("_max_elapsed") * 60 / pl.col("_max_dist"))
            .alias("overall_pace_min_per_mi")
        )
        .select(["year", "bib", "overall_pace_min_per_mi"])
    )

    df = (
        df.drop("overall_pace_min_per_mi")
        .join(per_runner, on=["year", "bib"], how="left")
        .with_columns(
            (pl.col("as_interval_pace") / pl.col("overall_pace_min_per_mi")).alias(
                "as_interval_pace_ratio"
            ),
            pl.col("is_finisher").cast(pl.Boolean),
        )
    )

    return df.select(
        [
            "year",
            "bib",
            "name",
            "gender",
            "age",
            "is_finisher",
            "as_index",
            "as_name",
            "as_num",
            "as_dist_from_start",
            "interval_dist_mi",
            "spans_missing_as",
            "as_check_in__elapsed__min",
            "as_check_out__elapsed__min",
            "check_in_imputed",
            "check_out_imputed",
            "as_stoppage_time_min",
            "stoppage_imputed",
            "interval_time_min",
            "as_interval_pace",
            "overall_pace_min_per_mi",
            "as_interval_pace_ratio",
            "MaxAS",
            "FinishRank",
            "OverallRank",
        ]
    )
