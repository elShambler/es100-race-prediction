"""
Data quality tests for es_splits_all.

These tests load the pipeline's final output CSV and verify correctness against
known race results and specific runners. They are meant to catch regressions in
the enrichment logic (elapsed time computation, FinishRank assignment, midnight
rollover handling, etc.).

Run with:
    uv run pytest tests/test_data_quality.py -v
"""

import polars as pl
import pytest
from pathlib import Path

ROOT = Path(__file__).parent.parent
ALL_SPLITS = ROOT / "data/02_intermediate/es_splits_all.csv"
INTERVAL_FEATURES = ROOT / "data/04_feature/es_interval_features.csv"


@pytest.fixture(scope="module")
def df() -> pl.DataFrame:
    # The leading 2016-2017 rows have no check-out data, so schema inference
    # sees only nulls and types the column as String — pin it to Float64.
    return pl.read_csv(
        ALL_SPLITS,
        schema_overrides={"as_check_out__elapsed__min": pl.Float64},
    )


@pytest.fixture(scope="module")
def runners(df) -> pl.DataFrame:
    """One row per (year, bib) — runner-level view."""
    return df.unique(subset=["year", "bib"])


# ---------------------------------------------------------------------------
# Sanity: all expected years are present
# ---------------------------------------------------------------------------


def test_all_six_years_present(df):
    years = set(df["year"].drop_nulls().cast(int).unique().to_list())
    assert years == {2016, 2017, 2021, 2022, 2023, 2025}, f"Unexpected years: {years}"


# ---------------------------------------------------------------------------
# Known winner: Devon Olson, 2016, bib 157
#
# Devon won 2016 with an official rank of 1.  His name comes from finish_times
# (the raw 2016-2017 CSV only has names on a handful of rows).
# Bug history: before the flag_finish fix, runners who bypassed the AS_17
# checkpoint in 2016 showed FinishRank='DNF' despite having official results.
# ---------------------------------------------------------------------------


def test_devon_olson_present(df):
    rows = df.filter((pl.col("year") == 2016) & (pl.col("bib") == 157))
    assert rows.shape[0] > 0, "Devon Olson (bib 157, 2016) not found in es_splits_all"


def test_devon_olson_name_populated(df):
    rows = df.filter((pl.col("year") == 2016) & (pl.col("bib") == 157))
    assert rows["name"].null_count() == 0, "Some rows for Devon Olson have null name"
    names = rows["name"].unique().to_list()
    assert names == ["DEVON OLSON"], f"Unexpected name values: {names}"


def test_devon_olson_finish_rank_and_max_as(df):
    row = (
        df.filter((pl.col("year") == 2016) & (pl.col("bib") == 157))
        .select(["FinishRank", "MaxAS"])
        .unique()
    )
    assert (
        row["FinishRank"][0] == "1"
    ), f"Expected FinishRank='1', got {row['FinishRank'][0]!r}"
    assert (
        row["MaxAS"][0] == "FINISH"
    ), f"Expected MaxAS='FINISH', got {row['MaxAS'][0]!r}"


def test_devon_olson_elapsed_monotonic(df):
    rows = df.filter((pl.col("year") == 2016) & (pl.col("bib") == 157)).sort(
        "as_check_in__elapsed__min"
    )
    elapsed = rows["as_check_in__elapsed__min"].drop_nulls().to_list()
    for i in range(1, len(elapsed)):
        assert elapsed[i] > elapsed[i - 1], (
            f"Non-monotonic elapsed for Devon Olson at position {i}: "
            f"{elapsed[i-1]:.4f}h → {elapsed[i]:.4f}h"
        )


# ---------------------------------------------------------------------------
# Known winner: Ben Quatromoni, 2021, bib 335
# ---------------------------------------------------------------------------


def test_2021_winner_identity_and_rank(df):
    row = (
        df.filter((pl.col("year") == 2021) & (pl.col("bib") == 335))
        .select(["name", "FinishRank", "OverallRank", "MaxAS"])
        .unique()
    )
    assert row["name"][0] == "BEN QUATROMONI"
    assert row["FinishRank"][0] == "1"
    assert int(row["OverallRank"][0]) == 1
    assert row["MaxAS"][0] == "FINISH"


def test_2021_winner_elapsed_monotonic(df):
    rows = df.filter((pl.col("year") == 2021) & (pl.col("bib") == 335)).sort(
        "as_check_in__elapsed__min"
    )
    elapsed = rows["as_check_in__elapsed__min"].drop_nulls().to_list()
    for i in range(1, len(elapsed)):
        assert elapsed[i] > elapsed[i - 1], (
            f"Non-monotonic elapsed for bib 335 (2021) at position {i}: "
            f"{elapsed[i-1]:.4f}h → {elapsed[i]:.4f}h"
        )


def test_2021_winner_finish_elapsed_reasonable(df):
    """Ben Quatromoni's official finish was 22:31 — elapsed should be ~22.5h."""
    row = (
        df.filter((pl.col("year") == 2021) & (pl.col("bib") == 335))
        .select("finish_elapsed_hrs")
        .drop_nulls()
        .head(1)
    )
    hrs = row["finish_elapsed_hrs"][0]
    assert 22.0 < hrs < 23.0, f"Expected ~22.5h finish, got {hrs:.3f}h"


# ---------------------------------------------------------------------------
# Midnight rollover: bib 286, 2021
#
# This runner arrived at AS_13 at 01:19 TOD, which is after midnight on Aug 15.
# The pipeline must detect the backward TOD jump and add a day offset so the
# datetime lands on 2021-08-15 (not 2021-08-14).
# ---------------------------------------------------------------------------


def test_midnight_crosser_datetime_on_correct_day(df):
    as13 = df.filter(
        (pl.col("year") == 2021)
        & (pl.col("bib") == 286)
        & (pl.col("as_index") == "AS_13")
    )
    assert as13.shape[0] == 1, "Expected exactly one AS_13 row for bib 286 (2021)"
    dt = as13["as_check_in__tod__datetime"][0]
    assert "2021-08-15" in str(
        dt
    ), f"AS_13 for bib 286 (2021) should be on Aug 15, got datetime: {dt!r}"


def test_midnight_crosser_elapsed_reasonable(df):
    """AS_13 arrival should be ~20.3h elapsed (01:19 on day 2, race started 05:00 day 1)."""
    as13 = df.filter(
        (pl.col("year") == 2021)
        & (pl.col("bib") == 286)
        & (pl.col("as_index") == "AS_13")
    )
    elapsed = as13["as_check_in__elapsed__min"][0]
    assert (
        20.0 < elapsed < 21.0
    ), f"Expected ~20.3h at AS_13 for bib 286 (2021), got {elapsed:.4f}h"


def test_midnight_crosser_elapsed_monotonic(df):
    rows = df.filter((pl.col("year") == 2021) & (pl.col("bib") == 286)).sort(
        "as_check_in__elapsed__min"
    )
    elapsed = rows["as_check_in__elapsed__min"].drop_nulls().to_list()
    for i in range(1, len(elapsed)):
        assert elapsed[i] > elapsed[i - 1], (
            f"Non-monotonic elapsed for midnight-crossing bib 286 (2021) at position {i}: "
            f"{elapsed[i-1]:.4f}h → {elapsed[i]:.4f}h"
        )


# ---------------------------------------------------------------------------
# Bug regression: FinishRank must agree with official_rank
#
# Two separate issues were found:
#   1. 2016-2017: runners who bypassed the AS_17 checkpoint had flag_finish=False
#      despite having an official_rank in finish_times.
#   2. 2025: the finish checkpoint (AS_16) was absent from the split data entirely,
#      so all 133 finishers showed as DNF.
#
# The fix: use official_rank.is_not_null() as the has_finish indicator in both
# process_2016_2017_splits and enrich_2021_2025_splits.
# ---------------------------------------------------------------------------


def test_no_finisher_marked_as_dnf(runners):
    """Any runner with an official_rank must have FinishRank != 'DNF'."""
    bad = runners.filter(
        (pl.col("official_rank").is_not_null()) & (pl.col("FinishRank") == "DNF")
    )
    if bad.shape[0] > 0:
        details = bad.select(["year", "bib", "name", "MaxAS", "official_rank"]).sort(
            ["year", "bib"]
        )
        pytest.fail(
            f"{bad.shape[0]} runner(s) have an official_rank but FinishRank='DNF':\n{details}"
        )


def test_2016_charles_ardan_finisher(df):
    """Charles Ardan (bib 7, 2016) skipped AS_17 and only has an AS_18 row — must show as finisher."""
    row = (
        df.filter((pl.col("year") == 2016) & (pl.col("bib") == 7))
        .select(["FinishRank", "MaxAS"])
        .unique()
    )
    assert (
        row["FinishRank"][0] == "31"
    ), f"Expected FinishRank='31', got {row['FinishRank'][0]!r}"
    assert row["MaxAS"][0] == "FINISH"


def test_2025_finishers_have_correct_finish_rank(runners):
    """All 133 finishers in 2025 must have FinishRank != 'DNF' and MaxAS='FINISH'."""
    finishers_2025 = runners.filter(
        (pl.col("year") == 2025) & (pl.col("official_rank").is_not_null())
    )
    dnf_finishers = finishers_2025.filter(pl.col("FinishRank") == "DNF")
    assert (
        dnf_finishers.shape[0] == 0
    ), f"{dnf_finishers.shape[0]} 2025 finishers incorrectly marked DNF"
    wrong_max_as = finishers_2025.filter(pl.col("MaxAS") != "FINISH")
    assert (
        wrong_max_as.shape[0] == 0
    ), f"{wrong_max_as.shape[0]} 2025 finishers have MaxAS != 'FINISH'"


# ---------------------------------------------------------------------------
# Global elapsed time invariants
# ---------------------------------------------------------------------------


def test_no_negative_elapsed(df):
    """No elapsed time should be negative — would indicate a pre-race check-in time
    or a date error in the source data (as was the case in the original 2016-2017 file
    for 28 midnight-crossing rows in 2017 that have since been corrected)."""
    neg = df.filter(pl.col("as_check_in__elapsed__min") < 0)
    assert neg.shape[0] == 0, (
        f"{neg.shape[0]} rows have negative elapsed time:\n"
        f"{neg.select(['year','bib','as_index','as_check_in__elapsed__min']).sort(['year','bib'])}"
    )


def test_elapsed_within_36h_cutoff(df):
    """The race has a hard 36-hour cutoff — no split should exceed it."""
    over = df.filter(pl.col("as_check_in__elapsed__min") > 36)
    if over.shape[0] > 0:
        worst = (
            over.sort("as_check_in__elapsed__min", descending=True)
            .select(["year", "bib", "name", "as_index", "as_check_in__elapsed__min"])
            .head(5)
        )
        pytest.fail(f"{over.shape[0]} rows exceed the 36h cutoff:\n{worst}")


# ---------------------------------------------------------------------------
# Bug regression: departure (check-out) times must survive the wide-to-long join
#
# The raw 2021-2025 wide CSV has dep_tod cells for every year, but the unpivot
# join originally used runner-level columns (MaxTime, OverallRank, …) as join
# keys. Those columns are entirely null for 2022/2023/2025, and Polars joins
# never match null keys, so every departure time for those years was silently
# dropped. A second filter discarded 2025 rows that had only a departure time
# (2025 recorded mostly departures: 2,679 dep cells vs 1,219 arr cells).
# ---------------------------------------------------------------------------


def test_checkout_times_survive_for_all_years(df):
    """Non-null check-out elapsed counts must roughly match the raw dep_tod cells."""
    expected_min = {2021: 1600, 2022: 2500, 2023: 2100, 2025: 2600}
    counts = (
        df.filter(pl.col("as_check_out__elapsed__min").is_not_null())
        .group_by("year")
        .len()
    )
    by_year = dict(zip(counts["year"].to_list(), counts["len"].to_list()))
    for year, minimum in expected_min.items():
        got = by_year.get(year, 0)
        assert got >= minimum, (
            f"{year}: only {got} rows with check-out elapsed, expected >= {minimum} "
            "— departure times are being dropped again"
        )


def test_2025_departure_only_rows_present(df):
    """2025 station visits with only a departure time must not be filtered out."""
    n_2025 = df.filter(pl.col("year") == 2025).shape[0]
    assert n_2025 >= 2900, (
        f"2025 has only {n_2025} rows — departure-only station visits are being dropped "
        "(expected ~2,962; the arr-only filter regression gave 1,219)"
    )


def test_checkout_elapsed_within_race_bounds(df):
    """Check-out elapsed must be non-negative and within the 36h cutoff."""
    bad = df.filter(
        (pl.col("as_check_out__elapsed__min") < 0)
        | (pl.col("as_check_out__elapsed__min") > 36)
    )
    if bad.shape[0] > 0:
        worst = bad.select(
            ["year", "bib", "as_index", "as_check_out__elapsed__min"]
        ).head(10)
        pytest.fail(f"{bad.shape[0]} check-out rows outside [0, 36]h:\n{worst}")


def test_stoppage_inversions_are_small_and_rare(df):
    """Check-out before check-in only happens for known raw-data typos: small
    (< 1h) and rare (~24 rows). A jump in count or size means the midnight/AM-PM
    correction regressed."""
    both = df.filter(
        pl.col("as_check_in__elapsed__min").is_not_null()
        & pl.col("as_check_out__elapsed__min").is_not_null()
    ).with_columns(
        (
            (pl.col("as_check_out__elapsed__min") - pl.col("as_check_in__elapsed__min"))
            * 60
        ).alias("stoppage_min")
    )
    inversions = both.filter(pl.col("stoppage_min") < 0)
    assert (
        inversions.shape[0] <= 30
    ), f"{inversions.shape[0]} rows have check-out before check-in (expected <= 30)"
    big = inversions.filter(pl.col("stoppage_min") < -60)
    if big.shape[0] > 0:
        pytest.fail(
            f"{big.shape[0]} rows have check-out more than 1h before check-in:\n"
            f"{big.select(['year','bib','as_index','stoppage_min'])}"
        )


def test_every_2021_2025_row_has_a_time(df):
    """Every surviving 2021-2025 row must have at least one elapsed value."""
    bad = df.filter(
        pl.col("year").is_in([2021, 2022, 2023, 2025])
        & pl.col("as_check_in__elapsed__min").is_null()
        & pl.col("as_check_out__elapsed__min").is_null()
    )
    assert bad.shape[0] == 0, f"{bad.shape[0]} rows have neither check-in nor check-out"


def test_elapsed_monotonic_for_all_runners(df):
    """
    For every runner, elapsed time must strictly increase across their aid stations.
    Violations indicate a midnight rollover bug or an AM/PM data-entry error that
    wasn't caught by the correction logic.
    """
    failures = []
    for group in df.sort(["year", "bib", "as_check_in__elapsed__min"]).partition_by(
        ["year", "bib"], maintain_order=True
    ):
        elapsed = group["as_check_in__elapsed__min"].drop_nulls().to_list()
        for i in range(1, len(elapsed)):
            # Allow ties (e.g., 2016 bib 216 AS_14/AS_15 both recorded 07:55 in the
            # source) — flag only strict decreases, which indicate a real pipeline bug.
            if elapsed[i] < elapsed[i - 1]:
                yr = group["year"][0]
                bib = group["bib"][0]
                failures.append(
                    f"year={int(yr)} bib={int(bib)}: {elapsed[i-1]:.3f}h → {elapsed[i]:.3f}h"
                )
                break  # one failure per runner is enough
    if failures:
        pytest.fail(
            f"{len(failures)} runner(s) have non-monotonic elapsed times:\n"
            + "\n".join(failures[:15])
        )


# ---------------------------------------------------------------------------
# Feature engineering: stoppage imputation and interval pace sanity
#
# These read the feature_engineering pipeline output. Run
# `uv run kedro run` first if data/04_feature/es_interval_features.csv is stale.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def features() -> pl.DataFrame:
    return pl.read_csv(
        INTERVAL_FEATURES,
        schema_overrides={
            "as_check_in__elapsed__min": pl.Float64,
            "as_check_out__elapsed__min": pl.Float64,
        },
    )


def test_no_missing_check_in_after_imputation(features):
    """Every 2021-2025 station visit must have a check-in time post-imputation."""
    missing = features.filter(pl.col("as_check_in__elapsed__min").is_null())
    assert (
        missing.shape[0] == 0
    ), f"{missing.shape[0]} rows still have null check-in after imputation"


def test_imputed_check_in_not_after_check_out(features):
    """Imputed check-ins must be <= their departure, except for the handful of
    rows whose raw departure contradicts the previous station (the monotonic
    clamp wins there and the interval is nulled downstream)."""
    bad = features.filter(
        pl.col("check_in_imputed")
        & (pl.col("as_check_in__elapsed__min") > pl.col("as_check_out__elapsed__min"))
    )
    assert bad.shape[0] <= 3, (
        f"{bad.shape[0]} imputed check-ins exceed their check-out (expected <= 3):\n"
        f"{bad.select(['year','bib','as_index'])}"
    )


def test_stoppage_times_plausible(features):
    """Stoppage must be non-negative-ish and under 3h (longest observed ~2.5h)."""
    st = features.filter(pl.col("as_stoppage_time_min").is_not_null())
    too_big = st.filter(pl.col("as_stoppage_time_min") > 180)
    assert too_big.shape[0] == 0, (
        f"{too_big.shape[0]} stoppages exceed 3h — departure correction regressed:\n"
        f"{too_big.select(['year','bib','as_index','as_stoppage_time_min']).head(10)}"
    )


def test_interval_paces_positive_and_plausible(features):
    """All computed paces positive; the bulk must be in a plausible range."""
    pace = features["as_interval_pace"].drop_nulls()
    assert (pace <= 0).sum() == 0, "non-positive interval paces present"
    outside = pace.filter((pace < 6) | (pace > 60))
    frac = outside.len() / pace.len()
    assert (
        frac < 0.01
    ), f"{outside.len()} of {pace.len()} paces outside 6-60 min/mi ({frac:.1%})"


def test_pace_ratio_centred_near_one(features):
    """Per-runner median interval pace ratio should sit a bit below 1.0
    (moving pace excludes stoppage time, overall pace includes it)."""
    med = (
        features.group_by(["year", "bib"])
        .agg(pl.col("as_interval_pace_ratio").median().alias("m"))["m"]
        .drop_nulls()
    )
    assert (
        0.8 < med.median() < 1.05
    ), f"per-runner median pace ratio {med.median():.3f} outside [0.8, 1.05]"
