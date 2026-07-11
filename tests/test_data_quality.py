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


@pytest.fixture(scope="module")
def df() -> pl.DataFrame:
    return pl.read_csv(ALL_SPLITS)


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
    row = df.filter((pl.col("year") == 2016) & (pl.col("bib") == 157)).select(["FinishRank", "MaxAS"]).unique()
    assert row["FinishRank"][0] == "1", f"Expected FinishRank='1', got {row['FinishRank'][0]!r}"
    assert row["MaxAS"][0] == "FINISH", f"Expected MaxAS='FINISH', got {row['MaxAS'][0]!r}"


def test_devon_olson_elapsed_monotonic(df):
    rows = df.filter(
        (pl.col("year") == 2016) & (pl.col("bib") == 157)
    ).sort("as_check_in__elapsed__min")
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
    row = df.filter((pl.col("year") == 2021) & (pl.col("bib") == 335)).select(["name", "FinishRank", "OverallRank", "MaxAS"]).unique()
    assert row["name"][0] == "BEN QUATROMONI"
    assert row["FinishRank"][0] == "1"
    assert int(row["OverallRank"][0]) == 1
    assert row["MaxAS"][0] == "FINISH"


def test_2021_winner_elapsed_monotonic(df):
    rows = df.filter(
        (pl.col("year") == 2021) & (pl.col("bib") == 335)
    ).sort("as_check_in__elapsed__min")
    elapsed = rows["as_check_in__elapsed__min"].drop_nulls().to_list()
    for i in range(1, len(elapsed)):
        assert elapsed[i] > elapsed[i - 1], (
            f"Non-monotonic elapsed for bib 335 (2021) at position {i}: "
            f"{elapsed[i-1]:.4f}h → {elapsed[i]:.4f}h"
        )


def test_2021_winner_finish_elapsed_reasonable(df):
    """Ben Quatromoni's official finish was 22:31 — elapsed should be ~22.5h."""
    row = df.filter(
        (pl.col("year") == 2021) & (pl.col("bib") == 335)
    ).select("finish_elapsed_hrs").drop_nulls().head(1)
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
        (pl.col("year") == 2021) & (pl.col("bib") == 286) & (pl.col("as_index") == "AS_13")
    )
    assert as13.shape[0] == 1, "Expected exactly one AS_13 row for bib 286 (2021)"
    dt = as13["as_check_in__tod__datetime"][0]
    assert "2021-08-15" in str(dt), (
        f"AS_13 for bib 286 (2021) should be on Aug 15, got datetime: {dt!r}"
    )


def test_midnight_crosser_elapsed_reasonable(df):
    """AS_13 arrival should be ~20.3h elapsed (01:19 on day 2, race started 05:00 day 1)."""
    as13 = df.filter(
        (pl.col("year") == 2021) & (pl.col("bib") == 286) & (pl.col("as_index") == "AS_13")
    )
    elapsed = as13["as_check_in__elapsed__min"][0]
    assert 20.0 < elapsed < 21.0, f"Expected ~20.3h at AS_13 for bib 286 (2021), got {elapsed:.4f}h"


def test_midnight_crosser_elapsed_monotonic(df):
    rows = df.filter(
        (pl.col("year") == 2021) & (pl.col("bib") == 286)
    ).sort("as_check_in__elapsed__min")
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
        details = bad.select(["year", "bib", "name", "MaxAS", "official_rank"]).sort(["year", "bib"])
        pytest.fail(
            f"{bad.shape[0]} runner(s) have an official_rank but FinishRank='DNF':\n{details}"
        )


def test_2016_charles_ardan_finisher(df):
    """Charles Ardan (bib 7, 2016) skipped AS_17 and only has an AS_18 row — must show as finisher."""
    row = df.filter((pl.col("year") == 2016) & (pl.col("bib") == 7)).select(["FinishRank", "MaxAS"]).unique()
    assert row["FinishRank"][0] == "31", f"Expected FinishRank='31', got {row['FinishRank'][0]!r}"
    assert row["MaxAS"][0] == "FINISH"


def test_2025_finishers_have_correct_finish_rank(runners):
    """All 133 finishers in 2025 must have FinishRank != 'DNF' and MaxAS='FINISH'."""
    finishers_2025 = runners.filter(
        (pl.col("year") == 2025) & (pl.col("official_rank").is_not_null())
    )
    dnf_finishers = finishers_2025.filter(pl.col("FinishRank") == "DNF")
    assert dnf_finishers.shape[0] == 0, (
        f"{dnf_finishers.shape[0]} 2025 finishers incorrectly marked DNF"
    )
    wrong_max_as = finishers_2025.filter(pl.col("MaxAS") != "FINISH")
    assert wrong_max_as.shape[0] == 0, (
        f"{wrong_max_as.shape[0]} 2025 finishers have MaxAS != 'FINISH'"
    )


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
        worst = over.sort("as_check_in__elapsed__min", descending=True).select(
            ["year", "bib", "name", "as_index", "as_check_in__elapsed__min"]
        ).head(5)
        pytest.fail(f"{over.shape[0]} rows exceed the 36h cutoff:\n{worst}")


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
