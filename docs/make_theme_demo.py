"""Demo of the Eastern States matplotlib theme (mpl_theme.py): recreates the
raincloud chart of elapsed time to reach each aid station, ES 2021.

Run from the repo root:  uv run python docs/make_theme_demo.py
"""

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from eastern_states_pace_predict import mpl_theme
from scipy.stats import gaussian_kde

YEAR = 2021
MIN_RUNNERS = 5

C = mpl_theme.apply()
rng = np.random.default_rng(7)

df = pl.read_csv(
    "data/02_intermediate/es_splits_2021_2025_processed.csv",
    schema_overrides={
        "as_check_in__elapsed__min": pl.Float64,
        "as_check_out__elapsed__min": pl.Float64,
    },
).filter(pl.col("year") == YEAR)

stations = (
    df.unique(subset=["as_index"])
    .sort("as_index")
    .select(["as_index", "as_name"])
    .rows()
)

fig, ax = plt.subplots(figsize=(12, 8))

for i, (as_index, as_name) in enumerate(stations):
    vals = (
        df.filter(pl.col("as_index") == as_index)["as_check_in__elapsed__min"]
        .drop_nulls()
        .to_numpy()
    )
    if len(vals) < MIN_RUNNERS:
        continue
    # jittered strip, left of center
    ax.scatter(
        i - 0.16 + rng.normal(0, 0.055, len(vals)), vals,
        s=6, color=C["dot"], alpha=0.85, edgecolors="none", zorder=3,
    )
    # thin full-range line at the violin's spine
    ax.plot([i + 0.12, i + 0.12], [vals.min(), vals.max()],
            color=C["range"], linewidth=0.8, zorder=2)
    # right-facing half violin
    kde = gaussian_kde(vals)
    ys = np.linspace(vals.min(), vals.max(), 120)
    dens = kde(ys)
    ax.fill_betweenx(
        ys, i + 0.12, i + 0.12 + dens / dens.max() * 0.34,
        color=C["green"], linewidth=0, zorder=2,
    )

ax.set_xticks(range(len(stations)),
              [name for _, name in stations], rotation=90)
ax.set_yticks(range(0, 44, 4))
ax.set_ylim(-1, 41)
ax.set_xlim(-0.7, len(stations) - 0.2)

mpl_theme.set_title(ax, "Elapsed time to reach aid stations", f"Eastern States {YEAR}")
mpl_theme.set_labels(ax, "Aid station", "Elapsed time [hrs]")

fig.savefig("docs/img/mpl_theme_demo.png")
print("saved docs/img/mpl_theme_demo.png")  # noqa: T201
