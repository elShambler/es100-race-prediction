"""Regenerate docs/img/stoppage_model_eval.png — predicted vs actual on the
2023 holdout, plus per-station MAE vs the naive baseline.

Run from the repo root:  uv run python <this file>
"""

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import yaml
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

from eastern_states_pace_predict.pipelines.feature_engineering.nodes import (
    FINISH_CUTOFF_MI,
    _stoppage_features,
    _to_matrix,
)

# Palette (dataviz reference, light mode)
BLUE = "#2a78d6"
DEEMPH = "#c9c8c1"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
SURFACE = "#fcfcfb"

params = yaml.safe_load(open("conf/base/parameters_feature_engineering.yml"))[
    "stoppage_model"
]
features = params["features"]
holdout = params["validation"]["holdout_year"]

splits = pl.read_csv(
    "data/02_intermediate/es_splits_2021_2025_processed.csv",
    schema_overrides={
        "as_check_in__elapsed__min": pl.Float64,
        "as_check_out__elapsed__min": pl.Float64,
    },
)
fdf = _stoppage_features(splits)
train_df = fdf.filter(
    pl.col("as_stoppage_time_min").is_not_null()
    & (pl.col("as_stoppage_time_min") >= 0)
    & (pl.col("as_dist_from_start") < FINISH_CUTOFF_MI)
)
fit_df = train_df.filter(pl.col("year") != holdout)
val_df = train_df.filter(pl.col("year") == holdout)

model = HistGradientBoostingRegressor(**params["model"])
model.fit(_to_matrix(fit_df, features), fit_df["as_stoppage_time_min"].to_numpy())
pred = model.predict(_to_matrix(val_df, features))
actual = val_df["as_stoppage_time_min"].to_numpy()
mae = mean_absolute_error(actual, pred)

# Naive baseline: per-station median from the fit split
gm = fit_df["as_stoppage_time_min"].median()
med = dict(
    fit_df.group_by("as_num")
    .agg(pl.col("as_stoppage_time_min").median())
    .iter_rows()
)
naive = np.array([med.get(a, gm) for a in val_df["as_num"].to_list()])
mae_naive = mean_absolute_error(actual, naive)

fig, (ax1, ax2) = plt.subplots(
    1, 2, figsize=(11, 4.4), dpi=160, facecolor=SURFACE,
    gridspec_kw={"width_ratios": [1, 1.3], "wspace": 0.28},
)
for ax in (ax1, ax2):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=8.5)
    ax.grid(True, color=GRID, linewidth=0.7)
    ax.set_axisbelow(True)

# Panel A — predicted vs actual (axes clipped at the holdout's 99th percentile)
lim = float(np.quantile(actual, 0.99))
ax1.scatter(actual, pred, s=7, alpha=0.22, color=BLUE, edgecolors="none")
ax1.plot([0, lim], [0, lim], color=MUTED, linewidth=1)
ax1.set_xlim(0, lim)
ax1.set_ylim(0, lim)
ax1.set_xlabel("Actual stop (min)", color=INK2, fontsize=9.5)
ax1.set_ylabel("Predicted stop (min)", color=INK2, fontsize=9.5)
ax1.set_title(
    f"Predicted vs actual — {holdout} holdout (n={len(actual):,})",
    color="#0b0b0b", fontsize=10.5, loc="left",
)
ax1.annotate(
    f"MAE {mae:.2f} min\n(naive {mae_naive:.2f})",
    xy=(0.04, 0.96), xycoords="axes fraction", va="top",
    fontsize=9, color=INK2,
)

# Panel B — MAE by aid station, model vs naive
stations = sorted(val_df["as_num"].unique().to_list())
mae_m, mae_n = [], []
for a in stations:
    mask = np.array(val_df["as_num"].to_list()) == a
    mae_m.append(mean_absolute_error(actual[mask], pred[mask]))
    mae_n.append(mean_absolute_error(actual[mask], naive[mask]))
xs = np.arange(len(stations))
ax2.bar(xs - 0.19, mae_n, width=0.36, color=DEEMPH, label="Naive per-station median")
ax2.bar(xs + 0.19, mae_m, width=0.36, color=BLUE, label="Model")
ax2.set_xticks(xs, [f"AS{a:02d}" for a in stations], rotation=0, fontsize=7.5)
ax2.set_ylabel("MAE (min)", color=INK2, fontsize=9.5)
ax2.set_title(
    f"Error by aid station — {holdout} holdout", color="#0b0b0b",
    fontsize=10.5, loc="left",
)
ax2.legend(frameon=False, fontsize=8.5, labelcolor=INK2, loc="upper left")
ax2.grid(axis="x", visible=False)

fig.savefig("docs/img/stoppage_model_eval.png", bbox_inches="tight", facecolor=SURFACE)
print(f"saved. mae={mae:.3f} naive={mae_naive:.3f} lim={lim:.1f}")
