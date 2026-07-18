"""Eastern States matplotlib theme.

Blue-gray panel, dotted grid, Geist Mono everywhere, uppercase bold titles
with an ochre subtitle, and a slate-green + bright-green mark palette.

Usage:
    from eastern_states_pace_predict import mpl_theme

    mpl_theme.apply()
    fig, ax = plt.subplots(figsize=(12, 8))
    ...
    mpl_theme.set_title(ax, "Elapsed time to reach aid stations",
                        "Eastern States 2021")
    mpl_theme.set_labels(ax, "Aid station", "Elapsed time [hrs]")
"""

import matplotlib as mpl
from cycler import cycler
from matplotlib import font_manager

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------
COLORS = {
    "page": "#f0f0f2",       # figure background
    "panel": "#e6e8ef",      # axes background (blue-gray)
    "grid": "#c6cad6",       # dotted gridlines
    "ink": "#16161e",        # titles, axis labels, left spine
    "subtitle": "#a87b1e",   # ochre subtitle
    "tick": "#3a4a52",       # tick labels
    "dot": "#3d5a50",        # slate-green markers
    "green": "#4fae2b",      # bright green accent (violins, highlights)
    "range": "#8a8f99",      # thin range/whisker lines
    "navy": "#31435c",       # extra cycle slot
}

# Family name matplotlib reports for the Nerd Font build of Geist Mono; plain
# "Geist Mono" is included for machines with the vanilla release installed.
_FONT_STACK = ["GeistMono NF", "GeistMono NFM", "Geist Mono", "DejaVu Sans Mono"]


def _register_fonts() -> None:
    """Add any installed Geist font files to matplotlib's font manager.

    matplotlib's font cache does not pick up newly installed fonts until it is
    rebuilt; registering explicitly makes the theme work in a fresh process
    right after the font is installed.
    """
    for path in font_manager.findSystemFonts():
        if "geist" in path.lower():
            try:
                font_manager.fontManager.addfont(path)
            except Exception:  # noqa: BLE001 - a bad font file should not kill plotting
                pass


RC = {
    # canvas
    "figure.facecolor": COLORS["page"],
    "savefig.facecolor": COLORS["page"],
    "axes.facecolor": COLORS["panel"],
    "figure.dpi": 110,
    "savefig.dpi": 160,
    "savefig.bbox": "tight",
    # type
    "font.family": "monospace",
    "font.monospace": _FONT_STACK,
    "font.size": 10,
    # grid: fine dotted, drawn under the data
    "axes.grid": True,
    "grid.color": COLORS["grid"],
    "grid.linestyle": (0, (1, 3)),
    "grid.linewidth": 0.9,
    "axes.axisbelow": True,
    # frame: single dark left spine
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.spines.bottom": False,
    "axes.spines.left": True,
    "axes.edgecolor": COLORS["ink"],
    "axes.linewidth": 1.2,
    # titles & labels: bold, left/top aligned like the reference
    "axes.titlelocation": "left",
    "axes.titleweight": "bold",
    "axes.titlesize": 13,
    "axes.titlecolor": COLORS["ink"],
    "axes.titlepad": 30,
    "axes.labelweight": "bold",
    "axes.labelcolor": COLORS["ink"],
    "axes.labelsize": 10.5,
    "xaxis.labellocation": "right",
    "yaxis.labellocation": "top",
    # ticks: labels only, no tick marks
    "xtick.color": COLORS["panel"],
    "ytick.color": COLORS["panel"],
    "xtick.labelcolor": COLORS["tick"],
    "ytick.labelcolor": COLORS["tick"],
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "xtick.major.size": 0,
    "ytick.major.size": 0,
    # marks
    "lines.linewidth": 1.6,
    "axes.prop_cycle": cycler(
        color=[COLORS["dot"], COLORS["green"], COLORS["subtitle"],
               COLORS["navy"], COLORS["range"]]
    ),
    "legend.frameon": False,
}


def apply() -> dict:
    """Activate the theme for the current process; returns the palette."""
    _register_fonts()
    mpl.rcParams.update(RC)
    return COLORS


def set_title(ax, title: str, subtitle: str | None = None) -> None:
    """Uppercase bold title at the top-left, optional ochre subtitle under it."""
    ax.set_title(title.upper(), loc="left", pad=26 if subtitle else 14)
    if subtitle:
        ax.text(
            0, 1.006, subtitle,
            transform=ax.transAxes, va="bottom", ha="left",
            fontsize=10, color=COLORS["subtitle"],
        )


def set_labels(ax, xlabel: str | None = None, ylabel: str | None = None) -> None:
    """Uppercase axis labels (x sits bottom-right, y top-left per the theme)."""
    if xlabel:
        ax.set_xlabel(xlabel.upper())
    if ylabel:
        ax.set_ylabel(ylabel.upper())
