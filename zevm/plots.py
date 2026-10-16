"""Charts for the tracker and scenario notebooks.

Every function takes the DataFrames the package produces and returns a matplotlib Figure.
Conventions:
- Colour follows the entity, never its rank. Scenarios get palette slots in the order
  they were run, so a scenario keeps its colour across charts.
- Text is always in ink colours, never series colours. Lines are labelled at their right
  end as well as in a legend, because two palette slots are below 3:1 contrast on white.
- One y-axis per chart. Two measures in different units become separate panels.
- Projected years are shaded so they never pass for data.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

STYLE = Path(__file__).parent / "style" / "newautomotive.mplstyle"

SERIES = ["#416FF4", "#E06760", "#eda100", "#1baf7a", "#8a5cd6", "#008300"]
BLUE, CORAL, YELLOW, AQUA, VIOLET, GREEN = SERIES
INK, INK_2, MUTED, GRID, SHADE = "#0b0b0b", "#52514e", "#8b8a85", "#ebeae6", "#f4f3f0"
OTHER = "#b5b4ae"

# How non-ZEV registrations were covered. Fixed colours so every chart reads the same.
COVERAGE = [
    ("current_used", "Own allowances", BLUE),
    ("co2_credits", "CO2 credits", AQUA),
    ("banked_used", "Banked", VIOLET),
    ("traded", "Bought", YELLOW),
    ("borrowed", "Borrowed", GREEN),
    ("shortfall", "Shortfall (paid)", CORAL),
]


def use_style() -> None:
    """Apply the New AutoMotive matplotlib style."""
    plt.style.use(STYLE)


def colours_for(names) -> dict[str, str]:
    """Palette slot per name, in order; anything past six is grey (and labelled)."""
    names = list(dict.fromkeys(names))
    return {n: SERIES[i] if i < len(SERIES) else OTHER for i, n in enumerate(names)}


def _pct(ax, axis="y", decimals=0):
    fmt = mticker.PercentFormatter(1.0, decimals=decimals)
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def _thousands(ax, axis="y"):
    fmt = mticker.FuncFormatter(lambda v, _: f"{v / 1e3:,.0f}k" if abs(v) >= 1e3 or v == 0 else f"{v:,.0f}")
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def _end_labels(ax, items, fmt="{:.0%}", min_gap=None):
    """Label line ends: a dot in the series colour, text in secondary ink, nudged apart."""
    if not items:
        return
    lo, hi = ax.get_ylim()
    gap = min_gap if min_gap is not None else (hi - lo) * 0.045
    # Merge labels whose points coincide ("B, C  77%") instead of pushing one away.
    merged: list[list] = []
    for name, x, y, colour in sorted(items, key=lambda t: (t[1], t[2])):
        if merged and merged[-1][1] == x and abs(merged[-1][2] - y) <= (hi - lo) * 0.005:
            merged[-1][0] += f", {name}"
        else:
            merged.append([name, x, y, colour])
    items = sorted(merged, key=lambda t: t[2])
    placed = []
    for name, x, y, colour in items:
        ty = max(y, placed[-1] + gap) if placed else y
        placed.append(ty)
        ax.plot([x], [y], "o", color=colour, markersize=6, markeredgecolor="white", markeredgewidth=1.5, zorder=5)
        value = fmt.format(y) if fmt else ""
        ax.annotate(f"{name}  {value}".strip(), (x, y), xytext=(8, (ty - y) / (hi - lo) * ax.bbox.height),
                    textcoords="offset pixels", va="center", fontsize=9, color=INK_2, annotation_clip=False)


def _shade_projection(ax, years, basis):
    """Light band behind estimated and projected years, labelled once."""
    df = pd.DataFrame({"year": years, "basis": basis}).drop_duplicates("year")
    for kind, label in (("estimated", "est."), ("projected", "projected")):
        ys = df.loc[df.basis == kind, "year"]
        if ys.empty:
            continue
        ax.axvspan(ys.min() - 0.5, ys.max() + 0.5, color=SHADE, zorder=0, lw=0)
        ax.text(ys.min() - 0.4, 1.0, label, transform=ax.get_xaxis_transform(), va="top", fontsize=8, color=MUTED)


# ---------------------------------------------------------------- tracker

def zev_share_monthly(monthly: pd.DataFrame, params, vehicle_type: str = "car", rolling: int = 12, title: str | None = None):
    """Market ZEV share by month, its rolling average, and the scheme-year target."""
    m = monthly.assign(month=pd.to_datetime(monthly["month"])).groupby("month")[["total_registrations", "zev_registrations"]].sum()
    share = m["zev_registrations"] / m["total_registrations"]
    roll = m["zev_registrations"].rolling(rolling).sum() / m["total_registrations"].rolling(rolling).sum()

    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.plot(share.index, share.values, color=OTHER, lw=1.2, label="Monthly")
    ax.plot(roll.index, roll.values, color=BLUE, label=f"Rolling {rolling} months")
    years = [y for y in sorted(set(m.index.year)) if y in params.scheme_years]
    if years:
        tx = [pd.Timestamp(y, 1, 1) for y in years] + [pd.Timestamp(years[-1] + 1, 1, 1)]
        ty = [params.zev_target(vehicle_type, y) for y in years]
        ax.step(tx, ty + ty[-1:], where="post", color=INK_2, lw=1.2, label="Target (scheme year)")
    _pct(ax)
    ax.set_ylim(0, max(share.max(), max(ty) if years else 0) * 1.15)
    ax.set_title(title or f"New {vehicle_type} ZEV share vs mandate target")
    ax.legend(loc="upper left")
    last = roll.dropna()
    if not last.empty:
        _end_labels(ax, [("rolling", last.index[-1], last.iloc[-1], BLUE)])
    return fig


def position_bars(results: pd.DataFrame, year: int, top_n: int = 20, per_vehicle: bool = False, title: str | None = None):
    """Each participant's surplus (+) or shortfall (-) of allowances in `year`.

    The bar is the position after the participant's own CO2 credits and banked allowances,
    before trading or borrowing. The ring is the position from registration allowances alone.
    With per_vehicle=True both are shown as a share of the participant's registrations.
    """
    r = results[results["year"] == year].nlargest(top_n, "total_registrations").copy()
    r["before"] = r["allowances"] - r["nonzev_registrations"]
    r["after"] = r["own_position"]
    if per_vehicle:
        r[["before", "after"]] = r[["before", "after"]].div(r["total_registrations"], axis=0)
    r = r.sort_values("after")
    label = r["group"].str.replace(" (POOL)", " (pool)", regex=False).str.slice(0, 32)

    fig, ax = plt.subplots(figsize=(10, max(4, 0.32 * len(r) + 1.5)))
    y = np.arange(len(r))
    colours = np.where(r["after"] >= 0, BLUE, CORAL)
    ax.barh(y, r["after"], color=colours, height=0.62, edgecolor="white", linewidth=1.5)
    ax.scatter(r["before"], y, s=36, facecolor="white", edgecolor=INK_2, linewidth=1.3, zorder=4)
    ax.axvline(0, color=INK_2, lw=0.8)
    ax.set_yticks(y, label)
    ax.grid(axis="x"); ax.grid(axis="y", visible=False)
    _pct(ax, "x") if per_vehicle else _thousands(ax, "x")
    basis = r["basis"].iloc[0] if "basis" in r else ""
    ax.set_title(title or f"Allowance surplus / shortfall by participant, {year}" + (f" ({basis})" if basis and basis != "actual" else ""))
    ax.set_xlabel("share of registrations" if per_vehicle else "allowances")
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    ax.legend(handles=[
        Patch(color=BLUE, label="Surplus after own CO2 credits + bank"),
        Patch(color=CORAL, label="Shortfall after own CO2 credits + bank"),
        Line2D([], [], marker="o", ls="", markerfacecolor="white", markeredgecolor=INK_2, label="Registration allowances only"),
    ], loc="upper center", bbox_to_anchor=(0.5, -0.06 - 0.6 / len(r)), ncol=3, fontsize=9)
    return fig


# ---------------------------------------------------------------- scenarios

def scenario_lines(summary: pd.DataFrame, column: str, title: str, pct: bool = False, money: bool = False,
                   show_target: bool = False, ylabel: str | None = None):
    """One line per scenario for a market_summary column over the years."""
    cols = colours_for(summary["scenario"])
    fig, ax = plt.subplots(figsize=(10, 5.5))
    _shade_projection(ax, summary["year"], summary["basis"])
    ends = []
    for name, g in summary.groupby("scenario", sort=False):
        g = g.sort_values("year")
        ax.plot(g["year"], g[column], color=cols[name], marker="o", markersize=4, label=name)
        ends.append((name.split(".")[0] if "." in name[:3] else name[:18], g["year"].iloc[-1], g[column].iloc[-1], cols[name]))
    if show_target:
        t = summary.drop_duplicates("year").sort_values("year")
        ax.step(t["year"], t["zev_target"], where="mid", color=INK_2, lw=1.2, label="Target")
    if pct:
        _pct(ax)
    elif money:
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"£{v:,.0f}m"))
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.set_title(title)
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), ncol=3)
    fmt = "{:.0%}" if pct else ("£{:,.0f}m" if money else "{:,.0f}")
    _end_labels(ax, ends, fmt=fmt)
    ax.set_xlim(right=summary["year"].max() + 1.2)
    return fig


def coverage_stack(summary: pd.DataFrame, scenario: str, title: str | None = None):
    """How each year's non-ZEV registrations were covered, for one scenario."""
    s = summary[summary["scenario"] == scenario].sort_values("year")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    _shade_projection(ax, s["year"], s["basis"])
    bottom = np.zeros(len(s))
    grand = sum(s[col].clip(lower=0).sum() for col, _, _ in COVERAGE)
    for col, label, colour in COVERAGE:
        vals = s[col].clip(lower=0).to_numpy()
        if vals.sum() < 0.001 * grand:      # leave near-empty categories out of the legend
            continue
        ax.bar(s["year"], vals, bottom=bottom, color=colour, width=0.7, edgecolor="white", linewidth=1.5, label=label)
        bottom += vals
    _thousands(ax)
    ax.set_ylim(0, bottom.max() * 1.08)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.set_title(title or f"How non-ZEV registrations are covered: {scenario}")
    ax.set_ylabel("non-ZEV registrations")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), ncol=6, fontsize=9)
    return fig


def bank_and_debt(summary: pd.DataFrame, title: str = "Banked allowances and outstanding borrowing (end of year)"):
    """Two panels, same scale: banked allowances held and borrowing still owed."""
    cols = colours_for(summary["scenario"])
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    for ax, col, sub in zip(axes, ["bank_end", "debt_end"], ["Banked", "Borrowing owed"]):
        _shade_projection(ax, summary["year"], summary["basis"])
        for name, g in summary.groupby("scenario", sort=False):
            g = g.sort_values("year")
            ax.plot(g["year"], g[col], color=cols[name], marker="o", markersize=4, label=name)
        ax.set_title(sub, fontsize=12)
        _thousands(ax)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    axes[0].set_ylim(bottom=0)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, bbox_to_anchor=(0.5, -0.06), fontsize=9)
    fig.suptitle(title, x=0.01, ha="left", fontsize=14)
    return fig
