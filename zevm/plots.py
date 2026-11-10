"""Charts for the tracker and scenario notebooks.

Every function takes the DataFrames the package produces and returns a matplotlib Figure.
Conventions:
- Colour follows the entity, never its rank. Scenarios get palette slots in the order
  they were run, so a scenario keeps its colour across charts.
- Text is always in ink colours, never series colours. Lines are labelled at their right
  end as well as in a legend, because two palette slots are below 3:1 contrast on white.
- One y-axis per chart. Two measures in different units become separate panels.
- Projected years are shaded so they never pass for data.
- Layout: a bold title, a grey subtitle carrying the units (no y-axis label), the legend in
  a row above the plot, and an optional grey source line below it (_header, add_source).
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


FONTS = Path(__file__).parent / "style" / "fonts"


def use_style() -> None:
    """Apply the New AutoMotive matplotlib style, registering the bundled Roboto fonts
    (static Regular/Medium/Bold, Apache 2.0) so charts look the same on every machine."""
    from matplotlib import font_manager
    for f in sorted(FONTS.glob("*.ttf")):
        font_manager.fontManager.addfont(str(f))
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


def _left_edge(ax) -> float:
    """x, in axes fraction, of the leftmost thing drawn for this axes (tick labels included),
    so the title block and source line line up with the far left of the chart."""
    fig = ax.figure
    fig.canvas.draw()
    bb = ax.get_tightbbox(fig.canvas.get_renderer())
    return min(0.0, (bb.x0 - ax.bbox.x0) / ax.bbox.width)


def _header(ax, title: str, subtitle: str | None = None, ncol: int | None = None, handles=None, labels=None,
            legend: bool = True):
    """Title block above the plot, aligned with the chart's far left: bold title, grey
    subtitle, then the legend in a row (or rows) directly above the axes. Replaces
    matplotlib's title and bottom legend."""
    fig = ax.figure
    ax.set_title("")
    x = _left_edge(ax)
    y = 4.0
    if legend:
        if handles is None:
            handles, labels = ax.get_legend_handles_labels()
        elif labels is None:
            labels = [h.get_label() for h in handles]
        if handles:
            n = ncol or min(len(handles), 3)
            leg = ax.legend(handles, labels, loc="lower left", bbox_to_anchor=(x, 1.0), ncol=n, borderaxespad=0.3)
            fig.canvas.draw()
            y = leg.get_window_extent().height * 72 / fig.dpi + 10
    if subtitle:
        ax.annotate(subtitle, (x, 1), xycoords="axes fraction", xytext=(0, y), textcoords="offset points",
                    ha="left", va="bottom", fontsize=10, color="#666666", annotation_clip=False)
        y += 17
    ax.annotate(title, (x, 1), xycoords="axes fraction", xytext=(0, y), textcoords="offset points",
                ha="left", va="bottom", fontsize=14, fontweight="bold", color="#1a1a1a", annotation_clip=False)
    return ax


def add_source(ax, text: str):
    """Grey source line below the plot, aligned with the chart's far left."""
    below = 34 if ax.get_xlabel() else 22
    ax.annotate(text, (_left_edge(ax), 0), xycoords="axes fraction", xytext=(0, -below), textcoords="offset points",
                ha="left", va="top", fontsize=8, color=MUTED, annotation_clip=False)
    return ax


def _short(name: str) -> str:
    """End-label text: "A. On target" -> "A"; "Opt 1 (70%) | steady" -> "Opt 1 (70%)"."""
    if len(name) > 2 and name[1] == "." and name[0].isalnum():
        return name[0]
    return name.split(" | ")[0][:24]


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
    last = roll.dropna()
    if not last.empty:
        _end_labels(ax, [("rolling", last.index[-1], last.iloc[-1], BLUE)])
    _header(ax, title or f"New {vehicle_type} ZEV share vs mandate target",
            f"Share of new {vehicle_type} registrations that are zero-emission, %")
    add_source(ax, "Source: DVLA registrations (New AutoMotive); VETS Order 2023 as amended")
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
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    _header(ax, title or f"Allowance surplus / shortfall by participant, {year}" + (f" ({basis})" if basis and basis != "actual" else ""),
            "Share of registrations" if per_vehicle else "Allowances", handles=[
                Patch(color=BLUE, label="Surplus after own CO2 credits + bank"),
                Patch(color=CORAL, label="Shortfall after own CO2 credits + bank"),
                Line2D([], [], marker="o", ls="", markerfacecolor="white", markeredgecolor=INK_2, label="Registration allowances only"),
            ], ncol=2)
    add_source(ax, "Source: New AutoMotive ZEV mandate model; DVLA registrations")
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
        ends.append((_short(name), g["year"].iloc[-1], g[column].iloc[-1], cols[name]))
    if show_target:
        t = summary.drop_duplicates("year").sort_values("year")
        ax.step(t["year"], t["zev_target"], where="mid", color=INK_2, lw=1.2, label="Target")
    if pct:
        _pct(ax)
    elif money:
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"£{v:,.0f}m"))
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    _header(ax, title, ylabel)
    fmt = "{:.0%}" if pct else ("£{:,.0f}m" if money else "{:,.0f}")
    _end_labels(ax, ends, fmt=fmt)
    ax.set_xlim(right=summary["year"].max() + 1.2)
    add_source(ax, "Source: New AutoMotive ZEV mandate model")
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
    _header(ax, title or f"How non-ZEV registrations are covered: {scenario}", "Non-ZEV registrations", ncol=3)
    add_source(ax, "Source: New AutoMotive ZEV mandate model")
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
        ax.annotate(sub, (0, 1), xycoords="axes fraction", xytext=(0, 6), textcoords="offset points",
                    ha="left", va="bottom", fontsize=10, fontweight="bold", color=INK_2)
        _thousands(ax)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    axes[0].set_ylim(bottom=0)
    handles, labels = axes[0].get_legend_handles_labels()
    axes[1].legend(handles, labels, loc="upper left", fontsize=8.5)
    ax0 = axes[0]
    ax0.annotate(title, (0, 1), xycoords="axes fraction", xytext=(0, 40), textcoords="offset points",
                 ha="left", va="bottom", fontsize=14, fontweight="bold", color="#1a1a1a", annotation_clip=False)
    ax0.annotate("Allowances, end of year", (0, 1), xycoords="axes fraction", xytext=(0, 23), textcoords="offset points",
                 ha="left", va="bottom", fontsize=10, color="#666666", annotation_clip=False)
    add_source(ax0, "Source: New AutoMotive ZEV mandate model")
    return fig


# ---------------------------------------------------------------- fleet

# Fuel groups take palette slots 1-4 in this order (passes adjacent CVD checks). Yellow and
# aqua are under 3:1 on white, so every fuel line is end-labelled.
FUEL_COLOURS = {"petrol": BLUE, "diesel": CORAL, "hybrid": YELLOW, "bev": AQUA}
FUEL_LABELS = {"petrol": "Petrol", "diesel": "Diesel", "hybrid": "Hybrid", "bev": "BEV"}


def _millions(ax, axis="y"):
    fmt = mticker.FuncFormatter(lambda v, _: f"{v / 1e6:g}m")
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def emissions_vs_naei(validation: pd.DataFrame, totals: pd.DataFrame | None = None, co2_scale: float = 1.0,
                      title: str = "Car tailpipe CO2e: NAEI, fuel-based model and scenarios"):
    """NAEI 1A3bi (cars) against CO2 from DESNZ fuel use (fleet.validate), and optionally
    each scenario's projection (fleet.totals). Modelled lines are multiplied by `co2_scale`
    (fleet.calibration()["co2_scale"]) for history; projections in `totals` already are."""
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ends = []
    if totals is not None:
        proj = totals[totals["basis"] == "projected"]
        _shade_projection(ax, proj["year"], proj["basis"])
        cols = colours_for(proj["scenario"])
        for name, g in proj.groupby("scenario", sort=False):
            g = g.sort_values("year")
            ax.plot(g["year"], g["co2_mt"], color=cols[name], marker="o", markersize=4, label=name)
            ends.append((name[:24], g["year"].iloc[-1], g["co2_mt"].iloc[-1], cols[name]))
    naei = validation["naei_mt"].dropna() if "naei_mt" in validation else pd.Series(dtype=float)
    naei = naei[naei.index >= validation.index.min()]
    ax.plot(naei.index, naei.values, color=INK, marker="o", markersize=4, label="NAEI 1A3bi cars")
    model = validation["co2_total_mt"] * co2_scale
    ax.plot(model.index, model.values, color=MUTED, ls="--", lw=1.6,
            label="From DESNZ fuel use" + (f" (x{co2_scale:.3f} to NAEI)" if co2_scale != 1 else ""))
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    _header(ax, title, "Mt CO2e a year", ncol=2)
    if not naei.empty:
        ends.append(("NAEI", naei.index[-1], naei.iloc[-1], INK))
    _end_labels(ax, ends, fmt="{:.1f} Mt")
    ax.set_xlim(right=ax.get_xlim()[1] + 1.2)
    add_source(ax, "Source: NAEI; DESNZ road fuel consumption; New AutoMotive fleet model")
    return fig


def parc_by_fuel(tidy: pd.DataFrame, scenario: str, column: str = "parc", title: str | None = None):
    """One line per fuel group for one scenario of fleet.run_many output (observed years
    plus projection, projected years shaded)."""
    s = tidy[tidy["scenario"] == scenario] if "scenario" in tidy else tidy
    fig, ax = plt.subplots(figsize=(10, 5.5))
    _shade_projection(ax, s["year"], s["basis"])
    ends = []
    for fuel, colour in FUEL_COLOURS.items():
        g = s[s["fuel"] == fuel].dropna(subset=[column]).sort_values("year")
        if g.empty:
            continue
        ax.plot(g["year"], g[column], color=colour, label=FUEL_LABELS[fuel])
        ends.append((FUEL_LABELS[fuel], g["year"].iloc[-1], g[column].iloc[-1], colour))
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    if column == "co2_mt":
        fmt = "{:.1f} Mt"
        unit = "Mt CO2e a year"
    else:
        _millions(ax)
        fmt = None
        unit = "Vehicles"
    default = {"parc": "Cars on the road by fuel", "co2_mt": "Tailpipe CO2e by fuel"}.get(column, f"{column} by fuel")
    _header(ax, title or f"{default}: {scenario}", unit, ncol=4)
    _end_labels(ax, ends, fmt=fmt)
    ax.set_xlim(right=s["year"].max() + 1.5)
    add_source(ax, "Source: DfT vehicle licensing statistics; New AutoMotive fleet model")
    return fig


# ---------------------------------------------------------------- oil

def history_and_scenarios(df: pd.DataFrame, value: str, scenarios: list[str], labels: dict | None = None,
                          end_labels: dict | None = None, ymin: float | None = None, ymax: float | None = None,
                          title: str = "", ylabel: str = "", history_label: str = "Actual", pct: bool = False,
                          source: str | None = "Source: New AutoMotive ZEV mandate model"):
    """One measure over time: the observed history once (rows with basis 'actual'), then one
    line per chosen scenario from the last observed year. At most six scenarios, so colours
    stay distinct. A line chart need not start at zero; set ymin to show the spread."""
    if len(scenarios) > len(SERIES):
        raise ValueError(f"At most {len(SERIES)} scenarios; facet or pick fewer")
    labels, end_labels = labels or {}, end_labels or {}
    cols = colours_for(scenarios)
    fig, ax = plt.subplots(figsize=(10, 6))
    _shade_projection(ax, df["year"], df["basis"])
    hist = df[(df["scenario"] == scenarios[0]) & (df["basis"] == "actual")].sort_values("year")
    ax.plot(hist["year"], hist[value], color=INK_2, label=history_label)
    last = hist.iloc[-1]
    ends = []
    for s in scenarios:
        g = df[(df["scenario"] == s) & (df["basis"] != "actual")].sort_values("year")
        ax.plot([last["year"]] + list(g["year"]), [last[value]] + list(g[value]), color=cols[s], label=labels.get(s, s))
        ends.append((end_labels.get(s, _short(labels.get(s, s))), g["year"].iloc[-1], g[value].iloc[-1], cols[s]))
    lo = ymin if ymin is not None else 0
    hi = ymax if ymax is not None else max(hist[value].max(), df[value].max()) * 1.08
    ax.set_ylim(lo, hi)
    if pct:
        _pct(ax)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    _header(ax, title, ylabel, ncol=2)
    _end_labels(ax, ends, fmt="{:.1%}" if pct else "{:.1f}")
    ax.set_xlim(right=df["year"].max() + 2.5)
    if source:
        add_source(ax, source)
    return fig


def oil_intensity_lines(oil: pd.DataFrame, scenarios: list[str], labels: dict | None = None,
                        end_labels: dict | None = None, ymin: float | None = 10,
                        title: str = "UK oil intensity of GDP by ZEV mandate scenario"):
    """Oil per £m of real GDP (zevm.oil.scenario_oil output), history then scenarios."""
    return history_and_scenarios(oil, "oil_t_per_gdp_m", scenarios, labels, end_labels, ymin=ymin, title=title,
                                 ylabel="Tonnes of oil per £m of real GDP (2024 prices)",
                                 history_label="Actual (DUKES ÷ ONS GDP)",
                                 source="Source: DESNZ DUKES and EEP; ONS; OBR; New AutoMotive ZEV mandate model")


def electricity_share_lines(elec: pd.DataFrame, scenarios: list[str], mode: str = "eep", labels: dict | None = None,
                            end_labels: dict | None = None, ymin: float | None = 0.15, ymax: float | None = 0.30,
                            title: str | None = None, ex_intl_aviation: bool = False):
    """Electricity as a share of UK final energy (zevm.electricity.scenario_electricity output)
    for one rest-of-economy mode, history then scenarios."""
    value = "electricity_share_ex_intl_aviation" if ex_intl_aviation else "electricity_share"
    default = {"eep": "Electricity share of UK final energy by ZEV mandate scenario",
               "held": "Electricity share of UK final energy: the ZEV mandate's effect alone"}[mode]
    return history_and_scenarios(elec[elec["mode"] == mode], value, scenarios, labels, end_labels, ymin=ymin, ymax=ymax,
                                 title=title or default, pct=True, history_label="Actual (DUKES)",
                                 ylabel="Electricity, % of UK final energy consumption"
                                        + (" (excl. international aviation)" if ex_intl_aviation else ""),
                                 source="Source: DESNZ DUKES and EEP; New AutoMotive ZEV mandate model")


def oil_gap_bars(oil: pd.DataFrame, reference: str, year: int | None = None, cumulative_from: int | None = None,
                 title: str | None = None):
    """Extra oil in each scenario vs `reference`: in `year`, or summed from `cumulative_from`
    to the last year. One colour (it's one measure); value labels at the bar ends."""
    proj = oil[oil["basis"] != "actual"]
    if cumulative_from is not None:
        v = proj[proj["year"] >= cumulative_from].groupby("scenario")["oil_kt"].sum() / 1e3
        what = f"{cumulative_from}–{int(proj['year'].max())}, cumulative"
    else:
        year = year or int(proj["year"].max())
        v = proj[proj["year"] == year].set_index("scenario")["oil_kt"] / 1e3
        what = str(year)
    gap = (v - v[reference]).drop(reference).sort_values()
    fig, ax = plt.subplots(figsize=(10, max(3.5, 0.45 * len(gap) + 1.2)))
    y = np.arange(len(gap))
    ax.barh(y, gap.values, color=BLUE, height=0.6, edgecolor="white", linewidth=1.5)
    for yi, val in zip(y, gap.values):
        ax.text(val + gap.abs().max() * 0.01, yi, f"{val:+.1f} Mt", va="center", fontsize=9, color=INK_2)
    # Drop a " | <suffix>" every bar shares (the title already says it), else show it as " · ".
    suffix = reference.split(" | ", 1)[1] if " | " in reference else None
    if suffix and all(f" | {suffix}" in n for n in gap.index):
        names = [n.replace(f" | {suffix}", "").replace(", non-ZEV", " · non-ZEV") for n in gap.index]
    else:
        names = [n.replace(" | ", " · ") for n in gap.index]
    ax.set_yticks(y, names, fontsize=9)
    ax.axvline(0, color=INK_2, lw=0.8)
    ax.grid(axis="x"); ax.grid(axis="y", visible=False)
    ax.set_xlim(min(0, gap.min() * 1.15), gap.max() * 1.18)
    _header(ax, title or f"Extra UK oil demand vs {reference.replace(' | ', ', ')}", f"Million tonnes of oil, {what}",
            legend=False)
    add_source(ax, "Source: DESNZ DUKES and EEP; New AutoMotive ZEV mandate model")
    return fig


def electrification_gap_bars(elec: pd.DataFrame, scenarios: list[str], labels: dict | None = None,
                             target: float = 0.35, year: int | None = None, ex_intl_aviation: bool = True):
    """One bar per scenario from the base year's electricity share up to `target`, split into
    what cars and vans add (mode 'held' minus base), what the rest of the economy adds on the
    DESNZ projection (mode 'eep' minus 'held'), and what is still to find."""
    from matplotlib.patches import Patch
    labels = labels or {}
    col = "electricity_share_ex_intl_aviation" if ex_intl_aviation else "electricity_share"
    year = year or int(elec["year"].max())
    base_year = int(elec.loc[elec["basis"] == "actual", "year"].max())
    s0 = elec[(elec["year"] == base_year)][col].iloc[0]
    rows = []
    for k in scenarios:
        g = elec[(elec["scenario"] == k) & (elec["year"] == year)].set_index("mode")[col]
        rows.append((labels.get(k, k), s0, g["held"] - s0, g["eep"] - g["held"], target - g["eep"]))
    road = [r[2] for r in rows]
    fig, ax = plt.subplots(figsize=(9, 0.62 * len(rows) + 1.3))
    y = np.arange(len(rows))[::-1]
    fills = [OTHER, BLUE, "#9aa7b8", "white"]
    for yi, (_, *vals) in zip(y, rows):
        left = 0.0
        for i, (v, c) in enumerate(zip(vals, fills)):
            kw = dict(color=c, height=0.58, edgecolor="white", linewidth=1.2)
            if i == 3:
                kw.update(edgecolor=CORAL, linewidth=1.0, hatch="////")
            ax.barh(yi, v, left=left, **kw)
            if i in (1, 2):
                ax.text(left + v / 2, yi, f"+{v * 100:.1f}", ha="center", va="center",
                        color="white" if i == 1 else INK, fontsize=8.5)
            elif i == 3 and v > 0:
                ax.text(left + v / 2, yi, f"{v * 100:.1f}", ha="center", va="center", color=INK, fontsize=8.5,
                        bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none"))
            left += v
    ax.axvline(target, color="#1a1a1a", lw=1)
    ax.set_yticks(y, [r[0] for r in rows])
    ax.grid(axis="y", visible=False); ax.grid(axis="x")
    _pct(ax, "x")
    ax.set_xlim(0, target * 1.06)
    handles = [Patch(color=OTHER, label=f"{base_year} ({s0:.1%})"), Patch(color=BLUE, label="Cars and vans"),
               Patch(color="#9aa7b8", label="Rest of economy, DESNZ projection"),
               Patch(facecolor="white", edgecolor=CORAL, hatch="////", label=f"Still to find for {target:.0%}")]
    lo, hi = min(road) * 100, max(road) * 100
    _header(ax, f"Cars and vans deliver {lo:.1f}–{hi:.1f} of the {(target - s0) * 100:.1f} points needed for {target:.0%} by {year}",
            f"Electricity, % of UK final energy{' (excl. international aviation)' if ex_intl_aviation else ''}, {year}. "
            "Labels in percentage points", handles=handles, ncol=4)
    add_source(ax, "Source: DESNZ DUKES and Energy and Emissions Projections (Feb 2026); New AutoMotive ZEV mandate model")
    return fig
