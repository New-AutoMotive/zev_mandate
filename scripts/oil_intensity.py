"""Oil intensity of UK GDP to 2035 under each ZEV mandate scenario.

    .venv/bin/python scripts/oil_intensity.py [--offline] [--out outputs]

Writes:
- oil_intensity_by_scenario.csv: one row per scenario-year (2010-2035). Oil in thousand
  tonnes split into cars, vans, other road and non-road, plus real GDP (£m, 2024 prices),
  tonnes of oil per £m of GDP, and an index (2025 = 100).
- oil_intensity_lines.png: history and five headline scenarios.
- oil_gap_cumulative.png: extra oil 2026-35 in every scenario vs the current mandate on target.

Scenarios come from zevm.scenarios.policy_paths (the current mandate and the four review
options, on target and with heavy flexibility use). The method is described in zevm/oil.py.
Input files are listed in data/raw/SOURCES.md.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

from zevm import oil, plots, scenarios

ROOT = Path(__file__).resolve().parent.parent
HEADLINE = {
    "Current mandate | on target": ("Current mandate (cars 80% in 2030), on target", "Current"),
    "Review Option 1 (cars 70%, vans 60%) | on target": ("Option 1 (cars 70%), on target", "Opt 1"),
    "Review Option 2 (cars 60%, vans 50%) | on target": ("Option 2 (cars 60%), on target", "Opt 2"),
    "Review Option 3 (cars 50%, vans 40%) | on target": ("Option 3 (cars 50%), on target", "Opt 3"),
    "Review Option 3 (cars 50%, vans 40%) | heavy flex": ("Option 3 (cars 50%), heavy flexibility use", "Opt 3 + flex"),
}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(ROOT / "outputs"))
    ap.add_argument("--car-market-2026", type=float, default=2.183e6)
    ap.add_argument("--growth", type=float, default=0.015)
    ap.add_argument("--offline", action="store_true", help="use cached BigQuery results only")
    a = ap.parse_args()
    out_dir = Path(a.out); out_dir.mkdir(parents=True, exist_ok=True)

    paths, info = scenarios.policy_paths(a.car_market_2026, a.growth, offline=a.offline)
    op = oil.load_params()
    result = oil.scenario_oil(paths, oil.load_inputs(op), op)
    result.to_csv(out_dir / "oil_intensity_by_scenario.csv", index=False)

    plots.use_style()
    plots.oil_intensity_lines(result, list(HEADLINE), {k: v[0] for k, v in HEADLINE.items()},
                              {k: v[1] for k, v in HEADLINE.items()}).savefig(out_dir / "oil_intensity_lines.png", bbox_inches="tight")
    plots.oil_gap_bars(result, "Current mandate | on target", cumulative_from=op.data["base_year"] + 1).savefig(
        out_dir / "oil_gap_cumulative.png", bbox_inches="tight")

    y = result[result["year"] == result["year"].max()].set_index("scenario")["oil_t_per_gdp_m"]
    print(f"Wrote {out_dir}: oil_intensity_by_scenario.csv ({len(result)} rows), 2 charts. Assumptions: {info}")
    print(y.round(2).to_string())


if __name__ == "__main__":
    main()
