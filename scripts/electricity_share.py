"""Electricity as a share of UK final energy to 2035 under each ZEV mandate scenario.

    .venv/bin/python scripts/electricity_share.py [--offline] [--as-of 2026-10-01] [--out outputs]

Writes:
- electricity_share_by_scenario.csv: one row per scenario x mode x year (2010-2035). It
  holds final energy and electricity (ktoe) split into parts, electricity from cars and vans,
  and the share with and without international aviation.
  - mode "eep": energy outside cars and vans follows the DESNZ Energy and Emissions
    Projections reference scenario.
  - mode "held": that energy is held at 2025 levels, so the change is the ZEV mandate's alone.
- electricity_share_lines.png (mode eep) and electricity_share_held_lines.png (mode held).
- electrification_gap_2035.png: the gap from 2025 to `--target` (default 35%, the COP31 /
  Electrify Now global goal) split into cars and vans, the rest of the economy (DESNZ), and
  what is still to find. Uses the share excluding international aviation (UK 2023: 20.4%;
  reported publicly as about 21%).

Scenarios come from zevm.scenarios.policy_paths (current mandate and the four review
options, on target and with continued use of flex). The charts show continued use of flex. The method is described in
zevm/electricity.py. `--as-of` pins the registrations data cut (default: the latest month).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

from zevm import electricity, plots, scenarios

ROOT = Path(__file__).resolve().parent.parent
HEADLINE = {
    "Current mandate | continued use of flex": ("Current mandate (cars 80% in 2030)", "Current"),
    "Review Option 1 (cars 70%, vans 60%) | continued use of flex": ("Option 1 (cars 70%)", "Opt 1"),
    "Review Option 2 (cars 60%, vans 50%) | continued use of flex": ("Option 2 (cars 60%)", "Opt 2"),
    "Review Option 3 (cars 50%, vans 40%) | continued use of flex": ("Option 3 (cars 50%)", "Opt 3"),
    "Review Option 4 (cars 80%, vans 70%, flex to 2034) | continued use of flex": ("Option 4 (cars 80%, flex to 2034)", "Opt 4"),
}
REFERENCE = "Current mandate | continued use of flex"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(ROOT / "outputs"))
    ap.add_argument("--car-market-2026", type=float, default=2.183e6)
    ap.add_argument("--growth", type=float, default=0.0, help="annual car and van market growth after 2026 (default flat)")
    ap.add_argument("--as-of", default=None, help="registrations data cut, e.g. 2026-10-01 (default: latest month)")
    ap.add_argument("--target", type=float, default=0.35, help="electrification target for the gap chart (share, e.g. 0.35)")
    ap.add_argument("--offline", action="store_true", help="use cached BigQuery results only")
    a = ap.parse_args()
    out_dir = Path(a.out); out_dir.mkdir(parents=True, exist_ok=True)

    kw = {"offline": a.offline} | ({"as_of": a.as_of} if a.as_of else {})
    paths, info = scenarios.policy_paths(a.car_market_2026, a.growth, **kw)
    result = electricity.scenario_electricity(paths)
    result.to_csv(out_dir / "electricity_share_by_scenario.csv", index=False)

    plots.use_style()
    labels, ends = {k: v[0] for k, v in HEADLINE.items()}, {k: v[1] for k, v in HEADLINE.items()}
    plots.electricity_share_lines(result, list(HEADLINE), "eep", labels, ends,
                                  title="Electricity share of UK final energy, continued use of flex").savefig(
        out_dir / "electricity_share_lines.png")
    plots.electricity_share_lines(result, list(HEADLINE), "held", labels, ends,
                                  title="Electricity share of UK final energy: cars and vans alone, continued use of flex").savefig(
        out_dir / "electricity_share_held_lines.png")
    plots.electrification_gap_bars(result, list(HEADLINE), labels, target=a.target).savefig(
        out_dir / f"electrification_gap_{int(result['year'].max())}.png")

    last = result[result["year"] == result["year"].max()]
    print(f"Wrote {out_dir}: electricity_share_by_scenario.csv ({len(result)} rows), 2 charts. Assumptions: {info}")
    print(last.pivot_table(index="scenario", columns="mode", values="electricity_share", sort=False).round(4).to_string())


if __name__ == "__main__":
    main()
