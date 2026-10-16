"""Export ZEV mandate scenarios in the ev_bill_model.py scenario-file format.

    .venv/bin/python scripts/export_ev_bill_scenarios.py [--out outputs/ev_bill_scenarios.csv] [--offline]

Output: a long CSV with columns scenario, year, vehicle_type (bev_car, phev_car, bev_van),
measure, value. Every value is `sales` (new registrations) for 2026-2035. There are no 2025
stock rows, so ev_bill_model uses its default opening fleet and shows a warning.

Scenarios
- "<policy> | on target": the market ZEV share equals that policy's headline target each
  year. Policies: current mandate (law to 2030 plus the indicative path to 2035) and the
  four ZEV Mandate Review options (params/scenarios/review_option*.yaml). Option 4 on
  target is identical to the current mandate, because its targets are the same; it differs
  only through flexibility use.
- "<policy> | heavy flex": the lowest ZEV share that still complies each year
  (zevm.scenarios.minimum_compliant_path). The market is pooled, CO2->ZEV credits and
  banked allowances are used in full, there's no borrowing, and non-ZEV CO2 is held at its
  2026 level. The share never falls below 2026's.
- "Review Option 3 | heavy flex, non-ZEV CO2 -6%/yr": the worst case, where manufacturers
  also cut non-ZEV CO2 hard to earn more credits.

Assumptions (change with the flags)
- Car market: SMMT's 2026 forecast, 2.183M, growing 1.5% a year.
- Van market: our 2026 VES estimate scaled by the same VES-to-SMMT ratio as cars, growing
  1.5% a year.
- 2026 BEV shares: our full-year estimates (scenarios.prepare). 2026 is the same in every
  scenario.
- PHEV car sales: 20% of non-BEV car sales (the ECC share for Apr-Sep 2026), held
  constant. The model doesn't project PHEVs. The -6%/yr CO2 case would in reality need a
  higher PHEV share.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from zevm import rules, scenarios

ROOT = Path(__file__).resolve().parent.parent
YEARS = range(2026, 2036)
POLICIES = {
    "Current mandate": "scenarios/indicative_to_2035",
    "Review Option 1 (cars 70%, vans 60%)": "scenarios/review_option1_cars70_vans60",
    "Review Option 2 (cars 60%, vans 50%)": "scenarios/review_option2_cars60_vans50",
    "Review Option 3 (cars 50%, vans 40%)": "scenarios/review_option3_cars50_vans40",
    "Review Option 4 (cars 80%, vans 70%, flex to 2034)": "scenarios/review_option4_cars80_vans70_extended",
}
VEHICLE_TYPES = ("bev_car", "phev_car", "bev_van")
MEASURES = ("sales", "stock")


def build(car_market_2026: float, growth: float, phev_share: float, offline: bool) -> tuple[pd.DataFrame, dict]:
    kw = {"offline": offline}
    hc, hv = scenarios.prepare("car", **kw), scenarios.prepare("van", **kw)

    def last(h):
        y = h.yearly[h.yearly["year"] == 2026]
        return y["total_registrations"].sum(), y["zev_registrations"].sum() / y["total_registrations"].sum()

    car_ves, car_share26 = last(hc)
    van_ves, van_share26 = last(hv)
    scale = car_market_2026 / car_ves
    markets = {
        "car": {y: car_market_2026 * (1 + growth) ** (y - 2026) for y in YEARS},
        "van": {y: van_ves * scale * (1 + growth) ** (y - 2026) for y in YEARS},
    }
    share26 = {"car": car_share26, "van": van_share26}
    hist = {"car": hc, "van": hv}
    proj_years = [y for y in YEARS if y > 2026]

    def on_target(params, vt):
        p = rules.load(params)
        return {2026: share26[vt], **{y: p.zev_target(vt, y) for y in proj_years}}

    def heavy_flex(params, vt, co2=0.0):
        mp = scenarios.minimum_compliant_path(
            hist[vt], params, {y: markets[vt][y] for y in proj_years}, nonzev_co2_change=co2,
            years=proj_years, floor=share26[vt])
        return {2026: share26[vt], **mp["min_zev_share"].to_dict()}

    paths = {}
    for name, params in POLICIES.items():
        paths[f"{name} | on target"] = {vt: on_target(params, vt) for vt in ("car", "van")}
    for name, params in POLICIES.items():
        paths[f"{name} | heavy flex"] = {vt: heavy_flex(params, vt) for vt in ("car", "van")}
    o3 = POLICIES["Review Option 3 (cars 50%, vans 40%)"]
    paths["Review Option 3 (cars 50%, vans 40%) | heavy flex, non-ZEV CO2 -6%/yr"] = {
        "car": heavy_flex(o3, "car", -0.06), "van": heavy_flex(o3, "van")}

    rows = []
    for scen, by_vt in paths.items():
        for y in YEARS:
            cars, vans = markets["car"][y], markets["van"][y]
            bev_car = by_vt["car"][y] * cars
            rows += [
                (scen, y, "bev_car", "sales", bev_car),
                (scen, y, "phev_car", "sales", phev_share * (cars - bev_car)),
                (scen, y, "bev_van", "sales", by_vt["van"][y] * vans),
            ]
    df = pd.DataFrame(rows, columns=["scenario", "year", "vehicle_type", "measure", "value"])
    df["value"] = df["value"].round(0)
    info = {"car_market_2026": car_market_2026, "van_market_2026": markets["van"][2026], "growth": growth,
            "car_bev_share_2026": car_share26, "van_bev_share_2026": van_share26, "phev_share_of_non_bev": phev_share}
    return df, info


def validate(df: pd.DataFrame) -> list[str]:
    """Check the rules in the ev_bill_model scenario-file spec. Returns a list of problems."""
    problems = []
    cols = {c.lower() for c in df.columns}
    for c in ("scenario", "year", "vehicle_type", "measure", "value"):
        if c not in cols:
            problems.append(f"missing column {c}")
    if problems:
        return problems
    if not df["vehicle_type"].isin(VEHICLE_TYPES).all():
        problems.append(f"unknown vehicle_type: {set(df.vehicle_type) - set(VEHICLE_TYPES)}")
    if not df["measure"].isin(MEASURES).all():
        problems.append(f"unknown measure: {set(df.measure) - set(MEASURES)}")
    if df["value"].isna().any() or (df["value"] < 0).any():
        problems.append("blank or negative values")
    if not df["year"].between(2025, 2035).all():
        problems.append("years outside 2025-2035")
    if ((df["year"] == 2025) & (df["measure"] == "sales")).any():
        problems.append("sales rows for 2025 (would be ignored)")
    key = ["scenario", "year", "vehicle_type", "measure"]
    if df.duplicated(key).any():
        problems.append(f"{df.duplicated(key).sum()} duplicate rows")
    for (s, vt), g in df.groupby(["scenario", "vehicle_type"]):
        if g["measure"].nunique() > 1:
            problems.append(f"{s} / {vt}: mixed measures")
        missing = set(YEARS) - set(g.loc[g.year >= 2026, "year"])
        if missing:
            problems.append(f"{s} / {vt}: missing years {sorted(missing)}")
    return problems


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(ROOT / "outputs" / "ev_bill_scenarios.csv"))
    ap.add_argument("--car-market-2026", type=float, default=2.183e6)
    ap.add_argument("--growth", type=float, default=0.015)
    ap.add_argument("--phev-share", type=float, default=0.20, help="PHEV share of non-BEV car sales")
    ap.add_argument("--offline", action="store_true", help="use cached BigQuery results only")
    a = ap.parse_args()

    df, info = build(a.car_market_2026, a.growth, a.phev_share, a.offline)
    problems = validate(df)
    if problems:
        raise SystemExit("Output fails the scenario-file spec:\n  " + "\n  ".join(problems))
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Wrote {out} ({len(df)} rows, {df.scenario.nunique()} scenarios). Assumptions: {info}")


if __name__ == "__main__":
    main()
