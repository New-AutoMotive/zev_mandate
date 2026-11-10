"""Electricity as a share of UK final energy consumption, by ZEV mandate scenario.

    from zevm import electricity, scenarios
    paths, _ = scenarios.policy_paths()
    out = electricity.scenario_electricity(paths)    # scenario x mode x year

How it works (ktoe throughout; method mirrors zevm.oil)
- History: DUKES Table 1.1, final consumption minus non-energy use (the headline
  definition, which includes international aviation), and its electricity column.
- From the base year, final energy is projected in parts, each anchored on the DUKES
  base-year value:
  - Car and van petrol/diesel (incl. blended biofuel): their share of DUKES road liquid
    fuel (DESNZ road fuel split) x the fleet model's fuel-use index for the scenario.
  - Car and van electricity: battery-electric vehicle miles from the fleet model, plus
    the electric share of plug-in hybrid miles, x kWh per mile x (1 + charging losses).
    It's added to DUKES base-year road electricity as a change from the modelled base
    year: DUKES records more road electricity than the modelled car and van fleet uses
    (buses, measurement), and that remainder is held.
  - HGVs, buses, motorcycles (liquid fuel): params/oil.yaml other_road.annual_change.
  - Road gas: held.
  - Everything outside road transport, in two parts (electricity, other fuels), each
    indexed to the DESNZ EEP reference projection (mode "eep"), or held at the base year
    (mode "held") to show the ZEV mandate's own effect.
- Shares: electricity / final energy, and electricity / (final energy - international
  aviation). International aviation = DUKES air x EEP's international share of aviation.

Plug-in hybrids sit inside the fleet model's hybrid group. Their stock is tracked
separately here (DfT VEH0142 / VEH1153 history, then `phev_share_of_non_bev` of each
scenario's non-battery-electric sales), only to count their electric miles. Their
petrol use stays within the fleet model's calibrated hybrid fuel use.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from zevm import fleet, oil
from zevm.rules import PARAMS_DIR, _load_raw

ROOT = Path(__file__).resolve().parent.parent
KWH_PER_KTOE = 11.63e6
PARTS = ["car_liquid", "van_liquid", "other_road_liquid", "road_gas", "road_electricity",
         "nonroad_electricity", "nonroad_other"]


@dataclass(frozen=True)
class ElecParams:
    name: str
    data: dict
    path: Path

    def __getitem__(self, key):
        return self.data[key]


def load_params(name_or_path: str | Path = "electricity") -> ElecParams:
    path = Path(name_or_path)
    if not path.suffix:
        path = PARAMS_DIR / f"{name_or_path}.yaml"
    data = _load_raw(path)
    return ElecParams(data.get("name", path.stem), data, path)


# ---------------------------------------------------------------- readers

def read_dukes_balance(path: Path, header: int, columns: dict, rows: dict) -> pd.DataFrame:
    """Selected rows x fuels of DUKES Table 1.1 for every year sheet, ktoe.
    Columns of the result: '<row>_<fuel>' (e.g. final_electricity, road_petroleum)."""
    xl = pd.ExcelFile(path, engine="calamine")
    out = {}
    for sheet in xl.sheet_names:
        if not str(sheet).strip().isdigit():
            continue
        df = pd.read_excel(xl, sheet_name=sheet, header=header)
        label = df[df.columns[0]].astype(str).str.strip()
        rec = {}
        for rk, rname in rows.items():
            r = df[label == rname]
            for fk, fcol in columns.items():
                rec[f"{rk}_{fk}"] = float(pd.to_numeric(r[fcol], errors="coerce").fillna(0.0).iloc[0]) if fcol in df and len(r) else 0.0
        out[int(sheet)] = rec
    return pd.DataFrame.from_dict(out, orient="index").sort_index().rename_axis("year")


def read_eep_fuels(path: Path, sheet: str, header: int) -> pd.DataFrame:
    """Annex F reference: one row per (coverage, fuel), years as columns (int), ktoe."""
    df = pd.read_excel(path, sheet_name=sheet, header=header, engine="calamine")
    years = {c: int(str(c).split(".")[0]) for c in df.columns if str(c)[:4].isdigit()}
    df = df.rename(columns=years)
    df["coverage"] = df["coverage"].astype(str).str.strip()
    df["fuel"] = df["fuel"].astype(str).str.strip()
    return df.set_index(["coverage", "fuel"])[list(years.values())]


def eep_series(eep: pd.DataFrame) -> pd.DataFrame:
    """The EEP aggregates the projection needs, by year (ktoe)."""
    tot, tr = "Total including international aviation", "Transport"
    g = lambda cov, fuel: eep.loc[(cov, fuel)] if (cov, fuel) in eep.index else pd.Series(0.0, index=eep.columns)
    out = pd.DataFrame({
        "total": g(tot, "Total"),
        "electricity": g(tot, "Electricity"),
        "transport_electricity": g(tr, "Electricity"),
        "road_petroleum": g(tr, "Petroleum products (Road transport)"),
        "transport_renewables": g(tr, "Renewables"),
        "transport_gas": g(tr, "Natural gas"),
        "aviation": g(tr, "Petroleum products (Aviation)"),
        "international_aviation": g("International aviation", "Petroleum products (Aviation)")
                                  + g("International aviation", "Renewables"),
    })
    out["nonroad_electricity"] = out["electricity"] - out["transport_electricity"]
    out["nonroad_other"] = (out["total"] - out["electricity"] - out["road_petroleum"]
                            - out["transport_renewables"] - out["transport_gas"])
    return out.rename_axis("year")


# ---------------------------------------------------------------- inputs

@dataclass
class ElecInputs:
    dukes: pd.DataFrame          # DUKES 1.1 rows x fuels by year
    eep: pd.DataFrame            # eep_series
    road_split: pd.DataFrame     # from zevm.oil: car/van/other shares of road fuel
    base_year: int


def load_inputs(ep: ElecParams | None = None, op: oil.OilParams | None = None) -> ElecInputs:
    ep, op = ep or load_params(), op or oil.load_params()
    d, e, rs = ep["dukes"], ep["eep"], op["road_split"]
    dukes = read_dukes_balance(ROOT / d["file"], d["header"], d["columns"], d["rows"])
    base = int(ep.data.get("base_year") or dukes.index.max())
    return ElecInputs(
        dukes=dukes,
        eep=eep_series(read_eep_fuels(ROOT / e["file"], e["sheet"], e["header"])),
        road_split=oil.read_road_split(ROOT / rs["file"], rs["car_columns"], rs["van_columns"], rs["total_column"]),
        base_year=base,
    )


def _intl_aviation_share(inp: ElecInputs, year: int) -> float:
    e = inp.eep
    y = year if year in e.index else int(e.index.min())
    return float(e.loc[y, "international_aviation"] / e.loc[y, "aviation"])


def base_parts(inp: ElecInputs) -> pd.Series:
    """Base-year final energy (excl. non-energy use) in PARTS, from DUKES (ktoe)."""
    d = inp.dukes.loc[inp.base_year]
    rs = inp.road_split
    s = rs.loc[inp.base_year] if inp.base_year in rs.index else rs.loc[rs.index.max()]
    road_liquid = d["road_petroleum"] + d["road_bioenergy"]
    final = d["final_total"] - d["non_energy_total"]
    elec = d["final_electricity"]
    return pd.Series({
        "car_liquid": road_liquid * s.car_share,
        "van_liquid": road_liquid * s.van_share,
        "other_road_liquid": road_liquid * s.other_share,
        "road_gas": d["road_gas"],
        "road_electricity": d["road_electricity"],
        "nonroad_electricity": elec - d["road_electricity"],
        "nonroad_other": final - elec - road_liquid - d["road_gas"],
    })


def history(inp: ElecInputs, start: int = 2010) -> pd.DataFrame:
    rows = []
    for y in [y for y in inp.dukes.index if start <= y <= inp.base_year]:
        d = inp.dukes.loc[y]
        final = d["final_total"] - d["non_energy_total"]
        intl = d["air_total"] * _intl_aviation_share(inp, y)
        rows.append({"year": y, "final_energy": final, "electricity": d["final_electricity"],
                     "road_electricity": d["road_electricity"], "international_aviation": intl})
    return _shares(pd.DataFrame(rows)).assign(basis="actual")


def _shares(df: pd.DataFrame) -> pd.DataFrame:
    df["electricity_share"] = df["electricity"] / df["final_energy"]
    df["electricity_share_ex_intl_aviation"] = df["electricity"] / (df["final_energy"] - df["international_aviation"])
    return df


# ---------------------------------------------------------------- EV electricity

def phev_miles(paths: pd.DataFrame, ep: ElecParams, fp: fleet.FleetParams | None = None,
               hist: fleet.FleetHistory | None = None) -> pd.DataFrame:
    """Plug-in hybrid car miles by scenario and year (stock model on PHEVs alone), and the
    base-year figure in attrs['base_phev_miles']."""
    fp = fp or fleet.load_params()
    hist = hist or fleet.load_history(fp)
    files = fp.data["files"]
    parc, regs = fleet.read_plugin_hybrids(fleet.ROOT / files["veh0142"], fleet.ROOT / files["veh1153"])
    base = hist.base_year
    share = ep["ev"]["phev_share_of_non_bev"]
    cars = paths[(paths["vehicle_type"] == "car") & (paths["year"] > base)]
    years = sorted(cars["year"].unique())
    miles = fleet.mileage_path(hist, fp, [base] + years)["hybrid"]
    out = []
    for name, g in cars.groupby("scenario", sort=False):
        g = g.set_index("year").loc[years]
        new = pd.DataFrame({"phev": share * g["total_registrations"] * (1 - g["zev_share"])})
        st = fleet.stock_model(pd.Series({"phev": float(parc.loc[base])}), regs.to_frame(), new,
                               fp.get("retirement.lifespan_years"), fp.get("retirement.spread_years"))
        out.append(pd.DataFrame({"scenario": name, "year": years,
                                 "phev_parc": st.parc["phev"].to_numpy(),
                                 "phev_miles": st.parc["phev"].to_numpy() * miles.loc[years].to_numpy()}))
    res = pd.concat(out, ignore_index=True)
    res.attrs["base_phev_miles"] = float(parc.loc[base] * miles.loc[base])
    return res


def ev_electricity_ktoe(bev_car_miles, bev_van_miles, phev_car_miles, ep: ElecParams):
    ev = ep["ev"]
    m = ev["miles_per_kwh"]
    kwh = (bev_car_miles / m["car"] + bev_van_miles / m["van"]
           + phev_car_miles * ev["phev_electric_share"] / m["phev"]) * (1 + ev["charging_losses"])
    return kwh / KWH_PER_KTOE


# ---------------------------------------------------------------- projection

def project(inp: ElecInputs, ep: ElecParams, op: oil.OilParams, car: pd.DataFrame, van: pd.DataFrame,
            phev: pd.DataFrame, mode: str = "eep", end_year: int = 2035) -> pd.DataFrame:
    base = inp.base_year
    for name, f in (("car", car), ("van", van)):
        if f.attrs.get("fleet_base_year", base) != base:
            raise ValueError(f"{name} fleet base year {f.attrs['fleet_base_year']} != base year {base}")
    b = base_parts(inp)
    e = inp.eep
    idx = (lambda col, y: e.loc[y, col] / e.loc[base, col]) if mode == "eep" else (lambda col, y: 1.0)
    trend = op["other_road"]["annual_change"] if mode == "eep" else 0.0
    ev0 = ev_electricity_ktoe(car.attrs["base_bev_miles"], van.attrs["base_bev_miles"], phev.attrs["base_phev_miles"], ep)
    df = (car.merge(van, on=["scenario", "year"], suffixes=("_car", "_van"))
             .merge(phev, on=["scenario", "year"]))
    df = df[(df["year"] > base) & (df["year"] <= end_year)].copy()
    ev = ev_electricity_ktoe(df["bev_miles_car"], df["bev_miles_van"], df["phev_miles"], ep)
    out = pd.DataFrame({
        "scenario": df["scenario"], "year": df["year"],
        "car_liquid": b.car_liquid * df["fuel_index_car"],
        "van_liquid": b.van_liquid * df["fuel_index_van"],
        "other_road_liquid": b.other_road_liquid * (1 + trend) ** (df["year"] - base),
        "road_gas": b.road_gas,
        "road_electricity": b.road_electricity + ev - ev0,
        "nonroad_electricity": [b.nonroad_electricity * idx("nonroad_electricity", y) for y in df["year"]],
        "nonroad_other": [b.nonroad_other * idx("nonroad_other", y) for y in df["year"]],
        "ev_electricity": ev,
    })
    out["final_energy"] = out[PARTS].sum(axis=1)
    out["electricity"] = out["road_electricity"] + out["nonroad_electricity"]
    intl0 = inp.dukes.loc[base, "air_total"] * _intl_aviation_share(inp, base)
    out["international_aviation"] = [intl0 * idx("international_aviation", y) for y in out["year"]]
    return _shares(out.reset_index(drop=True)).assign(basis="projected", mode=mode)


def scenario_electricity(paths: pd.DataFrame, inp: ElecInputs | None = None, ep: ElecParams | None = None,
                         op: oil.OilParams | None = None, start: int = 2010, end_year: int = 2035) -> pd.DataFrame:
    """History (repeated per scenario and mode) + projection for each rest-of-economy mode."""
    ep, op = ep or load_params(), op or oil.load_params()
    inp = inp or load_inputs(ep, op)
    car, van = oil.fleet_fuel(paths, "car"), oil.fleet_fuel(paths, "van")
    ph = phev_miles(paths, ep)
    past = history(inp, start)
    frames = []
    for mode in ep["rest_of_economy"]["modes"]:
        proj = project(inp, ep, op, car, van, ph, mode, end_year)
        frames += [past.assign(scenario=s, mode=mode) for s in proj["scenario"].unique()] + [proj]
    cols = ["scenario", "mode", "year", "basis", *PARTS, "ev_electricity", "final_energy", "electricity",
            "international_aviation", "electricity_share", "electricity_share_ex_intl_aviation"]
    out = pd.concat(frames, ignore_index=True).reindex(columns=cols)
    return out.sort_values(["mode", "scenario", "year"], kind="stable").reset_index(drop=True)
