"""UK oil demand and the oil intensity of GDP, by ZEV mandate scenario.

    from zevm import oil, scenarios
    op = oil.load_params()                         # params/oil.yaml
    inputs = oil.load_inputs(op)                   # DUKES, DESNZ road fuel + EEP, ONS/OBR GDP
    paths, _ = scenarios.policy_paths()            # or any frame with the same columns
    out = oil.scenario_oil(paths, inputs, op)      # scenario x year: oil by component, GDP, intensity

How it works
- History (to `base_year`): oil = DUKES total demand for oil products (thousand tonnes).
  Road is the DUKES road row. It's split into cars, vans and other road using that year's
  shares of road fuel in the DESNZ sub-national tables.
- Projection: oil(y) = cars(y) + vans(y) + other road(y) + non-road(y), each anchored on its
  base-year DUKES value:
  - Cars and vans: the base-year value × the fleet model's fuel use in year y ÷ its
    base-year fuel use (zevm.fleet, run on the scenario's registrations). This is the
    only part that differs between scenarios.
  - Other road (HGVs, buses, motorcycles): grows at `other_road.annual_change`.
  - Non-road: the DESNZ EEP reference projection's petroleum total (incl. international
    aviation) minus its road transport, indexed to the base year.
- GDP: ONS ABMI to the base year, then `gdp.growth` (OBR). Intensity = oil ÷ real GDP,
  tonnes per £m (2024 prices), and as an index with the base year = 100.

Why anchored indices: DUKES (fossil oil by mass), DESNZ road fuel (ktoe, includes
biofuel, bottom-up) and EEP (ktoe, modelled from 2024) don't add up to each other exactly.
Each source only contributes growth rates or shares, and the levels are DUKES.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from zevm import fleet
from zevm.rules import PARAMS_DIR, _load_raw, year_value

ROOT = Path(__file__).resolve().parent.parent
UK_CODE = "K02000001"


@dataclass(frozen=True)
class OilParams:
    name: str
    data: dict
    path: Path

    def __getitem__(self, key):
        return self.data[key]


def load_params(name_or_path: str | Path = "oil") -> OilParams:
    path = Path(name_or_path)
    if not path.suffix:
        path = PARAMS_DIR / f"{name_or_path}.yaml"
    data = _load_raw(path)
    return OilParams(data.get("name", path.stem), data, path)


# ---------------------------------------------------------------- readers

def read_dukes(path: Path, header: int = 3, total_row: str = "Total demand", road_row: str = "Road") -> pd.DataFrame:
    """Total and road demand for oil products (thousand tonnes) from DUKES 3.2, one sheet per year."""
    xl = pd.ExcelFile(path, engine="calamine")
    rows = {}
    for sheet in xl.sheet_names:
        if not str(sheet).strip().isdigit():
            continue
        df = pd.read_excel(xl, sheet_name=sheet, header=header)
        label = df[df.columns[0]].astype(str).str.strip()
        val = lambda name: float(df.loc[label == name, "Total products"].iloc[0])
        rows[int(sheet)] = {"total_kt": val(total_row), "road_kt": val(road_row)}
    return pd.DataFrame.from_dict(rows, orient="index").sort_index().rename_axis("year")


def read_road_split(path: Path, car_columns: list[str], van_columns: list[str], total_column: str) -> pd.DataFrame:
    """Shares of UK road fuel used by cars, vans and everything else, by year (DESNZ sub-national tables)."""
    xl = pd.ExcelFile(path, engine="calamine")
    rows = {}
    for sheet in xl.sheet_names:
        if not str(sheet).strip().isdigit():
            continue
        raw = pd.read_excel(xl, sheet_name=sheet, header=None)
        hdr = raw.index[raw.apply(lambda r: r.astype(str).str.contains("Local Authority Code", regex=False).any(), axis=1)][0]
        df = raw.iloc[hdr + 1:].set_axis([str(c).split("[")[0].replace("\n", "").strip() for c in raw.iloc[hdr]], axis=1)
        uk = df[df["Local Authority Code"] == UK_CODE].iloc[0]
        clean = lambda c: c.split("[")[0].replace("\n", "").strip()
        num = lambda cols: sum(float(uk[clean(c)]) for c in cols if clean(c) in uk.index)
        total = float(uk[clean(total_column)])
        rows[int(sheet)] = {"car_share": num(car_columns) / total, "van_share": num(van_columns) / total}
    out = pd.DataFrame.from_dict(rows, orient="index").sort_index().rename_axis("year")
    out["other_share"] = 1 - out["car_share"] - out["van_share"]
    return out


def read_eep(path: Path, sheet: str, header: int, total: dict, road: dict) -> pd.DataFrame:
    """DESNZ EEP Annex F petroleum (ktoe): total incl. international aviation, road, and non-road."""
    df = pd.read_excel(path, sheet_name=sheet, header=header, engine="calamine")
    years = {c: int(str(c).split(".")[0]) for c in df.columns if str(c)[:4].isdigit()}
    pick = lambda spec: df[(df["coverage"].astype(str).str.strip() == spec["coverage"]) &
                           (df["fuel"].astype(str).str.strip() == spec["fuel"])].iloc[0]
    t, r = pick(total), pick(road)
    out = pd.DataFrame({"total_ktoe": [float(t[c]) for c in years], "road_ktoe": [float(r[c]) for c in years]},
                       index=pd.Index(list(years.values()), name="year"))
    out["non_road_ktoe"] = out["total_ktoe"] - out["road_ktoe"]
    return out


def read_gdp(path: Path) -> pd.Series:
    """ONS ABMI annual real GDP (£m) from the ONS time-series CSV."""
    raw = pd.read_csv(path, header=None, names=["period", "value"], dtype=str)
    annual = raw[raw["period"].str.fullmatch(r"\d{4}", na=False)]
    return pd.Series(annual["value"].astype(float).to_numpy(), index=annual["period"].astype(int).to_numpy(),
                     name="gdp_m").rename_axis("year")


def gdp_path(history: pd.Series, growth: dict, end_year: int) -> pd.Series:
    """Real GDP: history, then compounded growth (year table) to end_year. `basis` is in the name only."""
    g = history.copy()
    for y in range(int(history.index.max()) + 1, end_year + 1):
        g.loc[y] = g.loc[y - 1] * (1 + year_value(growth, y))
    return g.sort_index()


# ---------------------------------------------------------------- inputs

@dataclass
class OilInputs:
    dukes: pd.DataFrame            # year: total_kt, road_kt
    road_split: pd.DataFrame       # year: car_share, van_share, other_share
    eep: pd.DataFrame              # year: total_ktoe, road_ktoe, non_road_ktoe
    gdp: pd.Series                 # year: £m, ONS history then OBR growth
    base_year: int


def load_inputs(op: OilParams | None = None, end_year: int = 2035) -> OilInputs:
    op = op or load_params()
    p = lambda f: ROOT / f
    d, rs, nr, g = op["dukes"], op["road_split"], op["non_road"], op["gdp"]
    dukes = read_dukes(p(d["file"]), d["header"], d["total_row"], d["road_row"])
    base = int(op.data.get("base_year") or dukes.index.max())
    if base not in dukes.index:
        raise ValueError(f"No DUKES data for base year {base}")
    return OilInputs(
        dukes=dukes,
        road_split=read_road_split(p(rs["file"]), rs["car_columns"], rs["van_columns"], rs["total_column"]),
        eep=read_eep(p(nr["file"]), nr["sheet"], nr["header"], nr["total"], nr["road"]),
        gdp=gdp_path(read_gdp(p(g["file"])), g["growth"], end_year),
        base_year=base,
    )


def _split(inputs: OilInputs, year: int) -> pd.Series:
    rs = inputs.road_split
    return rs.loc[year] if year in rs.index else rs.loc[rs.index.max()]


def history(inputs: OilInputs, start: int = 2010) -> pd.DataFrame:
    """Observed years: oil by component (thousand tonnes), GDP and intensity."""
    rows = []
    for y in [y for y in inputs.dukes.index if start <= y <= inputs.base_year]:
        tot, road = inputs.dukes.loc[y, ["total_kt", "road_kt"]]
        s = _split(inputs, y)
        rows.append({"year": y, "car_kt": road * s.car_share, "van_kt": road * s.van_share,
                     "other_road_kt": road * s.other_share, "non_road_kt": tot - road, "oil_kt": tot,
                     "road_split_year": y if y in inputs.road_split.index else int(inputs.road_split.index.max())})
    return _finish(pd.DataFrame(rows), inputs).assign(basis="actual")


def _finish(df: pd.DataFrame, inputs: OilInputs) -> pd.DataFrame:
    df["gdp_m"] = df["year"].map(inputs.gdp)
    df["oil_t_per_gdp_m"] = df["oil_kt"] * 1e3 / df["gdp_m"]
    base = inputs.dukes.loc[inputs.base_year, "total_kt"] * 1e3 / inputs.gdp.loc[inputs.base_year]
    df["intensity_index"] = 100 * df["oil_t_per_gdp_m"] / base
    return df


# ---------------------------------------------------------------- scenarios

def fleet_fuel(paths: pd.DataFrame, vehicle_type: str, fp: fleet.FleetParams | None = None,
               hist: fleet.FleetHistory | None = None) -> pd.DataFrame:
    """Fuel use (ktoe) and battery-electric vehicle miles by scenario and year from the fleet
    model, with fuel as an index on the fleet's base-year fuel use. `paths`: scenario,
    vehicle_type, year, total_registrations, zev_share, basis (e.g. scenarios.policy_paths).
    attrs: fleet_base_year, base_fuel_ktoe, base_bev_miles."""
    fp = fp or fleet.load_params("fleet" if vehicle_type == "car" else "fleet_van")
    hist = hist or fleet.load_history(fp)
    summary = paths[paths["vehicle_type"] == vehicle_type].drop(columns="vehicle_type")
    base = base_fuel(hist, fp)
    parts = []
    # A scenario's `nonzev_co2_change` (annual change in new non-ZEV CO2) becomes the fleet
    # model's change in new-car fuel per mile, one-for-one. That's an upper bound on the
    # saving: real-world fuel use falls less than test CO2, especially for plug-in hybrids.
    change = summary["nonzev_co2_change"] if "nonzev_co2_change" in summary else pd.Series(0.0, index=summary.index)
    for c, g in summary.groupby(change, sort=False):
        fpc = fp
        if c:
            data = {**fp.data, "fuel_use": {**fp.data["fuel_use"], "new_annual_change": {f: float(c) for f in fleet.LIQUID}}}
            fpc = fleet.FleetParams(fp.name, data, fp.path)
        regs = fleet.registrations_from_summary(g.drop(columns="nonzev_co2_change", errors="ignore"), hist, fpc)
        parts.append(fleet.run_many(hist, regs, fpc, include_history=False))
    out = pd.concat(parts, ignore_index=True)
    order = list(dict.fromkeys(summary["scenario"]))
    fuel = out.groupby(["scenario", "year"])["fuel_ktoe"].sum().rename("fuel_ktoe").reset_index()
    bev = out[out["fuel"] == "bev"].groupby(["scenario", "year"])["vehicle_miles"].sum().rename("bev_miles")
    fuel = fuel.merge(bev.reset_index(), on=["scenario", "year"], how="left")
    fuel = fuel.sort_values("scenario", key=lambda s: s.map(order.index), kind="stable").reset_index(drop=True)
    fuel["fuel_index"] = fuel["fuel_ktoe"] / base
    miles0 = fleet.mileage_path(hist, fp, [hist.base_year]).loc[hist.base_year]
    fuel.attrs.update(fleet_base_year=hist.base_year, base_fuel_ktoe=base,
                      base_bev_miles=float(hist.parc.loc[hist.base_year, "bev"] * miles0["bev"]))
    return fuel


def base_fuel(hist: fleet.FleetHistory, fp: fleet.FleetParams) -> float:
    """Modelled fuel use (ktoe) of the base-year parc, on the same calibration as the projection."""
    cal = fleet.calibration(hist, fp)
    miles = fleet.mileage_path(hist, fp, [hist.base_year]).loc[hist.base_year]
    parc = hist.parc.loc[hist.base_year, fleet.FUELS]
    return float((parc * miles * cal["fuel_per_mile"]).fillna(0.0).sum())


def project(inputs: OilInputs, op: OilParams, car: pd.DataFrame, van: pd.DataFrame, end_year: int = 2035) -> pd.DataFrame:
    """Projected oil by scenario-year. `car`/`van` are fleet_fuel outputs; their fleet base
    year must equal the oil base year (the indices are relative to it)."""
    base = inputs.base_year
    for name, f in (("car", car), ("van", van)):
        if f.attrs.get("fleet_base_year", base) != base:
            raise ValueError(f"{name} fleet base year {f.attrs['fleet_base_year']} != oil base year {base}")
    road0 = inputs.dukes.loc[base, "road_kt"]
    s = _split(inputs, base)
    car0, van0, other0 = road0 * s.car_share, road0 * s.van_share, road0 * s.other_share
    non_road0 = inputs.dukes.loc[base, "total_kt"] - road0
    eep = inputs.eep["non_road_ktoe"]
    chg = op["other_road"]["annual_change"]
    years = range(base + 1, end_year + 1)
    idx = car.merge(van, on=["scenario", "year"], suffixes=("_car", "_van"))
    idx = idx[idx["year"].isin(years)]
    df = pd.DataFrame({
        "scenario": idx["scenario"], "year": idx["year"],
        "car_kt": car0 * idx["fuel_index_car"], "van_kt": van0 * idx["fuel_index_van"],
        "other_road_kt": other0 * (1 + chg) ** (idx["year"] - base),
        "non_road_kt": non_road0 * idx["year"].map(eep) / eep.loc[base],
    })
    df["oil_kt"] = df[["car_kt", "van_kt", "other_road_kt", "non_road_kt"]].sum(axis=1)
    return _finish(df.reset_index(drop=True), inputs).assign(basis="projected")


def scenario_oil(paths: pd.DataFrame, inputs: OilInputs | None = None, op: OilParams | None = None,
                 start: int = 2010, end_year: int = 2035) -> pd.DataFrame:
    """History (repeated per scenario) + projection, one row per scenario-year."""
    op = op or load_params()
    inputs = inputs or load_inputs(op, end_year)
    car, van = fleet_fuel(paths, "car"), fleet_fuel(paths, "van")
    proj = project(inputs, op, car, van, end_year)
    hist = history(inputs, start)
    past = pd.concat([hist.assign(scenario=s) for s in proj["scenario"].unique()], ignore_index=True)
    cols = ["scenario", "year", "basis", "car_kt", "van_kt", "other_road_kt", "non_road_kt", "oil_kt",
            "gdp_m", "oil_t_per_gdp_m", "intensity_index"]
    return pd.concat([past, proj], ignore_index=True)[cols].sort_values(["scenario", "year"], kind="stable").reset_index(drop=True)


def road_history(inputs: OilInputs) -> pd.DataFrame:
    """DUKES road oil split by vehicle group, thousand tonnes: the evidence behind other_road.annual_change."""
    h = history(inputs, start=int(inputs.road_split.index.min()))
    return h.set_index("year")[["car_kt", "van_kt", "other_road_kt"]]
