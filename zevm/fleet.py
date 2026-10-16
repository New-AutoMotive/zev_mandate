"""Car fleet (parc) and tailpipe emissions: new registrations -> parc -> vehicle miles -> fuel -> CO2.

    from zevm import fleet, scenarios
    fp = fleet.load_params()                       # params/fleet.yaml
    hist = fleet.load_history(fp)                  # DfT/DESNZ/NAEI tables in data/
    summary = scenarios.market_summary(res)        # res from scenarios.run_many
    regs = fleet.registrations_from_summary(summary, hist, fp, extend_to=2035)
    out = fleet.run_many(hist, regs, fp)           # tidy: scenario x year x fuel
    fleet.totals(out)                              # one row per scenario-year
    fleet.validate(hist, fp)                       # fuel-based CO2 vs NAEI, by year

A port of archive notebook "01) UK Car Scenarios", Parts 1-3. Assumptions and sources are
in params/fleet.yaml.

How it works
1. The parc starts from the DfT year-end (Q4) count in `base_year`, by fuel group.
2. New registrations come from a mandate scenario: ZEVs are "bev"; non-ZEVs are split into
   petrol / diesel / hybrid by `registrations.nonzev_split`.
3. Retirements. A car leaves the parc between ages lifespan - spread/2 and lifespan +
   spread/2, evenly (the notebook's fixed lifespan with a rolling mean). Cars on the road in
   the base year retire on the same schedule applied to their registration years (VEH1153),
   scaled per fuel so that exactly the base-year parc retires. Cars registered after the
   base year retire unscaled. So parc[t] = parc[t-1] + additions[t] - retirements[t] holds
   exactly and no cohort retires more cars than it had.
4. Vehicle miles = parc x mean annual mileage per fuel (MOT data), held at its last value.
5. Fuel. Fuel per vehicle mile is calibrated from DESNZ fuel consumption over the last few
   years with data. Hybrids burn `hybrid_vs_petrol` times a petrol car's fuel per mile.
   Cars registered after the base year use `new_vs_fleet` times the base-year fleet figure.
   The fleet figure is a vehicle-weighted mean of fuel per mile, not of MPG.
6. CO2 = fuel (toe) x kWh/toe x DESNZ kg CO2e/kWh, optionally scaled to NAEI 1A3bi.

Differences from the notebook (bugs and fudges not carried forward)
- The notebook converted ktoe to gallons with petrol 0.86 / diesel 0.98 and back with the
  two swapped, and used 2023 emission factors for history but 2021 ones for projections.
  Everything here stays in toe, with one set of factors.
- Its retirement factor also scaled the retirement of new cars, so a cohort could retire
  more cars than were registered. Here it applies to the base-year stock only.
- Fleet fuel economy was an arithmetic mean of MPG; fuel use needs the mean of fuel per
  mile, which is what is averaged here.
- VEH0105 "Other fuels" already contains the plug-ins listed in VEH0142, so only the
  VEH0142 plug-in counts are used for them.
- Pre-2015 UK registrations are GB x the UK/GB ratio, not GB + a BigQuery NI pull.
  They only shape the retirement schedule, which is rescaled to the parc anyway.

Not modelled: mileage by age, scrappage responding to sales, electricity emissions,
other fuels (gas etc.), vans.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from zevm import rules
from zevm.rules import PARAMS_DIR, _load_raw, year_value

ROOT = Path(__file__).resolve().parent.parent
FUELS = ["petrol", "diesel", "hybrid", "bev"]
LIQUID = ["petrol", "diesel", "hybrid"]
BURNS = {"petrol": "petrol", "diesel": "diesel", "hybrid": "petrol"}   # which fuel each group uses

# DfT fuel type -> fuel group. None = left out (gas, other).
DFT_FUEL_MAP = {
    "Petrol": "petrol",
    "Diesel": "diesel",
    "Hybrid electric (petrol)": "hybrid",
    "Plug-in hybrid electric (petrol)": "hybrid",
    "Range extended electric": "hybrid",
    "Hybrid electric (diesel)": "diesel",
    "Plug-in hybrid electric (diesel)": "diesel",
    "Battery electric": "bev",
    "Fuel cell electric": "bev",
    "Gas": None, "Others": None, "Other fuels": None,
}
MILEAGE_FUEL_MAP = {"Petrol": "petrol", "Diesel": "diesel", "Hybrid": "hybrid", "Pure electric": "bev"}
UK_CODE = "K02000001"
OUTPUT_COLUMNS = ["parc", "new_registrations", "retirements", "vehicle_miles", "fuel_ktoe", "co2_mt"]


# ---------------------------------------------------------------- params

@dataclass(frozen=True)
class FleetParams:
    name: str
    data: dict
    path: Path

    def get(self, dotted_key: str, year: int | None = None) -> Any:
        """e.g. fp.get("retirement.lifespan_years"), fp.get("registrations.nonzev_split.hybrid", 2027)."""
        node: Any = self.data
        for part in dotted_key.split("."):
            node = node[part]
        return year_value(node, year) if year is not None else node


def load_params(name_or_path: str | Path = "fleet") -> FleetParams:
    path = Path(name_or_path)
    if not path.suffix:
        path = PARAMS_DIR / f"{name_or_path}.yaml"
    data = _load_raw(path)
    r = data["retirement"]
    if r["lifespan_years"] - (r["spread_years"] - 1) // 2 < 1:
        raise ValueError(f"{path}: retirement spread reaches age 0")
    return FleetParams(name=data.get("name", path.stem), data=data, path=path)


# ---------------------------------------------------------------- stock model (pure)

def retirement_kernel(lifespan: int, spread: int) -> pd.Series:
    """Share of a cohort retiring at each age. Matches a centred pandas rolling mean of
    width `spread` applied to registrations shifted by `lifespan` (ages L-4..L+5 for 18, 10)."""
    ages = np.arange(lifespan - (spread - 1) // 2, lifespan + spread // 2 + 1)
    return pd.Series(1.0 / len(ages), index=ages)


def _survival(kernel: pd.Series, max_age: int) -> np.ndarray:
    """Share of a cohort still on the road at the end of each age 0..max_age."""
    w = kernel.reindex(np.arange(max_age + 1), fill_value=0.0).to_numpy()
    return 1.0 - np.cumsum(w)


@dataclass
class Stock:
    """Stock model output. Frames are year x fuel; `survivors` is year x cohort x fuel."""
    parc: pd.DataFrame
    additions: pd.DataFrame
    retirements: pd.DataFrame
    existing: pd.DataFrame          # base-year stock still on the road
    survivors: np.ndarray           # cars from each post-base cohort still on the road
    retirement_factor: pd.Series    # scale on the base-year stock's retirements


def stock_model(parc0: pd.Series, hist_regs: pd.DataFrame, new_regs: pd.DataFrame,
                lifespan: int = 18, spread: int = 10) -> Stock:
    """Roll the parc forward from `parc0` (end of the year before new_regs starts).

    hist_regs: registrations by year x fuel up to the base year (shapes when the existing
    stock retires; years before it starts copy its first year). new_regs: registrations
    by year x fuel for consecutive years after it.
    """
    fuels = list(parc0.index)
    years = np.asarray(new_regs.index, dtype=int)
    if len(years) and not np.array_equal(years, np.arange(years[0], years[0] + len(years))):
        raise ValueError("new_regs must cover consecutive years")
    base = int(years[0]) - 1
    kernel = retirement_kernel(lifespan, spread)
    max_age = int(kernel.index.max())
    surv = _survival(kernel, max_age + len(years) + 200)
    w = kernel.reindex(np.arange(len(surv)), fill_value=0.0).to_numpy()

    H = hist_regs.reindex(columns=fuels).fillna(0.0)
    H = H[H.index <= base]
    # Cohorts older than the registration series (VEH1153 starts in 2001) are assumed to
    # match its first year, so the existing stock has a full age range to retire from.
    first = int(H.index.min())
    if first > base - max_age:
        H = pd.concat([pd.DataFrame([H.iloc[0].to_numpy()] * (first - base + max_age), columns=fuels,
                                    index=range(base - max_age, first)), H])
    age_h = base - H.index.to_numpy(dtype=int)                       # age at end of base year
    T = len(years)
    # Unscaled future retirements of the historic cohorts, t = base+1 .. base+T.
    future_age = age_h[None, :] + np.arange(1, T + 1)[:, None]       # T x R
    R0 = np.einsum("tr,rf->tf", w[np.minimum(future_age, len(w) - 1)], H.to_numpy())
    alive = surv[np.minimum(age_h, len(surv) - 1)] @ H.to_numpy()   # historic regs still alive
    factor = pd.Series(np.where(alive > 0, parc0.to_numpy() / np.where(alive > 0, alive, 1), np.nan), index=fuels)
    retire_ex = R0 * np.nan_to_num(factor.to_numpy())
    # A fuel with stock but no registration history retires evenly over `lifespan` years.
    for j in np.flatnonzero(np.isnan(factor.to_numpy())):
        retire_ex[:min(T, lifespan), j] = parc0.iloc[j] / lifespan
    existing = parc0.to_numpy() - np.cumsum(retire_ex, axis=0)

    N = new_regs.reindex(columns=fuels).fillna(0.0).to_numpy()       # C x F (cohort = year)
    age = years[:, None] - years[None, :]                           # t x c
    S = np.where(age >= 0, surv[np.clip(age, 0, len(surv) - 1)], 0.0)
    W = np.where(age >= 1, w[np.clip(age, 0, len(w) - 1)], 0.0)
    survivors = S[:, :, None] * N[None, :, :]                        # t x c x f
    retire_new = np.einsum("tc,cf->tf", W, N)

    frame = lambda a: pd.DataFrame(a, index=pd.Index(years, name="year"), columns=fuels)
    return Stock(
        parc=frame(existing + survivors.sum(axis=1)),
        additions=frame(N),
        retirements=frame(retire_ex + retire_new),
        existing=frame(existing),
        survivors=survivors,
        retirement_factor=factor,
    )


def fuel_use(stock: Stock, mileage: pd.DataFrame, base_fuel_per_mile: pd.Series,
             new_vs_fleet: pd.Series, new_annual_change: pd.Series) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Vehicle miles and fuel (same unit as base_fuel_per_mile x miles) by year x fuel.

    The existing stock keeps the base-year fuel per mile; each new cohort c uses
    base x new_vs_fleet x (1 + new_annual_change) ** (c - first projected year).
    """
    fuels = list(stock.parc.columns)
    years = stock.parc.index.to_numpy()
    miles = mileage.reindex(index=years, columns=fuels).to_numpy()
    e0 = base_fuel_per_mile.reindex(fuels).fillna(0.0).to_numpy()
    rel = new_vs_fleet.reindex(fuels).fillna(1.0).to_numpy()
    chg = new_annual_change.reindex(fuels).fillna(0.0).to_numpy()
    e_new = e0 * rel * (1 + chg) ** (years - years[0])[:, None]     # c x f
    weighted = stock.existing.to_numpy() * e0 + np.einsum("tcf,cf->tf", stock.survivors, e_new)
    frame = lambda a: pd.DataFrame(a, index=stock.parc.index, columns=fuels)
    return frame(stock.parc.to_numpy() * miles), frame(weighted * miles)


def co2_mt(fuel_ktoe: pd.DataFrame, fp: FleetParams) -> pd.DataFrame:
    """Tailpipe Mt CO2e from fuel in ktoe; BEV columns are zero."""
    kwh = fuel_ktoe * 1e3 * fp.get("emissions.kwh_per_toe")
    factors = pd.Series({f: fp.get("emissions.kg_co2e_per_kwh")[BURNS[f]] if f in BURNS else 0.0 for f in fuel_ktoe.columns})
    return kwh * factors / 1e9


# ---------------------------------------------------------------- data

@dataclass
class FleetHistory:
    parc: pd.DataFrame              # year-end parc, year x fuel (UK)
    registrations: pd.DataFrame     # new registrations, year x fuel (UK; pre-2015 = GB x ratio)
    mileage: pd.DataFrame           # mean annual miles per car, year x fuel
    fuel: pd.DataFrame              # ktoe: columns petrol_cars, diesel_cars
    naei_mt: pd.Series              # Mt CO2e, NAEI 1A3bi
    base_year: int
    uk_gb_ratio: float
    notes: dict = field(default_factory=dict)


def _read_ods(path: Path, sheet: str, header: int | None = 4) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet, header=header, engine="calamine")
    if header is not None:
        df.columns = [str(c).split(" [")[0].strip() for c in df.columns]
    return df


def _group_fuels(wide: pd.DataFrame) -> pd.DataFrame:
    """Sum DfT fuel-type columns into FUELS."""
    out = pd.DataFrame(0.0, index=wide.index, columns=FUELS)
    for col, g in DFT_FUEL_MAP.items():
        if g and col in wide:
            out[g] += pd.to_numeric(wide[col], errors="coerce").fillna(0.0)
    return out


def read_parc(veh0105: Path, veh0142: Path, body_type: str = "Cars") -> pd.DataFrame:
    """Year-end (Q4) licensed vehicles by fuel group, UK, all keepers."""
    def q4(path, sheet, scale):
        df = _read_ods(path, sheet)
        fuel_col = "Fuel"
        df = df[(df["BodyType"] == body_type) & (df["Keepership"] == "Total") & (df["ONS Code"] == UK_CODE)
                & (df[fuel_col] != "Total")]
        cols = [c for c in df.columns if c.endswith("Q4")]
        wide = df.set_index(fuel_col)[cols].apply(pd.to_numeric, errors="coerce").T * scale
        wide.index = [int(c.split()[0]) for c in cols]
        return wide
    a = q4(veh0105, "VEH0105", 1000)                       # thousands
    b = q4(veh0142, "VEH0142", 1)                          # number
    a = a.dropna(how="any")                                # UK figures start at 2014 Q4 ("[x]" before)
    b = b.reindex(a.index)
    # Plug-ins are inside VEH0105 "Other fuels"; take them from VEH0142 instead.
    wide = pd.concat([a.drop(columns=["Other fuels"], errors="ignore"), b], axis=1)
    return _group_fuels(wide).sort_index().rename_axis("year").dropna(how="all")


def read_registrations(veh1153: Path, body_type: str = "Cars") -> tuple[pd.DataFrame, float]:
    """Annual new registrations by fuel group, UK. Years before UK data exist are GB x the
    mean UK/GB ratio of total registrations. Returns (registrations, ratio)."""
    df = _read_ods(veh1153, "VEH1153a_RoadUsing")
    df = df[(df["BodyType"] == body_type) & (df["Keepership"] == "Total") & (df["Units"] == "Thousands")
            & (df["Date Interval"] == "Annual")]
    df = df.assign(year=pd.to_numeric(df["Date"], errors="coerce")).dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    by_geo = {g: _group_fuels(d.set_index("year")) * 1000 for g, d in df.groupby("Geography")}
    uk, gb = by_geo["United Kingdom"], by_geo["Great Britain"]
    both = uk.index.intersection(gb.index)
    ratio = float((uk.loc[both].sum(axis=1) / gb.loc[both].sum(axis=1)).mean())
    early = gb[gb.index < uk.index.min()] * ratio
    return pd.concat([early, uk]).sort_index().rename_axis("year"), ratio


def read_mileage(path: Path, first_year: int = 2007) -> pd.DataFrame:
    df = pd.read_csv(path)
    year_col = "completedYear" if "completedYear" in df else "year"
    df = df.assign(fuel=df["fuelType"].map(MILEAGE_FUEL_MAP), year=df[year_col].astype(int)).dropna(subset=["fuel"])
    out = df[df["year"] >= first_year].pivot_table(index="year", columns="fuel", values="annual_mileage")
    return out.reindex(columns=FUELS).rename_axis("year")


def read_fuel(path: Path) -> pd.DataFrame:
    """UK petrol and diesel car fuel consumption (ktoe) from the DESNZ sub-national tables,
    one sheet per year."""
    xl = pd.ExcelFile(path, engine="calamine")
    rows = {}
    for sheet in xl.sheet_names:
        if not sheet.strip().isdigit():
            continue
        raw = xl.parse(sheet, header=None)
        hdr = raw.index[raw.apply(lambda r: r.astype(str).str.contains("Local Authority Code", regex=False).any(), axis=1)][0]
        df = raw.iloc[hdr + 1:].set_axis([str(c).split("[")[0].strip() for c in raw.iloc[hdr]], axis=1)
        uk = df[df["Local Authority Code"] == UK_CODE].iloc[0]
        rows[int(sheet)] = {"petrol_cars": float(uk["Petrol cars total"]), "diesel_cars": float(uk["Diesel cars total"])}
    return pd.DataFrame.from_dict(rows, orient="index").sort_index().rename_axis("year")


def read_naei(path: Path, sheet: str = "UK By Source_AR5", row: str = "1A3bi_Cars") -> pd.Series:
    """NAEI emissions for one IPCC row, kt CO2e -> Mt, by year."""
    raw = pd.read_excel(path, sheet_name=sheet, header=None, engine="calamine")
    hdr = raw.index[raw.apply(lambda r: r.astype(str).eq("IPCC_name").any(), axis=1)][0]
    df = raw.iloc[hdr + 1:].set_axis(raw.iloc[hdr], axis=1)
    name_col = [c for c in df.columns if str(c) == "IPCC_name"][0]
    r = df[df[name_col] == row].iloc[0]
    years = {int(float(c)): float(r[c]) for c in df.columns if str(c).replace(".0", "").isdigit()}
    return (pd.Series(years).sort_index() / 1000).rename("naei_mt").rename_axis("year")


def load_history(fp: FleetParams | None = None, data_dir: Path | None = None) -> FleetHistory:
    """Read the DfT, DESNZ and NAEI tables. Takes a few seconds (calamine reader)."""
    fp = fp or load_params()
    d = data_dir or ROOT / "data"
    path = lambda p: d / Path(p).name if data_dir else ROOT / p
    parc = read_parc(d / "veh0105.ods", d / "veh0142.ods")
    regs, ratio = read_registrations(d / "veh1153.ods")
    base = fp.get("base_year") or int(parc.index.max())
    return FleetHistory(
        parc=parc,
        registrations=regs,
        mileage=read_mileage(path(fp.get("mileage.file")), fp.get("mileage.first_year")),
        fuel=read_fuel(path(fp.get("fuel_use.file"))),
        naei_mt=read_naei(path(fp.get("naei.file")), fp.get("naei.sheet"), fp.get("naei.row")),
        base_year=int(base),
        uk_gb_ratio=ratio,
    )


# ---------------------------------------------------------------- calibration + validation

def validate(hist: FleetHistory, fp: FleetParams) -> pd.DataFrame:
    """Per year with parc, mileage and fuel data: vehicle miles, fuel per mile and fuel-based
    CO2 by fuel, against NAEI 1A3bi. `ratio` = NAEI / modelled."""
    years = hist.parc.index.intersection(hist.mileage.index).intersection(hist.fuel.index)
    vmt = (hist.parc.loc[years, FUELS] * hist.mileage.loc[years, FUELS]).fillna(0.0)
    h = fp.get("fuel_use.hybrid_vs_petrol")
    petrol_equiv = vmt["petrol"] + h * vmt["hybrid"]
    fuel = pd.DataFrame({
        "petrol": hist.fuel.loc[years, "petrol_cars"] * vmt["petrol"] / petrol_equiv,
        "hybrid": hist.fuel.loc[years, "petrol_cars"] * h * vmt["hybrid"] / petrol_equiv,
        "diesel": hist.fuel.loc[years, "diesel_cars"],
        "bev": 0.0,
    })[FUELS]
    co2 = co2_mt(fuel, fp)
    out = pd.concat({"vehicle_miles": vmt, "fuel_ktoe": fuel, "fuel_per_mile": fuel / vmt.replace(0, np.nan),
                     "co2_mt": co2}, axis=1)
    out["co2_total_mt"] = co2.sum(axis=1)
    out["naei_mt"] = hist.naei_mt.reindex(years)
    out["ratio"] = out["naei_mt"] / out["co2_total_mt"]
    return out.rename_axis("year")


def calibration(hist: FleetHistory, fp: FleetParams) -> dict:
    """Base fuel per mile (ktoe per vehicle mile) by fuel and the NAEI scale factor."""
    v = validate(hist, fp)
    n = fp.get("fuel_use.calibration_years")
    recent = v[~v.index.isin(fp.get("fuel_use.exclude_years") or [])].tail(n)
    # NAEI ratio over the same years: it drifts down over time (0.97 in 2014, 0.91 in 2020).
    scale = float(recent["ratio"].mean()) if fp.get("emissions.calibrate_to_naei") and recent["ratio"].notna().all() else 1.0
    return {
        "fuel_per_mile": recent["fuel_per_mile"].mean().reindex(FUELS).fillna(0.0),
        "calibration_years": [int(y) for y in recent.index],
        "co2_scale": scale,
    }


def mileage_path(hist: FleetHistory, fp: FleetParams, years) -> pd.DataFrame:
    """Observed mileage where available, then the last value changed by `annual_change`."""
    m = hist.mileage.reindex(columns=FUELS)
    last_year = int(m.dropna(how="all").index.max())
    last = m.loc[last_year]
    chg = pd.Series(fp.get("mileage.annual_change")).reindex(FUELS).fillna(0.0)
    rows = {y: (m.loc[y] if y in m.index else last * (1 + chg) ** (y - last_year)) for y in years}
    return pd.DataFrame.from_dict(rows, orient="index").ffill().rename_axis("year")


# ---------------------------------------------------------------- scenario inputs

def split_registrations(zev: pd.Series, nonzev: pd.Series, fp: FleetParams) -> pd.DataFrame:
    """year x fuel: ZEVs -> bev; non-ZEVs -> petrol/diesel/hybrid by nonzev_split."""
    rows = {}
    for y in zev.index:
        h = fp.get("registrations.nonzev_split.hybrid", int(y))
        d = fp.get("registrations.nonzev_split.diesel", int(y))
        if h + d > 1:
            raise ValueError(f"nonzev_split for {y}: hybrid + diesel > 1")
        n = nonzev.loc[y]
        rows[int(y)] = {"petrol": n * (1 - h - d), "diesel": n * d, "hybrid": n * h, "bev": zev.loc[y]}
    return pd.DataFrame.from_dict(rows, orient="index")[FUELS].rename_axis("year")


def registrations_from_summary(summary: pd.DataFrame, hist: FleetHistory, fp: FleetParams,
                               mandate_params: str = "vets", extend_to: int | None = None) -> pd.DataFrame:
    """New registrations by scenario x year x fuel from scenarios.market_summary output.

    - GB-only scheme years are scaled to UK (registrations.scale_gb_to_uk).
    - Years between the fleet base year and the scenario's first year come from DfT.
    - `extend_to` holds the last year's market size and ZEV share to that year.
    Returns long: scenario, year, fuel, registrations, basis.
    """
    geo = rules.load(mandate_params).data.get("geography", {})
    out = []
    for name, g in summary.groupby("scenario", sort=False):
        g = g.sort_values("year").set_index("year")
        total = g["total_registrations"].astype(float)
        zev = total * g["zev_share"]
        basis = g["basis"].copy()
        if fp.get("registrations.scale_gb_to_uk") and geo:
            gb = np.array([year_value(geo, int(y)) == "GB" if int(y) >= min(geo) else False for y in total.index])
            total, zev = total.where(~gb, total * hist.uk_gb_ratio), zev.where(~gb, zev * hist.uk_gb_ratio)
        if extend_to and extend_to > total.index.max():
            last = int(total.index.max())
            extra = pd.Index(range(last + 1, extend_to + 1))
            share = zev.loc[last] / total.loc[last]
            total = pd.concat([total, pd.Series(total.loc[last], index=extra)])
            zev = pd.concat([zev, pd.Series(total.loc[last] * share, index=extra)])
            basis = pd.concat([basis, pd.Series("extended", index=extra)])
        regs = split_registrations(zev, total - zev, fp)
        gap = range(hist.base_year + 1, int(regs.index.min()))
        if len(gap):
            missing = [y for y in gap if y not in hist.registrations.index]
            if missing:
                raise ValueError(f"No registrations for {missing}: between fleet base year {hist.base_year} and scenario start")
            regs = pd.concat([hist.registrations.loc[list(gap), FUELS], regs])
            basis = pd.concat([pd.Series("actual", index=list(gap)), basis])
        regs = regs[regs.index > hist.base_year]
        long = regs.reset_index().melt(id_vars="year", var_name="fuel", value_name="registrations")
        long["basis"] = long["year"].map(basis)
        long.insert(0, "scenario", name)
        out.append(long)
    return pd.concat(out, ignore_index=True)


# ---------------------------------------------------------------- projection

def project(hist: FleetHistory, new_regs: pd.DataFrame, fp: FleetParams, cal: dict | None = None) -> pd.DataFrame:
    """Tidy projection (year x fuel rows) from registrations by year x fuel after the base year."""
    cal = cal or calibration(hist, fp)
    base = hist.base_year
    if base not in hist.parc.index:
        raise ValueError(f"No year-end parc for base year {base}")
    new_regs = new_regs.reindex(columns=FUELS).fillna(0.0)
    if new_regs.index.min() != base + 1:
        raise ValueError(f"Registrations must start in {base + 1}, the year after the base year")
    stock = stock_model(hist.parc.loc[base, FUELS], hist.registrations, new_regs,
                        fp.get("retirement.lifespan_years"), fp.get("retirement.spread_years"))
    miles = mileage_path(hist, fp, stock.parc.index)
    rel = pd.Series(fp.get("fuel_use.new_vs_fleet"))
    chg = pd.Series(fp.get("fuel_use.new_annual_change"))
    vmt, fuel = fuel_use(stock, miles, cal["fuel_per_mile"], rel, chg)
    co2 = co2_mt(fuel, fp) * cal["co2_scale"]
    parts = {"parc": stock.parc, "new_registrations": stock.additions, "retirements": stock.retirements,
             "vehicle_miles": vmt, "fuel_ktoe": fuel, "co2_mt": co2}
    out = pd.concat({k: v.stack() for k, v in parts.items()}, axis=1).rename_axis(["year", "fuel"]).reset_index()
    out["basis"] = "projected"
    return out


def history_table(hist: FleetHistory, fp: FleetParams, cal: dict | None = None, start: int = 2010) -> pd.DataFrame:
    """Observed years in the same tidy shape as project(). Fuel and CO2 only where DESNZ fuel
    data exist (CO2 scaled like the projection); parc and miles to the base year."""
    cal = cal or calibration(hist, fp)
    years = [y for y in hist.parc.index if start <= y <= hist.base_year]
    v = validate(hist, fp)
    miles = mileage_path(hist, fp, years)
    parts = {
        "parc": hist.parc.loc[years, FUELS],
        "new_registrations": hist.registrations.reindex(years)[FUELS],
        "vehicle_miles": hist.parc.loc[years, FUELS] * miles,
        "fuel_ktoe": v["fuel_ktoe"].reindex(years),
        "co2_mt": v["co2_mt"].reindex(years) * cal["co2_scale"],
    }
    out = pd.concat({k: v.stack(future_stack=True) for k, v in parts.items()}, axis=1).rename_axis(["year", "fuel"]).reset_index()
    out["retirements"] = np.nan
    out["basis"] = "actual"
    return out[["year", "fuel"] + OUTPUT_COLUMNS + ["basis"]]


def run_many(hist: FleetHistory, regs: pd.DataFrame, fp: FleetParams, include_history: bool = True) -> pd.DataFrame:
    """Project every scenario in `regs` (registrations_from_summary output).
    With include_history, each scenario also carries the observed years."""
    cal = calibration(hist, fp)
    past = history_table(hist, fp, cal) if include_history else None
    out = []
    for name, g in regs.groupby("scenario", sort=False):
        wide = g.pivot(index="year", columns="fuel", values="registrations")
        p = project(hist, wide, fp, cal)
        p = pd.concat([past, p], ignore_index=True) if past is not None else p
        p.insert(0, "scenario", name)
        out.append(p)
    return pd.concat(out, ignore_index=True)


def totals(tidy: pd.DataFrame) -> pd.DataFrame:
    """One row per scenario-year: summed outputs, plus BEV share of the parc and of miles."""
    keys = [c for c in ("scenario", "year") if c in tidy]
    g = tidy.groupby(keys)
    out = g[OUTPUT_COLUMNS].sum(min_count=1)
    out["basis"] = g["basis"].first()
    bev = tidy[tidy["fuel"] == "bev"].set_index(keys)
    out["bev_share_of_parc"] = bev["parc"] / out["parc"]
    out["bev_share_of_miles"] = bev["vehicle_miles"] / out["vehicle_miles"]
    return out.reset_index()


def backtest(hist: FleetHistory, fp: FleetParams, start_year: int) -> pd.DataFrame:
    """Run the stock model from the actual parc at `start_year` with actual DfT registrations
    and compare with the actual parc. Returns year x fuel with modelled, actual, error."""
    years = [y for y in hist.registrations.index if y > start_year and y in hist.parc.index]
    stock = stock_model(hist.parc.loc[start_year, FUELS], hist.registrations, hist.registrations.loc[years, FUELS],
                        fp.get("retirement.lifespan_years"), fp.get("retirement.spread_years"))
    out = pd.concat({"modelled": stock.parc.stack(), "actual": hist.parc.loc[years, FUELS].stack()}, axis=1)
    out["error"] = out["modelled"] / out["actual"] - 1
    return out.rename_axis(["year", "fuel"]).reset_index()
