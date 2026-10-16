"""Scenarios: observed history, the current year estimated, and projections to the end of the scheme.

    from zevm import scenarios
    from zevm.scenarios import Scenario
    h = scenarios.prepare("car")                      # 2024 on, by pool; current year estimated
    res = scenarios.run_many(h, [
        Scenario("On target"),
        Scenario("2pp below target", zev_share_vs_target=-0.02),
        Scenario("No borrowing", params="scenarios/example_no_borrowing_from_2027"),
    ])
    scenarios.market_summary(res)

How the history is built
1. Late-data uplift. VES keeps adding records for a month after it is first counted. The
   rise is measured by comparing today's VES counts with the ECC figure first published for
   each month (VES_dataset.VRN_reported), as a function of the month's age. It is fitted as
   ratio(age) = 1 + R(1 - exp(-(age - 1) / tau)), separately for ZEVs and non-ZEVs. Each
   month's counts are then scaled up to the fitted plateau, (1 + R) / ratio(age).
   - Only months up to `max_fit_age` (18) old are used in the fit. Older months sit higher,
     probably because the ECC method changed in early 2025.
   - Whether the late records are all genuine new first registrations has not been checked
     against DfT or SMMT figures.
   - Vans have no published series, so they use the car curves.
2. The current year is estimated to full year using the market's seasonality. That is the
   share of the year's registrations that fell in the months seen so far, averaged over the
   prior `prior_years` years. For the rest of the year's ZEV share, each group's
   year-to-date share is shifted in log-odds by the market's shift between year-to-date
   and rest-of-year shares in those years. The shift is usually positive: ZEV share rises
   towards year end.
   Non-ZEV mean CO2 stays at its year-to-date level.

How projections work (years after the estimated year)
- Market size: held at the last year, grown at `market_growth`, or set per year in `market_size`.
- Each group keeps its share of the market from the last year.
- Market ZEV share follows `zev_share`: "target" (plus `zev_share_vs_target`), a constant,
  or a year table.
- Group ZEV shares: by default ("logit") every group's log-odds of a ZEV sale shift by the
  same amount until the market share is reached. Leaders stay ahead, nobody passes 100%,
  and laggards still move. See allocate_zev_shares for "scale" and "uniform".
- Each group's non-ZEV mean CO2 changes by `nonzev_co2_change` a year.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import pandas as pd

from zevm import compliance, data, rules
from zevm.compliance import Strategy
from zevm.rules import Params

FLOWS = ["total_registrations", "zev_registrations", "nonzev_registrations", "co2_activity"]


# ---------------------------------------------------------------- late-data uplift

def months_old(month: pd.Series | pd.Timestamp, as_of: pd.Timestamp) -> pd.Series | int:
    """1 for the month before `as_of`, 2 for the one before that, and so on."""
    m = pd.to_datetime(month)
    if isinstance(m, pd.Timestamp):
        return (as_of.year - m.year) * 12 + as_of.month - m.month
    return (as_of.year - m.dt.year) * 12 + as_of.month - m.dt.month


@dataclass(frozen=True)
class LateDataCurve:
    R: float          # eventual rise over the first count
    tau: float        # months
    n_points: int
    n_outliers: int

    def ratio(self, age) -> np.ndarray:
        a = np.maximum(np.asarray(age, dtype=float), 1.0)
        return 1 + self.R * (1 - np.exp(-(a - 1) / self.tau))

    def factor(self, age) -> np.ndarray:
        """Multiplier that takes a count of this age up to its expected final value."""
        return (1 + self.R) / self.ratio(age)


def fit_curve(age: np.ndarray, ratio: np.ndarray) -> LateDataCurve:
    """Least-squares fit of ratio = 1 + R(1 - exp(-(age-1)/tau)) over a grid of tau, with
    points more than 3 robust SDs from the first fit dropped."""
    age, ratio = np.asarray(age, float), np.asarray(ratio, float)
    best = None
    for tau in np.arange(0.25, 24.01, 0.25):
        x = 1 - np.exp(-(age - 1) / tau)
        if not (x > 0).any():
            continue
        R0 = (x * (ratio - 1)).sum() / (x * x).sum()
        resid = ratio - 1 - R0 * x
        # Floor the spread at 1 point so near-perfect fits don't flag rounding noise.
        mad = max(np.median(np.abs(resid - np.median(resid))) * 1.4826, 0.01)
        keep = np.abs(resid - np.median(resid)) <= 3 * mad
        R = (x[keep] * (ratio[keep] - 1)).sum() / (x[keep] ** 2).sum()
        sse = ((ratio[keep] - 1 - R * x[keep]) ** 2).mean()
        if best is None or sse < best[0]:
            best = (sse, R, tau, int(keep.sum()), int((~keep).sum()))
    _, R, tau, n, out = best
    return LateDataCurve(R=float(R), tau=float(tau), n_points=n, n_outliers=out)


def fit_late_data_curves(raw: pd.DataFrame, reported: pd.DataFrame, as_of: pd.Timestamp, max_fit_age: int = 18) -> dict[str, LateDataCurve]:
    """Fit ZEV and non-ZEV curves from car VES counts against the ECC figures first published."""
    ves = data.classify(raw).groupby("month")[["registrations", "zev_registrations"]].sum()
    rep = reported.assign(month=pd.to_datetime(reported["month"]))
    rep_total = rep.groupby("month")["registrations"].sum()
    rep_zev = rep[rep["fuel"] == "BEV"].groupby("month")["registrations"].sum()
    df = pd.DataFrame({
        "zev": ves["zev_registrations"] / rep_zev,
        "nonzev": (ves["registrations"] - ves["zev_registrations"]) / (rep_total - rep_zev),
    }).dropna()
    df["age"] = months_old(df.index.to_series(), as_of).values
    df = df[(df["age"] >= 1) & (df["age"] <= max_fit_age)]
    return {k: fit_curve(df["age"].values, df[k].values) for k in ("zev", "nonzev")}


def apply_late_data_uplift(raw: pd.DataFrame, curves: dict[str, LateDataCurve], as_of: pd.Timestamp) -> pd.DataFrame:
    """Scale each raw row's counts and CO2 sums to their expected final values."""
    out = raw.copy()
    age = months_old(out["month"], as_of)
    zev_row = out["fuel_type"].isin(data.ZEV_FUELS)
    f = np.where(zev_row, curves["zev"].factor(age), curves["nonzev"].factor(age))
    for col in ("registrations", "with_co2", "co2_sum"):
        out[col] = out[col].astype(float) * f
    return out


# ---------------------------------------------------------------- current-year estimate

def _logit(p, lo=0.002, hi=0.998):
    p = np.clip(p, lo, hi)
    return np.log(p / (1 - p))


def _inv_logit(x):
    return 1 / (1 + np.exp(-x))


def extrapolate_year(monthly: pd.DataFrame, year: int, prior_years: int = 2) -> tuple[pd.DataFrame, dict]:
    """Full-year estimate for `year` from a by-group monthly summary (data.summarise(freq="month"))."""
    m = monthly.assign(month=pd.to_datetime(monthly["month"]))
    ytd = m[m["month"].dt.year == year]
    last_month = int(ytd["month"].dt.month.max())
    market = m.groupby("month")[FLOWS].sum()

    seen, shifts = [], []
    for p in range(year - prior_years, year):
        mp = market[market.index.year == p]
        if len(mp) != 12:
            continue
        first, rest = mp[mp.index.month <= last_month], mp[mp.index.month > last_month]
        seen.append(first["total_registrations"].sum() / mp["total_registrations"].sum())
        if len(rest):
            shifts.append(_logit(rest["zev_registrations"].sum() / rest["total_registrations"].sum())
                          - _logit(first["zev_registrations"].sum() / first["total_registrations"].sum()))
    if not seen:
        raise ValueError(f"No complete prior year to take seasonality from for {year}")
    share_seen = float(np.mean(seen))
    zev_shift = float(np.mean(shifts)) if shifts else 0.0

    g = ytd.groupby("group")[FLOWS].sum()
    rest_total = g["total_registrations"] * (1 - share_seen) / share_seen
    rest_zev_share = _inv_logit(_logit(g["zev_registrations"] / g["total_registrations"]) + zev_shift)
    rest_zev = rest_total * rest_zev_share
    rest_nonzev = rest_total - rest_zev
    market_mean_co2 = g["co2_activity"].sum() / g["nonzev_registrations"].sum()
    mean_co2 = (g["co2_activity"] / g["nonzev_registrations"].where(g["nonzev_registrations"] > 0)).fillna(market_mean_co2)

    full = pd.DataFrame({
        "total_registrations": g["total_registrations"] + rest_total,
        "zev_registrations": g["zev_registrations"] + rest_zev,
        "nonzev_registrations": g["nonzev_registrations"] + rest_nonzev,
        "co2_activity": g["co2_activity"] + rest_nonzev * mean_co2,
    }).reset_index()
    full["year"] = year
    info = {"year": year, "months_seen": last_month, "share_of_year_seen": share_seen, "rest_of_year_zev_logit_shift": zev_shift,
            "prior_years_used": len(seen)}
    return full, info


# ---------------------------------------------------------------- history

@dataclass
class History:
    vehicle_type: str
    by: str
    yearly: pd.DataFrame          # one row per group per year; basis = actual | estimated
    monthly: pd.DataFrame
    as_of: pd.Timestamp
    notes: dict = field(default_factory=dict)


def prepare(
    vehicle_type: str = "car",
    by: str | None = None,
    params: str | Params = "vets",
    start: str = "2022-01-01",
    as_of: str | pd.Timestamp | None = None,
    late_data_uplift: bool = True,
    max_fit_age: int = 18,
    prior_years: int = 2,
    **query_kwargs,
) -> History:
    """Pull registrations and build the history the scenarios start from.

    `start` is earlier than the first scheme year so seasonality has prior years to use.
    """
    p = rules.load(params) if not isinstance(params, Params) else params
    by = by or ("pool" if vehicle_type == "car" else "entity")
    as_of = pd.Timestamp(as_of or data.last_complete_month_end()).replace(day=1)
    raw = data.registrations(vehicle_type, start=start, end=as_of.date(), **query_kwargs)
    notes: dict = {"as_of": str(as_of.date())}

    if late_data_uplift:
        car_raw = raw if vehicle_type == "car" else data.registrations("car", start=start, end=as_of.date(), **query_kwargs)
        curves = fit_late_data_curves(car_raw, data.reported_monthly(start=start, end=as_of.date(), **query_kwargs), as_of, max_fit_age)
        raw = apply_late_data_uplift(raw, curves, as_of)
        notes["late_data_curves"] = {k: vars(c) for k, c in curves.items()}
        if vehicle_type != "car":
            notes["late_data_curves_source"] = "car"

    pools = data.pool_baselines(**query_kwargs) if vehicle_type == "car" else None
    monthly = data.summarise(raw, by=by, freq="month", params=p, vehicle_type=vehicle_type, pools=pools)
    monthly["year"] = pd.to_datetime(monthly["month"]).dt.year

    first_scheme = p.scheme_years.start
    last_month = pd.to_datetime(monthly["month"]).max()
    complete_years = [y for y in range(first_scheme, last_month.year + 1) if y < last_month.year or last_month.month == 12]
    yearly = (monthly[monthly["year"].isin(complete_years)].groupby(["group", "year"])[FLOWS].sum().reset_index()
              .assign(basis="actual"))
    if last_month.month != 12:
        est, info = extrapolate_year(monthly, last_month.year, prior_years)
        yearly = pd.concat([yearly, est.assign(basis="estimated")], ignore_index=True)
        notes["current_year"] = info

    if vehicle_type == "car":
        yearly = compliance.attach_baselines(yearly, pools, data.entity_baselines("car", **query_kwargs))
    else:
        est_b = data.entity_baselines("van", **query_kwargs).dropna(subset=["co2_target"]).groupby("manufacturer")["co2_target"].first()
        yearly["co2_baseline"] = yearly["group"].map(est_b)
        yearly["co2_baseline_source"] = yearly["co2_baseline"].notna().map({True: "estimated", False: None})
    return History(vehicle_type, by, yearly.sort_values(["year", "group"]).reset_index(drop=True), monthly, as_of, notes)


# ---------------------------------------------------------------- scenarios

@dataclass(frozen=True)
class Scenario:
    name: str
    params: str | Params = "vets"
    strategy: Strategy = Strategy()
    zev_share: Literal["target"] | float | dict = "target"
    zev_share_vs_target: float = 0.0           # added to the target when zev_share="target"
    market_growth: float = 0.0                 # annual, when market_size is not given
    market_size: dict | None = None            # {year: registrations}; carries forward
    nonzev_co2_change: float = 0.0             # annual relative change in each group's non-ZEV g/km
    group_zev: Literal["logit", "scale", "uniform"] = "logit"
    end_year: int | None = None

    def load_params(self) -> Params:
        return self.params if isinstance(self.params, Params) else rules.load(self.params)


def _market_zev_share(s: Scenario, p: Params, vt: str, year: int) -> float:
    if s.zev_share == "target":
        return min(max(p.zev_target(vt, year) + s.zev_share_vs_target, 0.0), 1.0)
    if isinstance(s.zev_share, dict):
        return rules.year_value(s.zev_share, year)
    return float(s.zev_share)


def allocate_zev_shares(totals: pd.Series, base_shares: pd.Series, market_share: float,
                        method: Literal["logit", "scale", "uniform"] = "logit") -> pd.Series:
    """Group ZEV shares whose registration-weighted mean equals `market_share`.

    - logit: shift every group's log-odds by the same amount. Keeps the ranking, stays
      within 0-1, and moves groups that start at 0%. Shares are clipped to 0.2%-99.8% first.
    - scale: proportional to base shares, capped at 100%. Groups at 0% stay at 0%.
    - uniform: every group gets the market share.
    """
    w = totals / totals.sum()
    if market_share >= 1:
        return pd.Series(1.0, index=totals.index)
    if market_share <= 0 or method == "uniform":
        return pd.Series(max(market_share, 0.0), index=totals.index)
    if method == "logit":
        p0 = base_shares.clip(0.002, 0.998)
        logit = np.log(p0 / (1 - p0))
        lo, hi = -30.0, 30.0
        for _ in range(100):                      # bisection on the common shift
            mid = (lo + hi) / 2
            mean = (w / (1 + np.exp(-(logit + mid)))).sum()
            lo, hi = (mid, hi) if mean < market_share else (lo, mid)
        return 1 / (1 + np.exp(-(logit + (lo + hi) / 2)))
    # scale
    shares = pd.Series(0.0, index=totals.index)
    free = totals.index[(totals > 0) & (base_shares > 0)]
    remaining = market_share * totals.sum()
    for _ in range(len(free) + 1):
        weight = (base_shares[free] * totals[free]).sum()
        if weight <= 0 or remaining <= 0:
            break
        trial = base_shares[free] * remaining / weight
        capped = trial[trial >= 1].index
        if capped.empty:
            shares[free] = trial
            break
        shares[capped] = 1.0
        remaining -= totals[capped].sum()
        free = free.difference(capped)
    return shares.clip(0, 1)


def project(h: History, s: Scenario) -> pd.DataFrame:
    """History plus projected years, ready for compliance.run."""
    p = s.load_params()
    vt = h.vehicle_type
    base_year = int(h.yearly["year"].max())
    end = s.end_year or (p.scheme_years.stop - 1)
    base = h.yearly[h.yearly["year"] == base_year].set_index("group")
    base = base[base["total_registrations"] > 0]
    market0 = base["total_registrations"].sum()
    group_weight = base["total_registrations"] / market0
    zev_share0 = base["zev_registrations"] / base["total_registrations"]
    mean_co2_0 = (base["co2_activity"] / base["nonzev_registrations"].where(base["nonzev_registrations"] > 0))
    mean_co2_0 = mean_co2_0.fillna(base["co2_activity"].sum() / base["nonzev_registrations"].sum())

    rows = []
    for y in range(base_year + 1, end + 1):
        if s.market_size:
            market = rules.year_value(s.market_size, y)
        else:
            market = market0 * (1 + s.market_growth) ** (y - base_year)
        totals = group_weight * market
        z = allocate_zev_shares(totals, zev_share0, _market_zev_share(s, p, vt, y), s.group_zev)
        nonzev = totals * (1 - z)
        rows.append(pd.DataFrame({
            "group": base.index, "year": y,
            "total_registrations": totals.values, "zev_registrations": (totals * z).values,
            "nonzev_registrations": nonzev.values,
            "co2_activity": (nonzev * mean_co2_0 * (1 + s.nonzev_co2_change) ** (y - base_year)).values,
            "co2_baseline": base["co2_baseline"].values,
            "co2_baseline_source": base.get("co2_baseline_source", pd.Series(None, index=base.index)).values,
            "basis": "projected",
        }))
    out = pd.concat([h.yearly] + rows, ignore_index=True)
    return out[out["year"] <= end]


def run(h: History, s: Scenario) -> pd.DataFrame:
    inputs = project(h, s)
    res = compliance.run(inputs.drop(columns=["basis", "co2_baseline_source"], errors="ignore"), s.load_params(), h.vehicle_type, s.strategy)
    res = res.merge(inputs[["group", "year", "basis"]], on=["group", "year"], how="left")
    res["basis"] = res["basis"].fillna("projected")
    res.insert(0, "scenario", s.name)
    return res


def run_many(h: History, scenarios: list[Scenario]) -> pd.DataFrame:
    return pd.concat([run(h, s) for s in scenarios], ignore_index=True)


def market_summary(results: pd.DataFrame) -> pd.DataFrame:
    """One row per scenario per year: market totals and flexibility use."""
    g = results.groupby(["scenario", "year"])
    out = g.agg(
        basis=("basis", "first"),
        total_registrations=("total_registrations", "sum"),
        zev_registrations=("zev_registrations", "sum"),
        zev_target=("zev_target", "first"),
        allowances=("allowances", "sum"),
        co2_credits=("co2_credits", "sum"),
        banked_used=("banked_used", "sum"),
        traded=("bought", "sum"),
        borrowed=("borrowed", "sum"),
        repaid=("repaid", "sum"),
        bank_end=("bank_end", "sum"),
        debt_end=("debt_end", "sum"),
        shortfall=("shortfall", "sum"),
        payment_gbp=("payment_gbp", "sum"),
        paying=("payment_gbp", lambda x: int((x > 1).sum())),
    )
    out.insert(3, "zev_share", out["zev_registrations"] / out["total_registrations"])
    # ZEV share the market needed once CO2 credits are counted, before banking/borrowing/trading.
    out.insert(5, "required_after_co2", 1 - (out["allowances"] + out["co2_credits"]) / out["total_registrations"])
    out["payment_gbp_m"] = out.pop("payment_gbp") / 1e6
    return out.drop(columns="zev_registrations").reset_index()
