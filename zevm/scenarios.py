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
1. Late-data uplift: OFF by default (late_data_uplift=False), and it should stay off.
   VES keeps adding records for a month after it is first counted, but the additions
   aren't genuine new registrations. Checked on 16 Oct 2026:
   - DfT VEH1153 (UK cars, monthly, to June 2024): VES today is 9-17% above DfT, and the
     gap is bigger for older months (2022 +17%, 2023 +13%, H1 2024 +10%).
   - SMMT: September 2026 is +1.3% at one month old, Jan-Sep 2026 is +3.8%, and full-year
     2025 is +8.6% (2.195M vs 2,020,520).
   - The ECC figure as first published is within about 2% of SMMT for 2025-26.
   - VES ZEV share matches DfT's BEV share to within about 0.5 points, so shares are
     reliable but totals for older years are inflated.
   The fitting code is kept (fit_late_data_curves) for diagnostics: ratio(age) =
   1 + R(1 - exp(-(age - 1) / tau)) against VRN_reported.
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
    late_data_uplift: bool = False,
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
        nonzev_registrations=("nonzev_registrations", "sum"),
        allowances=("allowances", "sum"),
        current_used=("current_used", "sum"),
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


def trailing_inputs(h: History, months: int = 12) -> pd.DataFrame:
    """The last `months` months per group, as one pseudo scheme year labelled with the
    latest month's year. Used to check performance on a rolling basis against that year's
    target, with no banking, borrowing or debt history."""
    m = h.monthly.assign(month=pd.to_datetime(h.monthly["month"]))
    last = m["month"].max()
    window = m[m["month"] > last - pd.DateOffset(months=months)]
    out = window.groupby("group")[FLOWS].sum().reset_index()
    out["year"] = last.year
    base = h.yearly.drop_duplicates("group").set_index("group")
    out["co2_baseline"] = out["group"].map(base["co2_baseline"])
    out["basis"] = f"trailing {months}m to {last:%b %Y}"
    return out


def minimum_compliant_path(
    h: History,
    params: str | Params,
    market_size: dict,
    nonzev_co2_change: float = 0.0,
    years=None,
    floor: float = 0.0,
    tolerance: float = 0.5,
) -> pd.DataFrame:
    """The lowest market ZEV share each year that still complies with no payments.

    A worst case for ZEV volumes: the market is pooled as one participant (perfect
    trading), CO2->ZEV credits and banked allowances are used in full, and borrowing is
    off. Borrowing only moves compliance between years and must be repaid by the deadline.
    Shares are found year by year by bisection. `floor` stops the share falling below a
    level, e.g. today's share, because a fall below today's share isn't credible.

    Returns one row per year: target, min_zev_share, market, co2_credits_share,
    bank_used_share.
    """
    p = params if isinstance(params, Params) else rules.load(params)
    vt = h.vehicle_type
    years = list(years or range(int(h.yearly["year"].max()) + 1, p.scheme_years.stop))
    strat = Strategy(borrowing=False, trading="none", co2_trading="none")

    def year_result(path, y):
        s = Scenario("min", params=p, zev_share=path, market_size=market_size,
                     nonzev_co2_change=nonzev_co2_change, end_year=y, group_zev="uniform")
        inp = compliance.market_total(project(h, s).drop(columns=["basis", "co2_baseline_source"], errors="ignore"))
        return compliance.run(inp, p, vt, strat).set_index("year").loc[y]

    path, rows = {}, []
    for y in years:
        lo, hi = 0.0, 1.0
        for _ in range(30):
            mid = (lo + hi) / 2
            if year_result({**path, y: mid}, y).shortfall > tolerance:
                lo = mid
            else:
                hi = mid
        path[y] = max(hi, floor)
        r = year_result(path, y)
        rows.append({"year": y, "target": p.zev_target(vt, y), "min_zev_share": path[y],
                     "market": r.total_registrations,
                     "co2_credits_share": r.co2_credits / r.total_registrations,
                     "bank_used_share": r.banked_used / r.total_registrations})
    return pd.DataFrame(rows).set_index("year")


# ---------------------------------------------------------------- review policy scenarios

FLEX = "continued use of flex"     # scenario-name suffix for minimum_compliant_path runs
TREND = "non-ZEV CO2 on trend"     # flex with non-ZEV CO2 falling at its recent rate


def nonzev_co2_trend(vehicle_type: str = "car", first_year: int = 2021, as_of: str | pd.Timestamp | None = None,
                     **query_kwargs) -> dict:
    """The recent annual change in the mean official CO2 of new non-ZEVs (g/km), market-wide.

    Compares the same months each year (January to the latest complete month), so the
    current part-year is like for like. The rate is the compound change from `first_year`
    to the latest year. For cars most of the fall comes from hybrids taking a bigger share
    of non-ZEV sales, not from cleaner petrol cars.

    Returns {"rate", "first_year", "last_year", "months", "mean_co2": {year: g/km}}.
    """
    as_of = pd.Timestamp(as_of or data.last_complete_month_end()).replace(day=1)
    raw = data.registrations(vehicle_type, start=f"{first_year}-01-01", end=as_of.date(), **query_kwargs)
    r = raw[~raw["fuel_type"].isin(["ELECTRICITY", "FUEL CELLS"])]
    last_month = (as_of - pd.DateOffset(months=1)).month
    r = r[r["month"].dt.month <= last_month]
    g = r.groupby(r["month"].dt.year)[["co2_sum", "with_co2"]].sum()
    mean = g["co2_sum"] / g["with_co2"]
    y0, y1 = int(mean.index.min()), int(mean.index.max())
    rate = (mean.loc[y1] / mean.loc[y0]) ** (1 / (y1 - y0)) - 1
    return {"rate": float(rate), "first_year": y0, "last_year": y1, "months": f"Jan-{pd.Timestamp(2000, last_month, 1):%b}",
            "mean_co2": mean.round(1).to_dict()}

REVIEW_POLICIES = {
    "Current mandate": "scenarios/indicative_to_2035",
    "Review Option 1 (cars 70%, vans 60%)": "scenarios/review_option1_cars70_vans60",
    "Review Option 2 (cars 60%, vans 50%)": "scenarios/review_option2_cars60_vans50",
    "Review Option 3 (cars 50%, vans 40%)": "scenarios/review_option3_cars50_vans40",
    "Review Option 4 (cars 80%, vans 70%, flex to 2034)": "scenarios/review_option4_cars80_vans70_extended",
}


def policy_paths(car_market_2026: float = 2.183e6, growth: float = 0.0, policies: dict | None = None,
                 continued_flex: bool = True, co2_trend: bool = True, worst_case: bool = True,
                 histories: dict | None = None,
                 end_year: int = 2035, **prepare_kwargs) -> tuple[pd.DataFrame, dict]:
    """Market size and ZEV share by scenario, vehicle type and year, 2024 to `end_year`, for
    the current mandate and the review options.

    Scenarios, for each policy in `policies` (default REVIEW_POLICIES):
    - "<policy> | on target": the market ZEV share equals the headline target.
    - "<policy> | continued use of flex": minimum_compliant_path with non-ZEV CO2 flat, floored at
      this year's share.
    - "<policy> | continued use of flex, non-ZEV CO2 on trend" (co2_trend=True): the same,
      but non-ZEV CO2 keeps falling at its recent market-wide rate (nonzev_co2_trend,
      2021 to this year, about -3.5%/yr for cars and -3.3%/yr for vans in 2026). More
      credits are earned each year, so flexibility use grows rather than staying at today's.
    - "Review Option 3 ... | continued use of flex, non-ZEV CO2 -6%/yr" (worst_case=True).
    "Continued use of flex" = manufacturers keep using the flexibilities at today's level of
    performance: all CO2 credits earned at 2026 non-ZEV CO2, banked allowances, perfect
    trading. It is not the legal maximum.
    The `nonzev_co2_change` column carries each scenario's annual change in new non-ZEV
    CO2, so the fleet model can use the same assumption. The trend rates are in info.

    Markets are on an official basis, not VES. Cars use `car_market_2026` (default SMMT's
    2026 forecast) growing at `growth` (default 0: flat). Vans and the 2024-25 history are VES counts scaled
    by the same ratio, because VES overcounts official totals. History years (basis actual or
    estimated) are the same in every scenario. `histories` = {"car": History, "van": History}
    skips the BigQuery step.

    Returns (long frame: scenario, vehicle_type, year, total_registrations, zev_share,
    basis; info dict of the assumptions used).
    """
    policies = policies or REVIEW_POLICIES
    hist = histories or {vt: prepare(vt, **prepare_kwargs) for vt in ("car", "van")}
    est_year = int(hist["car"].yearly["year"].max())

    def market_hist(h):
        g = h.yearly.groupby("year")[["total_registrations", "zev_registrations"]].sum()
        return g["total_registrations"], g["zev_registrations"] / g["total_registrations"], h.yearly.groupby("year")["basis"].first()

    car_tot, car_share, basis = market_hist(hist["car"])
    van_tot, van_share, _ = market_hist(hist["van"])
    scale = car_market_2026 / car_tot.loc[est_year]
    future = range(est_year + 1, end_year + 1)
    markets = {
        "car": {**(car_tot * scale).to_dict(), **{y: car_market_2026 * (1 + growth) ** (y - est_year) for y in future}},
        "van": {**(van_tot * scale).to_dict(), **{y: van_tot.loc[est_year] * scale * (1 + growth) ** (y - est_year) for y in future}},
    }
    share_hist = {"car": car_share.to_dict(), "van": van_share.to_dict()}

    def on_target(params, vt):
        p = rules.load(params)
        return {y: p.zev_target(vt, y) for y in future}

    def flex(params, vt, co2=0.0):
        mp = minimum_compliant_path(hist[vt], params, {y: markets[vt][y] for y in future},
                                    nonzev_co2_change=co2, years=list(future), floor=share_hist[vt][est_year])
        return mp["min_zev_share"].to_dict()

    paths = {f"{n} | on target": {vt: on_target(p, vt) for vt in ("car", "van")} for n, p in policies.items()}
    co2_change = {s: {"car": 0.0, "van": 0.0} for s in paths}
    if continued_flex:
        for n, p in policies.items():
            paths[f"{n} | {FLEX}"] = {vt: flex(p, vt) for vt in ("car", "van")}
            co2_change[f"{n} | {FLEX}"] = {"car": 0.0, "van": 0.0}
    trends = {}
    if co2_trend:
        prep_keys = {"by", "params", "start", "late_data_uplift", "max_fit_age", "prior_years"}
        qkw = {k: v for k, v in prepare_kwargs.items() if k not in prep_keys}
        trends = {vt: nonzev_co2_trend(vt, **qkw) for vt in ("car", "van")}
        rate = {vt: trends[vt]["rate"] for vt in trends}
        for n, p in policies.items():
            paths[f"{n} | {FLEX}, {TREND}"] = {vt: flex(p, vt, rate[vt]) for vt in ("car", "van")}
            co2_change[f"{n} | {FLEX}, {TREND}"] = dict(rate)
    o3 = "Review Option 3 (cars 50%, vans 40%)"
    if worst_case and o3 in policies:
        paths[f"{o3} | {FLEX}, non-ZEV CO2 -6%/yr"] = {"car": flex(policies[o3], "car", -0.06),
                                                           "van": flex(policies[o3], "van")}
        co2_change[f"{o3} | {FLEX}, non-ZEV CO2 -6%/yr"] = {"car": -0.06, "van": 0.0}
    rows = []
    for scen, by_vt in paths.items():
        for vt in ("car", "van"):
            for y in sorted(markets[vt]):
                hist_year = y in share_hist[vt]
                rows.append((scen, vt, y, markets[vt][y], share_hist[vt][y] if hist_year else by_vt[vt][y],
                             basis.get(y, "projected") if hist_year else "projected", co2_change[scen][vt]))
    df = pd.DataFrame(rows, columns=["scenario", "vehicle_type", "year", "total_registrations", "zev_share", "basis",
                                     "nonzev_co2_change"])
    info = {"car_market": car_market_2026, "market_year": est_year, "growth": growth, "ves_to_official_scale": scale,
            "van_market": markets["van"][est_year],
            "car_zev_share_" + str(est_year): share_hist["car"][est_year], "van_zev_share_" + str(est_year): share_hist["van"][est_year]}
    info |= {f"{vt}_nonzev_co2_trend": round(t["rate"], 4) for vt, t in trends.items()}
    return df, info
