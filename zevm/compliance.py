"""ZEV mandate compliance engine.

    inputs = compliance.attach_baselines(data.summarise(raw, by="pool", freq="year", params=p), pools)
    result = compliance.run(inputs, rules.load("vets"), "car", Strategy())

`run` steps through scheme years in order, because banked allowances and borrowings carry
state from one year to the next. Each year has three passes over the participants:

  A. Own position.
     - Registration allowances: (1 - target) x registrations, rounded to 0.1 (art. 13).
       Low-volume participants get registrations, up to 2,499.
     - CO2 scheme, only if non-ZEVs > 999:
       - CO2 allowances = non-ZEVs x baseline (art. 34).
       - surplus or deficit = allowances - CO2 activity.
       - optional CO2 market: surpluses cover other participants' deficits, pro rata.
     - Convert own CO2 surplus to ZEV credits: whole credits at 167 g each (vans 216),
       capped at cap x registrations (art. 23). Credits cover non-ZEVs first (art. 27(2)),
       which frees current allowances to bank or sell.
     - Cover the obligation (one unit per non-ZEV) with credits, then current allowances,
       then banked allowances, oldest first.
  B. Market (trading="market"). Spare current and banked allowances are sold to cover
     remaining deficits, pro rata on both sides. Borrowed allowances and converted credits
     can't be traded (art. 15(15), 23(6)). The model assumes the market clears whatever it
     can, at no stated price.
  C. Close the year.
     - A remaining deficit is borrowed, up to cap x registrations (art. 15). Anything
       beyond that is paid at the per-vehicle rate (art. 28).
     - Spare allowances are used, in this order, to:
       1. cover a CO2 deficit, at 135 g per allowance (vans 206). Only allowed where the
          obligation was met without borrowing (art. 35).
       2. repay borrowings, oldest first, with uplift 1.035^k - 1 rounded up to whole
          allowances (art. 15(8)-(11)).
       3. bank the rest. Not allowed while debt is outstanding (art. 15(14)).
     - Banked allowances expire after 3 further years (art. 14).
     - Debt still outstanding in the repay_by year is paid at the per-vehicle rate.
     - A CO2 deficit still left is paid at £86 per g/km (art. 39).

Not modelled yet: car-van exchanges (art. 23A, 25A, 55A, 57A), SPV and car-club credits,
pooling decisions (pools come in as given groups), the art. 12(3) aggregate cap, and the
3-year duration of small-volume status (eligibility is tested year by year).
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Literal

import pandas as pd

from zevm.rules import Params

EPS = 1e-9


@dataclass(frozen=True)
class Strategy:
    """How participants use the flexibilities. Each one can be switched off for a scenario."""
    co2_to_zev: bool = True
    zev_to_co2: bool = True
    banking: bool = True
    borrowing: bool = True
    trading: Literal["none", "market"] = "market"
    # CO2 surpluses far exceed deficits in 2024-26 data, so deficits are assumed bought in.
    co2_trading: Literal["none", "market"] = "market"
    small_volume_apply: bool = True   # eligible 1,000-2,499 producers apply for low-volume status


@dataclass
class _State:
    bank: list[list[float]] = field(default_factory=list)   # [vintage year, allowances]
    debt: list[list[float]] = field(default_factory=list)   # [year borrowed for, allowances]

    def bank_total(self) -> float:
        return sum(a for _, a in self.bank)

    def debt_total(self) -> float:
        return sum(a for _, a in self.debt)

    def take_bank(self, amount: float) -> float:
        """Remove up to `amount` from the bank, oldest first. Returns the amount taken."""
        taken = 0.0
        for lot in self.bank:
            use = min(lot[1], amount - taken)
            lot[1] -= use
            taken += use
            if amount - taken <= EPS:
                break
        self.bank = [lot for lot in self.bank if lot[1] > EPS]
        return taken


def _rule(params: Params, vt: str, key: str, year: int):
    return params.get(vt, key, year)



def run(inputs: pd.DataFrame, params: Params, vehicle_type: str = "car", strategy: Strategy = Strategy()) -> pd.DataFrame:
    """Run the scheme over every year in `inputs`.

    `inputs` has one row per participant per year with columns: group, year,
    total_registrations, nonzev_registrations, co2_activity, and optionally co2_baseline
    (g/km; NaN leaves the participant out of the CO2 scheme, and this is flagged) and
    low_volume_eligible (default: False for groups whose name contains "(POOL)", True
    otherwise).

    Returns one row per participant per year with every step of the accounting.
    """
    df = inputs.copy()
    if "co2_baseline" not in df:
        df["co2_baseline"] = float("nan")
    if "low_volume_eligible" not in df:
        df["low_volume_eligible"] = ~df["group"].str.contains("(POOL)", regex=False)
    years = list(range(int(df["year"].min()), int(df["year"].max()) + 1))
    df = _complete_panel(df, years)

    lv = params.data.get("low_volume", {})
    states: dict[str, _State] = {}
    out = []
    for year in years:
        rows = [_own_position(r, year, params, vehicle_type, strategy, lv, states.setdefault(r.group, _State()))
                for r in df[df["year"] == year].itertuples(index=False)]
        if strategy.co2_trading == "market":
            _clear_co2_market(rows)
            for row in rows:
                _convert_co2(row, year, params, vehicle_type, strategy)
                _meet_obligation(row, states[row["group"]], strategy)
        if strategy.trading == "market":
            _clear_market(rows, states, _rule(params, vehicle_type, "borrowing.uplift_rate", year), year)
        for row in rows:
            _close_year(row, year, params, vehicle_type, strategy, states[row["group"]])
        out.extend(rows)

    res = pd.DataFrame(out)
    res.attrs["strategy"] = asdict(strategy)
    res.attrs["params"] = params.name
    return res


def _complete_panel(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    """Give every group a row in every year, with zero registrations where it had none,
    so banked allowances still expire and debts still fall due."""
    idx = pd.MultiIndex.from_product([df["group"].unique(), years], names=["group", "year"])
    full = df.set_index(["group", "year"]).reindex(idx)
    for col in ("total_registrations", "nonzev_registrations", "co2_activity"):
        full[col] = full[col].fillna(0.0)
    full["co2_baseline"] = full.groupby(level="group")["co2_baseline"].transform(lambda s: s.ffill().bfill())
    full["low_volume_eligible"] = full.groupby(level="group")["low_volume_eligible"].transform(lambda s: s.ffill().bfill()).astype(bool)
    return full.reset_index()


# ---------------------------------------------------------------- pass A

def _own_position(r, year, params, vt, strategy, lv, state) -> dict:
    total, nonzev = float(r.total_registrations), float(r.nonzev_registrations)
    target = params.zev_target(vt, year)
    row = {
        "group": r.group, "year": year, "total_registrations": total, "nonzev_registrations": nonzev,
        "zev_registrations": total - nonzev, "zev_share": (total - nonzev) / total if total else float("nan"),
        "zev_target": target,
    }

    # Registration allowances (art. 13), with low-volume treatment (Sch. 4).
    low_volume = False
    if lv and r.low_volume_eligible:
        micro = total < lv["micro_threshold"]
        small = (total < lv["small_threshold"] and strategy.small_volume_apply
                 and year not in lv.get("small_status_excluded_years", []))
        low_volume = micro or small
    if low_volume:
        allowances = min(total, lv["allowance_cap"])
    else:
        allowances = round((1 - target) * total, 1)
    row.update(low_volume=low_volume, allowances=allowances)

    # CO2 scheme (art. 32, 34).
    min_nz = _rule(params, vt, "co2_scheme.min_non_zev_registrations", year)
    baseline = r.co2_baseline
    in_co2 = nonzev >= min_nz and not pd.isna(baseline)
    co2_allow = nonzev * baseline if in_co2 else 0.0
    co2_activity = float(r.co2_activity) if in_co2 else 0.0
    row.update(
        in_co2_scheme=in_co2, co2_baseline_missing=bool(nonzev >= min_nz and pd.isna(baseline)),
        co2_baseline=baseline, co2_allowances=co2_allow, co2_activity=co2_activity,
        co2_position=co2_allow - co2_activity, co2_traded=0.0,
    )

    # Expire bank lots past their life before using them (art. 14, 30).
    life = _rule(params, vt, "banking.allowance_life_years", year)
    expired = sum(a for v, a in state.bank if year - v >= life)
    state.bank = [lot for lot in state.bank if year - lot[0] < life]
    row.update(bank_start=state.bank_total(), bank_expired=expired, debt_start=state.debt_total())

    if strategy.co2_trading != "market":
        _convert_co2(row, year, params, vt, strategy)
        _meet_obligation(row, state, strategy)
    return row


def _convert_co2(row, year, params, vt, strategy):
    cap_share = _rule(params, vt, "co2_to_zev.cap", year)
    grams = _rule(params, vt, "co2_to_zev.grams_per_credit", year)
    possible = 0.0
    if strategy.co2_to_zev and row["in_co2_scheme"] and row["co2_position"] > 0 and cap_share > 0:
        possible = min(math.floor(row["co2_position"] / grams), math.floor(cap_share * row["total_registrations"] + EPS))
    credits = min(possible, math.ceil(row["nonzev_registrations"]))   # no use converting beyond the obligation
    row.update(co2_credit_cap=cap_share * row["total_registrations"], co2_credits=credits,
               co2_position_after_conversion=row["co2_position"] - credits * grams)


def _meet_obligation(row, state, strategy):
    need = row["nonzev_registrations"] - row["co2_credits"]
    current_used = min(row["allowances"], max(need, 0.0))
    need -= current_used
    banked_used = state.take_bank(need) if (strategy.banking and need > EPS) else 0.0
    need -= banked_used
    row.update(current_used=current_used, spare_current=row["allowances"] - current_used,
               banked_used=banked_used, deficit_before_market=max(need, 0.0),
               # surplus (+) or shortfall (-) after own credits and bank, before any trading
               own_position=row["allowances"] - current_used - max(need, 0.0),
               bought=0.0, sold=0.0, sold_from_bank=0.0)


def _clear_co2_market(rows):
    supply = sum(r["co2_position"] for r in rows if r["in_co2_scheme"] and r["co2_position"] > 0)
    demand = sum(-r["co2_position"] for r in rows if r["in_co2_scheme"] and r["co2_position"] < 0)
    traded = min(supply, demand)
    if traded <= 0:
        return
    for r in rows:
        if not r["in_co2_scheme"]:
            continue
        if r["co2_position"] > 0:
            r["co2_traded"] = -traded * r["co2_position"] / supply
        elif r["co2_position"] < 0:
            r["co2_traded"] = traded * -r["co2_position"] / demand
        r["co2_position"] += r["co2_traded"]


# ---------------------------------------------------------------- pass B

def _clear_market(rows, states, uplift_rate, year):
    # Participants keep enough spare current allowances to repay their own debt first.
    offers = {}
    for r in rows:
        st = states[r["group"]]
        owed = sum(a * (1 + uplift_rate) ** (year - y) for y, a in st.debt if y < year)
        offers[r["group"]] = max(r["spare_current"] - owed, 0.0) + st.bank_total()
    demand = sum(r["deficit_before_market"] for r in rows)
    supply = sum(offers.values())
    traded = min(supply, demand)
    if traded <= EPS:
        return
    for r in rows:
        if r["deficit_before_market"] > 0:
            r["bought"] = traded * r["deficit_before_market"] / demand
        offer = offers[r["group"]]
        if offer > 0:
            sell = traded * offer / supply
            from_current = min(sell, r["spare_current"])
            r["spare_current"] -= from_current
            r["sold_from_bank"] = states[r["group"]].take_bank(sell - from_current)
            r["sold"] = from_current + r["sold_from_bank"]


# ---------------------------------------------------------------- pass C

def _close_year(row, year, params, vt, strategy, state):
    deficit = max(row["deficit_before_market"] - row["bought"], 0.0)

    borrow_cap = _rule(params, vt, "borrowing.cap", year) * row["total_registrations"]
    borrowed = min(deficit, borrow_cap) if strategy.borrowing else 0.0
    if borrowed > EPS:
        state.debt.append([year, borrowed])
    shortfall = deficit - borrowed

    spare = row["spare_current"]

    # 1. Cover a CO2 deficit with spare registration allowances (art. 35).
    zev_to_co2 = 0
    co2_left = row["co2_position_after_conversion"] if row["in_co2_scheme"] else 0.0
    if strategy.zev_to_co2 and co2_left < 0 and borrowed <= EPS and shortfall <= EPS and spare > 0:
        g = _rule(params, vt, "zev_to_co2.grams_per_allowance", year)
        zev_to_co2 = min(math.ceil(-co2_left / g), math.floor(spare + EPS))
        spare -= zev_to_co2
        co2_left = min(co2_left + zev_to_co2 * g, 0.0)

    # 2. Repay earlier borrowings, oldest first, with uplift.
    rate = _rule(params, vt, "borrowing.uplift_rate", year)
    repaid = uplift_paid = 0.0
    for lot in state.debt:
        if lot[0] >= year or spare <= EPS:
            continue
        k = year - lot[0]
        cost_per = 1 + ((1 + rate) ** k - 1)
        principal = min(lot[1], spare / cost_per)
        uplift = math.ceil(principal * ((1 + rate) ** k - 1) - EPS) if principal > EPS else 0
        if principal + uplift > spare + EPS:          # rounding up the uplift may not fit
            principal = max(spare - uplift, 0.0)
        lot[1] -= principal
        spare -= principal + uplift
        repaid += principal
        uplift_paid += uplift
    state.debt = [lot for lot in state.debt if lot[1] > EPS]

    # 3. Bank what's left, unless debt is outstanding (art. 15(14)).
    banked = 0.0
    last_usable = _rule(params, vt, "banking.last_usable_year", year)
    if strategy.banking and spare > EPS and not state.debt and year < last_usable:
        state.bank.append([year, spare])
        banked = spare
    unused = spare - banked

    # Debt still owed at the repayment deadline is paid at the per-vehicle rate (art. 28(2)(c)).
    payment_rate = _rule(params, vt, "payment_per_vehicle_gbp", year)
    debt_defaulted = 0.0
    if year >= _rule(params, vt, "borrowing.repay_by", year) and state.debt:
        debt_defaulted = sum(a * (1 + rate) ** (year - y) for y, a in state.debt)
        state.debt = []

    co2_rate = _rule(params, vt, "co2_scheme.payment_per_gram_gbp", year)
    row.update(
        borrowed=borrowed, shortfall=shortfall, zev_to_co2_allowances=zev_to_co2,
        repaid=repaid, uplift_paid=uplift_paid, banked=banked, unused_allowances=unused,
        bank_end=state.bank_total(), debt_end=state.debt_total(), debt_defaulted=debt_defaulted,
        co2_deficit_unpaid=max(-co2_left, 0.0),
        zev_payment_gbp=(shortfall + debt_defaulted) * payment_rate,
        co2_payment_gbp=max(-co2_left, 0.0) * co2_rate,
    )
    row["payment_gbp"] = row["zev_payment_gbp"] + row["co2_payment_gbp"]
    # ZEV share the participant needed after CO2 credits, before banking/borrowing/trading.
    row["required_zev_share_after_co2"] = (
        1 - (row["allowances"] + row["co2_credits"]) / row["total_registrations"] if row["total_registrations"] else float("nan")
    )


# ---------------------------------------------------------------- inputs

def attach_baselines(summary: pd.DataFrame, pools: pd.DataFrame, entity_baselines: pd.DataFrame | None = None) -> pd.DataFrame:
    """Add co2_baseline to a car summary from data.summarise(by="pool" or "entity").

    The official baseline comes from VETS_Pool_Baselines: a pool's baseline, or a standalone
    entity's own baseline. Groups with no official figure fall back to the estimated
    co2_target in zev_entity_baseline, matched on entity name. `co2_baseline_source`
    records which was used.
    """
    official = pools.dropna(subset=["co2_baseline"])
    by_pool = official[official["compliance_pool"] != "Standalone"].groupby("compliance_pool")["co2_baseline"].first()
    by_entity = official[official["compliance_pool"] == "Standalone"].groupby("regulated_entity")["co2_baseline"].first()
    lookup = pd.concat([by_pool, by_entity])
    lookup = lookup[~lookup.index.duplicated()]

    out = summary.copy()
    out["co2_baseline"] = out["group"].map(lookup)
    out["co2_baseline_source"] = out["co2_baseline"].notna().map({True: "official", False: None})
    if entity_baselines is not None:
        est = entity_baselines.dropna(subset=["co2_target"]).groupby("manufacturer")["co2_target"].first()
        fill = out["co2_baseline"].isna() & out["group"].isin(est.index)
        out.loc[fill, "co2_baseline"] = out.loc[fill, "group"].map(est)
        out.loc[fill, "co2_baseline_source"] = "estimated"
    return out


def market_total(inputs: pd.DataFrame, name: str = "MARKET") -> pd.DataFrame:
    """Collapse participants into one, so trading is perfect and frictionless.

    The CO2 baseline becomes the non-ZEV-weighted mean, so total CO2 allowances are unchanged.
    """
    df = inputs.copy()
    df["_allow"] = df["nonzev_registrations"] * df.get("co2_baseline", float("nan"))
    g = df.groupby("year").agg(
        total_registrations=("total_registrations", "sum"),
        nonzev_registrations=("nonzev_registrations", "sum"),
        co2_activity=("co2_activity", "sum"),
        _allow=("_allow", "sum"),
    ).reset_index()
    g["co2_baseline"] = g["_allow"] / g["nonzev_registrations"]
    g["group"] = name
    g["low_volume_eligible"] = False
    return g.drop(columns="_allow")
