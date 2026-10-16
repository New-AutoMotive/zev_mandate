"""Hand-worked cases for the compliance engine under params/vets.yaml (cars).

Car rules used below: targets 2024-30 = 22/28/33/38/52/66/80%; CO2 conversion cap
2025 = 25.2% of registrations at 167 g/credit; borrowing cap 2025 = 19.6%, 2029 = 6.6%,
2030 = 0; uplift 3.5% compound; payment £15k (2024) then £12k; ZEV->CO2 at 135 g.
"""
import math

import pandas as pd
import pytest

from zevm import compliance, rules
from zevm.compliance import Strategy

NAN = float("nan")


@pytest.fixture(scope="module")
def vets():
    return rules.load("vets")


def inputs(rows):
    """rows: (group, year, total, nonzev, co2_activity, baseline)"""
    df = pd.DataFrame(rows, columns=["group", "year", "total_registrations", "nonzev_registrations", "co2_activity", "co2_baseline"])
    df["low_volume_eligible"] = False
    return df


def run(vets, rows, **strategy):
    res = compliance.run(inputs(rows), vets, "car", Strategy(**strategy))
    return res.set_index(["group", "year"])


def test_surplus_is_banked_and_used_next_year(vets):
    r = run(vets, [("A", 2025, 10_000, 6_000, 0, NAN), ("A", 2026, 10_000, 7_000, 0, NAN)])
    assert r.loc[("A", 2025), "allowances"] == 7_200
    assert r.loc[("A", 2025), "banked"] == 1_200
    assert r.loc[("A", 2026), "banked_used"] == 300       # 2026 allowances 6,700 vs 7,000 non-ZEVs
    assert r.loc[("A", 2026), "bank_end"] == 900
    assert r["payment_gbp"].sum() == 0


def test_co2_credits_then_borrow_then_repay_with_uplift(vets):
    r = run(vets, [
        ("A", 2025, 10_000, 8_000, 8_000 * 120, 130),   # CO2 surplus 80,000 g -> 479 credits
        ("A", 2026, 10_000, 6_000, 6_000 * 130, 130),   # spare 700, no CO2 surplus
    ])
    y1, y2 = r.loc[("A", 2025)], r.loc[("A", 2026)]
    assert y1.co2_credits == math.floor(80_000 / 167) == 479
    assert y1.deficit_before_market == pytest.approx(8_000 - 479 - 7_200)   # 321
    assert y1.borrowed == pytest.approx(321)
    assert y1.banked == 0 and y1.payment_gbp == 0
    assert y2.repaid == pytest.approx(321)
    assert y2.uplift_paid == math.ceil(321 * 0.035)                         # 12, rounded up
    assert y2.banked == pytest.approx(700 - 321 - 12)
    assert y2.debt_end == 0


def test_co2_conversion_cap_binds(vets):
    # Huge CO2 surplus, but credits are capped at 25.2% of 10,000 registrations.
    r = run(vets, [("A", 2025, 10_000, 10_000, 10_000 * 50, 150)])
    assert r.loc[("A", 2025), "co2_credits"] == 2_520


def test_market_trade_instead_of_borrowing(vets):
    rows = [("SELLER", 2025, 10_000, 6_200, 0, NAN), ("BUYER", 2025, 10_000, 7_600, 0, NAN)]
    traded = run(vets, rows)
    assert traded.loc[("BUYER", 2025), "bought"] == pytest.approx(400)
    assert traded.loc[("BUYER", 2025), "borrowed"] == 0
    assert traded.loc[("SELLER", 2025), "sold"] == pytest.approx(400)
    assert traded.loc[("SELLER", 2025), "banked"] == pytest.approx(600)
    alone = run(vets, rows, trading="none")
    assert alone.loc[("BUYER", 2025), "borrowed"] == pytest.approx(400)


def test_market_short_on_supply_is_pro_rata(vets):
    rows = [("S", 2025, 10_000, 7_100, 0, NAN),        # 100 spare
            ("B1", 2025, 10_000, 7_500, 0, NAN),       # 300 short
            ("B2", 2025, 10_000, 7_300, 0, NAN)]       # 100 short
    r = run(vets, rows, borrowing=False)
    assert r.loc[("B1", 2025), "bought"] == pytest.approx(75)
    assert r.loc[("B2", 2025), "bought"] == pytest.approx(25)
    assert r.loc[("B1", 2025), "zev_payment_gbp"] == pytest.approx(225 * 12_000)


def test_seller_repays_own_debt_before_selling(vets):
    r = run(vets, [
        ("S", 2025, 10_000, 7_400, 0, NAN),   # borrows 200
        ("S", 2026, 10_000, 6_200, 0, NAN),   # 500 spare; owes 200 x 1.035 = 207
        ("B", 2025, 10_000, 7_200, 0, NAN),
        ("B", 2026, 10_000, 7_700, 0, NAN),   # 1,000 short
    ])
    s = r.loc[("S", 2026)]
    assert s.sold == pytest.approx(500 - 200 * 1.035)
    assert s.repaid == pytest.approx(200, abs=1)


def test_shortfall_paid_at_year_rate(vets):
    r = run(vets, [("A", 2024, 10_000, 8_000, 0, NAN), ("A", 2025, 10_000, 8_000, 0, NAN)], borrowing=False)
    assert r.loc[("A", 2024), "zev_payment_gbp"] == pytest.approx(200 * 15_000)
    assert r.loc[("A", 2025), "zev_payment_gbp"] == pytest.approx(800 * 12_000)


def test_debt_unpaid_in_2030_is_charged_with_uplift(vets):
    rows = [("A", y, 10_000, (1 - t) * 10_000, 0, NAN) for y, t in zip(range(2024, 2029), [.22, .28, .33, .38, .52])]
    rows += [("A", 2029, 10_000, 3_500, 0, NAN), ("A", 2030, 10_000, 2_000, 0, NAN)]   # 2029: 100 short
    r = run(vets, rows)
    assert r.loc[("A", 2029), "borrowed"] == pytest.approx(100)
    last = r.loc[("A", 2030)]
    assert last.borrowed == 0                       # no borrowing for 2030
    assert last.debt_defaulted == pytest.approx(100 * 1.035)
    assert last.zev_payment_gbp == pytest.approx(100 * 1.035 * 12_000)


def test_banked_allowances_expire_after_three_more_years(vets):
    rows = [("A", 2024, 10_000, 6_800, 0, NAN)]     # 1,000 spare in 2024
    rows += [("A", y, 10_000, (1 - t) * 10_000, 0, NAN) for y, t in zip(range(2025, 2029), [.28, .33, .38, .52])]
    r = run(vets, rows, trading="none")
    assert r.loc[("A", 2027), "bank_start"] == 1_000
    assert r.loc[("A", 2028), "bank_expired"] == 1_000
    assert r.loc[("A", 2028), "bank_start"] == 0


def test_spare_allowances_cover_co2_deficit(vets):
    r = run(vets, [("A", 2025, 10_000, 6_000, 6_000 * 140, 130)])   # CO2 deficit 60,000 g
    a = r.loc[("A", 2025)]
    assert a.zev_to_co2_allowances == math.ceil(60_000 / 135)
    assert a.co2_payment_gbp == 0
    assert a.banked == pytest.approx(1_200 - 445)


def test_co2_deficit_paid_when_no_spare(vets):
    r = run(vets, [("A", 2025, 10_000, 7_200, 7_200 * 131, 130)])
    assert r.loc[("A", 2025), "co2_payment_gbp"] == pytest.approx(7_200 * 86)


def test_low_volume(vets):
    df = inputs([("MICRO LTD", 2025, 500, 500, 0, NAN), ("SMALL LTD", 2030, 2_000, 2_000, 0, NAN)])
    df["low_volume_eligible"] = True
    r = compliance.run(df, vets, "car").set_index(["group", "year"])
    assert r.loc[("MICRO LTD", 2025), "low_volume"] and r.loc[("MICRO LTD", 2025), "shortfall"] == 0
    assert not r.loc[("SMALL LTD", 2030), "low_volume"]      # small status not available for 2030


def test_pools_default_to_not_low_volume(vets):
    df = inputs([("TINY (POOL)", 2025, 500, 500, 0, NAN)]).drop(columns="low_volume_eligible")
    r = compliance.run(df, vets, "car")
    assert not r.low_volume.iloc[0]


def test_group_missing_a_year_still_ages_its_bank(vets):
    rows = [("A", 2024, 10_000, 6_800, 0, NAN), ("A", 2028, 10_000, 4_800, 0, NAN),
            ("B", 2025, 1, 0, 0, NAN), ("B", 2026, 1, 0, 0, NAN), ("B", 2027, 1, 0, 0, NAN)]
    r = run(vets, rows, trading="none")
    assert r.loc[("A", 2026), "total_registrations"] == 0
    assert r.loc[("A", 2028), "bank_expired"] == 1_000


def test_flexibilities_off_means_pay_for_every_gap(vets):
    r = run(vets, [("A", 2025, 10_000, 8_000, 8_000 * 100, 130)],
            co2_to_zev=False, borrowing=False, banking=False, trading="none", zev_to_co2=False)
    assert r.loc[("A", 2025), "zev_payment_gbp"] == pytest.approx(800 * 12_000)


def test_co2_market_reduces_convertible_surplus(vets):
    rows = [("GREEN", 2025, 10_000, 8_000, 8_000 * 120, 130),   # +80,000 g
            ("DIRTY", 2025, 10_000, 7_000, 7_000 * 135, 130)]   # -35,000 g
    r = run(vets, rows, co2_trading="market")
    assert r.loc[("GREEN", 2025), "co2_credits"] == math.floor(45_000 / 167)
    assert r.loc[("DIRTY", 2025), "co2_payment_gbp"] == 0


def test_market_total_preserves_co2_allowances():
    df = inputs([("A", 2025, 100, 80, 80 * 120, 130), ("B", 2025, 100, 60, 60 * 140, 150)])
    m = compliance.market_total(df)
    assert m.co2_baseline.iloc[0] * m.nonzev_registrations.iloc[0] == pytest.approx(80 * 130 + 60 * 150)


def test_attach_baselines():
    summary = pd.DataFrame({"group": ["VOLKSWAGEN PC (POOL)", "BMW AG", "TESLA INC", "NEWCO"], "year": 2025})
    pools = pd.DataFrame({
        "make": ["VW", "BMW", "TESLA"], "regulated_entity": ["VOLKSWAGEN AG", "BMW AG", "TESLA INC"],
        "compliance_pool": ["VOLKSWAGEN PC (POOL)", "Standalone", "Standalone"], "co2_baseline": [139.011, 130.872, NAN],
    })
    est = pd.DataFrame({"manufacturer": ["TESLA INC"], "co2_target": [112.7]})
    out = compliance.attach_baselines(summary, pools, est).set_index("group")
    assert out.loc["VOLKSWAGEN PC (POOL)", "co2_baseline"] == 139.011
    assert out.loc["BMW AG", "co2_baseline_source"] == "official"
    assert out.loc["TESLA INC", "co2_baseline_source"] == "estimated"
    assert pd.isna(out.loc["NEWCO", "co2_baseline"])
