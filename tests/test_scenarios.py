"""Scenario layer on synthetic data: no BigQuery needed."""
import numpy as np
import pandas as pd
import pytest

from zevm import rules, scenarios
from zevm.scenarios import History, LateDataCurve, Scenario

AS_OF = pd.Timestamp("2026-10-01")


def test_months_old():
    assert scenarios.months_old(pd.Timestamp("2026-09-01"), AS_OF) == 1
    assert scenarios.months_old(pd.Timestamp("2025-10-01"), AS_OF) == 12


def test_fit_curve_recovers_parameters_despite_outlier():
    age = np.arange(1, 19)
    true = LateDataCurve(R=0.12, tau=3.0, n_points=0, n_outliers=0)
    ratio = true.ratio(age)
    ratio[11] = 1.28                      # like Oct 2025
    fit = scenarios.fit_curve(age, ratio)
    assert fit.R == pytest.approx(0.12, abs=0.005)
    assert fit.tau == pytest.approx(3.0, abs=0.3)
    assert fit.n_outliers == 1


def test_factor_lifts_new_months_most():
    c = LateDataCurve(R=0.1, tau=3, n_points=0, n_outliers=0)
    assert c.factor(1) == pytest.approx(1.1)
    assert c.factor(60) == pytest.approx(1.0, abs=1e-6)


def test_apply_uplift_uses_fuel_specific_curve():
    raw = pd.DataFrame({
        "month": ["2026-09-01", "2026-09-01"], "region": "GB", "make": "A", "manufacturer": "A",
        "fuel_type": ["ELECTRICITY", "PETROL"], "registrations": [100, 100], "with_co2": [0, 100], "co2_sum": [0.0, 12000.0],
    })
    curves = {"zev": LateDataCurve(0.05, 3, 0, 0), "nonzev": LateDataCurve(0.10, 3, 0, 0)}
    out = scenarios.apply_late_data_uplift(raw, curves, AS_OF)
    assert out.registrations.tolist() == pytest.approx([105, 110])
    assert out.co2_sum.iloc[1] == pytest.approx(13200)


def synthetic_monthly(current_months=9):
    """Two groups, 2024-2025 complete, 2026 partial; same seasonality every year."""
    season = np.array([6, 4, 14, 6, 7, 9, 7, 4, 14, 7, 7, 15], float)   # % of year
    zev_by_month = np.array([.20] * 9 + [.30] * 3)                        # ZEV share jumps in Q4
    rows = []
    for year in (2024, 2025, 2026):
        months = range(1, 13) if year < 2026 else range(1, current_months + 1)
        for g, size in (("A", 1000), ("B", 500)):
            for m in months:
                tot = size * season[m - 1]
                z = tot * zev_by_month[m - 1]
                rows.append({"group": g, "month": pd.Timestamp(year, m, 1), "total_registrations": tot,
                             "zev_registrations": z, "nonzev_registrations": tot - z, "co2_activity": (tot - z) * 120})
    return pd.DataFrame(rows)


def test_extrapolate_year_recovers_full_year():
    full, info = scenarios.extrapolate_year(synthetic_monthly(), 2026)
    a = full.set_index("group").loc["A"]
    assert info["share_of_year_seen"] == pytest.approx(0.71)
    assert a.total_registrations == pytest.approx(100_000)
    assert a.zev_registrations == pytest.approx(0.20 * 71_000 + 0.30 * 29_000)   # Q4 jump carried over
    assert info["rest_of_year_zev_logit_shift"] > 0
    assert a.co2_activity / a.nonzev_registrations == pytest.approx(120)


def test_allocate_logit_hits_market_keeps_order_moves_laggards():
    totals = pd.Series({"lead": 100.0, "mid": 300.0, "zero": 100.0})
    base = pd.Series({"lead": 0.9, "mid": 0.2, "zero": 0.0})
    z = scenarios.allocate_zev_shares(totals, base, 0.6)
    assert (z * totals).sum() / totals.sum() == pytest.approx(0.6, abs=1e-6)
    assert z["lead"] > z["mid"] > z["zero"] > 0.002
    assert z.max() < 1
    assert (scenarios.allocate_zev_shares(totals, base, 1.0) == 1).all()


def test_allocate_scale_caps_at_one():
    totals = pd.Series({"a": 100.0, "b": 100.0})
    z = scenarios.allocate_zev_shares(totals, pd.Series({"a": 0.8, "b": 0.2}), 0.7, method="scale")
    assert z["a"] == 1.0 and z["b"] == pytest.approx(0.4)


def tiny_history():
    yearly = pd.DataFrame([
        # group, year, total, zev, nonzev, co2_activity, baseline
        ("A (POOL)", 2024, 100_000, 22_000, 78_000, 78_000 * 125, 130.0, "actual"),
        ("B LTD", 2024, 50_000, 5_000, 45_000, 45_000 * 128, 130.0, "actual"),
        ("A (POOL)", 2025, 100_000, 30_000, 70_000, 70_000 * 125, 130.0, "actual"),
        ("B LTD", 2025, 50_000, 7_000, 43_000, 43_000 * 128, 130.0, "actual"),
        ("A (POOL)", 2026, 100_000, 36_000, 64_000, 64_000 * 124, 130.0, "estimated"),
        ("B LTD", 2026, 50_000, 10_000, 40_000, 40_000 * 127, 130.0, "estimated"),
    ], columns=["group", "year", "total_registrations", "zev_registrations", "nonzev_registrations", "co2_activity",
                "co2_baseline", "basis"])
    return History("car", "pool", yearly, pd.DataFrame(), AS_OF)


def test_project_follows_target_and_growth():
    h = tiny_history()
    inp = scenarios.project(h, Scenario("t", market_growth=0.02, nonzev_co2_change=-0.01, zev_share_vs_target=0.01))
    vets = rules.load("vets")
    proj = inp[inp.basis == "projected"]
    assert sorted(proj.year.unique()) == [2027, 2028, 2029, 2030]
    for y, g in proj.groupby("year"):
        assert g.total_registrations.sum() == pytest.approx(150_000 * 1.02 ** (y - 2026))
        assert g.zev_registrations.sum() / g.total_registrations.sum() == pytest.approx(vets.zev_target("car", y) + 0.01, abs=1e-6)
    a29 = proj[(proj.group == "A (POOL)") & (proj.year == 2029)].iloc[0]
    assert a29.co2_activity / a29.nonzev_registrations == pytest.approx(124 * 0.99 ** 3)


def test_project_constant_share_and_explicit_market_size():
    inp = scenarios.project(tiny_history(), Scenario("t", zev_share=0.4, market_size={2027: 160_000, 2029: 120_000}))
    p = inp[inp.basis == "projected"].groupby("year")[["total_registrations", "zev_registrations"]].sum()
    assert p.loc[2028, "total_registrations"] == pytest.approx(160_000)
    assert p.loc[2030, "total_registrations"] == pytest.approx(120_000)
    assert (p.zev_registrations / p.total_registrations).tolist() == pytest.approx([0.4] * 4)


def test_run_many_and_summary():
    h = tiny_history()
    res = scenarios.run_many(h, [
        Scenario("on target"),
        Scenario("5pp short, no flex", zev_share_vs_target=-0.05,
                 strategy=scenarios.Strategy(co2_to_zev=False, borrowing=False, banking=False, trading="none")),
    ])
    assert set(res.scenario) == {"on target", "5pp short, no flex"}
    assert res[res.year == 2024].basis.eq("actual").all()
    s = scenarios.market_summary(res).set_index(["scenario", "year"])
    assert s.loc[("5pp short, no flex", 2028), "payment_gbp_m"] > 0
    assert s.loc[("on target", 2028), "zev_share"] == pytest.approx(0.52, abs=1e-6)
    assert s.loc[("on target", 2026), "basis"] == "estimated"


def test_scenario_params_file_changes_result():
    h = tiny_history()
    short = dict(zev_share_vs_target=-0.03, strategy=scenarios.Strategy(trading="none"))
    base = scenarios.market_summary(scenarios.run(h, Scenario("law", **short))).set_index("year")
    nob = scenarios.market_summary(scenarios.run(h, Scenario("no borrowing", params="scenarios/example_no_borrowing_from_2027", **short))).set_index("year")
    assert base.loc[2027, "borrowed"] > 0
    assert nob.loc[2027, "borrowed"] == 0
    assert nob.loc[2027, "payment_gbp_m"] > base.loc[2027, "payment_gbp_m"]
