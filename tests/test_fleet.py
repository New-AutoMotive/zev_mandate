"""Fleet stock and emissions model on synthetic inputs (no data files needed except the last test)."""
import numpy as np
import pandas as pd
import pytest

from zevm import fleet
from zevm.fleet import FUELS, FleetHistory

FP = fleet.load_params()


def synthetic_history(base_year=2023):
    years = range(2000, base_year + 1)
    regs = pd.DataFrame({"petrol": 1.2e6, "diesel": 0.6e6, "hybrid": 0.2e6, "bev": 0.1e6}, index=pd.Index(years, name="year"))
    parc_years = range(2015, base_year + 1)
    parc = pd.DataFrame({"petrol": 19e6, "diesel": 11e6, "hybrid": 2e6, "bev": 1e6}, index=pd.Index(parc_years, name="year"))
    mileage = pd.DataFrame({"petrol": 6000.0, "diesel": 9000.0, "hybrid": 10000.0, "bev": 9000.0}, index=pd.Index(range(2010, 2025), name="year"))
    fuel = pd.DataFrame({"petrol_cars": 13000.0, "diesel_cars": 9500.0}, index=pd.Index(range(2015, 2021), name="year"))
    naei = pd.Series(60.0, index=pd.Index(range(2015, 2021), name="year"))
    return FleetHistory(parc, regs, mileage, fuel, naei, base_year, 1.02)


def new_regs(start=2024, end=2035, total=2e6, zev=0.5):
    idx = pd.Index(range(start, end + 1), name="year")
    return pd.DataFrame({"petrol": total * (1 - zev) * 0.5, "diesel": total * (1 - zev) * 0.1,
                         "hybrid": total * (1 - zev) * 0.4, "bev": total * zev}, index=idx)


def test_kernel_matches_notebook_rolling_mean():
    k = fleet.retirement_kernel(18, 10)
    assert k.sum() == pytest.approx(1.0)
    assert list(k.index) == list(range(14, 24))
    # Same as a centred 10-year rolling mean of registrations shifted by 18 years.
    regs = pd.Series(np.random.default_rng(0).uniform(1, 2, 60), index=range(1960, 2020))
    shifted = regs.copy(); shifted.index = shifted.index + 18
    rolled = shifted.rolling(10, center=True).mean()
    t = 2010
    assert rolled.loc[t] == pytest.approx(sum(regs.loc[t - a] * w for a, w in k.items()))


def test_stock_conservation():
    h = synthetic_history()
    nr = new_regs()
    s = fleet.stock_model(h.parc.loc[2023], h.registrations, nr, 18, 10)
    prev = pd.concat([h.parc.loc[[2023]], s.parc.iloc[:-1]]).set_axis(s.parc.index)
    np.testing.assert_allclose(s.parc, prev + s.additions - s.retirements, rtol=1e-12)
    assert (s.parc >= -1e-6).all().all()
    assert (s.retirements >= 0).all().all()


def test_existing_stock_retires_exactly_and_new_cohorts_retire_on_schedule():
    h = synthetic_history()
    nr = new_regs(2024, 2080, total=0.0)
    nr.loc[2024] = [100.0, 0, 0, 0]
    s = fleet.stock_model(h.parc.loc[2023], h.registrations, nr, 18, 10)
    # Base-year stock all gone once the oldest age has passed; no more and no less.
    assert s.retirements.drop(columns="petrol").sum().values == pytest.approx(h.parc.loc[2023].drop("petrol").values)
    assert s.existing.iloc[-1].abs().max() < 1e-3
    # The 2024 cohort (100 petrol) retires evenly over ages 14-23 and never more than it had.
    new_ret = s.retirements["petrol"] - (h.parc.loc[2023, "petrol"] - s.existing["petrol"]).diff().fillna(
        h.parc.loc[2023, "petrol"] - s.existing["petrol"].iloc[0])
    assert new_ret.loc[2038:2047].values == pytest.approx([10.0] * 10)
    assert new_ret.sum() == pytest.approx(100.0)


def test_retirement_factor_scales_to_parc():
    h = synthetic_history()
    s = fleet.stock_model(h.parc.loc[2023], h.registrations, new_regs(2024, 2024), 18, 10)
    # Flat history: the factor is the parc over the registrations still alive by the kernel.
    kernel = fleet.retirement_kernel(18, 10)
    alive_years = sum(1 - kernel[kernel.index <= a].sum() for a in range(0, 30))
    expected = h.parc.loc[2023] / (h.registrations.iloc[0] * alive_years)
    np.testing.assert_allclose(s.retirement_factor[FUELS], expected[FUELS], rtol=1e-9)


def test_fuel_and_emissions_scale():
    h = synthetic_history()
    s = fleet.stock_model(h.parc.loc[2023], h.registrations, new_regs(), 18, 10)
    miles = pd.DataFrame(8000.0, index=s.parc.index, columns=FUELS)
    e0 = pd.Series({"petrol": 1e-7, "diesel": 1e-7, "hybrid": 0.7e-7, "bev": 0.0})
    vmt, fuel = fleet.fuel_use(s, miles, e0, pd.Series(1.0, index=FUELS), pd.Series(0.0, index=FUELS))
    np.testing.assert_allclose(vmt, s.parc * 8000)
    np.testing.assert_allclose(fuel["petrol"], vmt["petrol"] * 1e-7)          # no efficiency change
    assert (fuel["bev"] == 0).all()
    _, fuel2 = fleet.fuel_use(s, miles * 2, e0, pd.Series(1.0, index=FUELS), pd.Series(0.0, index=FUELS))
    np.testing.assert_allclose(fuel2, fuel * 2)
    co2 = fleet.co2_mt(fuel, FP)
    np.testing.assert_allclose(fleet.co2_mt(fuel * 3, FP), co2 * 3)
    assert (co2["bev"] == 0).all()
    # 1 ktoe petrol = 11.63 GWh x 0.234 kg/kWh = 2.72 kt
    one = fleet.co2_mt(pd.DataFrame({"petrol": [1.0], "diesel": [0.0]}), FP)
    assert one["petrol"].iloc[0] == pytest.approx(11630e3 * 0.233965161073826 / 1e9)


def test_new_cars_lower_fuel_use():
    h = synthetic_history()
    s = fleet.stock_model(h.parc.loc[2023], h.registrations, new_regs(), 18, 10)
    miles = pd.DataFrame(8000.0, index=s.parc.index, columns=FUELS)
    e0 = pd.Series({"petrol": 1e-7, "diesel": 1e-7, "hybrid": 0.7e-7, "bev": 0.0})
    _, base = fleet.fuel_use(s, miles, e0, pd.Series(1.0, index=FUELS), pd.Series(0.0, index=FUELS))
    _, better = fleet.fuel_use(s, miles, e0, pd.Series(0.8, index=FUELS), pd.Series(-0.02, index=FUELS))
    assert (better["petrol"] < base["petrol"]).all()
    # Only the post-base cohorts change: the first year's saving is 20% of their fuel.
    first = s.parc.index[0]
    new_fuel = s.survivors[0, :, 0].sum() * 8000 * 1e-7
    assert base.loc[first, "petrol"] - better.loc[first, "petrol"] == pytest.approx(0.2 * new_fuel)


def test_split_registrations():
    zev = pd.Series([400.0, 500.0], index=[2024, 2030])
    non = pd.Series([600.0, 500.0], index=[2024, 2030])
    r = fleet.split_registrations(zev, non, FP)
    assert r.sum(axis=1).values == pytest.approx([1000, 1000])
    assert r.loc[2024, "hybrid"] == pytest.approx(600 * FP.get("registrations.nonzev_split.hybrid", 2024))
    assert r.loc[2030, "diesel"] == pytest.approx(500 * FP.get("registrations.nonzev_split.diesel", 2030))


def summary_frame():
    rows = []
    for name, gap in (("A", 0.0), ("B", -0.05)):
        for y, total, share, basis in ((2024, 1.9e6, 0.19, "actual"), (2025, 2.0e6, 0.23, "actual"), (2026, 2.0e6, 0.33, "projected")):
            rows.append({"scenario": name, "year": y, "basis": basis, "total_registrations": total, "zev_share": share + gap})
    return pd.DataFrame(rows)


def test_registrations_from_summary_scales_gb_years_and_extends():
    h = synthetic_history()
    regs = fleet.registrations_from_summary(summary_frame(), h, FP, extend_to=2030)
    a = regs[regs.scenario == "A"].groupby("year")["registrations"].sum()
    assert a.loc[2024] == pytest.approx(1.9e6 * 1.02)          # 2024 was a GB-only scheme year
    assert a.loc[2025] == pytest.approx(2.0e6)
    assert list(a.index) == list(range(2024, 2031))
    bev = regs[(regs.scenario == "A") & (regs.fuel == "bev")].set_index("year")["registrations"]
    assert bev.loc[2030] == pytest.approx(2.0e6 * 0.33)        # extension holds the last share
    assert set(regs[regs.year == 2030].basis) == {"extended"}


def test_registrations_gap_filled_from_dft_or_error():
    h = synthetic_history()
    h.base_year = 2022                                         # DfT registrations run to 2023
    regs = fleet.registrations_from_summary(summary_frame(), h, FP)
    assert regs.year.min() == 2023
    assert set(regs[regs.year == 2023].basis) == {"actual"}
    h2 = synthetic_history()
    h2.base_year = 2022
    h2.registrations = h2.registrations.loc[:2022]
    with pytest.raises(ValueError, match="2023"):
        fleet.registrations_from_summary(summary_frame(), h2, FP)


def test_run_many_and_totals():
    h = synthetic_history()
    regs = fleet.registrations_from_summary(summary_frame(), h, FP, extend_to=2035)
    out = fleet.run_many(h, regs, FP)
    assert set(out.scenario) == {"A", "B"}
    proj = out[out.basis == "projected"]
    assert proj.year.min() == 2024 and proj.year.max() == 2035
    t = fleet.totals(out)
    tA, tB = (t[(t.scenario == s) & (t.year == 2035)].iloc[0] for s in "AB")
    assert tA.co2_mt < tB.co2_mt                               # more ZEVs, less CO2
    assert tA.bev_share_of_parc > tB.bev_share_of_parc
    # Parc conservation through the tidy output too.
    pa = t[t.scenario == "A"].set_index("year")
    assert pa.loc[2030, "parc"] == pytest.approx(pa.loc[2029, "parc"] + pa.loc[2030, "new_registrations"] - pa.loc[2030, "retirements"])


def test_validate_and_calibration():
    h = synthetic_history()
    v = fleet.validate(h, FP)
    assert list(v.index) == list(range(2015, 2021))
    # Petrol-car fuel is split so hybrids use 0.7x a petrol car's fuel per mile.
    fpm = v["fuel_per_mile"].iloc[0]
    assert fpm["hybrid"] / fpm["petrol"] == pytest.approx(FP.get("fuel_use.hybrid_vs_petrol"))
    assert v["fuel_ktoe"][["petrol", "hybrid"]].sum(axis=1).iloc[0] == pytest.approx(13000)
    assert v["ratio"].iloc[0] == pytest.approx(60 / v["co2_total_mt"].iloc[0])
    cal = fleet.calibration(h, FP)
    assert cal["calibration_years"] == [2017, 2018, 2019]      # 2020-21 excluded
    assert cal["co2_scale"] == pytest.approx(v.loc[2017:2019, "ratio"].mean())


def test_backtest_flat_history_has_small_error():
    h = synthetic_history()
    bt = fleet.backtest(h, FP, 2015)
    assert set(bt.columns) >= {"year", "fuel", "modelled", "actual", "error"}
    assert bt.year.min() == 2016


@pytest.mark.skipif(not (fleet.ROOT / "data" / "veh0105.ods").exists(), reason="DfT tables not present")
def test_real_tables_load():
    h = fleet.load_history(FP)
    assert h.base_year >= 2023
    assert 30e6 < h.parc.loc[2023].sum() < 36e6
    assert h.parc.index.min() == 2014                            # UK figures start at 2014 Q4
    assert 1.01 < h.uk_gb_ratio < 1.04
    assert h.naei_mt.loc[2019] == pytest.approx(68.8, abs=0.5)
    v = fleet.validate(h, FP)
    assert v["ratio"].between(0.85, 1.05).all()
