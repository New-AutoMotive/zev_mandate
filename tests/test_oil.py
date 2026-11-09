"""Oil demand and intensity on synthetic inputs, plus a load test of the real files."""
import pandas as pd
import pytest

from zevm import oil
from zevm.oil import OilInputs

OP = oil.load_params()


def synthetic_inputs():
    years = pd.Index(range(2023, 2026), name="year")
    dukes = pd.DataFrame({"total_kt": [59000.0, 60000.0, 61000.0], "road_kt": [32000.0, 32000.0, 33000.0]}, index=years)
    split = pd.DataFrame({"car_share": [0.6, 0.6], "van_share": [0.2, 0.2], "other_share": [0.2, 0.2]},
                         index=pd.Index([2023, 2024], name="year"))
    eep_years = pd.Index(range(2023, 2036), name="year")
    eep = pd.DataFrame({"non_road_ktoe": [20000.0] * 3 + [20000.0 * (1 - 0.01 * (y - 2025)) for y in range(2026, 2036)]},
                       index=eep_years)
    gdp = oil.gdp_path(pd.Series([2.9e6, 2.95e6, 3.0e6], index=years), {2026: 0.02}, 2035)
    return OilInputs(dukes, split, eep, gdp, 2025)


def fuel(scenario, index_by_year):
    return pd.DataFrame({"scenario": scenario, "year": list(index_by_year), "fuel_index": list(index_by_year.values())})


def test_gdp_path_compounds_and_carries_growth_forward():
    g = oil.gdp_path(pd.Series([100.0], index=[2025]), {2026: 0.02, 2028: 0.01}, 2029)
    assert g.loc[2027] == pytest.approx(100 * 1.02 ** 2)
    assert g.loc[2029] == pytest.approx(100 * 1.02 ** 2 * 1.01 ** 2)


def test_history_splits_road_and_uses_latest_split_after_last_year():
    h = oil.history(synthetic_inputs(), start=2023).set_index("year")
    assert h.loc[2025, "oil_kt"] == 61000
    assert h.loc[2025, "car_kt"] == pytest.approx(33000 * 0.6)          # 2024 shares carried to 2025
    assert h.loc[2025, "non_road_kt"] == 28000
    assert h.loc[2025, "intensity_index"] == pytest.approx(100)
    assert h.loc[2025, "oil_t_per_gdp_m"] == pytest.approx(61000 * 1e3 / 3.0e6)


def test_projection_anchors_on_dukes_and_follows_indices():
    inp = synthetic_inputs()
    years = range(2026, 2036)
    car = fuel("S", {y: 1 - 0.05 * (y - 2025) for y in years})
    van = fuel("S", {y: 1.0 for y in years})
    p = oil.project(inp, OP, car, van).set_index("year")
    chg = OP["other_road"]["annual_change"]
    assert p.loc[2030, "car_kt"] == pytest.approx(33000 * 0.6 * 0.75)
    assert p.loc[2030, "van_kt"] == pytest.approx(33000 * 0.2)
    assert p.loc[2030, "other_road_kt"] == pytest.approx(33000 * 0.2 * (1 + chg) ** 5)
    assert p.loc[2030, "non_road_kt"] == pytest.approx(28000 * 0.95)
    assert p.loc[2030, "oil_kt"] == pytest.approx(p.loc[2030, ["car_kt", "van_kt", "other_road_kt", "non_road_kt"]].sum())


def test_scenarios_differ_only_through_cars_and_vans():
    inp = synthetic_inputs()
    years = range(2026, 2036)
    car = pd.concat([fuel("fast", {y: 0.9 for y in years}), fuel("slow", {y: 1.0 for y in years})])
    van = pd.concat([fuel("fast", {y: 1.0 for y in years}), fuel("slow", {y: 1.0 for y in years})])
    p = oil.project(inp, OP, car, van)
    w = p.pivot(index="year", columns="scenario", values=["car_kt", "non_road_kt", "other_road_kt"])
    assert (w["non_road_kt"]["fast"] == w["non_road_kt"]["slow"]).all()
    assert (w["car_kt"]["slow"] - w["car_kt"]["fast"]).iloc[0] == pytest.approx(33000 * 0.6 * 0.1)


def test_fleet_base_year_mismatch_rejected():
    inp = synthetic_inputs()
    car = fuel("S", {2026: 1.0}); car.attrs["fleet_base_year"] = 2024
    with pytest.raises(ValueError, match="base year"):
        oil.project(inp, OP, car, fuel("S", {2026: 1.0}), end_year=2026)


@pytest.mark.skipif(not (oil.ROOT / OP["dukes"]["file"]).exists(), reason="data/raw not downloaded")
def test_real_inputs_load():
    inp = oil.load_inputs(OP)
    assert inp.base_year == 2025
    assert inp.dukes.loc[2025, "total_kt"] == pytest.approx(60759, abs=1)
    assert 0.55 < inp.road_split.loc[2024, "car_share"] < 0.7
    assert inp.gdp.loc[2025] == pytest.approx(2940195)
    assert inp.gdp.loc[2026] == pytest.approx(2940195 * 1.011)
    h = oil.history(inp, 2010).set_index("year")
    assert h.loc[2010, "oil_t_per_gdp_m"] > h.loc[2025, "oil_t_per_gdp_m"]
