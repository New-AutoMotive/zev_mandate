"""Electricity share of final energy on synthetic inputs, plus a load test of the real files."""
import pandas as pd
import pytest

from zevm import electricity as el, oil
from zevm.electricity import ElecInputs

EP, OP = el.load_params(), oil.load_params()


def synthetic_inputs():
    dukes = pd.DataFrame({
        "final_total": [132000.0], "non_energy_total": [4000.0], "final_electricity": [24000.0],
        "road_petroleum": [36000.0], "road_bioenergy": [2000.0], "road_gas": [200.0], "road_electricity": [800.0],
        "air_total": [13000.0],
    }, index=pd.Index([2025], name="year"))
    years = pd.Index(range(2025, 2036), name="year")
    eep = pd.DataFrame({"nonroad_electricity": [20000.0 * (1 + 0.02 * (y - 2025)) for y in years],
                        "nonroad_other": [60000.0] * len(years),
                        "international_aviation": [12000.0] * len(years), "aviation": [13000.0] * len(years)}, index=years)
    split = pd.DataFrame({"car_share": [0.6], "van_share": [0.2], "other_share": [0.2]}, index=pd.Index([2025], name="year"))
    return ElecInputs(dukes, eep, split, 2025)


def fleet_frame(fuel_index, bev_miles, base_bev):
    years = sorted(fuel_index)
    f = pd.DataFrame({"scenario": "S", "year": years, "fuel_index": [fuel_index[y] for y in years],
                      "bev_miles": [bev_miles[y] for y in years]})
    f.attrs.update(fleet_base_year=2025, base_bev_miles=base_bev)
    return f


def phev_frame(miles, base):
    f = pd.DataFrame({"scenario": "S", "year": sorted(miles), "phev_miles": [miles[y] for y in sorted(miles)]})
    f.attrs["base_phev_miles"] = base
    return f


def test_base_parts_add_up_to_final_energy_excluding_non_energy():
    b = el.base_parts(synthetic_inputs())
    assert b.sum() == pytest.approx(132000 - 4000)
    assert b.road_electricity + b.nonroad_electricity == pytest.approx(24000)
    assert b.car_liquid == pytest.approx(38000 * 0.6)


def test_ev_electricity_conversion():
    # 3.5 mi/kWh, 10% losses: 3.5bn miles -> 1.1 TWh = 1.1e9/11.63e6 ktoe
    assert el.ev_electricity_ktoe(3.5e9, 0, 0, EP) == pytest.approx(1.1e9 / 11.63e6)
    # PHEV: only the electric share of miles counts
    assert el.ev_electricity_ktoe(0, 0, 3.2e9, EP) == pytest.approx(0.25 * 1.1e9 / 11.63e6)


def test_projection_moves_road_electricity_by_modelled_change_only():
    inp = synthetic_inputs()
    years = range(2026, 2031)
    car = fleet_frame({y: 0.8 for y in years}, {y: 10e9 for y in years}, base_bev=3.5e9)
    van = fleet_frame({y: 1.0 for y in years}, {y: 0.0 for y in years}, base_bev=0.0)
    ph = phev_frame({y: 0.0 for y in years}, base=0.0)
    p = el.project(inp, EP, OP, car, van, ph, mode="held", end_year=2030).set_index("year")
    added = el.ev_electricity_ktoe(10e9, 0, 0, EP) - el.ev_electricity_ktoe(3.5e9, 0, 0, EP)
    assert p.loc[2028, "road_electricity"] == pytest.approx(800 + added)
    assert p.loc[2028, "car_liquid"] == pytest.approx(38000 * 0.6 * 0.8)
    b = el.base_parts(inp)
    assert p.loc[2028, "nonroad_other"] == pytest.approx(b.nonroad_other)          # held
    assert p.loc[2028, "electricity_share"] == pytest.approx(p.loc[2028, "electricity"] / p.loc[2028, "final_energy"])


def test_eep_mode_indexes_rest_of_economy():
    inp = synthetic_inputs()
    years = range(2026, 2031)
    car = fleet_frame({y: 1.0 for y in years}, {y: 3.5e9 for y in years}, base_bev=3.5e9)
    van = fleet_frame({y: 1.0 for y in years}, {y: 0.0 for y in years}, base_bev=0.0)
    p = el.project(inp, EP, OP, car, van, phev_frame({y: 0.0 for y in years}, 0.0), mode="eep", end_year=2030).set_index("year")
    b = el.base_parts(inp)
    assert p.loc[2030, "nonroad_electricity"] == pytest.approx(b.nonroad_electricity * 1.10)
    assert p.loc[2030, "other_road_liquid"] == pytest.approx(b.other_road_liquid * (1 + OP["other_road"]["annual_change"]) ** 5)


def test_more_evs_raise_the_share_twice_over():
    """Electrifying miles adds electricity and removes (more) liquid fuel, so the share rises."""
    inp = synthetic_inputs()
    years = [2030]
    lo = el.project(inp, EP, OP, fleet_frame({2030: 0.9}, {2030: 5e9}, 3.5e9), fleet_frame({2030: 1.0}, {2030: 0}, 0),
                    phev_frame({2030: 0}, 0), mode="held", end_year=2030)
    hi = el.project(inp, EP, OP, fleet_frame({2030: 0.7}, {2030: 15e9}, 3.5e9), fleet_frame({2030: 1.0}, {2030: 0}, 0),
                    phev_frame({2030: 0}, 0), mode="held", end_year=2030)
    assert hi.electricity_share.iloc[0] > lo.electricity_share.iloc[0]
    assert hi.final_energy.iloc[0] < lo.final_energy.iloc[0]


@pytest.mark.skipif(not (el.ROOT / EP["dukes"]["file"]).exists(), reason="data/raw not downloaded")
def test_real_inputs_load():
    inp = el.load_inputs(EP)
    h = el.history(inp, 2010).set_index("year")
    assert h.loc[2025, "electricity_share"] == pytest.approx(23691 / (131875 - 4186), abs=1e-3)
    assert 0.17 < h.loc[2015, "electricity_share"] < 0.21
    assert h.loc[2025, "electricity_share_ex_intl_aviation"] > h.loc[2025, "electricity_share"]
