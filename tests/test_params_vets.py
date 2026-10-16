"""Checks on params/vets.yaml against the legislation.

The legal caps are stated as shares of registrations. Government documents describe
them as shares of the ZEV target, so both forms are checked: a typo in either
would break one of them.
"""
import pytest

from zevm import rules

YEARS = range(2024, 2030)


@pytest.fixture(scope="module")
def vets():
    return rules.load("vets")


def test_targets(vets):
    assert [vets.zev_target("car", y) for y in range(2024, 2031)] == [0.22, 0.28, 0.33, 0.38, 0.52, 0.66, 0.80]
    assert [vets.zev_target("van", y) for y in range(2024, 2031)] == [0.10, 0.16, 0.24, 0.34, 0.46, 0.58, 0.70]


@pytest.mark.parametrize("vt", ["car", "van"])
def test_co2_conversion_cap_is_falling_share_of_target(vets, vt):
    shares = [vets.get(vt, "co2_to_zev.cap", y) / vets.zev_target(vt, y) for y in YEARS]
    assert shares == pytest.approx([0.65, 0.90, 0.80, 0.70, 0.60, 0.50], abs=1e-3)
    assert vets.get(vt, "co2_to_zev.cap", 2030) == 0


@pytest.mark.parametrize("vt, first", [("car", 0.75), ("van", 0.90)])
def test_borrowing_cap_as_share_of_target(vets, vt, first):
    shares = [vets.get(vt, "borrowing.cap", y) / vets.zev_target(vt, y) for y in YEARS]
    assert shares == pytest.approx([first, 0.70, 0.25, 0.20, 0.15, 0.10], abs=1e-3)
    assert vets.get(vt, "borrowing.cap", 2030) == 0


def test_borrowing_uplift_matches_order_table(vets):
    rate = vets.get("car", "borrowing.uplift_rate")
    # art. 15(8)-(10C): 3.5%, 7.12%, 10.87%, 14.75%, 18.77%, 22.93%
    assert [round((1 + rate) ** k - 1, 4) for k in range(1, 7)] == [0.035, 0.0712, 0.1087, 0.1475, 0.1877, 0.2293]


def test_payments(vets):
    assert vets.get("car", "payment_per_vehicle_gbp", 2024) == 15000
    assert vets.get("car", "payment_per_vehicle_gbp", 2027) == 12000
    assert vets.get("van", "payment_per_vehicle_gbp", 2024) == 9000
    assert vets.get("van", "payment_per_vehicle_gbp", 2030) == 15000


def test_law_ends_in_2030(vets):
    with pytest.raises(KeyError, match="outside"):
        vets.zev_target("car", 2031)


def test_indicative_extends_to_2035():
    p = rules.load(rules.PARAMS_DIR / "scenarios" / "indicative_to_2035.yaml")
    assert p.zev_target("car", 2028) == 0.52  # inherited from the law
    assert p.zev_target("car", 2035) == 1.0
    assert p.zev_target("van", 2031) == 0.76
    assert p.get("car", "borrowing.cap", 2033) == 0  # 2030's zero carries forward


@pytest.mark.parametrize("path", sorted((rules.PARAMS_DIR / "scenarios").glob("*.yaml")), ids=lambda p: p.stem)
def test_every_scenario_loads(path):
    rules.load(path)
