"""The ZEV Mandate Review (Aug 2026) option files against the consultation tables."""
import pytest

from zevm import rules

D = rules.PARAMS_DIR / "scenarios"
OPTS = {
    "review_option1_cars70_vans60": ([.36, .46, .58, .70, .76, .82, .88, .94, 1], [.33, .42, .51, .60, .68, .76, .84, .92, 1]),
    "review_option2_cars60_vans50": ([.36, .44, .52, .60, .68, .76, .84, .92, 1], [.31, .37, .44, .50, .60, .70, .80, .90, 1]),
    "review_option3_cars50_vans40": ([.35, .37, .42, .50, .60, .70, .80, .90, 1], [.28, .32, .36, .40, .52, .64, .76, .88, 1]),
    "review_option4_cars80_vans70_extended": ([.38, .52, .66, .80, .84, .88, .92, .96, 1], [.34, .46, .58, .70, .76, .82, .88, .94, 1]),
}
YEARS = range(2027, 2036)


@pytest.fixture(params=sorted(OPTS), ids=lambda s: s.split("_")[1])
def option(request):
    return request.param, rules.load(D / f"{request.param}.yaml")


def test_targets_match_consultation_tables(option):
    name, p = option
    cars, vans = OPTS[name]
    assert [p.zev_target("car", y) for y in YEARS] == pytest.approx(cars)
    assert [p.zev_target("van", y) for y in YEARS] == pytest.approx(vans)
    assert [p.zev_target("car", y) for y in (2024, 2025, 2026)] == [0.22, 0.28, 0.33]   # legislated years untouched


def test_flexibilities_run_to_2034(option):
    _, p = option
    for vt in ("car", "van"):
        assert p.get(vt, "borrowing.cap", 2034) > 0 and p.get(vt, "borrowing.cap", 2035) == 0
        assert p.get(vt, "borrowing.repay_by") == 2035
        assert p.get(vt, "co2_to_zev.cap", 2035) == 0
        # borrowing stays at 10% of target after 2029
        assert p.get(vt, "borrowing.cap", 2032) == pytest.approx(0.10 * p.zev_target(vt, 2032))


def test_co2_cap_shape(option):
    name, p = option
    share = [p.get("car", "co2_to_zev.cap", y) / p.zev_target("car", y) for y in range(2027, 2035)]
    if "option4" in name:
        assert share == pytest.approx([.7, .6, .5, .5, .5, .5, .5, .5])
    else:
        assert share == pytest.approx([.7, .6, .5, .4, .3, .2, .1, 0])


def test_option4_matches_legislated_caps_to_2029():
    law, o4 = rules.load("vets"), rules.load(D / "review_option4_cars80_vans70_extended.yaml")
    for vt in ("car", "van"):
        for y in (2027, 2028, 2029):
            assert o4.get(vt, "co2_to_zev.cap", y) == pytest.approx(law.get(vt, "co2_to_zev.cap", y))
            assert o4.get(vt, "borrowing.cap", y) == pytest.approx(law.get(vt, "borrowing.cap", y))
