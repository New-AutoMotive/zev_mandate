"""Smoke tests: every chart renders from package outputs without error."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import pytest

from zevm import plots, rules, scenarios
from zevm.scenarios import Scenario, Strategy
from tests.test_scenarios import synthetic_monthly, tiny_history


@pytest.fixture(scope="module", autouse=True)
def style():
    plots.use_style()
    yield
    plt.close("all")


@pytest.fixture(scope="module")
def summary_and_results():
    res = scenarios.run_many(tiny_history(), [
        Scenario("A. On target"),
        Scenario("B. Short", zev_share_vs_target=-0.05, strategy=Strategy(trading="none")),
    ])
    return scenarios.market_summary(res), res


def test_zev_share_monthly():
    assert plots.zev_share_monthly(synthetic_monthly(), rules.load("vets")).axes


@pytest.mark.parametrize("per_vehicle", [False, True])
def test_position_bars(summary_and_results, per_vehicle):
    _, res = summary_and_results
    assert plots.position_bars(res[res.scenario == "A. On target"], 2026, per_vehicle=per_vehicle).axes


def test_scenario_charts(summary_and_results):
    summary, _ = summary_and_results
    plots.scenario_lines(summary, "zev_share", "share", pct=True, show_target=True)
    plots.scenario_lines(summary, "payment_gbp_m", "payments", money=True)
    plots.coverage_stack(summary, "B. Short")
    assert len(plots.bank_and_debt(summary).axes) == 2


def test_colours_follow_names_not_rank():
    a = plots.colours_for(["x", "y", "z"])
    assert a["y"] == plots.SERIES[1]
    many = plots.colours_for([str(i) for i in range(8)])
    assert many["7"] == plots.OTHER


def test_coverage_columns_add_up_to_non_zev(summary_and_results):
    summary, _ = summary_and_results
    parts = summary[[c for c, _, _ in plots.COVERAGE]].sum(axis=1)
    assert parts.to_numpy() == pytest.approx(summary["nonzev_registrations"].to_numpy(), rel=1e-6)


def test_fleet_charts():
    from zevm import fleet
    from tests.test_fleet import FP, summary_frame, synthetic_history
    h = synthetic_history()
    out = fleet.run_many(h, fleet.registrations_from_summary(summary_frame(), h, FP, extend_to=2035), FP)
    v = fleet.validate(h, FP)
    assert plots.emissions_vs_naei(v, fleet.totals(out), co2_scale=0.95).axes
    assert plots.parc_by_fuel(out, "A").axes
    assert plots.parc_by_fuel(out, "B", column="co2_mt").axes
