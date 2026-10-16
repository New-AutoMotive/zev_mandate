# zev_mandate

A model of the UK ZEV mandate (Vehicle Emissions Trading Schemes) and its flexibilities.
It pulls real registrations from BigQuery, applies a set of mandate parameters, and produces
compliance and fleet scenarios.

## Layout

```
params/      Mandate rules. vets.yaml = law in force (to 2030); scenarios/*.yaml extend it.
zevm/        The Python package
  rules.py     load and query params
  compliance.py  scheme accounting per participant-year: allowances, CO2<->ZEV conversion,
               banking, borrowing + uplift, market trading, payments
  scenarios.py history (late-data uplift, current-year estimate) + projections to 2030/35
  data.py      BigQuery pulls (cached to data/cache/), ZEV/non-ZEV split, grouping by make/entity/pool
  plots.py     charts for the notebooks (style in zevm/style/)
notebooks/   tracker.ipynb (this year by manufacturer), scenarios.ipynb (projections)
sql/         One query per file; {project} and @start/@end are filled in by zevm.data
tests/
scripts/     DfT/NAEI data download scripts
reference/   Source documents and old spreadsheet outputs used as test checks
data/        Downloaded DfT tables (data/cache/ is local only)
archive/     Original 2023–25 notebooks, kept for reference (see archive/README.md)
```

Planned next: `zevm/fleet.py` (port of the stock model from archive notebook 01).

Start with `notebooks/tracker.ipynb` or `notebooks/scenarios.ipynb`. Outputs are stripped on commit;
CSV exports go to `outputs/` (not committed).

## Setup

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv -e ".[dev]"
.venv/bin/nbstripout --install --attributes /dev/null   # strips outputs from notebooks/*.ipynb on commit
gcloud auth application-default login                    # BigQuery access, no key file needed
```

## Getting registrations

```python
from zevm import data, rules
raw = data.registrations("car")                    # 2021 to last complete month; ~$0.14 per fresh pull
pools = data.summarise(raw, by="pool", freq="year", params=rules.load("vets"))
vans = data.summarise(data.registrations("van"), by="entity", vehicle_type="van")
```

Results are cached for 7 days (`max_age_days=`); `refresh=True` forces a pull and
`offline=True` uses whatever is cached. The ZEV count covers `ELECTRICITY` vehicles, plus
`FUEL CELLS` vehicles rated 0 g/km. Non-ZEVs with no CO2 value get the mean of their make,
fuel and month, so they don't count as 0 g/km (about 1% of CO2 activity is filled in this way).
In GB-only scheme years (2024), Northern Ireland is dropped.

Pool grouping (cars only) uses `VES_dataset.VETS_Pool_Baselines`. Makes not in that table
fall back to the legal entity in `parentZEVMandateM1`.

## Compliance

```python
from zevm import compliance
from zevm.compliance import Strategy
pools = data.pool_baselines()
inputs = compliance.attach_baselines(data.summarise(raw, by="pool", freq="year", params=p), pools,
                                     data.entity_baselines("car"))
result = compliance.run(inputs, p, "car", Strategy())                  # every flexibility, trading on
alone = compliance.run(inputs, p, "car", Strategy(trading="none"))     # no ZEV allowance trading
market = compliance.run(compliance.market_total(inputs), p, "car")     # whole market as one participant
```

Each row of the result is one participant-year with every step: allowances, CO2 credits
(and the cap), banked/borrowed/bought/sold, repayments and uplift, shortfall and £ payments.
The step order and what isn't modelled yet are documented at the top of `zevm/compliance.py`.
Market trading clears pro rata with no price: it shows whether enough allowances exist,
not who pays whom.

## Scenarios

```python
from zevm import scenarios
from zevm.scenarios import Scenario, Strategy

h = scenarios.prepare("car")      # 2024 on by pool; recent months uplifted; current year estimated
res = scenarios.run_many(h, [
    Scenario("On target"),
    Scenario("3pp below target", zev_share_vs_target=-0.03),
    Scenario("3pp below, no trading", zev_share_vs_target=-0.03, strategy=Strategy(trading="none")),
    Scenario("Slow growth", zev_share={2027: .315, 2028: .355, 2029: .395, 2030: .435}),
    Scenario("Indicative to 2035", params="scenarios/indicative_to_2035"),
])
scenarios.market_summary(res)     # one row per scenario-year; res has every participant-year
```

`h.notes` records the fitted late-data curves and the seasonality used for the current year.
Scenario options: `zev_share` ("target", a constant, or a year table), `zev_share_vs_target`,
`market_growth` / `market_size`, `nonzev_co2_change`, `group_zev` ("logit", "scale",
"uniform"), `params` (any params file) and `strategy`. How each one works is documented at
the top of `zevm/scenarios.py`.

Caveats:
- The late-data uplift assumes the records VES adds to a month later are genuine new first
  registrations. That hasn't been checked against DfT or SMMT figures.
- Vans use the car uplift curve.

## Changing the mandate

```python
from zevm import rules
p = rules.load("vets")               # params/vets.yaml: the law as amended, to 2030
p.zev_target("car", 2027)
p.get("car", "co2_to_zev.cap", 2026)
```

To test a variant, make a small file that overrides only what changes:

```yaml
# params/scenarios/no_borrowing.yaml (see example_no_borrowing_from_2027.yaml)
extends: ../vets.yaml
name: VETS, no borrowing
vehicle_types:
  car:
    borrowing:
      cap: {2024: 0.0}
```

Year tables carry forward: a year that isn't listed takes the latest earlier value.

## Tests

```bash
.venv/bin/pytest
```
