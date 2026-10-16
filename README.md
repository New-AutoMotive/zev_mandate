# zev_mandate

A model of the UK ZEV mandate (Vehicle Emissions Trading Schemes) and its flexibilities.
It pulls real registrations from BigQuery, applies a set of mandate parameters, and produces
compliance and fleet scenarios.

## Layout

```
params/      Mandate rules. vets.yaml = law in force (to 2030); scenarios/*.yaml extend it.
             fleet.yaml = fleet/emissions assumptions (lifespan, mileage, fuel use, CO2 factors).
zevm/        The Python package
  rules.py     load and query params
  compliance.py  scheme accounting per participant-year: allowances, CO2<->ZEV conversion,
               banking, borrowing + uplift, market trading, payments
  scenarios.py history (current-year estimate) + projections to 2030/35
  data.py      BigQuery pulls (cached to data/cache/), ZEV/non-ZEV split, grouping by make/entity/pool
  fleet.py     car parc -> vehicle miles -> fuel -> tailpipe CO2, driven by scenario registrations;
               validated against NAEI
  plots.py     charts for the notebooks (style in zevm/style/)
notebooks/   tracker.ipynb (this year by manufacturer), scenarios.ipynb (projections),
             fleet.ipynb (parc and emissions to 2035 by scenario)
sql/         One query per file; {project} and @start/@end are filled in by zevm.data
tests/
scripts/     DfT/NAEI data download scripts
reference/   Source documents and old spreadsheet outputs used as test checks
data/        Downloaded DfT tables (data/cache/ is local only)
archive/     Original 2023–25 notebooks, kept for reference (see archive/README.md)
```

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

h = scenarios.prepare("car")      # 2024 on by pool; current year estimated
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

Caveat on counts: VES totals for a month drift upward for a year or two after it is first
published, and end up 9-17% above DfT/SMMT. The ZEV share stays right. Recent months are
close to official figures; older years are inflated, so absolute shortfalls and £ payments
for 2024-25 are overstated by roughly that much. The late-data uplift that tried to correct
for this made it worse and is off by default (`late_data_uplift=False`). Evidence is in the
`zevm/scenarios.py` docstring.

## Fleet and emissions

```python
from zevm import fleet
fp = fleet.load_params()                    # params/fleet.yaml
hist = fleet.load_history(fp)               # DfT VEH0105/0142/1153, DESNZ fuel, NAEI, MOT mileage (data/)
regs = fleet.registrations_from_summary(scenarios.market_summary(res), hist, fp, extend_to=2035)
out = fleet.run_many(hist, regs, fp)        # scenario x year x fuel: parc, additions, retirements,
                                            #   vehicle_miles, fuel_ktoe, co2_mt
fleet.totals(out)                           # one row per scenario-year, with BEV share of parc and miles
fleet.validate(hist, fp)                    # CO2 from DESNZ fuel use vs NAEI 1A3bi, by year
fleet.backtest(hist, fp, 2014)              # stock model from the 2014 parc vs the actual parc
```

The parc starts from the DfT year-end count (fuel groups petrol, diesel, hybrid, bev). Each
scenario's ZEVs are added as BEVs. Its non-ZEVs are split by `registrations.nonzev_split`.
Cars retire at 14–23 years old. The existing stock's retirements are scaled so the base-year
parc retires exactly, which means parc[t] = parc[t-1] + additions − retirements holds by
construction. Fuel per mile is calibrated from DESNZ fuel consumption, and new cars use
`fuel_use.new_vs_fleet` times the fleet figure. CO2 is scaled to NAEI.
`zevm/fleet.py` lists the archive notebook's bugs that were not carried forward.

Caveats:
- The data files in `data/` run to 2023 (parc, registrations), 2020 (DESNZ fuel, NAEI) and
  2024 (mileage). `scripts/data_downloader.py` fetches newer DfT/DESNZ tables, but the
  readers have only been tested on the current files.
- Diesel retires faster in reality than the fixed lifespan implies: from 2014, the backtest
  overstates the 2023 diesel parc by 15%.
- The post-2023 split of non-ZEVs, new-car fuel use and flat mileage are assumptions
  (marked in `params/fleet.yaml`). The CO2 factors are the DESNZ 2023 ones from the notebook.
- Only tailpipe CO2 is counted. Electricity is not.

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
