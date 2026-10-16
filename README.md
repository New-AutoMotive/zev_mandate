# zev_mandate

A model of the UK ZEV mandate (Vehicle Emissions Trading Schemes) and its flexibilities.
It pulls real registrations from BigQuery, applies a set of mandate parameters, and produces
compliance and fleet scenarios.

## Layout

```
params/      Mandate rules. vets.yaml = law in force (to 2030); scenarios/*.yaml extend it.
zevm/        The Python package
  rules.py     load and query params
  data.py      BigQuery pulls (cached to data/cache/), ZEV/non-ZEV split, grouping by make/entity/pool
  plots.py     chart style
sql/         One query per file; {project} and @start/@end are filled in by zevm.data
tests/
scripts/     DfT/NAEI data download scripts
reference/   Source documents and old spreadsheet outputs used as test checks
data/        Downloaded DfT tables (data/cache/ is local only)
archive/     Original 2023–25 notebooks, kept for reference (see archive/README.md)
```

Planned next: `zevm/compliance.py` (allowances,
CO2 conversion, banking/borrowing, trading), `zevm/scenarios.py`, `zevm/fleet.py`, and
slim notebooks in `notebooks/`.

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
