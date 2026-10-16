# zev_mandate

A model of the UK ZEV mandate (Vehicle Emissions Trading Schemes) and its flexibilities.
It pulls real registrations from BigQuery, applies a set of mandate parameters, and produces
compliance and fleet scenarios.

## Layout

```
params/      Mandate rules. vets.yaml = law in force (to 2030); scenarios/*.yaml extend it.
zevm/        The Python package
  rules.py     load and query params
  plots.py     chart style
tests/
scripts/     DfT/NAEI data download scripts
reference/   Source documents and old spreadsheet outputs used as test checks
data/        Downloaded DfT tables (data/cache/ is local only)
archive/     Original 2023–25 notebooks, kept for reference (see archive/README.md)
```

Planned next: `zevm/data.py` + `sql/` (BigQuery pulls), `zevm/compliance.py` (allowances,
CO2 conversion, banking/borrowing, trading), `zevm/scenarios.py`, `zevm/fleet.py`, and
slim notebooks in `notebooks/`.

## Setup

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv -e ".[dev]"
.venv/bin/nbstripout --install --attributes /dev/null   # strips outputs from notebooks/*.ipynb on commit
gcloud auth application-default login                    # BigQuery access, no key file needed
```

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
