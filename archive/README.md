# Archive

These are the original analysis notebooks (2023–2025), kept for reference while their logic
is moved into the `zevm` package. They are not maintained. They expect to be run from the
repo root (paths like `data/...`, `design.pkl`, `modules.connector`), so they will not
run from here as they are.

## Notebooks

| Notebook | What it does | Moves to |
|---|---|---|
| `00) UK Transport Emissions` | NAEI transport emissions by source | `zevm.fleet` (validation) |
| `01) UK Car Scenarios` | Sales → parc → miles → fuel → CO2 projection to 2050, high-mileage driver (HMD) scenarios | `zevm.fleet` |
| `02) UK Van Scenarios` | Van version of 01 | `zevm.fleet` |
| `02.01) Consultation 2 - Questions` | Borrowing caps, worked compliance example | `zevm.compliance` |
| `02.02) General ZEV mandate graphics` | Explainer charts of allowances and borrowing | `zevm.plots` |
| `02.03) CO2 and ZEV trade-off` | Implied CO2 target after CO2→ZEV conversion | `zevm.compliance` |
| `03) Analysis of Outputs` | Small output checks | — |
| `04) UK Sales Variance from Target` | Market vs trajectory | `zevm.scenarios` |
| `04.01) CO2 to ZEV` | Toy CO2→ZEV conversion curve (8.5% cap) | `zevm.compliance` |
| `05.01) Compliance tracker` | Rolling 12-month tracker by make, writes `zevMandate.carRollingPerformance` | `zevm.compliance` + `notebooks/tracker` |
| `05.01 NEW 2024 Car Tracker` | Start of a 2024 calendar-year tracker | `notebooks/tracker` |
| `05.01a) Compliance tracker manufacturer` | Variant of 05.01 with CO2 market-clearing adjustment | `zevm.compliance` |
| `05.02) van compliance tracker` | Van tracker | `zevm.compliance` |
| `05.03)/05.04) ... Visualisations` | Tracker charts (05.04 is a duplicate) | `zevm.plots` |
| `06.01) Scotland`, `07.01) Regional MOTs` | Regional one-offs | — |
| `08.01)`–`08.03)` | Pushback response, fuel price model, second-hand prices | — |
| `09 Reform` | Per-manufacturer CO2 flex headroom using `parentZEVMandateM1` (GB) | `zevm.compliance` |
| `uk_car_emissions_analysis` | Companion to `scripts/download_data.py` | `zevm.fleet` |

## Known problems in this code (do not copy forward)

- CO2→ZEV cap coded inconsistently: `0.6 × target`, `0.25 × target`, `8.5% of sales`, `0.78 × 0.143`.
- The trackers group by `make` (brand), not by scheme manufacturer or pool, and baselines are each make's 2021 average.
- Rolling 12-month totals are compared with a single fixed 22% target, not calendar scheme years.
- `05.01a`: `ukevs` is used before it is defined, and `co2ComplianceAdj` is computed before `co2Compliance` exists.
- `datetime.today().replace(month=month-1)` crashes in January.
- `01)`: ktoe→gallons uses petrol 0.86 / diesel 0.98 going in and the two swapped coming back;
  history uses 2023 CO2 factors but projections use 2021 ones; the retirement factor also
  scales new cars' retirements; fleet MPG is an arithmetic mean of MPG.

## Other folders

- `outputs/`: CSVs and gifs that used to sit at the repo root.
- `plots/`: old chart exports.
- `data_old/`: superseded copies of the DfT tables.
- `design.pkl`: old matplotlib rcParams, now `zevm/style/newautomotive.mplstyle`.
