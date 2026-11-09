# Raw data sources

All files downloaded on **19 October 2026** from official publishers (gov.uk, ons.gov.uk, obr.uk).
Files are unmodified as downloaded. One rename: the ONS CSV came without a filename and is saved as `abmi_ukea.csv`.
Sizes are in bytes.

Note: the repo `.gitignore` excludes `*.csv` and `*.xlsx`, so those files are not tracked by git. The `.ods` and `.pdf` files are.

---

## dukes_oil/ — DESNZ, Digest of UK Energy Statistics (DUKES) 2026

DUKES 2026 was published on 30 July 2026 and covers data to 2025.
Collection page: https://www.gov.uk/government/statistics/petroleum-chapter-3-digest-of-united-kingdom-energy-statistics-dukes

| File | Size | URL | Contents |
|---|---|---|---|
| `DUKES_3.1.2.xlsx` | 44,782 | https://assets.publishing.service.gov.uk/media/6a69f0d8f23ff62c51c1a855/DUKES_3.1.2.xlsx | Inland deliveries of petroleum, 1970–2025, **million tonnes**. Sheet `3.1.2`, header on row 4. Columns: Total inland deliveries; deliveries by product (Motor spirit, DERV fuel, ATF, …); Total deliveries for energy uses; Deliveries for non-energy uses; use by sector (incl. Transport use). |
| `DUKES_3.2.xlsx` | 311,755 | https://assets.publishing.service.gov.uk/media/6a70bb691f66ec7b77ed4692/DUKES_3.2.xlsx | Petroleum products commodity balances, one sheet per year from 1998 to 2025, **thousand tonnes**. Header on row 4. Rows include Marine bunkers, Total demand, Final consumption, Transport > Air / Rail / Road / National navigation, and Non energy use. |
| `DUKES_3.3.xlsx` | 44,239 | https://assets.publishing.service.gov.uk/media/6a69f0744dda8076771a433d/DUKES_3.3.xlsx | Supply and disposal of petroleum, 1997–2025, thousand tonnes. Has a `Total demand` row split into Energy use and Non-energy use. |
| `DUKES_3.4.xlsx` | 38,865 | https://assets.publishing.service.gov.uk/media/6a69f0810825cc51a6c37d06/DUKES_3.4.xlsx | Inland deliveries of selected products, 1999–2025, thousand tonnes. Gives hydrocarbon and bio components of motor spirit and road diesel separately, plus supermarket sales. |
| `DUKES_3.6.xlsx` | 40,328 | https://assets.publishing.service.gov.uk/media/6a69f09a4dda8076771a433e/DUKES_3.6.xlsx | Inland deliveries for non-energy uses by product, 2006–2025, thousand tonnes. |
| `DUKES_1.1.xlsx` | 259,917 | https://assets.publishing.service.gov.uk/media/6a69ee13f23ff62c51c1a850/DUKES_1.1.xlsx | Aggregate energy balances, one sheet per year from 1998 to 2025, **ktoe** on a gross calorific value basis. Its "Petroleum products" column is on the same basis as EEP Annex F. Chapter 1 page: https://www.gov.uk/government/statistics/energy-chapter-1-digest-of-united-kingdom-energy-statistics-dukes |

## desnz_eep/ — DESNZ, Energy and Emissions Projections 2024 to 2050 (EEP 2024)

Published on 3 February 2026. This is the latest edition in the collection as of 19 October 2026.
Page: https://www.gov.uk/government/publications/energy-and-emissions-projections-2024-to-2050

| File | Size | URL | Contents |
|---|---|---|---|
| `Annex_F_final_energy_demand_ktoe.ods` | 302,157 | https://assets.publishing.service.gov.uk/media/6981d62b8c1e89ed1e91bb5b/Annex_F_final_energy_demand_ktoe.ods | Final energy demand by sector and fuel, 2000–2050, ktoe. Sheets: `Reference`, `FFP_Low`, `FFP_High`, `GDP_Low`, `GDP_High`, `Existing`. Header on row 3. |
| `Annex_E_primary_energy_demand_Mtoe.ods` | 44,328 | https://assets.publishing.service.gov.uk/media/6981d60019d3abdb495f373c/Annex_E_primary_energy_demand_Mtoe.ods | Primary energy demand by fuel, including Oil, 2000–2050, Mtoe. Covers transformation and non-energy use. |
| `Annex_M_assumptions_growth_price.ods` | 85,962 | https://assets.publishing.service.gov.uk/media/6981d725f2ea511998158f75/Annex_M_assumptions_growth_price.ods | GDP, population, price and carbon price assumptions behind the projections. GDP is from the ONS March 2025 release and the OBR March 2025 EFO. |
| `Web_tables_EEP_2024_2050.ods` | 8,044 | https://assets.publishing.service.gov.uk/media/6981d93e8c1e89ed1e91bb61/Web_tables_EEP_2024_2050.ods | Emissions summary tables from the report. |
| `energy-emissions-projections-report-2024-2050.pdf` | 376,892 | https://assets.publishing.service.gov.uk/media/6981e958015e2ba11991bb7a/energy-emissions-projections-report-2024-2050.pdf | Main report. Policy cut-off June 2025 ("EEP-ready" policies). |

## desnz_road_fuel/ — DESNZ, Sub-national road transport fuel consumption, 2005 to 2024

Published on 25 June 2026; the page was last updated on 30 June 2026.
Page: https://www.gov.uk/government/statistics/uk-road-transport-fuel-consumption-at-regional-and-local-authority-level-2005-to-2024

| File | Size | URL | Contents |
|---|---|---|---|
| `subnational-road-transport-fuel-consumption-tables-2005-2024.xlsx` | 4,544,586 | https://assets.publishing.service.gov.uk/media/6a3ba06f4c7605ab567238bb/subnational-road-transport-fuel-consumption-tables-2005-2024.xlsx | One sheet per year from 2005 to 2024, ktoe. Gives fuel by vehicle type (buses, diesel and petrol cars, motorcycles, diesel and natural gas HGV, diesel, petrol and LPG LGV) and road type, by local authority, with a UK total. Header on row 4. Totals include blended biofuel. |

## ons_gdp/ — ONS, GDP chained volume measure (ABMI)

| File | Size | URL | Contents |
|---|---|---|---|
| `abmi_ukea.csv` | 6,967 | https://www.ons.gov.uk/generator?format=csv&uri=/economy/grossdomesticproductgdp/timeseries/abmi/ukea | ABMI series: Gross Domestic Product, chained volume measures, seasonally adjusted, £m. Dataset UKEA, released 30 September 2026 (Quarterly National Accounts). Annual 1948–2025, then quarterly 1955 Q1–2026 Q2. The reference year is 2024: CVM equals current-price GDP (YBHA) in 2024. Series page: https://www.ons.gov.uk/economy/grossdomesticproductgdp/timeseries/abmi/ukea |

## obr_efo/ — Office for Budget Responsibility

The OBR's own download links redirect to the URLs below. The two FRS files are hosted on assets.publishing.service.gov.uk.

| File | Size | URL | Contents |
|---|---|---|---|
| `efo-march-2026-charts-and-tables-annex-tables.xlsx` | 117,066 | https://obr.uk/docs/d055fbf02d5b3g6jq8l2/efo-march-2026-charts-and-tables-annex-tables.xlsx | March 2026 EFO (3 March 2026), Annex A tables. Sheet `TA.1` "Economy forecast" gives percentage change on a year earlier for 2024 (outturn) and 2025–2030 (forecast). |
| `efo-march-2026-detailed-forecast-tables-economy.xlsx` | 483,251 | https://obr.uk/docs/d055fbf02d5b3g6jq8l2/efo-march-2026-detailed-forecast-tables-economy.xlsx | March 2026 EFO detailed economy tables. Sheet `1.1` gives real GDP in £bn chained-linked volumes, quarterly 2008Q1–2030Q4, calendar years 2008–2030 and fiscal years 2008-09 to 2030-31. |
| `Long-term-economic-determinants-March-2026-EFO.xlsx` | 40,388 | https://obr.uk/docs/dlm_uploads/Long-term-economic-determinants-March-2026-EFO.xlsx | Supplementary release dated 28 May 2026, consistent with the March 2026 EFO. Real GDP growth by **fiscal year** from 2023-24 to 2075-76. Sheet `Growth rate assumptions` compares the 2025 and 2026 long-term assumptions. Page: https://obr.uk/supplementary-forecast-information-release-long-term-economic-determinants-march-2026/ |
| `E03599617_OBR_FRS_2026_Accessible.pdf` | 1,375,079 | https://assets.publishing.service.gov.uk/media/6a4cb4c74889d85e75ab40e0/E03599617_OBR_FRS_2026_Accessible.pdf | Fiscal risks and sustainability report, July 2026 (7 July 2026). Table 2.1 in Chapter 2 sets the long-term assumptions: productivity 1.4% a year, real GDP around 1.5% a year. Page: https://obr.uk/frs/fiscal-risks-and-sustainability-july-2026/ |
| `7._Fiscal_risks_and_sustainability_July_2026_charts_and_tables_-_Annexes.xlsx` | 418,445 | https://assets.publishing.service.gov.uk/media/6a4cb54cc5aa1b17abab40d7/7._Fiscal_risks_and_sustainability_July_2026_charts_and_tables_-_Annexes.xlsx | FRS July 2026 annex tables: policy assumptions (B.1, B.2), age profiles (C.1–C.4) and debt decomposition (D.1). No GDP series. |

## dft_vehicles/ — DfT, Vehicle licensing statistics data tables

All three tables were last updated on 23 September 2026 with data to 2026 Q2. The next update is due January 2027.
Page: https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-tables

| File | Size | URL | Contents |
|---|---|---|---|
| `veh0105.ods` | 17,613,448 | https://assets.publishing.service.gov.uk/media/6aad561bce3f006bd4346ce1/veh0105.ods | Licensed vehicles at quarter end by body type, fuel (Petrol / Diesel / Hybrid electric (petrol) / Other fuel types / Total), keepership and local authority. Quarterly 2009 Q4–2026 Q2, thousands. UK figures from 2014 Q3. Sheet `VEH0105`, header on row 5. |
| `veh0142.ods` | 7,456,034 | https://assets.publishing.service.gov.uk/media/6aad561b086ce7c57b818707/veh0142.ods | Licensed plug-in vehicles at quarter end by body type, fuel (BEV / PHEV petrol / PHEV diesel / REX / Total), keepership and local authority. Quarterly 2009 Q4–2026 Q2, **number of vehicles**. UK figures from 2014 Q3. Sheet `VEH0142`, header on row 5. |
| `veh1153.ods` | 6,583,283 | https://assets.publishing.service.gov.uk/media/6aad561f3f8e4e96a6a66a38/veh1153.ods | First registrations by body type, keepership and detailed fuel, long format, thousands. Annual 2001–2025, quarterly 2001 Q1–2026 Q2, monthly Jan 2001–Jun 2026. GB from 2001, UK from July 2014. Sheets `VEH1153a_RoadUsing` (adds `Plugin` and `ZEV` columns) and `VEH1153b_All`, header on row 5. |
