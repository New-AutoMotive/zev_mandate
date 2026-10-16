-- Monthly new car registrations at the finest grain the model needs.
-- Grouping to entity or pool happens in Python (zevm.data.summarise), so one cached
-- pull serves every grouping. ~22 GB scanned (~$0.14 on demand) for 2021 onwards.
--
-- parentZEVMandateM1 is VRN_UK filtered to M1, from 2021, with motorhomes/trucks
-- removed and each make mapped to a legal entity (`manufacturer`).
-- co2_sum / with_co2 lets Python fill in missing CO2 values instead of treating them as 0 g/km.
SELECT
  DATE_TRUNC(DATE(monthOfFirstRegistration), MONTH) AS month,
  Region AS region,
  make,
  manufacturer,
  fuelType AS fuel_type,
  COUNT(*) AS registrations,
  COUNTIF(co2Emissions > 0) AS with_co2,
  SUM(IF(co2Emissions > 0, co2Emissions, 0)) AS co2_sum
FROM `{project}.VES_dataset.parentZEVMandateM1`
WHERE DATE(monthOfFirstRegistration) >= @start
  AND DATE(monthOfFirstRegistration) < @end
GROUP BY month, region, make, manufacturer, fuel_type
