-- Monthly new van registrations. Same columns as registrations_monthly_car.sql.
-- Van scope (art. 3(1)): N1, plus zero-emission N2 up to 4,250 kg. The weight test is
-- not applied here; the filter matches zev_van_credits_monthly_by_entity. ~10 GB scanned.
SELECT
  DATE_TRUNC(DATE(monthOfFirstRegistration), MONTH) AS month,
  Region AS region,
  make,
  manufacturer,
  fuelType AS fuel_type,
  COUNT(*) AS registrations,
  COUNTIF(co2Emissions > 0) AS with_co2,
  SUM(IF(co2Emissions > 0, co2Emissions, 0)) AS co2_sum
FROM `{project}.VES_dataset.parentZEVMandateN1`
WHERE (typeApproval = 'N1' OR (typeApproval = 'N2' AND fuelType = 'ELECTRICITY'))
  AND DATE(monthOfFirstRegistration) >= @start
  AND DATE(monthOfFirstRegistration) < @end
GROUP BY month, region, make, manufacturer, fuel_type
