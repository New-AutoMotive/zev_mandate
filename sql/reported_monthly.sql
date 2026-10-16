-- Published monthly new car registrations by fuel, for checking the VES-based counts.
SELECT
  monthOfFirstRegistration AS month,
  fuelType AS fuel,
  registrations,
  grand_total_for_month
FROM `{project}.VES_dataset.VRN_reported`
WHERE monthOfFirstRegistration >= @start
  AND monthOfFirstRegistration < @end
