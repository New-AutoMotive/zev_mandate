-- New AutoMotive's ECC new car registrations by fuel, as first published each month.
-- A snapshot taken at publication, not a corrected series: VES keeps adding late records,
-- so today's VES count for a month is normally higher than the published figure.
-- Comparing the two shows how much a month's count rises after it is first published.
SELECT
  monthOfFirstRegistration AS month,
  fuelType AS fuel,
  registrations,
  grand_total_for_month
FROM `{project}.VES_dataset.VRN_reported`
WHERE monthOfFirstRegistration >= @start
  AND monthOfFirstRegistration < @end
