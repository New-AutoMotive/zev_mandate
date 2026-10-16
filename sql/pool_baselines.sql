-- Official car pools and CCTS (CO2) baselines by make. ccts_baseline is a string in the
-- source table: blank or 'UNKNOWN' where not known; 0.0 for ZEV-only entrants.
SELECT
  make,
  uk_manufacturer_name AS regulated_entity,
  compliance_pool,
  SAFE_CAST(NULLIF(NULLIF(TRIM(ccts_baseline), ''), 'UNKNOWN') AS FLOAT64) AS co2_baseline
FROM `{project}.VES_dataset.VETS_Pool_Baselines`
