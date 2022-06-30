SELECT 
  PARSE_DATE("%F", '{{ ds }}') as ds,
  pim_id as participant_id,
  heart_rate_date_loc as datetime_local,
  LOWER(REPLACE(heart_rate_zone,' ','')) AS heart_rate_zone,
  min_bpm,
  max_bpm
FROM 
  `odp_level2.fitbit_heart_rate_zones`
WHERE 
  heart_rate_date_loc > DATE_SUB('{{ ds }}', INTERVAL 14 DAY)
  AND heart_rate_date_loc <= '{{ ds }}'
  AND min_bpm >= 40
  AND max_bpm >= 40
