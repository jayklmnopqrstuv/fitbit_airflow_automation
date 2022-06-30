SELECT 
  PARSE_DATE("%F", '{{ ds }}') as ds,
  pim_id as participant_id,
  DATE(heart_rate_datetime_loc) as datetime_local,
  bpm  
FROM 
  `odp_level2.fitbit_heart_rate`
WHERE 
  DATE(heart_rate_datetime_loc) > DATE_SUB('{{ ds }}', INTERVAL 14 DAY)
  AND DATE(heart_rate_datetime_loc) <= '{{ ds }}'
  AND bpm >= 40 
