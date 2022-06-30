SELECT 
  PARSE_DATE("%F", '{{ ds }}') as ds,
  pim_id as participant_id,
  resting_heart_rate,  
  resting_heart_heart_date_loc as datetime_local
FROM 
  `odp_level2.fitbit_resting_heart_rate`
WHERE 
  resting_heart_heart_date_loc > DATE_SUB('{{ ds }}', INTERVAL 14 DAY)
  AND resting_heart_heart_date_loc <= '{{ ds }}'
  AND resting_heart_rate >= 40