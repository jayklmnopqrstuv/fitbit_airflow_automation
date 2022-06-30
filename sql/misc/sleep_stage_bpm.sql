WITH
  sleep_stages AS(
  SELECT
    DISTINCT pim_id,
    sleep_datetime_loc AS sleep_stage_start_loc,
    CAST(TIMESTAMP_ADD( CAST(sleep_datetime_loc AS TIMESTAMP), INTERVAL (sleep_duration -1) SECOND) AS DATETIME) AS sleep_stage_end_loc,
    sleep_stage
  FROM
    odp_level2.fitbit_sleep_detail ),
  sleep_details AS(
  SELECT
    pim_id,
    sleep_date_loc,
    start_time_loc AS sleep_start_time,
    end_time_loc AS sleep_end_time,
  FROM
    odp_level2.fitbit_sleeps
  WHERE
    /* 
	today's complete sleep data may be available tomorrow
	Example, a person sleeps from 9PM (today) to 5AM tomorrow (local time),
	his/her data will possibly not be complete yet today's data upload, 
	so we summarize his/her sleep cycles from yesterday's sleep details
	(Example: {{ ds }} = '2021-04-27'
	   sleep_date_loc: '2021-04-26'
	   start_time_loc: '2021-04-26T23:00:00
	   end_time_loc:   '2021-04-27T04:00:00
	*/ 
  sleep_date_loc = DATE_SUB('{{ ds }}', INTERVAL 1 DAY) ),
  combine_sleep_info AS (
  SELECT
    a.pim_id,
    a.sleep_date_loc,
    a.sleep_start_time,
    a.sleep_end_time,
    b.sleep_stage_start_loc,
    b.sleep_stage_end_loc,
    b.sleep_stage
  FROM
    sleep_details a
  LEFT JOIN
    sleep_stages b
  ON
    a.pim_id = b.pim_id
  WHERE
    (b.sleep_stage IS NOT NULL) and (b.sleep_stage_start_loc BETWEEN a.sleep_start_time
      AND a.sleep_end_time)),
  bpm_stream AS(
  SELECT
    pim_id,
    heart_rate_datetime_loc AS datetime_local,
    bpm
  FROM
    odp_level2.fitbit_heart_rate
  WHERE
    DATE(heart_rate_datetime_loc) > DATE_SUB('{{ ds }}', INTERVAL 2 DAY)
    AND DATE(heart_rate_datetime_loc) <= '{{ ds }}'
    AND bpm >= 40 )
SELECT
  DISTINCT 
  PARSE_DATE("%F", '{{ ds }}') as ds,
  a.pim_id,
  a.datetime_local,
  a.bpm,
  b.sleep_stage_start_loc,
  b.sleep_stage_end_loc,
  LOWER(b.sleep_stage) AS sleep_stage,
FROM
  bpm_stream a
LEFT JOIN
  combine_sleep_info b
ON
  a.pim_id = b.pim_id
WHERE
  a.datetime_local BETWEEN b.sleep_stage_start_loc
  AND b.sleep_stage_end_loc
