WITH
  bpm_stream AS(
  SELECT
    pim_id,
    heart_rate_datetime_loc AS datetime_local,
    bpm
  FROM
    `research-01-217611.odp_level2.fitbit_heart_rate`
  WHERE
    DATE(heart_rate_datetime_loc) > DATE_SUB('{{ ds }}', INTERVAL 2 DAY)
    AND DATE(heart_rate_datetime_loc) <= '{{ ds }}'
    AND bpm >= 40 ),
  /*
  --------------------- sleep events --------------------------------------------
  */
  sleep_stages AS(
  SELECT
    DISTINCT pim_id,
    sleep_datetime_loc AS sleep_stage_start_loc,
    CAST(TIMESTAMP_ADD( CAST(sleep_datetime_loc AS TIMESTAMP), INTERVAL (sleep_duration -1) SECOND) AS DATETIME) AS sleep_stage_end_loc,
    sleep_stage
  FROM
    odp_level2.fitbit_sleep_detail ),
  sleep_start_end AS (
  SELECT
    pim_id,
    sleep_date_loc,
    start_time_loc AS sleep_start_time,
    CAST(TIMESTAMP_ADD( CAST(start_time_loc AS TIMESTAMP), INTERVAL 10 MINUTE )AS DATETIME) AS sleep_start_time_10mins,
    end_time_loc AS sleep_end_time,
    CAST(TIMESTAMP_ADD( CAST(end_time_loc AS TIMESTAMP), INTERVAL 10 MINUTE )AS DATETIME) AS sleep_end_time_10mins,
  FROM
    `research-01-217611.odp_level2.fitbit_sleeps`
  WHERE
    /* 
	today's complete sleep data may be available tomorrow
	Example, a person sleeps from 9PM (today) to 5AM tomorrow (local time),
	his/her data will possibly not be complete yet today's data upload, 
	so we summarize his/her sleep cycles from yesterday's sleep details
	(Example:  ds  = '2021-04-27'
	   sleep_date_loc: '2021-04-26'
	   start_time_loc: '2021-04-26T23:00:00
	   end_time_loc:   '2021-04-27T04:00:00
	*/ sleep_date_loc = DATE_SUB('{{ ds }}', INTERVAL 1 DAY)),
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
    sleep_start_end a
  LEFT JOIN
    sleep_stages b
  ON
    a.pim_id = b.pim_id
  WHERE
    (b.sleep_stage IS NOT NULL)
    AND (b.sleep_stage_start_loc BETWEEN a.sleep_start_time
      AND a.sleep_end_time)),

  /*
  --------------------- meal events --------------------------------------------
  */
  meal_events AS (
	SELECT
	  CAST(pim_id AS STRING) AS pim_id,
	  start_datetime_local AS meal_start_time,
	  CAST(TIMESTAMP_ADD( CAST(start_datetime_local AS TIMESTAMP), INTERVAL 10 MINUTE )AS DATETIME) AS meal_start_time_10mins,
	  peak_datetime_local  AS peak_start_time,
	  CAST(TIMESTAMP_ADD( CAST(peak_datetime_local AS TIMESTAMP), INTERVAL 10 MINUTE )AS DATETIME) AS peak_start_time_10mins,
	  return_to_baseline_datetime_local  AS baseline_start_time,
	  CAST(TIMESTAMP_ADD( CAST(return_to_baseline_datetime_local AS TIMESTAMP), INTERVAL 10 MINUTE )AS DATETIME) AS baseline_start_time_10mins
	FROM 
	  `research-01-217611.odp_level2_feature_store.meal_events` 
    WHERE 	 
	  DATE(start_datetime_local) = '{{ ds }}' )

  (
  SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id as participant_id,
    a.datetime_local,
    a.bpm,
    b.sleep_stage_start_loc AS start_time,
    b.sleep_stage_end_loc AS end_time,
    DATETIME_DIFF(b.sleep_stage_end_loc,
      b.sleep_stage_start_loc,
      MINUTE) AS duration,
    LOWER(b.sleep_stage) AS event,
  FROM
    bpm_stream a
  LEFT JOIN
    combine_sleep_info b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.sleep_stage_start_loc
    AND b.sleep_stage_end_loc)
UNION ALL (
  SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id as participant_id,
    a.datetime_local,
    a.bpm,
    b.sleep_start_time AS start_time,
    b.sleep_start_time_10mins AS end_time,
    DATETIME_DIFF(b.sleep_start_time_10mins,
      b.sleep_start_time,
      MINUTE) AS duration,
    'bedtime_start' AS event
  FROM
    bpm_stream a
  LEFT JOIN
    sleep_start_end b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.sleep_start_time
    AND b.sleep_start_time_10mins)
	
UNION ALL (
  SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id as participant_id,
    a.datetime_local,
    a.bpm,
    b.sleep_end_time AS start_time,
    b.sleep_end_time_10mins AS end_time,
    DATETIME_DIFF(b.sleep_end_time_10mins,
      b.sleep_end_time,
      MINUTE) AS duration,
    'bedtime_end' AS event
  FROM
    bpm_stream a
  LEFT JOIN
    sleep_start_end b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.sleep_end_time
    AND b.sleep_end_time_10mins)

UNION ALL (
  SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id as participant_id,
    a.datetime_local,
    a.bpm,
    b.meal_start_time as start_time,
    b.meal_start_time_10mins as end_time,
	DATETIME_DIFF(b.meal_start_time_10mins,
      b.meal_start_time,
      MINUTE) AS duration,
    'meal_start' AS event
  FROM
    bpm_stream a
  LEFT JOIN
    meal_events b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.meal_start_time
    AND b.meal_start_time_10mins )

 UNION ALL (
  SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id as participant_id,
    a.datetime_local,
    a.bpm,
    b.peak_start_time as start_time,
    b.peak_start_time_10mins as end_time,
	DATETIME_DIFF(b.peak_start_time_10mins,
      b.peak_start_time,
      MINUTE) AS duration,
    'peak_postpandrial_start' AS event
  FROM
    bpm_stream a
  LEFT JOIN
    meal_events b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.peak_start_time
    AND b.peak_start_time_10mins)

 UNION ALL (
  SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id as participant_id,
    a.datetime_local,
    a.bpm,
    b.baseline_start_time as start_time,
    b.baseline_start_time_10mins as end_time,
	DATETIME_DIFF(b.baseline_start_time_10mins,
      b.baseline_start_time,
      MINUTE) AS duration,
    'meal_return_baseline' AS event
  FROM
    bpm_stream a
  LEFT JOIN
    meal_events b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.peak_start_time
    AND b.peak_start_time_10mins)

