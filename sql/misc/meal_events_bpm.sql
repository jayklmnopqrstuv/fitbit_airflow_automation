WITH
  bpm_stream AS(
  SELECT
    pim_id,
    heart_rate_datetime_loc AS datetime_local,
    bpm
  FROM
    `research-01-217611.odp_level2.fitbit_heart_rate`
  WHERE
    DATE(heart_rate_datetime_loc) = '{{ ds }}'
    AND bpm >= 40 ),
	
  meal_events AS (
	SELECT
	  CAST(pim_id AS STRING) AS pim_id,
	  start_datetime_local AS meal_start_time,
	  CAST(TIMESTAMP_ADD( CAST(start_datetime_local AS TIMESTAMP), INTERVAL 10 MINUTE )AS DATETIME) AS meal_start_time_10mins,
	  peak_datetime_local  AS peak_start_time,
	  CAST(TIMESTAMP_ADD( CAST(peak_datetime_local AS TIMESTAMP), INTERVAL 10 MINUTE )AS DATETIME) AS peak_start_time_10mins
	FROM 
	  `research-01-217611.odp_level2_feature_store.meal_events` 
    WHERE 	 
	  DATE(start_datetime_local) = '{{ ds }}' )
	
	  
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
    'meal_start' AS meal_event
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
    'peak_postpandrial_start' AS meal_event
  FROM
    bpm_stream a
  LEFT JOIN
    meal_events b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.peak_start_time
    AND b.peak_start_time_10mins)

