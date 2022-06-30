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
	(Example: {{ ds }} = '2021-04-27'
	   sleep_date_loc: '2021-04-26'
	   start_time_loc: '2021-04-26T23:00:00
	   end_time_loc:   '2021-04-27T04:00:00
	*/ sleep_date_loc = DATE_SUB('{{ ds }}', INTERVAL 1 DAY)) 
  
  (SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id,
    a.datetime_local,
    a.bpm,
    b.sleep_start_time AS start_time,
    b.sleep_start_time_10mins AS end_time,
    'bedtime_start' AS sleep_event
  FROM
    bpm_stream a
  LEFT JOIN
    sleep_start_end b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.sleep_start_time
    AND b.sleep_start_time_10mins)
UNION ALL 
(SELECT
    DISTINCT PARSE_DATE("%F",
      '{{ ds }}') AS ds,
    a.pim_id,
    a.datetime_local,
    a.bpm,
    b.sleep_end_time AS start_time,
    b.sleep_end_time_10mins AS end_time,
    'bedtime_end' AS sleep_event
  FROM
    bpm_stream a
  LEFT JOIN
    sleep_start_end b
  ON
    a.pim_id = b.pim_id
  WHERE
    a.datetime_local BETWEEN b.sleep_end_time
    AND b.sleep_end_time_10mins)
