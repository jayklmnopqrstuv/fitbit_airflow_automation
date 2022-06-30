/*
Derive and estimate heart rate variability components from Fitbit BPM measurements 
Code extracted from: 
https://code.savvysherpa.com/Algoroup/cgm-algorithms/blob/master/t2dstudies/sql.py
*/

WITH
  heart_rate AS (
    SELECT
      pim_id as participant_id,
      TIMESTAMP(heart_rate_datetime_loc) AS hr_time,
      bpm,
      /* Per bpm collection per participant, take the bpm
        value from the previous collection. */
      lag(bpm) over (partition by pim_id 
        order by heart_rate_datetime_loc) as prev_bpm
    FROM
      odp_level2.fitbit_heart_rate
    WHERE 
      DATE(heart_rate_datetime_loc) = '{{ ds }}'
      AND bpm >= 40
  )
SELECT
  PARSE_DATE("%F", '{{ ds }}') as ds,
  hr.participant_id,
  /* Bucket timestamps on 5 minute intervals and compute
  heart rate variability statistics within each bucket. */
  datetime(TIMESTAMP_SECONDS( UNIX_SECONDS(hr.hr_time) 
    - MOD(UNIX_SECONDS(hr.hr_time), 5 * 60) )) AS datetime_local,
  AVG(hr.bpm) AS bpm_mean,
  /* exclude calculations
  COUNT(*) AS record_count,
  stddev(hr.bpm) AS bpm_stddev,
  MIN(hr.bpm) AS bpm_min,
  MAX(hr.bpm) AS bpm_max,
  */
  /* Invert heart rate to arrive at minutes/beat. Then convert
  units to milliseconds/beat. */
  AVG(60000.0 / hr.bpm) AS beat_interval_ms_mean,
  stddev(60000.0 / hr.bpm) AS beat_interval_ms_stddev,
  MIN(60000.0 / hr.bpm) AS beat_interval_ms_min,
  MAX(60000.0 / hr.bpm) AS beat_interval_ms_max,
  /* Compute the RMSSD of the NN intervals per 5 minute bucket.*/
  SQRT(AVG(POW(60000.0*(1/hr.bpm-1/hr.prev_bpm), 2)))
    as beat_interval_ms_rmssd
FROM
  heart_rate AS hr
WHERE 
  hr.prev_bpm IS NOT NULL
GROUP BY
  hr.participant_id,
  datetime_local
HAVING
  bpm_mean IS NOT NULL
