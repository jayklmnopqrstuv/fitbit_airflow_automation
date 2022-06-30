WITH
  heart_rate AS (
  SELECT
    DISTINCT h.pim_id AS participantid,
    TIMESTAMP(h.heart_rate_datetime_loc) AS hr_time,
    h.bpm,
    /* Per bpm collection per participant, take the bpm
        value from the previous collection. */ LAG(h.bpm) OVER (PARTITION BY h.pim_id ORDER BY h.heart_rate_datetime_loc) AS prev_bpm,
    60000 / h.bpm AS beat_interval,
    AVG(60000.0/h.bpm) OVER (PARTITION BY h.pim_id, DATE(TIMESTAMP(h.heart_rate_datetime_loc))) AS avg_rr
  FROM
    odp_level2.fitbit_heart_rate AS h
  WHERE
    DATE(heart_rate_datetime_loc) = '{{ ds }}'
    AND bpm >= 40 ),
  nn_interval_5min AS(
  SELECT
    participantid,
    /* aggregate in 5min*/ TIMESTAMP(DATETIME(TIMESTAMP_SECONDS( UNIX_SECONDS(hr_time) - MOD(UNIX_SECONDS(hr_time), 5 * 60) ))) AS start_time_5min,
    AVG(beat_interval) AS ave_nn
  FROM
    heart_rate
  GROUP BY
    participantid,
    start_time_5min ),
  sdann_table AS (
  SELECT
    participantid,
    datetime(TIMESTAMP_SECONDS( UNIX_SECONDS(start_time_5min) - MOD(UNIX_SECONDS(start_time_5min), 24 * 60 * 60) )) AS start_time,
    #1hour
    STDDEV(ave_nn) AS sdann,
    COUNT(*) AS record_count
  FROM
    nn_interval_5min
  GROUP BY
    participantid,
    start_time),
  beat_interval_bins AS (
  SELECT
    *,
    datetime(TIMESTAMP_SECONDS( UNIX_SECONDS(hr_time) - MOD(UNIX_SECONDS(hr_time), 24 * 60 * 60) )) AS start_time,
    #1hour
    ROUND((beat_interval*10000 - MOD(CAST(beat_interval*10000 AS int64), 312500))/10000,3) AS histogram_bin
  FROM
    heart_rate),
  beat_interval_count_per_day AS (
  SELECT
    participantid,
    start_time,
    COUNT(*) AS beat_interval_day_count
  FROM
    beat_interval_bins
  GROUP BY
    participantid,
    start_time),
  histogram AS (
  SELECT
    participantid,
    start_time,
    histogram_bin,
    COUNT(*) AS bin_frequency
  FROM
    beat_interval_bins
  GROUP BY
    participantid,
    start_time,
    histogram_bin),
  histogram_rank AS (
  SELECT
    participantid,
    start_time,
    histogram_bin,
    bin_frequency,
    RANK() OVER (PARTITION BY t.participantid, t.start_time ORDER BY t.bin_frequency DESC) AS rank_bin_frequency,
    RANK() OVER (PARTITION BY t.participantid, t.start_time ORDER BY t.histogram_bin ASC) AS rank_histogram_bin_asc,
    RANK() OVER (PARTITION BY t.participantid, t.start_time ORDER BY t.histogram_bin DESC) AS rank_histogram_bin_desc
  FROM
    histogram AS t),
  histogram_mode AS (
  SELECT
    *
  FROM
    histogram_rank
  WHERE
    rank_bin_frequency = 1),
  histogram_ij_mode AS (
  SELECT
    A.*,
    B.histogram_bin AS mode_bin,
    B.bin_frequency AS mode_frequency
  FROM
    histogram AS A
  INNER JOIN
    histogram_mode AS B
  ON
    A.participantid = B.participantid
    AND A.start_time = B.start_time),
  histogram_left_right_group AS (
  SELECT
    *,
    CASE
      WHEN histogram_bin <= mode_bin THEN 1
    ELSE
    0
  END
    AS left_bin,
    CASE
      WHEN histogram_bin >= mode_bin THEN 1
    ELSE
    0
  END
    AS right_bin
  FROM
    histogram_ij_mode),
  histogram_translation AS (
  SELECT
    *,
    histogram_bin - mode_bin AS histogram_bin_translate,
    bin_frequency - mode_frequency AS bin_frequency_translate
  FROM
    histogram_left_right_group),
  histogram_product AS (
  SELECT
    *,
    histogram_bin_translate*histogram_bin_translate AS bin_bin_product,
    histogram_bin_translate*bin_frequency_translate AS bin_frequency_product
  FROM
    histogram_translation),
  histogram_dot_product_left AS (
  SELECT
    participantid,
    start_time,
    SUM(bin_bin_product) AS bin_bin_dot_product_left,
    SUM(bin_frequency_product) AS bin_frequency_dot_product_left
  FROM
    histogram_product
  WHERE
    left_bin = 1
  GROUP BY
    participantid,
    start_time),
  histogram_slope_left AS (
  SELECT
    participantid,
    start_time,
    (1.0/bin_bin_dot_product_left)*bin_frequency_dot_product_left AS slope_left
  FROM
    histogram_dot_product_left
  WHERE
    bin_bin_dot_product_left <> 0
    AND bin_frequency_dot_product_left <> 0),
  histogram_dot_product_right AS (
  SELECT
    participantid,
    start_time,
    SUM(bin_bin_product) AS bin_bin_dot_product_right,
    SUM(bin_frequency_product) AS bin_frequency_dot_product_right
  FROM
    histogram_product
  WHERE
    right_bin = 1
  GROUP BY
    participantid,
    start_time),
  histogram_slope_right AS (
  SELECT
    participantid,
    start_time,
    (1.0/bin_bin_dot_product_right)*bin_frequency_dot_product_right AS slope_right
  FROM
    histogram_dot_product_right
  WHERE
    bin_bin_dot_product_right <> 0
    AND bin_frequency_dot_product_right <> 0),
  histogram_slope AS (
  SELECT
    A.*,
    B.slope_right,
    C.histogram_bin AS mode_bin,
    C.bin_frequency AS mode_frequency,
    D.beat_interval_day_count
  FROM
    histogram_slope_left AS A
  INNER JOIN
    histogram_slope_right AS B
  ON
    A.participantid = B.participantid
    AND A.start_time = B.start_time
  INNER JOIN
    histogram_mode AS C
  ON
    A.participantid = C.participantid
    AND A.start_time = C.start_time
  INNER JOIN
    beat_interval_count_per_day AS D
  ON
    A.participantid = D.participantid
    AND A.start_time = D.start_time ),
  histogram_N_M_rrtri AS (
  SELECT
    *,
    mode_bin - mode_frequency/slope_left AS N,
    mode_bin - mode_frequency/slope_right AS M,
    beat_interval_day_count/mode_frequency AS rrtri
  FROM
    histogram_slope
  WHERE
    slope_left <> 0
    AND slope_right <> 0),
  heart_rate_N_M_rrtri AS (
  SELECT
    participantid,
    start_time,
    M - N AS tinn,
    rrtri
  FROM
    histogram_N_M_rrtri),
  heart_rate_poincare AS (
  SELECT
    hr.participantid,
    /* Bucket timestamps on 1 day intervals and compute
               heart rate variability statistics within each bucket. */ datetime(TIMESTAMP_SECONDS( UNIX_SECONDS(hr.hr_time) - MOD(UNIX_SECONDS(hr.hr_time), 24 * 60 * 60) )) AS start_time,
    #1hour
    COUNT(*) AS record_count,
    AVG(hr.bpm) AS bpm_mean,
    stddev(hr.bpm) AS bpm_stddev,
    MIN(hr.bpm) AS bpm_min,
    MAX(hr.bpm) AS bpm_max,
    /* Invert heart rate to arrive at minutes/beat. Then convert
               units to milliseconds/beat. */ AVG(60000.0 / hr.bpm) AS beat_interval_ms_mean,
    stddev(60000.0 / hr.bpm) AS beat_interval_ms_stddev,
    MIN(60000.0 / hr.bpm) AS beat_interval_ms_min,
    MAX(60000.0 / hr.bpm) AS beat_interval_ms_max,
    SQRT(AVG(POW(60000.0*(1/hr.bpm-1/hr.prev_bpm), 2))) AS beat_interval_ms_rmssd,
    STDDEV(ABS(-60000.0/hr.prev_bpm + 60000.0/hr.bpm)/SQRT(2)) AS geom_poincare_sd1,
    STDDEV(ABS(60000.0/hr.prev_bpm + 60000.0/hr.bpm - 2*hr.avg_rr)/SQRT(2)) AS geom_poincare_sd2
  FROM
    heart_rate AS hr
  WHERE
    hr.prev_bpm IS NOT NULL
  GROUP BY
    hr.participantid,
    start_time
  HAVING
    bpm_mean IS NOT NULL)
SELECT
  PARSE_DATE("%F", '{{ ds }}') as ds,
  A.participantid,
  ---'24 hour' as hrv_type,           
  ---A.start_time AS start_time_local,
  ---TIMESTAMP_ADD(A.start_time,INTERVAL 24 HOUR) AS end_time_local,
 --- A.record_count,
 --- A.bpm_mean,
 --- A.bpm_stddev,
 --- A.bpm_min,
 --- A.bpm_max,
  A.beat_interval_ms_mean,
  A.beat_interval_ms_stddev,
  A.beat_interval_ms_min,
  A.beat_interval_ms_max,
  A.beat_interval_ms_rmssd,
  A.geom_poincare_sd1,
  A.geom_poincare_sd2,
  B.tinn,
  B.rrtri,
  C.sdann
FROM
  heart_rate_poincare AS A
LEFT JOIN
  heart_rate_N_M_rrtri AS B
ON
  A.participantid = B.participantid
  AND A.start_time = B.start_time
LEFT JOIN
  sdann_table AS C
ON
  A.participantid = C.participantid
  AND A.start_time = C.start_time
where A.bpm_mean is not null