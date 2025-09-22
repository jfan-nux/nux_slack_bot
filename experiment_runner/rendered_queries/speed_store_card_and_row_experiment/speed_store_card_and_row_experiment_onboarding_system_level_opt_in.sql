--------------------- experiment exposure

WITH experiment AS (
    SELECT bucket_key,
        max(a.result) AS bucket,
        min(a.exposure_time)::date AS first_exposure_date_utc,
        min(convert_timezone('UTC','America/Los_Angeles', a.exposure_time)) AS first_exposure_time,
        first_exposure_time::date AS first_exposure_date
    FROM proddb.public.fact_dedup_experiment_exposure a    
    WHERE 1=1
    AND a.experiment_name = 'speed_store_card_and_row_experiment'
    AND convert_timezone('UTC', 'America/Los_Angeles', a.exposure_time)::date >= '2025-09-17'
    AND tag <> 'overridden'
    GROUP BY all
    HAVING count(distinct a.result) = 1
)

, device_level_temp AS (
  SELECT
    consumer_id,
    scd_start_date AS event_ts,
    CASE
      WHEN system_level_status = 'off'
      AND coalesce(prev_system_level_status, '') <> 'on' THEN 1
      ELSE 0
    END AS system_push_opt_out,
    CASE
      WHEN system_level_status = 'on'
      AND coalesce(prev_system_level_status, '') <> 'on' THEN 1
      ELSE 0
    END AS system_push_opt_in
  FROM
    edw.consumer.dimension_consumer_device_push_settings_scd3
  WHERE scd_start_date >= '2025-09-17'
    AND experience = 'doordash'
)

, device_level AS (
  SELECT
    consumer_id,
    event_ts,
    max(system_push_opt_out) AS system_push_opt_out,
    max(system_push_opt_in) AS system_push_opt_in
  FROM
    device_level_temp
  GROUP BY all
)

, rollup as (
  SELECT a.bucket_key as consumer_id,
    a.bucket,
    b.event_ts,
    coalesce(b.system_push_opt_out, 0) AS system_level_push_opt_out,
    coalesce(b.system_push_opt_in, 0) AS system_level_push_opt_in
  FROM experiment a 
  left join device_level b 
  on a.bucket_key = b.consumer_id
) 

, opt_in_res AS (
select BUCKET 
, count(distinct consumer_id) as total_cx
, sum(system_level_push_opt_out) as system_level_push_opt_out
, sum(system_level_push_opt_out)/ count(distinct consumer_id) as system_level_push_opt_out_pct
, sum(system_level_push_opt_in) as system_level_push_opt_in
, sum(system_level_push_opt_in)/ count(distinct consumer_id) as system_level_push_opt_in_pct
from rollup 
group by all
)

, res AS (
SELECT o.*
FROM opt_in_res o
ORDER BY 1
)

SELECT r1.bucket
        , r1.total_cx AS exposure
        , r1.system_level_push_opt_out
        , r1.system_level_push_opt_out_pct
        , r1.system_level_push_opt_out_pct / NULLIF(r2.system_level_push_opt_out_pct,0) - 1 AS Lift_system_level_push_opt_out_pct
        , r1.system_level_push_opt_in
        , r1.system_level_push_opt_in_pct
        , r1.system_level_push_opt_in_pct / NULLIF(r2.system_level_push_opt_in_pct,0) - 1 AS Lift_system_level_push_opt_in_pct
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for rate variables
        , r2.total_cx AS control_exposure
        , r2.system_level_push_opt_out AS control_system_level_push_opt_out
        , r2.system_level_push_opt_out_pct AS control_system_level_push_opt_out_pct
        , r2.system_level_push_opt_in AS control_system_level_push_opt_in
        , r2.system_level_push_opt_in_pct AS control_system_level_push_opt_in_pct
        
FROM res r1
LEFT JOIN res r2
    ON r1.bucket != r2.bucket
    AND r2.bucket = 'control'
ORDER BY 1