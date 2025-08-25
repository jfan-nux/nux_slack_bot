{#
Jinja2 Template Variables:
- experiment_name: {{ experiment_name }}
- start_date: {{ start_date }}
- end_date: {{ end_date }}
- version: {{ version }}
- segments: {{ segments }}
#}

--------------------- experiment exposure
WITH experiment AS (
    SELECT replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered,
        bucket_key,
        max(a.result) AS bucket,
        min(a.exposure_time)::date AS first_exposure_date_utc,
        min(convert_timezone('UTC','America/Los_Angeles', a.exposure_time)) AS first_exposure_time,
        first_exposure_time::date AS first_exposure_date
    FROM proddb.public.fact_dedup_experiment_exposure a    

    WHERE 1=1
    AND a.experiment_name = '{{ experiment_name }}'
    AND convert_timezone('UTC', 'America/Los_Angeles', a.exposure_time)::date >= '{{ start_date }}'
    AND experiment_version = {{ version }}
{%- if segments %}
    AND segment IN ({% for segment in segments %}'{{ segment }}'{% if not loop.last %}, {% endif %}{% endfor %})
{%- endif %}
    GROUP BY 1,2
    HAVING count(distinct a.result) = 1
)

, device_level_temp AS (
  SELECT
    replace(lower(CASE WHEN device_id like 'dx_%' then device_id
                    else 'dx_'||device_id end), '-') AS dd_device_ID_filtered,
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
  WHERE scd_start_date >= '{{ start_date }}'
    AND experience = 'doordash'
)

, device_level AS (
  SELECT
    dd_device_ID_filtered,
    event_ts,
    max(system_push_opt_out) AS system_push_opt_out,
    max(system_push_opt_in) AS system_push_opt_in
  FROM
    device_level_temp
  GROUP BY
    1, 2
)

, rollup as (
  SELECT a.dd_device_ID_filtered , 
    a.bucket_key,
    a.bucket,
    b.event_ts,
    coalesce(b.system_push_opt_out, 0) AS system_level_push_opt_out,
    coalesce(b.system_push_opt_in, 0) AS system_level_push_opt_in,
  FROM experiment a 
  left join device_level b 
  on a.dd_device_ID_filtered = b.dd_device_ID_filtered
) 

, opt_in_res AS (
select BUCKET 
, count(distinct dd_device_ID_filtered) as total_cx
, sum(system_level_push_opt_out) as system_level_push_opt_out
, sum(system_level_push_opt_out)/ count(distinct dd_device_ID_filtered) as system_level_push_opt_out_pct
, sum(system_level_push_opt_in) as system_level_push_opt_in
, sum(system_level_push_opt_in)/ count(distinct dd_device_ID_filtered) as system_level_push_opt_in_pct
from rollup 
group by 1
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