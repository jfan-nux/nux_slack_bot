{#
Jinja2 Template Variables:
- experiment_name: {{ experiment_name }}
- start_date: {{ start_date }}
- end_date: {{ end_date }}
- version: {{ version }}
- segments: {{ segments }}
#}

WITH exposure AS (
SELECT distinct ee.tag
              , ee.bucket_key
              , LOWER(ee.segment) AS segments
              , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
              , case when cast(custom_attributes:consumer_id as varchar) not like 'dx_%' then cast(custom_attributes:consumer_id as varchar) else null end as consumer_id
              , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = '{{ experiment_name }}'
AND experiment_version = {{ version }}
{%- if segments %}
AND segment IN ({% for segment in segments %}'{{ segment }}'{% if not loop.last %}, {% endif %}{% endfor %})
{%- else %}
and segment = 'Users'
{%- endif %}
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY 1,2,3,4,5
)

, adjust_links_straight_to_app AS (
  SELECT 
    DISTINCT 
    replace(lower(CASE WHEN context_device_id like 'dx_%' then context_device_id else 'dx_'||context_device_id end), '-') AS app_device_id
    ,convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS day
    ,replace(lower(CASE WHEN split_part(split_part(DEEP_LINK_URL,'dd_device_id%3D',2),'%',1) like 'dx_%' then split_part(split_part(DEEP_LINK_URL,'dd_device_id%3D',2),'%',1) else 'dx_'||split_part(split_part(DEEP_LINK_URL,'dd_device_id%3D',2),'%',1) end), '-') as mweb_id
  FROM iguazu.server_events_production.m_deep_link
  WHERE DEEP_LINK_URL like '%device_id%'
    AND convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)

, adjust_link_app_store AS (  
SELECT distinct app_device_id
, day
, CASE WHEN mweb_id like 'dx_%' then replace(lower(CASE WHEN mweb_id like 'dx_%' then mweb_id else 'dx_'||mweb_id end), '-') else mweb_id end as mweb_id
from (
SELECT distinct replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id else 'dx_'||dd_device_id end), '-') AS app_device_id
, split_part(split_part(event_properties,'web_consumer_id%3D',2),'%',1) as mweb_id
-- , split_part(split_part(event_properties, 'adjust_source%3D',2),'%',1) as adjust_source
-- , split_part(split_part(event_properties, 'pageType%3D',2),'%',1) as page_type
, event_date as day 
FROM edw.growth.fact_singular_mobile_events 
WHERE 1=1
    AND event_properties LIKE '%web_consumer_id%'
and event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
order by event_date desc
)
)

, adjust_links AS (
  SELECT * FROM adjust_links_straight_to_app a 
  UNION ALL 
  SELECT * FROM adjust_link_app_store
)

----- Including both app_device_id and mobile_device_id, some mobile_device_id doesn't have corresponding apple_device_id
, exposure_with_both_ids_device as (
  SELECT DISTINCT e.*
    , ac.app_device_id
  FROM exposure e
    JOIN adjust_links ac 
    ON e.dd_device_ID_filtered = ac.mweb_id 
 --   OR e.consumer_id = ac.mweb_id
    AND e.day <= ac.day
)

, exposure_with_both_ids_consumer as (
  SELECT DISTINCT e.*
    , ac.app_device_id
  FROM exposure e
    JOIN adjust_links ac 
    ON e.consumer_id = ac.mweb_id
    AND e.day <= ac.day
)

, app_exposure_with_both_ids as (
select *
from exposure_with_both_ids_consumer
union all 
select * 
from exposure_with_both_ids_device
)

, exposure_with_both_ids as (
select e.*
    , ac.app_device_id
FROM exposure e
LEFT JOIN app_exposure_with_both_ids ac 
ON e.dd_device_ID_filtered = ac.dd_device_ID_filtered 
)

, signup_success_overall  AS ( 
SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , SOCIAL_PROVIDER AS Source
       , user_id 
from segment_events_RAW.consumer_production.social_login_new_user 
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
AND SOCIAL_PROVIDER IN ('google-plus','facebook','apple')

UNION 

SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'email' AS source
       , user_id 
from segment_events_RAW.consumer_production.doordash_signup_success 
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)

, SUMA AS 
(SELECT DISTINCT user_ID
                ,consumer_ID 
FROM edw.consumer.suma_consumers
)

, SUMA_signup AS
(SELECT DISTINCT s.dd_device_ID_filtered
                , s.day
                , su.user_ID
FROM signup_success_overall s
JOIN SUMA su
  ON s.user_ID = su.user_ID
)

, SUMA_res_mweb AS
(SELECT e.tag
        , e.segments
        , COUNT(DISTINCT e.dd_device_ID_filtered||e.day) AS exposure
        , COUNT(DISTINCT s.user_ID) AS SUMA
FROM exposure_with_both_ids e
LEFT JOIN SUMA_signup s
    ON e.dd_device_ID_filtered = s.dd_device_ID_filtered
    AND e.day <= s.day
WHERE TAG != 'reserve'
GROUP BY 1, 2
)

, SUMA_res_app AS
(SELECT e.tag
        , e.segments
        , COUNT(DISTINCT e.dd_device_ID_filtered||e.day) AS exposure
        , COUNT(DISTINCT s.user_ID) AS SUMA
FROM exposure_with_both_ids e
LEFT JOIN SUMA_signup s
    ON e.app_device_id = s.dd_device_ID_filtered
    AND e.day <= s.day
WHERE TAG != 'reserve'
GROUP BY 1, 2
)
, SUMA_res as (
select a.tag 
, a.segments
, sum(a.exposure) as exposure
, sum(a.SUMA) + sum(zeroifnull(b.SUMA)) as SUMA 
, (sum(a.SUMA) + sum(zeroifnull(b.SUMA)))/sum(a.exposure) as SUMA_rate
from SUMA_res_mweb a 
left join SUMA_res_app b 
on a.tag = b.tag AND a.segments = b.segments
group by 1, 2
)

, mweb_Auth_success AS
(SELECT e.tag
        , e.segments
        , COUNT(DISTINCT e.dd_device_ID_filtered||e.day) AS exposure
        , COUNT(DISTINCT s.dd_device_ID_filtered||s.day) AS overall_signup
FROM exposure e
LEFT JOIN signup_success_overall s
    ON e.dd_device_ID_filtered = s.dd_device_ID_filtered
    AND e.day <= s.day
WHERE TAG != 'reserve'
GROUP BY 1, 2
ORDER BY 1, 2
)

, app_Auth_success AS
(SELECT e.tag
        , e.segments
        , COUNT(DISTINCT e.dd_device_ID_filtered||e.day) AS exposure
        , COUNT(DISTINCT s.dd_device_ID_filtered||s.day) AS overall_signup
FROM exposure_with_both_ids e
LEFT JOIN signup_success_overall s
    ON e.app_device_id = s.dd_device_ID_filtered
    AND e.day <= s.day
WHERE TAG != 'reserve'
GROUP BY 1, 2
ORDER BY 1, 2
)
, auth_success as (
select a.tag 
, a.segments
, sum(a.exposure) as exposure
, sum(a.overall_signup) + sum(zeroifnull(b.overall_signup)) as overall_signup 
, (sum(a.overall_signup) + sum(zeroifnull(b.overall_signup)))/sum(a.exposure) as overall_signup_rate
from mweb_Auth_success a 
left join app_Auth_success b 
on a.tag = b.tag AND a.segments = b.segments
group by 1, 2
)

, res AS (
SELECT a.*
        , sr.SUMA
        , sr.SUMA_rate
FROM auth_success a
LEFT JOIN SUMA_res sr
  ON a.tag = sr.tag AND a.segments = sr.segments
ORDER BY 1, 2
)

SELECT r1.tag 
      , r1.segments
      , r1.exposure
      , r1.SUMA
      , r1.SUMA_rate
      , r1.overall_signup
      , r1.overall_signup_rate
      , r1.SUMA_rate / NULLIF(r2.SUMA_rate,0) - 1 AS lift_SUMA_rate
      , r1.overall_signup_rate / NULLIF(r2.overall_signup_rate,0) - 1 AS lift_overall_signup_rate
      
      -- Statistical variables for p-value calculations
      -- Control group statistics (r2) for rate variables
      , r2.exposure AS control_exposure
      , r2.SUMA AS control_SUMA
      , r2.SUMA_rate AS control_SUMA_rate
      , r2.overall_signup AS control_overall_signup
      , r2.overall_signup_rate AS control_overall_signup_rate
      
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
    AND r1.segments = r2.segments
ORDER BY 1, 2 desc