-- Run by sara.nordstrom@doordash.com
-- Run at 1745531954
-- Query URL https://modeanalytics.com/doordash/reports/2a8feb35cb95/runs/dbf3cf14b162/queries/8c61b103bd1f
-- Report URL https://modeanalytics.com/doordash/reports/2a8feb35cb95/runs/dbf3cf14b162

-- Manual run

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

--build in adjust logic 
, app_downloads_raw AS (  
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

, app_downloads as (
select e.tag as tag
, e.segments
, count(distinct e.dd_device_ID_filtered) as exposures
, count(distinct ad.app_device_id) as app_downloads
, count(distinct ad.app_device_id)/count(distinct e.dd_device_ID_filtered) as app_download_rate
  from exposure e
  left join app_downloads_raw ad
    ON (e.dd_device_ID_filtered = ad.mweb_id 
    OR e.consumer_id = ad.mweb_id)
    AND e.day <= ad.day
group by 1, 2
  ) 

, res AS (
SELECT c.*
FROM app_downloads c 
)

SELECT r1.*
        , r1.app_download_rate / NULLIF(r2.app_download_rate,0) - 1 AS Lift_app_download_rate
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for rate variables
        , r2.exposures AS control_exposures
        , r2.app_downloads AS control_app_downloads
        , r2.app_download_rate AS control_app_download_rate
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
    AND r1.segments = r2.segments
ORDER BY 1, 2 desc