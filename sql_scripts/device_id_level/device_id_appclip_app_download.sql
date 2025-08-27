
{#
Jinja2 Template Variables:
- experiment_name: {{ experiment_name }}
- start_date: {{ start_date }}
- end_date: {{ end_date }}
- version: {{ version }}
- segments: {{ segments }}
#}
with exposure as (
select tag
, LOWER(segment) AS segments
, custom_attributes:consumer_id::varchar as consumer_id
, min(exposure_time::date) as day
FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE 
where experiment_name = '{{ experiment_name }}'
and exposure_time between '{{ start_date }}' and '{{ end_date }}'
and bucket_key_type = 'device_id'
{%- if segments %}
AND segment IN ({% for segment in segments %}'{{ segment }}'{% if not loop.last %}, {% endif %}{% endfor %})
{%- endif %}
group by 1,2,3
) 


, signin as (
select distinct cast(IGUAZU_OTHER_PROPERTIES:logged_in_consumer_id as varchar) as logged_in_consumer_id
, cast(GUEST_CONSUMER_ID as varchar) as GUEST_CONSUMER_ID
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
from iguazu.consumer.m_app_clip_sign_in
where IGUAZU_OTHER_PROPERTIES:logged_in_consumer_id is not null
AND convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)

, full_consumer_id_list as (
select distinct case when s.GUEST_CONSUMER_ID is null then l.consumer_id
 when s.GUEST_CONSUMER_ID is not null then s.logged_in_consumer_id end as consumer_id
, l.day 
, l.tag
from exposure l 
left join signin s 
on s.GUEST_CONSUMER_ID = l.consumer_id
or s.logged_in_consumer_id = l.consumer_id
)

--install_attributed 

, app_installs as (
SELECT cast(consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',EVENT_TIMESTAMP)::date as day
from edw.growth.fact_consumer_app_open_events
where event_type in ('new_install')
and convert_timezone('UTC','America/Los_Angeles',EVENT_TIMESTAMP) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(consumer_id as varchar) in ( select consumer_id from full_consumer_id_list )
)

, app_downloads as (
SELECT e.tag
, e.segments
, COUNT(DISTINCT e.consumer_id) AS total_app_clip_launch
, count(distinct d.consumer_id) as total_app_installs
, count(distinct d.consumer_id)/total_app_clip_launch as app_install_rate
FROM full_consumer_id_list e
left join app_installs d
on e.consumer_id  = d.consumer_id
AND e.day <= d.day
GROUP BY 1, 2
) 


, res AS
(SELECT c.*
FROM app_downloads c
ORDER BY 1
)

SELECT r1.*
        , r1.app_install_rate / NULLIF(r2.app_install_rate,0) - 1 AS Lift_app_install_rate
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for rate variables
        , r2.total_app_clip_launch AS control_total_app_clip_launch
        , r2.total_app_installs AS control_total_app_installs
        , r2.app_install_rate AS control_app_install_rate
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
    AND r1.segments = r2.segments
ORDER BY 1, 2 desc