
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
{%- if version is not none %}
and experiment_version = {{ version }}
{%- endif %}
and exposure_time between '{{ start_date }}' and '{{ end_date }}'
and bucket_key_type = 'device_id'
{%- if segments %}
AND segment IN ({% for segment in segments %}'{{ segment }}'{% if not loop.last %}, {% endif %}{% endfor %})
{%- endif %}
group by 1,2,3
) 

, checkout_page AS (
SELECT cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_checkout_page_load
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
)

, checkout_apple_pay_tap as (
select distinct cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_checkout_apple_pay_tap
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
)

, checkout_success as (
select  cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_checkout_page_system_checkout_success
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
)

, checkout_page_system_checkout_error as (
select distinct cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_checkout_page_system_checkout_error
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
and description <> 'phone verification MFAErrorResponse'
)

, Checkout_Change_Payment_Cell_Tap as (
select distinct cast(consumer_id as varchar) as consumer_id
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
 from iguazu.consumer.m_app_clip_Checkout_Change_Payment_Cell_Tap_ice
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and APP_VERSION >= '5.39'
)


, Checkout_Place_Order_Tap as (
select distinct cast(consumer_id as varchar) as consumer_id
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_Checkout_Place_Order_Tap_ice
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and APP_VERSION >= '5.39'
)

, Payments_Add_Card as (
select distinct cast(consumer_id as varchar) as consumer_id
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_Payments_Add_Card_ice
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and  APP_VERSION >= '5.39'
)

, Payments_Add_Card_Cell_Tap as (
select distinct cast(consumer_id as varchar) as consumer_id
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_Payments_Add_Card_Cell_Tap_ice
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and APP_VERSION >= '5.39'
)

, Payments_Delete_Card as (
select distinct cast(consumer_id as varchar) as consumer_id
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
 , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_Payments_Delete_Card_ice
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and APP_VERSION >= '5.39'
)

-- , Payments_Set_Selected as (
-- select distinct cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
-- , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
-- , convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
-- from iguazu.consumer.m_app_clip_Payments_Set_Selected_ice
-- WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
-- and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
-- and APP_VERSION >= '5.39'
-- )

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
, s.GUEST_CONSUMER_ID
, l.tag
from exposure l 
left join signin s 
on s.GUEST_CONSUMER_ID = l.consumer_id
or s.logged_in_consumer_id = l.consumer_id
)

, funnel AS (
SELECT DISTINCT s.consumer_id
, s.tag
, s.segments
                , MAX(CASE WHEN cop.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS checkout_page
                , MAX(CASE WHEN cs.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS checkout_success
                , MAX(CASE WHEN cpsce.consumer_id IS NOT NULL and cpsce.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS checkout_page_system_checkout_error                    

                , MAX(CASE WHEN a.consumer_id IS NOT NULL and a.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS Checkout_Change_Payment_Cell_Tap                    
                , MAX(CASE WHEN b.consumer_id IS NOT NULL and b.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS Checkout_Place_Order_Tap                    
                , MAX(CASE WHEN c.consumer_id IS NOT NULL and c.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS Payments_Add_Card                    
                , MAX(CASE WHEN d.consumer_id IS NOT NULL and d.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS Payments_Add_Card_Cell_Tap                    
                , MAX(CASE WHEN e.consumer_id IS NOT NULL and e.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS Payments_Delete_Card                    
--                , MAX(CASE WHEN f.consumer_id IS NOT NULL and f.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS Payments_Set_Selected                    

FROM full_consumer_id_list s 
LEFT JOIN checkout_page cop 
on s.consumer_id  = cop.consumer_id 
and s.day = cop.day
LEFT JOIN checkout_success cs
on s.consumer_id  = cs.consumer_id
and s.day = cs.day
LEFT JOIN checkout_page_system_checkout_error cpsce
on s.consumer_id  = cpsce.consumer_id
and s.day = cpsce.day

LEFT JOIN Checkout_Change_Payment_Cell_Tap a
on s.consumer_id  = a.consumer_id 
and s.day = a.day
LEFT JOIN Checkout_Place_Order_Tap b
on s.consumer_id  = b.consumer_id 
and s.day = b.day
LEFT JOIN Payments_Add_Card c
on s.consumer_id  = c.consumer_id 
and s.day = c.day
LEFT JOIN Payments_Add_Card_Cell_Tap d
on s.consumer_id  = d.consumer_id 
and s.day = d.day
LEFT JOIN Payments_Delete_Card e
on s.consumer_id  = e.consumer_id 
and s.day = e.day
-- LEFT JOIN Payments_Set_Selected f
-- on s.consumer_id  = f.consumer_id 
-- and s.day = f.day

GROUP BY 1,2,3
)

, checkout_funnel_res AS (
  SELECT tag
, segments
, count(distinct consumer_id) as total_cx
, SUM(checkout_page) checkout_page
, SUM(checkout_page) / count(distinct consumer_id) as checkout_rate
, SUM(checkout_success) checkout_success
, SUM(checkout_success) / NULLIF(SUM(checkout_page),0) AS checkout_success_rate
, SUM(checkout_page_system_checkout_error) checkout_page_system_checkout_error
, SUM(checkout_page_system_checkout_error) / NULLIF(SUM(checkout_page),0) AS checkout_page_system_checkout_error_rate
, SUM(Checkout_Change_Payment_Cell_Tap) Checkout_Change_Payment_Cell_Tap
, SUM(Checkout_Change_Payment_Cell_Tap) / NULLIF(SUM(checkout_page),0) AS Checkout_Change_Payment_Cell_Tap_rate
, SUM(Checkout_Place_Order_Tap) Checkout_Place_Order_Tap
, SUM(Checkout_Place_Order_Tap) / NULLIF(SUM(checkout_page),0) AS Checkout_Place_Order_Tap_rate
, SUM(Payments_Add_Card) Payments_Add_Card
, SUM(Payments_Add_Card) / NULLIF(SUM(checkout_page),0) AS Payments_Add_Card_rate
, SUM(Payments_Add_Card_Cell_Tap) Payments_Add_Card_Cell_Tap
, SUM(Payments_Add_Card_Cell_Tap) / NULLIF(SUM(checkout_page),0) AS Payments_Add_Card_Cell_Tap_rate
, SUM(Payments_Delete_Card) Payments_Delete_Card
, SUM(Payments_Delete_Card) / NULLIF(SUM(checkout_page),0) AS Payments_Delete_Card_rate
-- , SUM(Payments_Set_Selected) Payments_Set_Selected
-- , SUM(Payments_Set_Selected) / NULLIF(SUM(checkout_page),0) AS Payments_Set_Selected_rate
FROM funnel e
GROUP BY 1, 2
)

, res AS (
SELECT c.*
FROM checkout_funnel_res c
ORDER BY 1
)

SELECT r1.tag
        , r1.total_cx
        , r1.checkout_page
        , r1.checkout_rate
        , r1.checkout_rate / NULLIF(r2.checkout_rate,0) - 1 AS Lift_checkout_rate
        , r1.checkout_success
        , r1.checkout_success_rate
        , r1.checkout_success_rate / NULLIF(r2.checkout_success_rate,0) - 1 AS Lift_checkout_success_rate
        , r1.checkout_page_system_checkout_error
        , r1.checkout_page_system_checkout_error_rate
        , r1.checkout_page_system_checkout_error_rate / NULLIF(r2.checkout_page_system_checkout_error_rate,0) - 1 AS Lift_checkout_page_system_checkout_error_rate
        , r1.Checkout_Change_Payment_Cell_Tap
        , r1.Checkout_Change_Payment_Cell_Tap_rate
        , r1.Checkout_Change_Payment_Cell_Tap_rate / NULLIF(r2.Checkout_Change_Payment_Cell_Tap_rate,0) - 1 AS Lift_Checkout_Change_Payment_Cell_Tap_rate
        , r1.Checkout_Place_Order_Tap
        , r1.Checkout_Place_Order_Tap_rate
        , r1.Checkout_Place_Order_Tap_rate / NULLIF(r2.Checkout_Place_Order_Tap_rate,0) - 1 AS Lift_Checkout_Place_Order_Tap_rate
        , r1.Payments_Add_Card
        , r1.Payments_Add_Card_rate
        , r1.Payments_Add_Card_rate / NULLIF(r2.Payments_Add_Card_rate,0) - 1 AS Lift_Payments_Add_Card_rate
        , r1.Payments_Add_Card_Cell_Tap
        , r1.Payments_Add_Card_Cell_Tap_rate
        , r1.Payments_Add_Card_Cell_Tap_rate / NULLIF(r2.Payments_Add_Card_Cell_Tap_rate,0) - 1 AS Lift_Payments_Add_Card_Cell_Tap_rate
        , r1.Payments_Delete_Card
        , r1.Payments_Delete_Card_rate
        , r1.Payments_Delete_Card_rate / NULLIF(r2.Payments_Delete_Card_rate,0) - 1 AS Lift_Payments_Delete_Card_rate
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for rate variables
        , r2.total_cx AS control_total_cx
        , r2.checkout_page AS control_checkout_page
        , r2.checkout_rate AS control_checkout_rate
        , r2.checkout_success AS control_checkout_success
        , r2.checkout_success_rate AS control_checkout_success_rate
        , r2.checkout_page_system_checkout_error AS control_checkout_page_system_checkout_error
        , r2.checkout_page_system_checkout_error_rate AS control_checkout_page_system_checkout_error_rate
        , r2.Checkout_Change_Payment_Cell_Tap AS control_Checkout_Change_Payment_Cell_Tap
        , r2.Checkout_Change_Payment_Cell_Tap_rate AS control_Checkout_Change_Payment_Cell_Tap_rate
        , r2.Checkout_Place_Order_Tap AS control_Checkout_Place_Order_Tap
        , r2.Checkout_Place_Order_Tap_rate AS control_Checkout_Place_Order_Tap_rate
        , r2.Payments_Add_Card AS control_Payments_Add_Card
        , r2.Payments_Add_Card_rate AS control_Payments_Add_Card_rate
        , r2.Payments_Add_Card_Cell_Tap AS control_Payments_Add_Card_Cell_Tap
        , r2.Payments_Add_Card_Cell_Tap_rate AS control_Payments_Add_Card_Cell_Tap_rate
        , r2.Payments_Delete_Card AS control_Payments_Delete_Card
        , r2.Payments_Delete_Card_rate AS control_Payments_Delete_Card_rate
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
    AND r1.segments = r2.segments
ORDER BY 1, 2 desc
