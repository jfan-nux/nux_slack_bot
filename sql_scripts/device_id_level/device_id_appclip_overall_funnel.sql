
{#
Jinja2 Template Variables:
- experiment_name: {{ experiment_name }}
- start_date: {{ start_date }}
- end_date: {{ end_date }}
- version: {{ version }}
#}
with exposure as (
select tag
, custom_attributes:consumer_id::varchar as consumer_id
, min(exposure_time::date) as day
FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE 
where experiment_name = '{{ experiment_name }}'
and exposure_time between '{{ start_date }}' and '{{ end_date }}'
and bucket_key_type = 'device_id'
group by 1,2
) 

, explore_page AS (
SELECT cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from IGUAZU.CONSUMER.M_APP_CLIP_HOME_CONTENT_PAGE_LOAD
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
)

, store_page AS (
SELECT cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from IGUAZU.CONSUMER.M_APP_CLIP_STORE_PAGE_LOAD
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
)

, item_page AS (
SELECT cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from IGUAZU.CONSUMER.M_APP_CLIP_ITEM_PAGE_ACTION_ADD_ITEM
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
)

, cart_page AS (
SELECT cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP)::date AS day
, convert_timezone('UTC','America/Los_Angeles',IGUAZU_TIMESTAMP) as timestamp
from iguazu.consumer.m_app_clip_order_cart_page_load
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) <> 'telemetryPlaceholder.consumerId'
and APP_VERSION >= '5.39'
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
                , MAX(CASE WHEN ep.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS explore_page 
                , MAX(CASE WHEN sp.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS store_page 
                , MAX(CASE WHEN ip.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS item_page               
                , MAX(CASE WHEN cp.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS cart_page 
                , MAX(CASE WHEN cop.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS checkout_page
                , MAX(CASE WHEN cs.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS checkout_success
                , MAX(CASE WHEN cop.consumer_id IS NOT NULL and cpsce.consumer_id IS NOT NULL THEN 1 ELSE 0 END) AS checkout_page_system_checkout_error                    

FROM full_consumer_id_list s 
LEFT JOIN explore_page ep 
on s.consumer_id  = ep.consumer_id
and s.day = ep.day
LEFT JOIN store_page sp 
on s.consumer_id  = sp.consumer_id
and s.day = sp.day
LEFT JOIN item_page ip 
on s.consumer_id  = ip.consumer_id
and s.day = ip.day
LEFT JOIN cart_page cp 
on s.consumer_id  = cp.consumer_id
and s.day = cp.day
LEFT JOIN checkout_page cop 
on s.consumer_id  = cop.consumer_id
and s.day = cop.day
LEFT JOIN checkout_success cs
on s.consumer_id  = cs.consumer_id
and s.day = cs.day
LEFT JOIN checkout_page_system_checkout_error cpsce
on s.consumer_id  = cpsce.consumer_id
and s.day = cpsce.day
GROUP BY 1,2
)

, res as (
  SELECT tag
, count(distinct consumer_id) as total_cx
, sum(explore_page) as explore_page
, sum(explore_page) / total_cx as explore_rate
, sum(store_page) as store_page
, sum(store_page) / total_cx as store_rate
, sum(item_page) as item_page
, sum(item_page) / sum(store_page) as item_rate
, SUM(cart_page) cart_page
, SUM(cart_page) / total_cx as cart_rate 
, SUM(checkout_page) checkout_page
, SUM(checkout_page) / NULLIF(SUM(cart_page),0) as checkout_rate
, SUM(checkout_success) checkout_success
, SUM(checkout_success) / NULLIF(SUM(checkout_page),0) AS checkout_success_rate
, SUM(checkout_page_system_checkout_error) checkout_page_system_checkout_error
, SUM(checkout_page_system_checkout_error) / NULLIF(SUM(checkout_page),0) AS checkout_page_system_checkout_error_rate
FROM funnel e
GROUP BY 1
) 

SELECT r1.tag 
        , r1.total_cx  
        , r1.explore_page
        , r1.explore_rate
        , r1.explore_rate / nullif(r2.explore_rate,0) -1 AS Lift_explore_rate
        , r1.store_page
        , r1.store_rate
        , r1.store_rate / nullif(r2.store_rate,0) -1 AS Lift_store_rate
        , r1.item_page
        , r1.item_rate
        , r1.item_rate / nullif(r2.item_rate,0) -1 AS Lift_item_rate    
        , r1.cart_page
        , r1.cart_rate
        , r1.cart_rate / nullif(r2.cart_rate,0) -1 AS Lift_cart_rate
        , r1.checkout_page
        , r1.checkout_rate
        , r1.checkout_rate / nullif(r2.checkout_rate,0) -1 AS Lift_checkout_rate   
        , r1.checkout_success
        , r1.checkout_success_rate
        , r1.checkout_success_rate / nullif(r2.checkout_success_rate,0) -1 AS Lift_checkout_success_rate  
        , r1.checkout_page_system_checkout_error
        , r1.checkout_page_system_checkout_error_rate
        , r1.checkout_page_system_checkout_error_rate / nullif(r2.checkout_page_system_checkout_error_rate,0) -1 AS Lift_checkout_page_system_checkout_error_rate        
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for rate variables
        , r2.total_cx AS control_total_cx
        , r2.explore_page AS control_explore_page
        , r2.explore_rate AS control_explore_rate
        , r2.store_page AS control_store_page
        , r2.store_rate AS control_store_rate
        , r2.item_page AS control_item_page
        , r2.item_rate AS control_item_rate
        , r2.cart_page AS control_cart_page
        , r2.cart_rate AS control_cart_rate
        , r2.checkout_page AS control_checkout_page
        , r2.checkout_rate AS control_checkout_rate
        , r2.checkout_success AS control_checkout_success
        , r2.checkout_success_rate AS control_checkout_success_rate
        , r2.checkout_page_system_checkout_error AS control_checkout_page_system_checkout_error
        , r2.checkout_page_system_checkout_error_rate AS control_checkout_page_system_checkout_error_rate
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
ORDER BY 1 desc