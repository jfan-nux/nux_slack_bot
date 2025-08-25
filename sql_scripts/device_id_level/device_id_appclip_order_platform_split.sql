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
 
, signin as (
select distinct cast(IGUAZU_OTHER_PROPERTIES:logged_in_consumer_id as varchar) as logged_in_consumer_id
, cast(GUEST_CONSUMER_ID as varchar) as GUEST_CONSUMER_ID
, IGUAZU_TIMESTAMP::date AS day
from iguazu.consumer.m_app_clip_sign_in
where IGUAZU_OTHER_PROPERTIES:logged_in_consumer_id is not null
AND iguazu_timestamp BETWEEN '{{ start_date }}' AND '{{ end_date }}'
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
, app_installs as (
SELECT cast(consumer_id as varchar) as consumer_id
, EVENT_TIMESTAMP::date as day
, dd_device_id
from edw.growth.fact_consumer_app_open_events
where 1=1
and cast(consumer_id as varchar) is not NULL
and EVENT_TIMESTAMP::date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and cast(consumer_id as varchar) in ( select consumer_id from full_consumer_id_list )
)
, full_consumer_id_list_w_app_downloads as (
select distinct l.consumer_id
 , l.day
 , l.tag
 , replace(lower(CASE WHEN a.DD_device_id like 'dx_%' then a.DD_device_id
                    else 'dx_'||a.DD_device_id end), '-') AS dd_device_ID_filtered
from full_consumer_id_list l 
left join app_installs a 
on l.consumer_id = a.consumer_id
and l.day <= a.day 
)
, app_clip_orders AS
(SELECT DISTINCT cast(IGUAZU_OTHER_PROPERTIES:consumer_id as varchar) as consumer_id
        , a.iguazu_timestamp::date as day
        , dd.delivery_ID
        , dd.is_first_ordercart_DD
        , dd.is_filtered_core
        , 'dx_123' as dd_device_ID_filtered --dummy device_id
        , 'app clip' as platform
FROM iguazu.consumer.m_app_clip_checkout_page_system_checkout_success a 
JOIN Dimension_deliveries dd
    ON a.order_UUID::varchar = dd.ORDER_CART_UUID::varchar
    AND dd.is_filtered_core = 1
    AND dd.created_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
WHERE a.iguazu_timestamp BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)
, app_orders AS (
SELECT DISTINCT cast(dd.creator_id as varchar) as consumer_id
, a.timestamp::date as day
, dd.delivery_ID
, dd.is_first_ordercart_DD
, dd.is_filtered_core
, replace(lower(CASE WHEN a.DD_device_id like 'dx_%' then a.DD_device_id
                    else 'dx_'||a.DD_device_id end), '-') AS dd_device_ID_filtered
, 'app' as platform
FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND dd.created_at BETWEEN '{{ start_date }}' AND '{{ end_date }}'
WHERE a.timestamp BETWEEN '{{ start_date }}' AND '{{ end_date }}'
and dd.submit_platform = 'ios'
and (dd.creator_id::varchar in ( select consumer_id::varchar from full_consumer_id_list_w_app_downloads )
or replace(lower(CASE WHEN a.DD_device_id like 'dx_%' then a.DD_device_id
                    else 'dx_'||a.DD_device_id end), '-') in (select dd_device_ID_filtered from full_consumer_id_list_w_app_downloads))
and dd.delivery_id not in (select delivery_id from app_clip_orders)
)
, all_orders as (
select * 
from app_clip_orders
union all 
select * 
from app_orders
)
, checkout_rollup AS (
SELECT e.tag
, e.consumer_id
, o.platform
, COUNT(DISTINCT o.delivery_ID ) orders
, IFF(COUNT(DISTINCT o.delivery_ID) = 0, NULL, COUNT(DISTINCT o.delivery_ID)) as order_freq_numerator
, COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 THEN o.delivery_ID ELSE NULL END) new_Cx
FROM full_consumer_id_list_w_app_downloads e
JOIN all_orders o
on (e.consumer_id = o.consumer_id
or e.dd_device_ID_filtered = o.dd_device_ID_filtered)
GROUP BY 1,2,3
)
, checkout as (
select tag 
, platform
, count(distinct consumer_id) as total_cx 
, sum(orders) as total_orders
, avg(orders) as avg_orders_per_cx
, sum(orders) / count(distinct consumer_id) as order_rate
from checkout_rollup 
group by 1,2
)

, res AS (
SELECT c.*
FROM checkout c
ORDER BY 1, 2
)

SELECT r1.tag
        , r1.platform
        , r1.total_cx
        , r1.total_orders
        , r1.avg_orders_per_cx
        , r1.order_rate
        , r1.order_rate / NULLIF(r2.order_rate,0) - 1 AS Lift_order_rate
        , r1.total_orders / NULLIF(r2.total_orders,0) - 1 AS Lift_total_orders
        , r1.avg_orders_per_cx / NULLIF(r2.avg_orders_per_cx,0) - 1 AS Lift_avg_orders_per_cx
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for continuous and rate variables
        , r2.total_cx AS control_total_cx
        , r2.total_orders AS control_total_orders
        , r2.avg_orders_per_cx AS control_avg_orders_per_cx
        , r2.order_rate AS control_order_rate
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r1.platform = r2.platform
    AND r2.tag = 'control'
ORDER BY 1, 2 desc