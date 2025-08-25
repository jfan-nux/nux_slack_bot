--------------------- experiment exposure

WITH exposure AS
(SELECT  ee.tag
               , ee.result
               , ee.bucket_key
               , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'cx_mobile_onboarding_preferences'
AND experiment_version::INT = 1
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-18' AND '2025-09-30'
GROUP BY 1,2,3,4
)

, explore_page AS
(SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS day
       , iguazu_user_id as user_id
from IGUAZU.SERVER_EVENTS_PRODUCTION.M_STORE_CONTENT_PAGE_LOAD
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '2025-08-18' AND '2025-09-30'
)

, store_page AS
(SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
from segment_events_RAW.consumer_production.m_store_page_load
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '2025-08-18' AND '2025-09-30'
)


, cart_page AS
(SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS day
from iguazu.consumer.m_order_cart_page_load
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '2025-08-18' AND '2025-09-30'
)

, checkout_page AS
(SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
from segment_events_RAW.consumer_production.m_checkout_page_load
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '2025-08-18' AND '2025-09-30'
)

, explore AS
(SELECT DISTINCT e.tag
                , e.dd_device_ID_filtered
                , e.day
                , MAX(CASE WHEN ep.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS explore_view
                , MAX(CASE WHEN sp.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS store_view
                , MAX(CASE WHEN cp.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS cart_view
                , MAX(CASE WHEN c.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS checkout_view            
FROM exposure e
LEFT JOIN explore_page ep
    ON e.dd_device_ID_filtered = ep.dd_device_ID_filtered
    AND e.day <= ep.day
LEFT JOIN store_page sp
    ON e.dd_device_ID_filtered = sp.dd_device_ID_filtered
    AND e.day <= sp.day
LEFT JOIN cart_page cp
    ON e.dd_device_ID_filtered = cp.dd_device_ID_filtered
    AND e.day <= cp.day
LEFT JOIN checkout_page c
    ON e.dd_device_ID_filtered = c.dd_device_ID_filtered
    AND e.day <= c.day
GROUP BY 1,2,3
)

, explore_res AS
(SELECT tag
        , count(distinct dd_device_ID_filtered) as exposure_onboard
        , SUM(explore_view) explore_view
        , SUM(explore_view) / COUNT(DISTINCT e.dd_device_ID_filtered||e.day) AS explore_rate
        , SUM(store_view) AS store_view
        , SUM(store_view) / nullif(SUM(explore_view),0) AS Store_rate
        , SUM(cart_view) AS cart_view
        , SUM(cart_view) / nullif(SUM(store_view),0) AS cart_rate
        , SUM(checkout_view)  AS checkout_view
        , SUM(checkout_view) / nullif(SUM(cart_view),0) AS checkout_rate
FROM explore e
GROUP BY 1
ORDER BY 1)

, res AS
(SELECT * 
FROM explore_res
ORDER BY 1
)

SELECT r1.tag 
        , r1.exposure_onboard  
        , r1.explore_view
        , r1.explore_rate
        , r1.explore_rate / nullif(r2.explore_rate,0) -1 AS Lift_explore_rate
        , r1.store_view
        , r1.store_rate
        , r1.store_rate / nullif(r2.store_rate,0) -1 AS Lift_store_rate
        , r1.cart_view
        , r1.cart_rate
        , r1.cart_rate / nullif(r2.cart_rate,0) -1 AS Lift_cart_rate
        , r1.checkout_view
        , r1.checkout_rate
        , r1.checkout_rate / nullif(r2.checkout_rate,0) -1 AS Lift_checkout_rate   
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for rate variables
        , r2.exposure_onboard AS control_exposure_onboard
        , r2.explore_view AS control_explore_view
        , r2.explore_rate AS control_explore_rate
        , r2.store_view AS control_store_view
        , r2.store_rate AS control_store_rate
        , r2.cart_view AS control_cart_view
        , r2.cart_rate AS control_cart_rate
        , r2.checkout_view AS control_checkout_view
        , r2.checkout_rate AS control_checkout_rate
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
ORDER BY 1 desc