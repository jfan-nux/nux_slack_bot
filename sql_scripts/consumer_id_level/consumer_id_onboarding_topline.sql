--------------------- experiment exposure
{#
Jinja2 Template Variables:
- experiment_name: {{ experiment_name }}
- start_date: {{ start_date }}
- end_date: {{ end_date }}
- version: {{ version }}
- segments: {{ segments }}
#}
WITH exposure AS
(SELECT  ee.tag
               , ee.result
               , ee.bucket_key
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = '{{ experiment_name }}'
AND experiment_version::INT = {{ version }}
{%- if segments %}
AND segment IN ({% for segment in segments %}'{{ segment }}'{% if not loop.last %}, {% endif %}{% endfor %})
{%- endif %}
AND tag <> 'overridden'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY all
)
, orders AS
(SELECT DISTINCT dd.creator_id as consumer_id
        , convert_timezone('UTC','America/Los_Angeles',a.timestamp)::date as day
        , dd.delivery_ID
        , dd.is_first_ordercart_DD
        , dd.is_filtered_core
        , dd.variable_profit * 0.01 AS variable_profit
        , dd.gov * 0.01 AS gov
FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'

)


, checkout AS
(SELECT  e.tag
        , COUNT(distinct e.bucket_key) as exposure_onboard
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) orders
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) new_Cx
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.bucket_key) order_rate
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.bucket_key) new_cx_rate
        , SUM(variable_profit) AS variable_profit
        , SUM(variable_profit) / COUNT(DISTINCT e.bucket_key) AS VP_per_device
        , SUM(gov) AS gov
        , SUM(gov) / COUNT(DISTINCT e.bucket_key) AS gov_per_device
        
        -- Statistical variables for p-value calculation
        -- For continuous variables: need std dev and sample size
        , STDDEV_SAMP(variable_profit) AS std_variable_profit
        , STDDEV_SAMP(gov) AS std_gov
        , COUNT(CASE WHEN is_filtered_core = 1 THEN o.delivery_ID END) AS n_orders_for_stats  -- sample size for continuous vars
        
        -- Rate variables already have numerator/denominator:
        -- order_rate: orders/exposure_onboard
        -- new_cx_rate: new_cx/exposure_onboard
        
FROM exposure e
LEFT JOIN orders o
    ON e.bucket_key = o.consumer_id 
    AND e.day <= o.day
WHERE TAG NOT IN ('internal_test','reserved')
GROUP BY all
ORDER BY 1)



,  MAU AS (
SELECT  e.tag
        , COUNT(DISTINCT o.consumer_id) as MAU
        , COUNT(DISTINCT o.consumer_id) / COUNT(DISTINCT e.bucket_key) as MAU_rate
FROM exposure e
LEFT JOIN orders o
    ON e.bucket_key = o.consumer_id 
    --AND e.day <= o.day
    AND o.day BETWEEN DATEADD('day',-28,current_date) AND DATEADD('day',-1,current_date) -- past 28 days orders
-- WHERE e.day <= DATEADD('day',-28,'{{ end_date }}') --- exposed at least 28 days ago
GROUP BY all
ORDER BY 1
)

, res AS
(SELECT c.*
        , m.MAU 
        , m.mau_rate
FROM checkout c
JOIN MAU m 
  on c.tag = m.tag
ORDER BY 1
)

SELECT r1.tag 
        , r1.exposure_onboard AS exposure
        , r1.orders
        , r1.order_rate
        , r1.order_rate / NULLIF(r2.order_rate,0) - 1 AS Lift_order_rate
        , r1.new_cx
        , r1.new_cx_rate
        , r1.new_cx_rate / NULLIF(r2.new_cx_rate,0) - 1 AS Lift_new_cx_rate
        
        , r1.variable_profit
        , r1.variable_profit / nullif(r2.variable_profit,0) - 1 AS Lift_VP
        , r1.VP_per_device
        , r1.VP_per_device / nullif(r2.VP_per_device,0) -1 AS Lift_VP_per_device   
        , r1.gov
        , r1.gov / r2.gov - 1 AS Lift_gov
        , r1.gov_per_device
        , r1.gov_per_device / r2.gov_per_device -1 AS Lift_gov_per_device
        , r1.mau 
        , r1.mau_rate
        , r1.mau_rate / nullif(r2.mau_rate,0) - 1 AS Lift_mau_rate
        
        -- Statistical variables for p-value calculations
        -- Treatment group statistics (r1)
        , r1.std_variable_profit AS std_variable_profit
        , r1.std_gov AS std_gov
        -- Control group statistics (r2)
        , r2.std_variable_profit AS control_std_variable_profit
        , r2.std_gov AS control_std_gov
        , r2.n_orders_for_stats AS control_n_orders
        , r2.exposure_onboard AS control_exposure
        , r2.orders AS control_orders
        , r2.new_cx AS control_new_cx
        , r2.mau AS control_mau
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
ORDER BY 1 desc