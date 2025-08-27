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

, login_success_overall AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , SOCIAL_PROVIDER AS Source
from segment_events_RAW.consumer_production.social_login_success
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
AND SOCIAL_PROVIDER IN ('google-plus','facebook','apple')

UNION 

SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'email' AS source
from segment_events_RAW.consumer_production.doordash_login_success  
WHERE  convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
 
UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_known' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
 AND type = 'login'
 AND sub_Type = 'bypass_login_wrong_credentials'
 AND bypass_login_category = 'bypass_login_magiclink'

UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_unknown' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
 AND type = 'login'
 AND sub_Type = 'bypass_login_wrong_credentials'
 AND bypass_login_category = 'bypass_login_unknown'


UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_option_known' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
 AND type = 'login'
 AND sub_Type = 'bypass_login_option'
 AND BYPASS_LOGIN_CATEGORY = 'bypass_login_magiclink'
 
UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_option_unknown' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
 AND type = 'login'
 AND sub_Type = 'bypass_login_option'
 AND BYPASS_LOGIN_CATEGORY = 'bypass_login_unknown' 


UNION 

SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'guided_login' AS source
from segment_events_RAW.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
AND sub_type = 'guided_login_v2'
)


, signup_success_overall  AS
( SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , SOCIAL_PROVIDER AS Source
from segment_events_RAW.consumer_production.social_login_new_user 
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
AND SOCIAL_PROVIDER IN ('google-plus','facebook','apple')

UNION 

SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'email' AS source
from segment_events_RAW.consumer_production.doordash_signup_success 
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)

, adjust_links_straight_to_app AS (
  SELECT 
    DISTINCT 
    replace(lower(CASE WHEN context_device_id like 'dx_%' then context_device_id else 'dx_'||context_device_id end), '-') AS app_device_id
    ,convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS day
    ,replace(lower(CASE WHEN split_part(split_part(DEEP_LINK_URL,'dd_device_id=',2),'&',1) like 'dx_%' then split_part(split_part(DEEP_LINK_URL,'dd_device_id=',2),'&',1) else 'dx_'||split_part(split_part(DEEP_LINK_URL,'dd_device_id=',2),'&',1) end), '-') AS mweb_id
  FROM
    iguazu.server_events_production.m_deep_link
  WHERE
    DEEP_LINK_URL like '%device_id%'
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

, adjust_links AS 
(
  SELECT * FROM adjust_links_straight_to_app a 
  UNION ALL 
  SELECT * FROM adjust_link_app_store
)

----- Including both app_device_id and mobile_device_id, some mobile_device_id doesn't have corresponding apple_device_id
, exposure_with_both_ids AS (
  SELECT
    DISTINCT e.*
    , ac.app_device_id
  FROM exposure e
    LEFT JOIN adjust_links ac 
    ON (e.dd_device_ID_filtered = ac.mweb_id 
    OR e.consumer_id = ac.mweb_id)
    AND e.day <= ac.day
)

, orders_data AS 
(
  SELECT 
    DISTINCT a.dd_device_id
    , platform_details
    , replace(lower(CASE WHEN a.DD_device_id like 'dx_%' then a.DD_device_id else 'dx_'||a.DD_device_id end), '-') AS dd_device_ID_filtered
    , a.order_cart_id
    , convert_timezone('UTC','America/Los_Angeles',a.timestamp)::date as day
    , dd.delivery_ID
    , dd.is_first_ordercart_DD
    , dd.is_filtered_core
    , dd.subtotal
    , dd.variable_profit
    , dd.gov
    , dd.created_at AS created_at
  FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  WHERE 
    convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)

, app_orders AS
(
  SELECT * FROM orders_data WHERE startswith(platform_details, 'ios') OR startswith(platform_details, 'android')
)

-- ,mweb_orders AS 
-- (
--   SELECT * FROM orders_data WHERE startswith(platform_details, 'desktop') OR startswith(platform_details, 'mobile-web')
-- )

--------------------- order rate data ---------------------
--- App is joined on app_device_id
, app_checkout AS 
(
  SELECT  
    DISTINCT e.tag AS tag
    , e.segments
    , e.dd_device_id_filtered AS dd_device_ID_filtered
    , o.delivery_id AS delivery_id
    , o.is_first_ordercart_dd AS is_first_ordercart_dd
    , subtotal * 0.01 AS subtotal
    , variable_profit * 0.01 as variable_profit
    , gov * 0.01 as gov
  FROM 
    exposure_with_both_ids e
    LEFT JOIN app_orders o ON e.app_device_id = o.dd_device_ID_filtered AND e.day <= o.day AND convert_timezone('UTC','America/Los_Angeles',o.created_at) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  WHERE 
    TAG != 'reserve'
)


--- mweb is joined on dd_device_ID_filtered
-- , mweb_checkout AS
-- (
--   SELECT  
--     DISTINCT e.tag AS tag
--     , e.dd_device_id_filtered AS dd_device_ID_filtered
--     , o.delivery_id AS delivery_id
--     , o.is_first_ordercart_dd AS is_first_ordercart_dd
--     , subtotal * 0.01 AS subtotal
--     , variable_profit * 0.01 as variable_profit
--     , gov * 0.01 as gov
--   FROM 
--       exposure_with_both_ids e
--       LEFT JOIN mweb_orders o ON e.dd_device_ID_filtered = o.dd_device_ID_filtered AND e.day <= o.day AND convert_timezone('UTC','America/Los_Angeles',o.created_at) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
--   WHERE 
--       TAG != 'reserve'
-- )

,checkout_root AS 
(
SELECT * FROM app_checkout
)


,checkout AS
(
  SELECT
      tag
    , segments
    , COUNT(DISTINCT dd_device_ID_filtered) AS exposure
    , COUNT(DISTINCT delivery_id) AS orders
    , COUNT(DISTINCT CASE WHEN is_first_ordercart_dd = TRUE THEN delivery_id END) AS new_cx
    
    ,orders/exposure AS order_rate
    ,new_cx/exposure AS new_cx_rate
  
    ,SUM(subtotal) AS subtotal
    ,SUM(subtotal)/exposure AS avg_subtotal_per_exposure
    ,SUM(subtotal)/orders AS avg_subtotal_per_order
    ,SUM(variable_profit) AS variable_profit
    ,SUM(variable_profit)/exposure AS avg_vp_per_exposure
    ,SUM(variable_profit)/orders AS avg_vp_per_order
    ,SUM(gov) AS gov
    ,SUM(gov)/exposure AS avg_gov_per_exposure
    ,SUM(gov)/orders AS avg_gov_per_order
    
    -- Statistical variables for p-value calculation
    -- For continuous variables: need std dev and sample size
    ,STDDEV_SAMP(subtotal) AS std_subtotal
    ,STDDEV_SAMP(variable_profit) AS std_variable_profit  
    ,STDDEV_SAMP(gov) AS std_gov
    ,COUNT(delivery_id) AS n_orders_for_stats  -- sample size for continuous vars
    
    -- Rate variables already have numerator/denominator:
    -- order_rate: orders/exposure
    -- new_cx_rate: new_cx/exposure
    
  FROM 
    checkout_root
  GROUP BY 1, 2
)

, res AS (
SELECT c.*
FROM checkout c 
)

SELECT r1.*
        , r1.order_rate / NULLIF(r2.order_rate,0) - 1 AS Lift_order_rate
        , r1.new_cx_rate / NULLIF(r2.new_cx_rate,0) - 1 AS Lift_new_cx_rate 
        , r1.subtotal / NULLIF(r2.subtotal,0) - 1 AS Lift_subtotal
        , r1.avg_subtotal_per_exposure / r2.avg_subtotal_per_exposure - 1 AS Lift_avg_subtotal_per_exposure
        , r1.avg_subtotal_per_order / r2.avg_subtotal_per_order - 1 AS Lift_avg_subtotal_per_order
        , r1.variable_profit / NULLIF(r2.variable_profit,0) - 1 AS Lift_variable_profit
        , r1.avg_vp_per_exposure / r2.avg_vp_per_exposure - 1 AS Lift_avg_vp_per_exposure
        , r1.avg_vp_per_order / r2.avg_vp_per_order - 1 AS Lift_avg_vp_per_order
        , r1.gov / NULLIF(r2.gov,0) - 1 AS Lift_gov
        , r1.avg_gov_per_exposure / r2.avg_gov_per_exposure - 1 AS Lift_avg_gov_per_exposure
        , r1.avg_gov_per_order / r2.avg_gov_per_order - 1 AS Lift_avg_gov_per_order
        
        -- Statistical variables for p-value calculations
        -- Control group statistics (r2)
        , r2.std_subtotal AS control_std_subtotal
        , r2.std_variable_profit AS control_std_variable_profit
        , r2.std_gov AS control_std_gov
        , r2.n_orders_for_stats AS control_n_orders
        , r2.exposure AS control_exposure
        , r2.orders AS control_orders
        , r2.new_cx AS control_new_cx
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
    AND r1.segments = r2.segments
ORDER BY 1, 2 desc