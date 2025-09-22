--------------------- experiment exposure

WITH exposure AS
(SELECT  ee.tag
               , ee.result
               , ee.bucket_key
               , LOWER(ee.segment) AS segments
               , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'special_menu_default_selection'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-09-15' AND '2025-10-30'
GROUP BY 1,2,3,4,5
)

, explore_page AS
(SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS day
       , iguazu_user_id as user_id
from IGUAZU.SERVER_EVENTS_PRODUCTION.M_STORE_CONTENT_PAGE_LOAD
WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN '2025-09-15' AND '2025-10-30'
)

, dp_subs AS (
    SELECT DISTINCT 
        dsa.consumer_id,
        dsa.user_ID,
        SUM(CASE
            WHEN is_partner_plan = FALSE 
                AND b.consumer_subscription_plan_id IS NULL                   
                AND is_in_intraday_trial_balance = TRUE
                AND is_new_subscription_date = TRUE
            THEN 1 ELSE 0
        END) AS dashpass_monthly_trial_signup,
        SUM(CASE
            WHEN is_partner_plan = FALSE 
                AND b.consumer_subscription_plan_id IS NOT NULL                   
                AND is_in_intraday_trial_balance = TRUE
                AND is_new_subscription_date = TRUE
            THEN 1 ELSE 0
        END) AS dashpass_annual_trial_signup,
        SUM(CASE
            WHEN is_partner_plan = FALSE
                AND is_in_intraday_pay_balance = TRUE
                AND is_new_paying_subscription_date = TRUE
                AND is_direct_to_pay_date = TRUE
                AND billing_period IS NOT NULL
                AND b.consumer_subscription_plan_id IS NULL
            THEN 1 ELSE 0
        END) AS dashpass_monthly_dtp_signup,
        SUM(CASE
            WHEN is_partner_plan = FALSE
                AND is_in_intraday_pay_balance = TRUE
                AND is_new_paying_subscription_date = TRUE
                AND is_direct_to_pay_date = TRUE
                AND billing_period IS NOT NULL
                AND b.consumer_subscription_plan_id IS NOT NULL
            THEN 1 ELSE 0
        END) AS dashpass_annual_dtp_signup,
        SUM(CASE
            WHEN is_partner_plan = TRUE                   
                AND is_in_intraday_trial_balance = TRUE
                AND is_new_subscription_date = TRUE
            THEN 1 ELSE 0
        END) AS dashpass_partner_trial_signup,
        SUM(CASE
            WHEN is_partner_plan = TRUE
                AND is_in_intraday_pay_balance = TRUE
                AND is_new_paying_subscription_date = TRUE
                AND is_direct_to_pay_date = TRUE
                AND billing_period IS NOT NULL
            THEN 1 ELSE 0
        END) AS dashpass_partner_dtp_signup
    FROM edw.consumer.fact_consumer_subscription__daily dsa
    LEFT JOIN proddb.static.dashpass_annual_plan_ids b
        ON dsa.consumer_subscription_plan_id = b.consumer_subscription_plan_id
    WHERE dsa.dte BETWEEN '2025-09-15' AND '2025-10-30'
    GROUP BY 
        1,2
)

, dp AS (
SELECT DISTINCT 
    e.tag,
    e.segments,
    e.dd_device_id_filtered, 
    cl.user_ID,
    subs.dashpass_monthly_trial_signup,
    subs.dashpass_annual_trial_signup,
    subs.dashpass_monthly_trial_signup + subs.dashpass_annual_trial_signup AS dashpass_trial_signup
FROM exposure e
JOIN explore_page cl
    ON e.dd_device_ID_filtered = cl.dd_device_ID_filtered 
    AND e.day <= cl.day
LEFT JOIN dp_subs subs
    ON try_to_number(cl.user_ID) = try_to_number(subs.consumer_ID)
)

, DP_trial_res AS
(SELECT tag
        , segments
        , COUNT(DISTINCT dd_device_id_filtered) AS exposure
        , SUM(dashpass_trial_signup) dashpass_trial_signup
        , SUM(dashpass_trial_signup)/count(distinct dd_device_id_filtered) dashpass_trial_signup_rate
FROM dp
GROUP BY 1, 2
)

, res AS
(SELECT dp.*
FROM DP_trial_res dp
ORDER BY 1
)

SELECT r1.tag 
        , r1.segments
        , r1.exposure
        , r1.dashpass_trial_signup
        , r1.dashpass_trial_signup_rate
        , r1.dashpass_trial_signup_rate / nullif(r2.dashpass_trial_signup_rate,0) - 1 AS Lift_dashpass_trial_signup_rate

        -- Statistical variables for p-value calculations
        -- Control group statistics (r2) for rate variables
        , r2.exposure AS control_exposure
        , r2.dashpass_trial_signup AS control_dashpass_trial_signup
        , r2.dashpass_trial_signup_rate AS control_dashpass_trial_signup_rate

FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
    AND r1.segments = r2.segments
ORDER BY 1, 2 desc