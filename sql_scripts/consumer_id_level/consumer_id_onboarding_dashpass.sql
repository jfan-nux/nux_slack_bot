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
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY all
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
    WHERE dsa.dte BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    GROUP BY 
        all
)

, dp AS (
SELECT DISTINCT 
    e.tag,
    e.bucket_key, 
    subs.dashpass_monthly_trial_signup,
    subs.dashpass_annual_trial_signup,
    subs.dashpass_monthly_trial_signup + subs.dashpass_annual_trial_signup AS dashpass_trial_signup
FROM exposure e
LEFT JOIN dp_subs subs
    ON try_to_number(e.bucket_key) = try_to_number(subs.consumer_ID)
)

, DP_trial_res AS
(SELECT tag
        , COUNT(DISTINCT bucket_key) AS exposure
        , SUM(dashpass_trial_signup) dashpass_trial_signup
        , SUM(dashpass_trial_signup)/count(distinct bucket_key) dashpass_trial_signup_rate
FROM dp
GROUP BY all
)

, res AS
(SELECT dp.*
FROM DP_trial_res dp
ORDER BY 1
)

SELECT r1.tag 
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
ORDER BY 1 desc