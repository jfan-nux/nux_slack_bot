-- Combined experiment metrics from both Curie and Mode sources
-- This query unions results from Curie analysis with existing experiment_metrics_results

WITH curie_results AS (
    -- Curie source data - using the existing get_curie_metrics.sql logic
    WITH latest_results AS (
        -- Get all results and identify the latest dimension_value for each metric/dimension combination
        SELECT
            dear.*,  -- Select all columns from dimension_experiment_analysis_results
            tm.desired_direction AS metric_desired_direction,
            tm.description AS metric_description,
            -- Extract base dimension value (before any parentheses) for deduplication
            CASE 
                WHEN dear.dimension_value IS NULL THEN NULL
                WHEN POSITION('(' IN dear.dimension_value) > 0 
                THEN TRIM(SUBSTRING(dear.dimension_value, 1, POSITION('(' IN dear.dimension_value) - 1))
                ELSE dear.dimension_value
            END AS base_dimension_value,
            -- Rank to get latest result for each metric/dimension/base_dimension_value combination
            ROW_NUMBER() OVER (
                PARTITION BY 
                    dear.experiment_name,
                    dear.analysis_name,
                    dear.metric_name,
                    dear.dimension_name,
                    CASE 
                        WHEN dear.dimension_value IS NULL THEN NULL
                        WHEN POSITION('(' IN dear.dimension_value) > 0 
                        THEN TRIM(SUBSTRING(dear.dimension_value, 1, POSITION('(' IN dear.dimension_value) - 1))
                        ELSE dear.dimension_value
                    END,
                    dear.variant_name
                ORDER BY dear.analyzed_at DESC
            ) AS rn,
        CASE 
            WHEN p_value IS NULL THEN 'unknown'
            WHEN p_value < 0.05 THEN 
                CASE
                    WHEN (metric_desired_direction = 'METRIC_DIRECTIONALITY_INCREASE' AND metric_impact_relative > 0) 
                      OR (metric_desired_direction = 'METRIC_DIRECTIONALITY_DECREASE' AND metric_impact_relative < 0) 
                    THEN 'significant positive'
                    ELSE 'significant negative'
                END
            WHEN p_value < 0.25 THEN 
                CASE
                    WHEN (metric_desired_direction = 'METRIC_DIRECTIONALITY_INCREASE' AND metric_impact_relative > 0) 
                      OR (metric_desired_direction = 'METRIC_DIRECTIONALITY_DECREASE' AND metric_impact_relative < 0) 
                    THEN 'directional positive'
                    ELSE 'directional negative'
                END
            ELSE 'flat'
        END AS stat_sig
        FROM
            proddb.public.dimension_experiment_analysis_results dear
            LEFT JOIN CONFIGURATOR_PROD.PUBLIC.TALLEYRAND_METRICS tm ON dear.metric_name = tm.name
        WHERE
            dear.experiment_name IN ({experiment_name_list})
            AND dear.metric_name IS NOT NULL
            AND dear.dimension_name is null
    ),
    control_data AS (
        SELECT
            experiment_name,
            analysis_id,
            analysis_name,
            metric_name,
            metric_desired_direction,
            metric_description,
            analyzed_at,
            -- Control columns (requested)
            metric_value AS control_metric_value,
            exposures AS control_exposures
        FROM latest_results
        WHERE rn = 1 AND LOWER(variant_name) = 'control'
    ),
    treatment_data AS (
        SELECT
            experiment_name,
            analysis_id,
            analysis_name,
            metric_name,
            metric_desired_direction,
            metric_description,
            analyzed_at,
            variant_name AS treatment_variant_name,
            -- Treatment arm columns
            metric_value AS treatment_metric_value,
            exposures AS treatment_exposures,
            metric_impact_relative AS treatment_metric_impact_relative,
            metric_impact_relative_lower_bound AS treatment_impact_relative_lower,
            metric_impact_relative_upper_bound AS treatment_impact_relative_upper,
            metric_impact_relative_global_lift AS treatment_relative_global_lift,
            metric_impact_absolute AS treatment_metric_impact_absolute,
            metric_impact_absolute_lower_bound AS treatment_impact_absolute_lower,
            metric_impact_absolute_upper_bound AS treatment_impact_absolute_upper,
            metric_impact_absolute_global_lift AS treatment_absolute_global_lift,
            p_value AS treatment_p_value,
            label AS treatment_label,
            metric_id AS treatment_metric_id,
            desired_direction AS treatment_desired_direction,
            stat_sig AS treatment_stat_sig
        FROM latest_results
        WHERE rn = 1 AND LOWER(variant_name) != 'control'
    )
    SELECT 
        'curie' AS source,
        t.experiment_name,
        t.metric_name,
        t.metric_desired_direction AS desired_direction,
        t.treatment_variant_name AS treatment_arm,
        -- Derive segments from analysis_name for Curie results
        LOWER(CASE 
            WHEN LOWER(t.analysis_name) LIKE '%ios%' THEN 'ios'
            WHEN LOWER(t.analysis_name) LIKE '%android%' THEN 'android'
            ELSE NULL
        END) AS segments,
        c.control_metric_value AS control_value,
        t.treatment_metric_value AS treatment_value,
        c.control_exposures AS control_exposure,
        t.treatment_exposures AS treatment_exposure,
        t.treatment_metric_impact_relative AS lift,
        t.treatment_p_value AS p_value,
        t.treatment_stat_sig AS statsig_string,
        t.analyzed_at AS analysis_timestamp,
        t.analysis_name,
        -- created_at: for curie, use analyzed_at
        t.analyzed_at AS created_at,
        t.treatment_impact_relative_lower AS confidence_interval_lower,
        t.treatment_impact_relative_upper AS confidence_interval_upper,
        t.treatment_label AS label,
        -- Map metrics to template_rank based on metrics_metadata.yaml
        CASE 
            WHEN t.metric_name IN ('order_rate', 'new_cx_rate', 'vp_per_device', 'vp', 'gov_per_device', 'gov', 'mau_rate', 'total_vp', 'total_gov') THEN 2  -- onboarding topline
            WHEN t.metric_name IN ('explore_rate', 'store_rate', 'cart_rate', 'checkout_rate') THEN 1  -- onboarding overall_funnel
            WHEN t.metric_name IN ('dashpass_trial_signup_rate') THEN 3  -- onboarding dashpass
            WHEN t.metric_name IN ('system_level_push_opt_in_pct', 'system_level_push_opt_out_pct') THEN 4  -- onboarding system_level_opt_in
            WHEN t.metric_name IN ('start_page_view_rate', 'start_page_click_rate', 'notification_view_rate', 'notification_click_rate', 'marketing_sms_view_rate', 'marketing_sms_click_rate', 'att_view_rate', 'att_click_rate', 'end_page_view_rate', 'end_page_click_rate', 'onboarding_completion') THEN 5  -- onboarding onboarding_funnel
            ELSE NULL
        END AS template_rank,
        -- Map metrics to metric_rank based on metrics_metadata.yaml  
        CASE 
            WHEN t.metric_name = 'order_rate' THEN 1
            WHEN t.metric_name = 'new_cx_rate' THEN 2
            WHEN t.metric_name = 'vp_per_device' THEN 3
            WHEN t.metric_name = 'vp' THEN 4
            WHEN t.metric_name = 'gov_per_device' THEN 5
            WHEN t.metric_name = 'gov' THEN 6
            WHEN t.metric_name = 'mau_rate' THEN 7
            WHEN t.metric_name = 'total_vp' THEN 8
            WHEN t.metric_name = 'total_gov' THEN 9
            WHEN t.metric_name = 'explore_rate' THEN 1
            WHEN t.metric_name = 'store_rate' THEN 2
            WHEN t.metric_name = 'cart_rate' THEN 3
            WHEN t.metric_name = 'checkout_rate' THEN 4
            WHEN t.metric_name = 'dashpass_trial_signup_rate' THEN 1
            WHEN t.metric_name = 'system_level_push_opt_in_pct' THEN 1
            WHEN t.metric_name = 'system_level_push_opt_out_pct' THEN 2
            WHEN t.metric_name = 'start_page_view_rate' THEN 1
            WHEN t.metric_name = 'start_page_click_rate' THEN 2
            WHEN t.metric_name = 'notification_view_rate' THEN 3
            WHEN t.metric_name = 'notification_click_rate' THEN 4
            WHEN t.metric_name = 'marketing_sms_view_rate' THEN 5
            WHEN t.metric_name = 'marketing_sms_click_rate' THEN 6
            WHEN t.metric_name = 'att_view_rate' THEN 7
            WHEN t.metric_name = 'att_click_rate' THEN 8
            WHEN t.metric_name = 'end_page_view_rate' THEN 9
            WHEN t.metric_name = 'end_page_click_rate' THEN 10
            WHEN t.metric_name = 'onboarding_completion' THEN 11
            ELSE NULL
        END AS metric_rank
    FROM treatment_data t
    LEFT JOIN control_data c ON (
        t.experiment_name = c.experiment_name
        AND t.analysis_id = c.analysis_id
        AND t.analysis_name = c.analysis_name
        AND t.metric_name = c.metric_name
    )
),
mode_results AS (
    SELECT 
        'mode' AS source,
        experiment_name,
        metric_name,
        desired_direction,
        treatment_arm,
        segments,
        control_value,
        treatment_value,
        control_sample_size AS control_exposure,
        treatment_sample_size AS treatment_exposure,
        lift,
        p_value,
        statsig_string,
        insert_timestamp AS analysis_timestamp,
        template_name AS analysis_name,
        -- created_at: use insert_timestamp to match available schema
        insert_timestamp AS created_at,
        confidence_interval_lower,
        confidence_interval_upper,
        NULL AS label,
        template_rank,
        metric_rank
    FROM proddb.fionafan.experiment_metrics_results 
    WHERE insert_timestamp = (
        SELECT MAX(insert_timestamp) 
        FROM proddb.fionafan.experiment_metrics_results
    )
)

-- Combine both sources
SELECT 
    source,
    experiment_name,
    metric_name,
    desired_direction,
    treatment_arm,
    segments,
    control_value,
    treatment_value,
    control_exposure,
    treatment_exposure,
    lift,
    p_value,
    statsig_string,
    analysis_timestamp,
    analysis_name,
    created_at,
    confidence_interval_lower,
    confidence_interval_upper,
    label,
    template_rank,
    metric_rank
FROM curie_results

UNION ALL

SELECT 
    source,
    experiment_name,
    metric_name,
    desired_direction,
    treatment_arm,
    segments,
    control_value,
    treatment_value,
    control_exposure,
    treatment_exposure,
    lift,
    p_value,
    statsig_string,
    analysis_timestamp,
    analysis_name,
    created_at,
    confidence_interval_lower,
    confidence_interval_upper,
    label,
    template_rank,
    metric_rank
FROM mode_results;
