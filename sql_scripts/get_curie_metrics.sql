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
                -- Group by platform when ios/android in analysis_name, otherwise by full analysis_name
                CASE 
                    WHEN LOWER(dear.analysis_name) LIKE '%ios%' THEN 'ios'
                    WHEN LOWER(dear.analysis_name) LIKE '%android%' THEN 'android'
                    ELSE dear.analysis_name
                END,
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
        dear.experiment_name IN ({experiment_name_list}) --,'should_pin_leaderboard_carousel','cx_mobile_onboarding_preferences' 
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
    t.experiment_name,
    t.analysis_id,
    t.analysis_name,
    t.metric_name,
    t.metric_description,
    t.metric_desired_direction,
    t.analyzed_at,
    t.treatment_variant_name,
    -- Control columns (requested)
    c.control_metric_value,
    c.control_exposures,
    -- Treatment columns  
    t.treatment_metric_value,
    t.treatment_exposures,
    t.treatment_metric_impact_relative,
    t.treatment_impact_relative_lower,
    t.treatment_impact_relative_upper,
    t.treatment_relative_global_lift,
    t.treatment_metric_impact_absolute,
    t.treatment_impact_absolute_lower,
    t.treatment_impact_absolute_upper,
    t.treatment_absolute_global_lift,
    t.treatment_p_value,
    t.treatment_label,
    t.treatment_metric_id,
    t.treatment_desired_direction,
    t.treatment_stat_sig
FROM treatment_data t
LEFT JOIN control_data c ON (
    t.experiment_name = c.experiment_name
    AND t.analysis_id = c.analysis_id
    AND t.analysis_name = c.analysis_name
    AND t.metric_name = c.metric_name
)
ORDER BY t.analysis_name, t.metric_name;