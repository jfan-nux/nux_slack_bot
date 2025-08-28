-- drop table if exists proddb.fionafan.combined_experiment_metrics ;

select distinct experiment_name,template_name from proddb.fionafan.experiment_metrics_results 
where insert_timestamp = (select max(insert_timestamp) from proddb.fionafan.experiment_metrics_results) 
and segments is null
;

select distinct experiment_name,template_name from proddb.fionafan.experiment_metrics_results 
where insert_timestamp = (select max(insert_timestamp) from proddb.fionafan.experiment_metrics_results) 
and segments is  not null
;
select * from proddb.fionafan.experiment_metrics_results 
where insert_timestamp = (select max(insert_timestamp) from proddb.fionafan.experiment_metrics_results) ;
-- and statsig_string = 'insufficient_data';
select * from proddb.fionafan.combined_experiment_metrics
where created_at = (select max(created_at) from proddb.fionafan.combined_experiment_metrics) ;
;
select experiment_name,
template_name, metric_name, treatment_value, control_value, lift, p_value, statsig_string, desired_direction,
from proddb.fionafan.experiment_metrics_results 
where insert_timestamp = (select max(insert_timestamp) from proddb.fionafan.experiment_metrics_results) 
and experiment_name = 'should_pin_leaderboard_carousel'
-- and treatment_value = control_value
-- and template_name = 'app_download'
order by experiment_name, template_rank, metric_rank
-- and template_rank is null or metric_rank is null
;

GRANT SELECT ON TABLE proddb.fionafan.experiment_metrics_results TO ROLE PUBLIC;

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
                -- dear.analysis_name,
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
        dear.experiment_name IN ('should_pin_leaderboard_carousel','cx_mobile_onboarding_preferences' ) --{experiment_name_list},'should_pin_leaderboard_carousel','cx_mobile_onboarding_preferences' 
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


SELECT  date_trunc(day, convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME)) as day, custom_attributes:platform, count(distinct bucket_key)
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'should_pin_leaderboard_carousel'--'leaderboard_customer_favorites_carousel_v3'
AND experiment_version::INT = 2
-- AND segment IN ('Users')
AND tag <> 'overridden'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-25' AND '2025-09-30'
GROUP BY all
order by all
;


with base as (SELECT  custom_attributes:platform as platform, bucket_key as consumer_ID
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'should_pin_leaderboard_carousel'--'leaderboard_customer_favorites_carousel_v3'
AND experiment_version::INT = 1
-- AND segment IN ('Users')
AND tag <> 'overridden'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-21' AND '2025-09-30'
GROUP BY all)

, newbase as (select a.platform
, count(distinct b.consumer_ID) as suma_users
, count(distinct a.consumer_ID) as enrolled_users
from base a left join edw.consumer.suma_consumers b on a.consumer_ID = b.consumer_ID 
group by all)
select *, suma_users/enrolled_users as suma_rate from newbase;


with base as (SELECT  custom_attributes:platform as platform, bucket_key as consumer_ID, tag
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'should_pin_leaderboard_carousel'--'leaderboard_customer_favorites_carousel_v3'
AND experiment_version::INT = 1
-- AND segment IN ('Users')
AND tag not in ('control','overridden')
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-21' AND '2025-09-30'
GROUP BY all)
, suma_base as (
select consumer_ID, count(distinct platform) as platform_count 
from base 
group by all having platform_count > 1
) 

, newbase as (select a.platform, case when b.consumer_ID is not null then 'suma' else 'non_suma' end as suma_flag
, count(1) as count 
from base a left join suma_base b on a.consumer_ID = b.consumer_ID group by all)
select *, sum(count) over (partition by platform) as total_count
, count/sum(count) over (partition by platform) as percentage from newbase
;





;

WITH daily_platform_counts AS (
    SELECT  
        date_trunc(day, convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME)) as day, 
        custom_attributes:platform::varchar as platform,
        count(distinct bucket_key) as exposures
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE experiment_name = 'should_pin_leaderboard_carousel'
    AND experiment_version::INT = 1
    AND tag <> 'overridden'
    AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-21' AND '2025-09-30'
    GROUP BY 1, 2
),

daily_totals AS (
    SELECT 
        day,
        sum(exposures) as total_exposures
    FROM daily_platform_counts
    GROUP BY day
),

platform_percentages AS (
    SELECT 
        dpc.day,
        dpc.platform,
        dpc.exposures,
        dt.total_exposures,
        ROUND(100.0 * dpc.exposures / dt.total_exposures, 2) as platform_pct
    FROM daily_platform_counts dpc
    JOIN daily_totals dt ON dpc.day = dt.day
)

SELECT 
    day,
    total_exposures,
    MAX(CASE WHEN UPPER(platform) = 'IOS' THEN platform_pct END) as ios_pct,
    MAX(CASE WHEN UPPER(platform) = 'ANDROID' THEN platform_pct END) as android_pct,
    MAX(CASE WHEN UPPER(platform) = 'IOS' THEN exposures END) as ios_exposures,
    MAX(CASE WHEN UPPER(platform) = 'ANDROID' THEN exposures END) as android_exposures
FROM platform_percentages
GROUP BY day, total_exposures
ORDER BY day;





select distinct experiment_name, segment, tag
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE 1=1 
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-24' AND '2025-09-30'
AND experiment_name like 'cx_mobile_onboarding_preferences%'--'leaderboard_customer_favorites_carousel_v3'
AND experiment_version::INT = 1
-- AND segment IN ('Users')
AND tag <> 'overridden'
GROUP BY all