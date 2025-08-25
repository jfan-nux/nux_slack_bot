-- Create experiment metrics results table
-- Run this in Snowflake to create the table in your schema

CREATE TABLE IF NOT EXISTS proddb.fionafan.experiment_metrics_results (
    -- Experiment Identifiers  
    experiment_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    version INT,
    
    -- Metric Details
    granularity VARCHAR(20), -- 'consumer_id' or 'device_id'
    template_name VARCHAR(100), -- e.g., 'onboarding_topline', 'app_download_suma'
    metric_name VARCHAR(100), -- e.g., 'order_rate', 'onboarding_completion'
    treatment_arm VARCHAR(50), -- e.g., 'treatment', 'variant_1' (NO control rows)
    metric_type VARCHAR(20), -- 'rate' or 'continuous'
    dimension VARCHAR(50), -- e.g., 'app', 'app_clip', NULL for most metrics
    
    -- Raw Treatment Values
    treatment_numerator FLOAT,
    treatment_denominator FLOAT,
    treatment_value FLOAT,
    treatment_sample_size INT,
    treatment_std FLOAT,
    
    -- Raw Control Values (as columns, not separate rows)
    control_numerator FLOAT,
    control_denominator FLOAT, 
    control_value FLOAT,
    control_sample_size INT,
    control_std FLOAT,
    
    -- Statistical Analysis
    lift FLOAT,
    absolute_difference FLOAT,
    p_value FLOAT,
    confidence_interval_lower FLOAT,
    confidence_interval_upper FLOAT,
    statsig_string VARCHAR(50),
    statistical_power FLOAT,
    
    -- Execution Metadata
    insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    query_execution_timestamp TIMESTAMP,
    query_runtime_seconds FLOAT,
    
    -- Note: Using composite primary key without dimension due to SQL constraints
    -- Unique constraint will handle dimension separately
    PRIMARY KEY (experiment_name, version, granularity, template_name, metric_name, treatment_arm)
);
