"""
Metrics Storage - Store experiment metrics to database
"""

from typing import List
from datetime import datetime
from .results_parser import ExperimentMetric

def store_metrics(metrics: List[ExperimentMetric]):
    """
    Store a batch of experiment metrics to the experiment_metrics_results table
    
    Args:
        metrics: List of ExperimentMetric objects to store
    """
    
    if not metrics:
        return
    
    from utils.snowflake_connection import execute_snowflake_query
    
    # Prepare insert statement
    insert_query = """
    INSERT INTO proddb.fionafan.experiment_metrics_results (
        experiment_name, start_date, end_date, version,
        granularity, template_name, metric_name, treatment_arm, metric_type, dimension, template_rank, metric_rank, desired_direction,
        treatment_numerator, treatment_denominator, treatment_value, treatment_sample_size, treatment_std,
        control_numerator, control_denominator, control_value, control_sample_size, control_std,
        lift, absolute_difference, p_value, confidence_interval_lower, confidence_interval_upper,
        statsig_string, statistical_power, query_execution_timestamp, query_runtime_seconds
    )
    VALUES {}
    """
    
    # Build values for batch insert
    values = []
    current_timestamp = datetime.now().isoformat()
    
    # Handle None and NaN values appropriately for SQL
    def safe_value(val):
        import pandas as pd
        import numpy as np
        
        if val is None or pd.isna(val) or (isinstance(val, float) and np.isnan(val)):
            return 'NULL'
        elif isinstance(val, str):
            # Escape single quotes for SQL
            escaped_val = val.replace("'", "''")
            return f"'{escaped_val}'"
        else:
            return str(val)
    
    for metric in metrics:
        
        value_tuple = f"""(
            {safe_value(metric.experiment_name)},
            {safe_value(metric.start_date)},
            {safe_value(metric.end_date)},
            {safe_value(metric.version)},
            {safe_value(metric.granularity)},
            {safe_value(metric.template_name)},
            {safe_value(metric.metric_name)},
            {safe_value(metric.treatment_arm)},
            {safe_value(metric.metric_type)},
            {safe_value(metric.dimension)},
            {safe_value(metric.template_rank)},
            {safe_value(metric.metric_rank)},
            {safe_value(metric.desired_direction)},
            {safe_value(metric.treatment_numerator)},
            {safe_value(metric.treatment_denominator)},
            {safe_value(metric.treatment_value)},
            {safe_value(metric.treatment_sample_size)},
            {safe_value(metric.treatment_std)},
            {safe_value(metric.control_numerator)},
            {safe_value(metric.control_denominator)},
            {safe_value(metric.control_value)},
            {safe_value(metric.control_sample_size)},
            {safe_value(metric.control_std)},
            {safe_value(metric.lift)},
            {safe_value(metric.absolute_difference)},
            {safe_value(metric.p_value)},
            {safe_value(metric.confidence_interval_lower)},
            {safe_value(metric.confidence_interval_upper)},
            {safe_value(metric.statsig_string)},
            {safe_value(metric.statistical_power)},
            {safe_value(current_timestamp)},
            {safe_value(metric.query_runtime_seconds)}
        )"""
        
        values.append(value_tuple)
    
    # Execute batch insert
    final_query = insert_query.format(','.join(values))
    
    try:
        # First check if table exists
        from utils.snowflake_connection import SnowflakeHook
        
        with SnowflakeHook() as hook:
            table_check_query = """
            SELECT COUNT(*) as table_exists 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE UPPER(TABLE_SCHEMA) = 'FIONAFAN' 
            AND UPPER(TABLE_NAME) = 'EXPERIMENT_METRICS_RESULTS'
            """
            
            result = hook.query_snowflake(table_check_query)
            table_exists = result.iloc[0, 0] > 0
            
            if not table_exists:
                print("Table does not exist, creating it...")
                create_metrics_table()  # Use the existing function
            
            # Now insert the data using query_without_result since INSERT doesn't return data
            hook.query_without_result(final_query)
            
        print(f"✓ Successfully stored {len(metrics)} metrics to database")
    except Exception as e:
        print(f"✗ Error storing metrics to database: {e}")
        # Could implement retry logic or fallback storage here
        raise

def create_metrics_table():
    """
    Create the experiment_metrics_results table if it doesn't exist
    """
    
    from utils.snowflake_connection import SnowflakeHook
    
    create_table_sql = """
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
        template_rank INT, -- From metrics_metadata.yaml
        metric_rank INT, -- From metrics_metadata.yaml
        desired_direction VARCHAR(20), -- From metrics_metadata.yaml
        
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
    )
    """
    
    try:
        with SnowflakeHook() as hook:
            hook.query_without_result(create_table_sql)
            # Grant SELECT permissions to PUBLIC for read-only access
            grant_sql = "GRANT SELECT ON TABLE proddb.fionafan.experiment_metrics_results TO ROLE PUBLIC;"
            hook.query_without_result(grant_sql)
        print("✓ Created/verified experiment_metrics_results table with PUBLIC read access")
    except Exception as e:
        print(f"✗ Error creating table: {e}")
        raise
