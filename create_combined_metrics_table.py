#!/usr/bin/env python3
"""
Script to create and populate the combined experiment metrics table.
Reads experiments from metadata, renders SQL template, creates table, and inserts data.
"""

import yaml
import os
from datetime import datetime
from pathlib import Path
try:
    # Python 3.9+
    from importlib.resources import files
except ImportError:
    # Python 3.8 fallback
    from importlib_resources import files

from utils.snowflake_connection import SnowflakeHook
from utils.logger import get_logger

logger = get_logger(__name__)

def load_experiments():
    """Load experiment names from the metadata file."""
    try:
        # Try package resource access first
        package_files = files("nux_slack_bot.data_models")
        metadata_file = package_files / "manual_experiments.yaml"
        with metadata_file.open('r') as f:
            data = yaml.safe_load(f)
    except (ImportError, FileNotFoundError, ModuleNotFoundError):
        # Fallback to relative path (for development)
        metadata_path = Path(__file__).parent / "data_models" / "manual_experiments.yaml"
        if not metadata_path.exists():
            # Last resort: current directory relative path
            metadata_path = Path("data_models/manual_experiments.yaml")
        
        with open(metadata_path, 'r') as f:
            data = yaml.safe_load(f)
    
    # Extract experiment names that are not expired
    experiments = []
    for exp_name, exp_data in data['experiments'].items():
        if not exp_data.get('expired', False):
            experiments.append(exp_name)
    
    logger.info(f"Found {len(experiments)} active experiments: {experiments}")
    return experiments

def render_sql_template(experiments):
    """Render the SQL template with the list of experiments."""
    
    try:
        # Try package resource access first
        package_files = files("nux_slack_bot.sql_scripts")
        template_file = package_files / "combined_experiment_metrics.sql"
        with template_file.open('r') as f:
            template = f.read()
    except (ImportError, FileNotFoundError, ModuleNotFoundError):
        # Fallback to relative path (for development)
        template_path = Path(__file__).parent / "sql_scripts" / "combined_experiment_metrics.sql"
        if not template_path.exists():
            # Last resort: current directory relative path
            template_path = Path("sql_scripts/combined_experiment_metrics.sql")
        
        with open(template_path, 'r') as f:
            template = f.read()
    
    # Format experiment names for SQL IN clause
    experiment_list = "'" + "','".join(experiments) + "'"
    
    # Replace the placeholder
    rendered_sql = template.replace('{experiment_name_list}', experiment_list)
    
    return rendered_sql

def create_table_sql():
    """Generate CREATE TABLE IF NOT EXISTS SQL."""
    return """
CREATE TABLE IF NOT EXISTS proddb.fionafan.combined_experiment_metrics (
    source VARCHAR(50) NOT NULL,
    experiment_name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    desired_direction VARCHAR(50),
    treatment_arm VARCHAR(100),
    segments VARCHAR(50),
    control_value FLOAT,
    treatment_value FLOAT,
    control_exposure INTEGER,
    treatment_exposure INTEGER,
    lift FLOAT,
    p_value FLOAT,
    statsig_string VARCHAR(50),
    analysis_timestamp TIMESTAMP_NTZ,
    analysis_name VARCHAR(255),
    created_at TIMESTAMP_NTZ,
    confidence_interval_lower FLOAT,
    confidence_interval_upper FLOAT,
    label VARCHAR(255),
    template_rank INTEGER,
    metric_rank INTEGER,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""





def main():
    """Main execution function."""
    logger.info("Starting combined experiment metrics table creation...")
    
    try:
        # Load experiments from metadata
        experiments = load_experiments()
        
        if not experiments:
            logger.error("No active experiments found in metadata")
            return
        
        # Render SQL template
        logger.info("Rendering SQL template...")
        rendered_query = render_sql_template(experiments)
        
        # Generate SQL statements
        create_sql = create_table_sql()
        
        # Connect to Snowflake using SnowflakeHook
        logger.info("Connecting to Snowflake...")
        with SnowflakeHook() as hook:
            # Execute CREATE TABLE
            logger.info("Creating table...")
            hook.query_without_result(create_sql)
            logger.info("✓ Table created successfully")
            
            # Execute GRANT permissions (split into individual statements)
            logger.info("Granting permissions...")
            grant_statements = [

                "GRANT SELECT ON proddb.fionafan.combined_experiment_metrics TO ROLE PUBLIC"
            ]
            for stmt in grant_statements:
                try:
                    hook.query_without_result(stmt)
                    logger.info(f"✓ Granted: {stmt}")
                except Exception as e:
                    logger.warning(f"Could not grant permission (this is often OK): {e}")
            
            
            logger.info("Inserting data...")
            insert_query = f"""
            INSERT INTO proddb.fionafan.combined_experiment_metrics (
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
            )
            {rendered_query.rstrip(';')}
            """
            hook.query_without_result(insert_query)
            
            # Verify results
            count_df = hook.query_snowflake("SELECT COUNT(*) as count FROM proddb.fionafan.combined_experiment_metrics")
            count = count_df['count'].iloc[0]
            
            logger.info(f"✓ Successfully created and populated table with {count} rows")
            
            # Show sample data
            sample_df = hook.query_snowflake("""
                SELECT source, experiment_name, metric_name, statsig_string, label, template_rank, metric_rank, created_at
                FROM proddb.fionafan.combined_experiment_metrics 
                ORDER BY experiment_name, source, metric_name 
                LIMIT 10
            """)
            
            if not sample_df.empty:
                logger.info("Sample data:")
                for _, row in sample_df.iterrows():
                    logger.info(f"  {row['source']} | {row['experiment_name']} | {row['metric_name']} | {row['statsig_string']} | label: {row['label']} | template_rank: {row['template_rank']} | metric_rank: {row['metric_rank']}")
            
        logger.info("Combined experiment metrics table creation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating combined experiment metrics table: {e}")
        raise

if __name__ == "__main__":
    main()
