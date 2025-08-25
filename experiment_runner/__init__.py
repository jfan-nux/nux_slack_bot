"""
Experiment Runner - Metadata-driven experiment analysis system

Main entry point for running experiment analysis based on YAML configuration.
"""

from .experiment_config import load_experiment_config
from .query_renderer import render_templates_for_experiment
from .results_parser import parse_results
from .analysis import ExperimentAnalysis
from .metrics_storage import store_metrics

def run_experiment_analysis(experiment_key: str):
    """
    Main execution function for running experiment analysis
    
    Args:
        experiment_key: Key from manual_experiments.yaml (e.g., 'should_pin_leaderboard_carousel')
    
    Returns:
        List of processed ExperimentMetric objects
    """
    
    # 1. Load experiment configuration from YAML
    config = load_experiment_config(experiment_key)
    print(f"Running analysis for experiment: {config['experiment_name']}")
    
    # 2. Render SQL templates based on config
    rendered_queries = render_templates_for_experiment(config)
    print(f"Rendered {len(rendered_queries)} query templates")
    
    # 3. Execute queries and analyze results
    analyzer = ExperimentAnalysis()
    all_metrics = []
    
    for template_name, query_path in rendered_queries.items():
        print(f"Processing template: {template_name}")
        
        # Execute from rendered file (allows manual override)
        from utils.snowflake_connection import execute_snowflake_query
        with open(query_path, 'r') as f:
            query = f.read()
        
        results = execute_snowflake_query(query)
        
        # Parse results into metrics objects
        metrics = parse_results(results, template_name, config)
        
        # Calculate statistical significance for each metric
        for metric in metrics:
            analyzer.calculate_statistics(metric)
            analyzer.apply_statsig_classification(metric)
        
        all_metrics.extend(metrics)
    
    # 4. Store all metrics to database
    store_metrics(all_metrics)
    
    print(f"✓ Analysis complete! Processed {len(all_metrics)} metrics")
    return all_metrics
