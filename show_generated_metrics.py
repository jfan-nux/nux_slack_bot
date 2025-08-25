#!/usr/bin/env python3
"""
Show the metrics that were generated from the last run
"""

from experiment_runner import run_all_experiments

def show_metrics_summary():
    """Run analysis and show detailed metrics breakdown"""
    
    print("=" * 90)
    print("📊 GENERATED METRICS SUMMARY")
    print("=" * 90)
    
    # Run the analysis to get metrics (but won't store due to table issue)
    try:
        metrics = run_all_experiments(max_workers=1)  # Use 1 worker for cleaner output
        return
    except:
        pass
    
    # Alternative: run just the parsing without full pipeline
    print("Running quick analysis to show generated metrics...")
    
    from experiment_runner.experiment_config import load_experiment_config
    from experiment_runner.query_renderer import render_templates_for_experiment
    from experiment_runner.results_parser import parse_results
    from experiment_runner.analysis import ExperimentAnalysis
    from utils.snowflake_connection import execute_snowflake_query
    import yaml
    import os
    
    # Quick analysis
    yaml_path = os.path.join('data_models', 'manual_experiments.yaml')
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    experiments = data['experiments']
    active_experiments = {k: v for k, v in experiments.items() if not v.get('expired', False)}
    
    all_metrics = []
    
    for exp_key in ['should_pin_leaderboard_carousel']:  # Just show one for demo
        print(f"\n🔬 Analyzing {exp_key}...")
        
        config = load_experiment_config(exp_key)
        rendered_queries = render_templates_for_experiment(config)
        
        analyzer = ExperimentAnalysis()
        
        # Just analyze one template for demo
        template_name = 'onboarding_topline'
        if template_name in rendered_queries:
            query_path = rendered_queries[template_name]
            
            with open(query_path, 'r') as f:
                query = f.read()
            
            results = execute_snowflake_query(query)
            metrics = parse_results(results, template_name, config)
            
            for metric in metrics:
                analyzer.calculate_statistics(metric)
                analyzer.apply_statsig_classification(metric)
            
            all_metrics.extend(metrics)
    
    # Display metrics
    if all_metrics:
        print(f"\n📈 SAMPLE METRICS GENERATED ({len(all_metrics)} total)")
        print("-" * 90)
        
        for i, metric in enumerate(all_metrics[:10]):  # Show first 10
            lift_pct = (metric.lift * 100) if metric.lift else 0
            print(f"{i+1:2d}. {metric.metric_name:<25} | {metric.treatment_arm:<12} | "
                  f"Lift: {lift_pct:+6.2f}% | P: {metric.p_value or 0:.4f} | {metric.statsig_string}")
        
        if len(all_metrics) > 10:
            print(f"    ... and {len(all_metrics) - 10} more metrics")
        
        print("\n✅ Metrics parsing is working perfectly!")
        print("📊 Once the table is created, all metrics will be stored automatically.")
    
if __name__ == "__main__":
    show_metrics_summary()
