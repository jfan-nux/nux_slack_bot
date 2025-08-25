#!/usr/bin/env python3
"""
Demo script for the Experiment Runner system

This shows how to use the metadata-driven experiment analysis system.
"""

from experiment_runner import run_experiment_analysis
from experiment_runner.metrics_storage import create_metrics_table

def main():
    """Demo the experiment runner"""
    
    print("=" * 50)
    print("Experiment Runner Demo")
    print("=" * 50)
    
    # 1. Create the database table (if it doesn't exist)
    print("Step 1: Setting up database table...")
    try:
        create_metrics_table()
    except Exception as e:
        print(f"Warning: Could not create table (might already exist): {e}")
    
    # 2. Run analysis for the first experiment
    experiment_key = "should_pin_leaderboard_carousel"
    print(f"\nStep 2: Running analysis for experiment '{experiment_key}'...")
    
    try:
        metrics = run_experiment_analysis(experiment_key)
        print(f"✓ Analysis completed! Generated {len(metrics)} metrics")
        
        # Show sample results
        if metrics:
            print("\nSample metrics:")
            for i, metric in enumerate(metrics[:3]):  # Show first 3 metrics
                print(f"  {i+1}. {metric.metric_name} ({metric.treatment_arm}): "
                     f"lift={metric.lift:.4f if metric.lift else 'N/A'}, "
                     f"p_value={metric.p_value:.4f if metric.p_value else 'N/A'}, "
                     f"sig={metric.statsig_string}")
    
    except Exception as e:
        print(f"✗ Error running analysis: {e}")
        import traceback
        traceback.print_exc()
    
    # 3. Show rendered query locations
    print(f"\n✓ Rendered queries saved to: experiment_runner/rendered_queries/{experiment_key}/")
    print("  You can manually edit these files if needed before re-running!")
    
    print("\n" + "=" * 50)
    print("Demo completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()
