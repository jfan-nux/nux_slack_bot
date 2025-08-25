#!/usr/bin/env python3
"""
Complete Experiment Runner Script

This script:
1. Reads experiments from manual_experiments.yaml
2. Renders SQL templates with experiment parameters
3. Executes the SQL queries on Snowflake
4. Parses results into structured metrics
5. Calculates statistical significance
6. Creates/inserts data into experiment_metrics_results table
"""

import yaml
import os
import time
from datetime import datetime
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import traceback

from experiment_runner.experiment_config import load_experiment_config
from experiment_runner.query_renderer import render_templates_for_experiment
from experiment_runner.results_parser import parse_results
from experiment_runner.analysis import ExperimentAnalysis
from experiment_runner.metrics_storage import create_metrics_table, store_metrics
from utils.snowflake_connection import execute_snowflake_query

# Thread-safe print lock
print_lock = Lock()

def thread_safe_print(*args, **kwargs):
    """Thread-safe printing function"""
    with print_lock:
        print(*args, **kwargs)

def execute_single_query(query_info: Dict) -> Dict:
    """
    Execute a single SQL query and return results with metadata
    
    Args:
        query_info: Dictionary with query execution information
    
    Returns:
        Dictionary with execution results and metadata
    """
    exp_key = query_info['exp_key']
    template_name = query_info['template_name']
    query_path = query_info['query_path']
    config = query_info['config']
    
    result = {
        'exp_key': exp_key,
        'template_name': template_name,
        'status': 'FAILED',
        'metrics': [],
        'execution_time': 0,
        'error': None,
        'result_count': 0
    }
    
    try:
        thread_safe_print(f"   🔍 [{exp_key}] Executing {template_name}...")
        
        # Read and execute query
        start_time = time.time()
        with open(query_path, 'r') as f:
            query = f.read()
        
        # Execute with pandas-only mode to avoid Spark issues
        results = execute_snowflake_query(query, method='pandas')
        execution_time = time.time() - start_time
        
        thread_safe_print(f"   ✅ [{exp_key}] {template_name} completed in {execution_time:.2f}s - {len(results)} rows")
        
        # Parse results into metrics
        analyzer = ExperimentAnalysis()
        metrics = parse_results(results, template_name, config)
        
        # Add execution metadata to metrics
        for metric in metrics:
            metric.query_execution_timestamp = datetime.now().isoformat()
            metric.query_runtime_seconds = execution_time
            
            # Calculate statistics
            analyzer.calculate_statistics(metric)
            analyzer.apply_statsig_classification(metric)
        
        result.update({
            'status': 'SUCCESS',
            'metrics': metrics,
            'execution_time': execution_time,
            'result_count': len(results)
        })
        
        if metrics:
            thread_safe_print(f"   📈 [{exp_key}] {template_name} generated {len(metrics)} metrics")
        else:
            thread_safe_print(f"   ⚠️  [{exp_key}] {template_name} generated 0 metrics (check query results)")
            
    except Exception as e:
        execution_time = time.time() - start_time if 'start_time' in locals() else 0
        error_msg = str(e)
        thread_safe_print(f"   ❌ [{exp_key}] {template_name} failed: {error_msg}")
        
        result.update({
            'status': 'FAILED',
            'execution_time': execution_time,
            'error': error_msg
        })
    
    return result

def execute_queries_parallel(query_infos: List[Dict], max_workers: int = 4) -> List[Dict]:
    """
    Execute multiple queries in parallel using ThreadPoolExecutor
    
    Args:
        query_infos: List of query information dictionaries
        max_workers: Maximum number of concurrent workers
        
    Returns:
        List of execution results
    """
    thread_safe_print(f"🚀 Starting parallel execution of {len(query_infos)} queries with {max_workers} workers...")
    
    results = []
    completed_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all queries
        future_to_query = {executor.submit(execute_single_query, query_info): query_info 
                          for query_info in query_infos}
        
        # Process completed queries as they finish
        for future in as_completed(future_to_query):
            query_info = future_to_query[future]
            
            try:
                result = future.result()
                results.append(result)
                
                if result['status'] == 'SUCCESS':
                    completed_count += 1
                else:
                    failed_count += 1
                    
                # Progress update
                total_processed = completed_count + failed_count
                thread_safe_print(f"📊 Progress: {total_processed}/{len(query_infos)} queries completed "
                                f"({completed_count} success, {failed_count} failed)")
                    
            except Exception as e:
                thread_safe_print(f"❌ Unexpected error processing {query_info['template_name']}: {e}")
                failed_count += 1
                
                results.append({
                    'exp_key': query_info['exp_key'],
                    'template_name': query_info['template_name'],
                    'status': 'FAILED',
                    'metrics': [],
                    'execution_time': 0,
                    'error': str(e),
                    'result_count': 0
                })
    
    thread_safe_print(f"🏁 Parallel execution complete: {completed_count} success, {failed_count} failed")
    return results

def run_all_experiments(max_workers: int = 4):
    """
    Main function to run complete experiment analysis pipeline
    """
    
    print("=" * 80)
    print("🚀 COMPLETE EXPERIMENT ANALYSIS PIPELINE (PARALLELIZED)")
    print("=" * 80)
    
    # Step 1: Load experiments from YAML
    print("📋 Step 1: Loading experiments from YAML...")
    yaml_path = os.path.join('data_models', 'manual_experiments.yaml')
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    
    experiments = data['experiments']
    active_experiments = {k: v for k, v in experiments.items() if not v.get('expired', False)}
    
    print(f"   Found {len(experiments)} total experiments")
    print(f"   Active experiments: {len(active_experiments)}")
    print(f"   Max concurrent workers: {max_workers}")
    
    # Step 2: Create database table
    print("\n🗃️  Step 2: Setting up database table...")
    try:
        create_metrics_table()
        print("   ✅ Table experiment_metrics_results ready")
    except Exception as e:
        print(f"   ⚠️  Warning: Table setup issue: {e}")
        print("   Continuing with execution...")
    
    # Step 3: Prepare all queries for parallel execution
    print(f"\n🎨 Step 3: Preparing queries for parallel execution...")
    all_query_infos = []
    experiment_configs = {}
    
    for exp_key, exp_data in active_experiments.items():
        print(f"   📁 Preparing {exp_key}...")
        print(f"      Project: {exp_data.get('project_name', 'N/A')}")
        print(f"      Granularity: {exp_data['bucket_key']} | Template: {exp_data['template']}")
        print(f"      Date Range: {exp_data['start_date']} to {exp_data['end_date']}")
        
        try:
            # Load config and render templates
            config = load_experiment_config(exp_key)
            experiment_configs[exp_key] = config
            rendered_queries = render_templates_for_experiment(config)
            print(f"      ✅ Prepared {len(rendered_queries)} templates for execution")
            
            # Prepare query info for parallel execution
            for template_name, query_path in rendered_queries.items():
                all_query_infos.append({
                    'exp_key': exp_key,
                    'template_name': template_name,
                    'query_path': query_path,
                    'config': config
                })
                
        except Exception as e:
            print(f"      ❌ Failed to prepare {exp_key}: {e}")
            continue
    
    print(f"\n📊 Total queries prepared for execution: {len(all_query_infos)}")
    
    # Step 4: Execute all queries in parallel
    print(f"\n⚡ Step 4: Executing queries in parallel...")
    start_time = time.time()
    
    execution_results = execute_queries_parallel(all_query_infos, max_workers=max_workers)
    
    total_execution_time = time.time() - start_time
    print(f"\n🏁 All queries completed in {total_execution_time:.2f} seconds")
    
    # Step 5: Process results and collect metrics
    print(f"\n🔧 Step 5: Processing results...")
    all_metrics = []
    execution_summary = []
    
    # Group results by experiment
    results_by_experiment = {}
    for result in execution_results:
        exp_key = result['exp_key']
        if exp_key not in results_by_experiment:
            results_by_experiment[exp_key] = []
        results_by_experiment[exp_key].append(result)
    
    # Process each experiment's results
    for exp_key in active_experiments.keys():
        exp_results = results_by_experiment.get(exp_key, [])
        
        templates_executed = len([r for r in exp_results if r['status'] == 'SUCCESS'])
        templates_failed = len([r for r in exp_results if r['status'] == 'FAILED'])
        
        # Collect all metrics from this experiment
        experiment_metrics = []
        for result in exp_results:
            experiment_metrics.extend(result['metrics'])
        
        all_metrics.extend(experiment_metrics)
        
        # Summary
        status = 'SUCCESS' if templates_failed == 0 else f'PARTIAL ({templates_failed} failed)' if templates_executed > 0 else 'FAILED'
        
        execution_summary.append({
            'experiment': exp_key,
            'templates_executed': templates_executed,
            'templates_failed': templates_failed,
            'metrics_generated': len(experiment_metrics),
            'status': status,
            'avg_execution_time': sum(r['execution_time'] for r in exp_results) / len(exp_results) if exp_results else 0
        })
        
        print(f"   📈 {exp_key}: {len(experiment_metrics)} metrics from {templates_executed} templates")
    
    # Step 6: Store all metrics to database
    print(f"\n💾 Step 6: Storing {len(all_metrics)} metrics to database...")
    
    if all_metrics:
        try:
            store_metrics(all_metrics)
            print("   ✅ All metrics successfully stored to experiment_metrics_results table")
        except Exception as e:
            print(f"   ❌ Storage failed: {e}")
            return False
    else:
        print("   ⚠️  No metrics to store")
    
    # Step 7: Show final summary
    print("\n" + "=" * 90)
    print("📊 PARALLEL EXECUTION SUMMARY")
    print("=" * 90)
    
    print(f"{'Experiment':<30} {'Success':<8} {'Failed':<8} {'Metrics':<8} {'Avg Time(s)':<12} {'Status':<20}")
    print("-" * 90)
    
    total_templates_success = 0
    total_templates_failed = 0
    total_metrics = 0
    successful_experiments = 0
    total_execution_times = []
    
    for summary in execution_summary:
        status_display = summary['status'][:19]
        avg_time = summary.get('avg_execution_time', 0)
        
        print(f"{summary['experiment'][:29]:<30} "
              f"{summary['templates_executed']:<8} "
              f"{summary.get('templates_failed', 0):<8} "
              f"{summary['metrics_generated']:<8} "
              f"{avg_time:<12.2f} "
              f"{status_display:<20}")
        
        total_templates_success += summary['templates_executed']
        total_templates_failed += summary.get('templates_failed', 0)
        total_metrics += summary['metrics_generated']
        
        if avg_time > 0:
            total_execution_times.append(avg_time)
        
        if summary['status'] == 'SUCCESS':
            successful_experiments += 1
    
    print("-" * 90)
    avg_query_time = sum(total_execution_times) / len(total_execution_times) if total_execution_times else 0
    print(f"{'TOTALS:':<30} {total_templates_success:<8} {total_templates_failed:<8} {total_metrics:<8} {avg_query_time:<12.2f}")
    
    print()
    print("⚡ PERFORMANCE SUMMARY:")
    print(f"   • Total execution time: {total_execution_time:.2f} seconds")
    print(f"   • Queries executed: {total_templates_success + total_templates_failed}")
    print(f"   • Success rate: {(total_templates_success/(total_templates_success + total_templates_failed)*100):.1f}%")
    print(f"   • Average query time: {avg_query_time:.2f} seconds")
    print(f"   • Parallel workers used: {max_workers}")
    
    speedup_estimate = avg_query_time * (total_templates_success + total_templates_failed) / total_execution_time if total_execution_time > 0 else 1
    print(f"   • Estimated speedup vs sequential: {speedup_estimate:.1f}x")
    
    print("\n📈 RESULTS BREAKDOWN:")
    print(f"   • Experiments processed: {successful_experiments}/{len(active_experiments)}")
    print(f"   • SQL templates executed successfully: {total_templates_success}")
    print(f"   • SQL templates failed: {total_templates_failed}")
    print(f"   • Metrics generated: {total_metrics}")
    print(f"   • Database table: experiment_metrics_results")
    
    if total_metrics > 0:
        print("\n🎯 METRICS ANALYSIS:")
        
        # Group metrics by type
        rate_metrics = [m for m in all_metrics if m.metric_type == 'rate']
        continuous_metrics = [m for m in all_metrics if m.metric_type == 'continuous']
        significant_metrics = [m for m in all_metrics if m.statsig_string in ['significant', 'highly_significant']]
        
        print(f"   • Rate metrics: {len(rate_metrics)}")
        print(f"   • Continuous metrics: {len(continuous_metrics)}")
        print(f"   • Statistically significant: {len(significant_metrics)}")
        
        # Show some example metrics
        if significant_metrics:
            print(f"\n🔥 TOP SIGNIFICANT FINDINGS:")
            for i, metric in enumerate(significant_metrics[:5]):
                lift_pct = (metric.lift * 100) if metric.lift else 0
                print(f"   {i+1}. {metric.metric_name} ({metric.experiment_name[:20]})")
                print(f"      Treatment Arm: {metric.treatment_arm}")
                print(f"      Lift: {lift_pct:+.2f}%, P-value: {metric.p_value:.4f}, {metric.statsig_string}")
    
    print("\n" + "=" * 90)
    print("🎉 PARALLEL PIPELINE COMPLETE!")
    print("=" * 90)
    
    return True

def show_table_query():
    """Show SQL query to examine results"""
    
    query = """
    -- Query to examine experiment results
    SELECT 
        experiment_name,
        granularity,
        template_name,
        metric_name,
        treatment_arm,
        metric_type,
        dimension,
        treatment_value,
        control_value,
        lift,
        p_value,
        statsig_string,
        insert_timestamp
    FROM proddb.fionafan.experiment_metrics_results
    ORDER BY experiment_name, template_name, metric_name;
    """
    
    print("\n📊 To examine your results, run this SQL query:")
    print("=" * 60)
    print(query)
    print("=" * 60)

if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments for worker count
    parser = argparse.ArgumentParser(description='Run parallelized experiment analysis pipeline')
    parser.add_argument('--workers', type=int, default=4, 
                       help='Number of parallel workers (default: 4)')
    args = parser.parse_args()
    
    # Validate worker count
    max_workers = max(1, min(args.workers, 10))  # Between 1 and 10 workers
    
    print(f"Starting parallelized experiment analysis pipeline with {max_workers} workers...")
    
    try:
        success = run_all_experiments(max_workers=max_workers)
        
        if success:
            show_table_query()
            print(f"\n✨ Success! Parallel execution with {max_workers} workers completed.")
            print("📊 Check the experiment_metrics_results table for your results.")
        else:
            print(f"\n💥 Pipeline completed with errors. Check logs above.")
            
    except KeyboardInterrupt:
        print(f"\n⛔ Pipeline interrupted by user.")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        print("Check logs above for details.")
