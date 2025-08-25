#!/usr/bin/env python3
"""
Debug script to examine actual query results and understand why no metrics are generated
"""

from utils.snowflake_connection import execute_snowflake_query
import os

def debug_query_results():
    """Examine actual query results to debug metrics parsing"""
    
    print("=" * 80)
    print("🔍 DEBUGGING QUERY RESULTS")
    print("=" * 80)
    
    # Check a few rendered query files
    test_queries = [
        {
            'path': 'experiment_runner/rendered_queries/should_pin_leaderboard_carousel/should_pin_leaderboard_carousel_onboarding_topline.sql',
            'name': 'should_pin_leaderboard_carousel - onboarding_topline'
        },
        {
            'path': 'experiment_runner/rendered_queries/should_pin_leaderboard_carousel/should_pin_leaderboard_carousel_onboarding_system_level_opt_in.sql',
            'name': 'should_pin_leaderboard_carousel - system_level_opt_in'
        }
    ]
    
    for query_info in test_queries:
        if not os.path.exists(query_info['path']):
            print(f"❌ File not found: {query_info['path']}")
            continue
            
        print(f"\n📄 Query: {query_info['name']}")
        print("-" * 60)
        
        try:
            # Read and execute the query
            with open(query_info['path'], 'r') as f:
                query = f.read()
            
            print(f"🔍 Query length: {len(query):,} characters")
            print("🎯 Executing query...")
            
            results = execute_snowflake_query(query, method='pandas')
            
            print(f"📊 Results: {len(results)} rows")
            
            if results:
                # Examine the structure of results
                first_row = results[0]
                print(f"🔑 Columns found: {len(first_row)} columns")
                print(f"📋 Column names: {list(first_row.keys())}")
                
                # Look for Lift_ columns specifically
                lift_columns = [col for col in first_row.keys() if col.startswith('Lift_')]
                if lift_columns:
                    print(f"🎯 Lift columns found: {lift_columns}")
                else:
                    print("⚠️  No 'Lift_' columns found!")
                
                # Look for common experiment columns
                exp_columns = [col for col in first_row.keys() if col.lower() in ['tag', 'result', 'bucket', 'experiment_name']]
                if exp_columns:
                    print(f"📊 Experiment columns: {exp_columns}")
                else:
                    print("⚠️  No standard experiment columns (tag, result, bucket) found!")
                
                # Show first few rows
                print(f"\n📋 Sample data (first {min(3, len(results))} rows):")
                for i, row in enumerate(results[:3]):
                    print(f"  Row {i+1}:")
                    for key, value in list(row.items())[:10]:  # Show first 10 columns
                        print(f"    {key}: {value}")
                    if len(row) > 10:
                        print(f"    ... and {len(row) - 10} more columns")
                    print()
            else:
                print("📭 No results returned")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print()

if __name__ == "__main__":
    debug_query_results()
