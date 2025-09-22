"""
Results Parser - Parse SQL query results into experiment metrics
"""

from dataclasses import dataclass
from typing import List, Optional, Any
import yaml
import os

@dataclass
class ExperimentMetric:
    """Data class for individual experiment metric"""
    
    # Experiment identifiers
    experiment_name: str
    start_date: str

    end_date: str
    
    # Metric details
    granularity: str  # 'consumer_id' or 'device_id'
    template_name: str
    metric_name: str
    treatment_arm: str  # 'treatment', 'variant_1', etc. (no control)
    metric_type: str  # 'rate' or 'continuous'
    version: Optional[int] = None
    dimension: Optional[str] = None  # e.g., 'app', 'app_clip', None
    segments: Optional[str] = None  # e.g., 'ios', 'android'
    template_rank: Optional[int] = None  # From metrics_metadata.yaml
    metric_rank: Optional[int] = None  # From metrics_metadata.yaml
    desired_direction: Optional[str] = None  # From metrics_metadata.yaml
    
    # Treatment values
    treatment_numerator: Optional[float] = None
    treatment_denominator: Optional[float] = None
    treatment_value: Optional[float] = None
    treatment_sample_size: Optional[int] = None
    treatment_std: Optional[float] = None
    
    # Control values (as columns, not separate rows)
    control_numerator: Optional[float] = None
    control_denominator: Optional[float] = None
    control_value: Optional[float] = None
    control_sample_size: Optional[int] = None
    control_std: Optional[float] = None
    
    # Statistical analysis (to be calculated)
    lift: Optional[float] = None
    absolute_difference: Optional[float] = None
    p_value: Optional[float] = None
    confidence_interval_lower: Optional[float] = None
    confidence_interval_upper: Optional[float] = None
    statsig_string: Optional[str] = None
    statistical_power: Optional[float] = None
    
    # Execution metadata
    query_execution_timestamp: Optional[str] = None
    query_runtime_seconds: Optional[float] = None

def _load_metrics_metadata():
    """Load metrics metadata from YAML file"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        metadata_path = os.path.join(project_root, 'data_models', 'metrics_metadata.yaml')
        
        with open(metadata_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not load metrics metadata: {e}")
        return {}

def _get_metric_metadata(template_name: str, metric_name: str, metadata: dict) -> tuple[Optional[int], Optional[int], Optional[str]]:
    """Get template_rank, metric_rank, and desired_direction for a specific metric"""
    try:
        templates = metadata.get('templates', {})
        
        # Handle different template naming patterns
        # e.g., 'onboarding_topline' -> template='onboarding', subcategory='topline'
        # e.g., 'app_download_topline' -> template='app_download', subcategory='topline'
        
        main_template = None
        subcategory = None
        
        # Try different parsing strategies
        if '_' in template_name:
            # Strategy 1: Check if full template name exists (e.g., 'app_download')
            parts = template_name.split('_')
            for i in range(len(parts) - 1, 0, -1):  # Try from longest to shortest
                potential_template = '_'.join(parts[:i])
                potential_subcategory = '_'.join(parts[i:])
                
                if potential_template in templates:
                    main_template = potential_template
                    subcategory = potential_subcategory
                    break
            
            # Strategy 2: Fall back to simple split if no match found
            if main_template is None:
                main_template, subcategory = template_name.split('_', 1)
        else:
            main_template = template_name
            subcategory = None
        
        if main_template not in templates:
            return None, None, None
        
        template_data = templates[main_template]
        
        if subcategory and subcategory in template_data:
            subcategory_data = template_data[subcategory]
            template_rank = subcategory_data.get('template_rank')
            
            if metric_name in subcategory_data:
                metric_data = subcategory_data[metric_name]
                metric_rank = metric_data.get('metric_rank')
                desired_direction = metric_data.get('desired_direction')
                return template_rank, metric_rank, desired_direction
        
        return None, None, None
        
    except Exception as e:
        print(f"Warning: Error getting metric metadata for {template_name}/{metric_name}: {e}")
        return None, None, None

def parse_results(results: List[dict], template_name: str, config: dict) -> List[ExperimentMetric]:
    """
    Parse SQL query results into ExperimentMetric objects
    
    Args:
        results: List of result row dictionaries from SQL query
        template_name: Name of the template that was executed
        config: Experiment configuration
    
    Returns:
        List of ExperimentMetric objects
    """
    
    if not results:
        return []
    
    # Load metrics metadata for ranking information
    metadata = _load_metrics_metadata()
    
    metrics = []
    
    # Check if this template has dimensions (e.g., appclip_order_platform_split has 'platform')
    has_dimension = template_name == 'appclip_order_platform_split'
    dimension_column = 'platform' if has_dimension else None
    
    # Process each result row
    for row in results:
        # Determine the treatment arm column (could be 'tag' or 'bucket')
        treatment_arm_col = None
        treatment_arm_value = None
        
        if 'tag' in row:
            treatment_arm_col = 'tag'
            treatment_arm_value = row['tag']
        elif 'bucket' in row:
            treatment_arm_col = 'bucket'
            treatment_arm_value = row['bucket']
        else:
            print(f"Warning: No treatment arm column found in row: {list(row.keys())}")
            continue
        
        # Skip control rows - we extract control data as columns
        if treatment_arm_value == 'control':
            continue
        
        # Get dimension value if applicable
        dimension_value = row.get(dimension_column) if has_dimension else None
        
        # Get segments value from SQL results
        segments_value = row.get('segments')
        
        # Find matching control row for this dimension
        control_row = find_control_row(results, dimension_value, has_dimension, dimension_column)
        
        # Extract all lift columns and create metrics (check both uppercase and lowercase)
        lift_columns = [col for col in row.keys() if col.lower().startswith('lift_')]
        
        for lift_col in lift_columns:
            # Handle both uppercase and lowercase lift column prefixes
            if lift_col.startswith('Lift_'):
                metric_name = lift_col.replace('Lift_', '')
            elif lift_col.startswith('lift_'):
                metric_name = lift_col.replace('lift_', '')
            else:
                metric_name = lift_col.lower().replace('lift_', '')
            
            # Determine metric type
            metric_type = determine_metric_type(metric_name, row, control_row)
            
            # Get template and metric metadata from YAML
            template_rank, metric_rank, desired_direction = _get_metric_metadata(template_name, metric_name, metadata)
            
            # Extract treatment values
            treatment_data = extract_metric_data(row, metric_name, 'treatment')
            
            # Extract control data from control columns in the same treatment row
            # (not from a separate control row, since we structured SQL to include control columns)
            control_data = extract_metric_data(row, metric_name, 'control')
            
            metric = ExperimentMetric(
                # Experiment identifiers
                experiment_name=config['experiment_name'],
                start_date=config['start_date'],
                end_date=config['end_date'],
                version=config.get('version', -1),
                
                # Metric details
                granularity=config['bucket_key'],
                template_name=template_name,
                metric_name=metric_name,
                treatment_arm=treatment_arm_value,  # treatment, variant_1, etc.
                metric_type=metric_type,
                dimension=dimension_value,
                segments=segments_value,
                template_rank=template_rank,
                metric_rank=metric_rank,
                desired_direction=desired_direction,
                
                # Treatment data
                treatment_numerator=treatment_data.get('numerator'),
                treatment_denominator=treatment_data.get('denominator'),
                treatment_value=treatment_data.get('value'),
                treatment_sample_size=treatment_data.get('sample_size'),
                treatment_std=treatment_data.get('std'),
                
                # Control data
                control_numerator=control_data.get('numerator'),
                control_denominator=control_data.get('denominator'),
                control_value=control_data.get('value'),
                control_sample_size=control_data.get('sample_size'),
                control_std=control_data.get('std'),
                
                # Lift (calculated in SQL)
                lift=row.get(lift_col)
            )
            
            metrics.append(metric)
    
    return metrics

def find_control_row(results: List[dict], dimension_value: Optional[str], 
                    has_dimension: bool, dimension_column: Optional[str]) -> Optional[dict]:
    """Find the matching control row for a given treatment row"""
    
    for row in results:
        # Check if this is a control row (could be in 'tag' or 'bucket' column)
        is_control = False
        if 'tag' in row and row['tag'] == 'control':
            is_control = True
        elif 'bucket' in row and row['bucket'] == 'control':
            is_control = True
        
        if not is_control:
            continue
            
        if has_dimension and dimension_column:
            # Match on dimension (e.g., same platform)
            if row.get(dimension_column) == dimension_value:
                return row
        else:
            # No dimension - return first control row
            return row
    
    return None

def determine_metric_type(metric_name: str, treatment_row: dict, control_row: Optional[dict]) -> str:
    """Determine if metric is 'rate' or 'continuous'"""
    
    # Explicit continuous variables (have std values, are monetary/count metrics)
    continuous_vars = ['vp', 'vp_per_device', 'gov', 'gov_per_device', 'variable_profit', 'subtotal']
    if metric_name.lower() in continuous_vars:
        return 'continuous'
    
    # Check if this metric has std columns (strong indicator of continuous)
    metric_name_mapping = {
        'vp': 'variable_profit',
        'vp_per_device': 'variable_profit',
        'gov': 'gov', 
        'gov_per_device': 'gov'
    }
    std_column_name = metric_name_mapping.get(metric_name, metric_name)
    
    if treatment_row and f"std_{std_column_name}" in treatment_row:
        return 'continuous'
    if control_row and f"control_std_{std_column_name}" in control_row:
        return 'continuous'
        
    # Rate indicators in metric name  
    rate_indicators = ['_rate', '_pct', 'completion', 'signup', 'download']
    if any(indicator in metric_name.lower() for indicator in rate_indicators):
        return 'rate'
    
    # Additional continuous patterns
    continuous_patterns = ['avg_', 'total_', 'sum_', 'subtotal', 'profit', 'gov']
    if any(pattern in metric_name.lower() for pattern in continuous_patterns):
        return 'continuous'
        
    return 'rate'  # Default assumption

def extract_metric_data(row: Optional[dict], metric_name: str, arm_type: str) -> dict:
    """Extract numerator, denominator, value, sample_size, std for a metric"""
    
    if not row:
        return {}
    
    data = {}
    prefix = 'control_' if arm_type == 'control' else ''
    
    # Try to find the base metric value with enhanced mapping
    # Map metric names to actual SQL column names
    value_mapping = {
        'vp': 'variable_profit',
        'vp_per_device': 'VP_per_device',  # Note: SQL uses uppercase VP_per_device
        'gov': 'gov',
        'gov_per_device': 'gov_per_device'
    }
    
    # Import pandas for nan checking
    try:
        import pandas as pd
    except ImportError:
        import numpy as np
        pd = np
    
    # Get the actual column name for value lookup
    value_column_name = value_mapping.get(metric_name, metric_name)
    
    if f"{prefix}{value_column_name}" in row:
        value = row[f"{prefix}{value_column_name}"]
        # Handle nan values
        if pd.isna(value) or (isinstance(value, str) and value.lower() == 'nan'):
            data['value'] = None
        else:
            data['value'] = value
    elif f"{prefix}{metric_name}" in row:
        value = row[f"{prefix}{metric_name}"]
        if pd.isna(value) or (isinstance(value, str) and value.lower() == 'nan'):
            data['value'] = None 
        else:
            data['value'] = value
    elif value_column_name in row:
        value = row[value_column_name]
        if pd.isna(value) or (isinstance(value, str) and value.lower() == 'nan'):
            data['value'] = None
        else:
            data['value'] = value
    elif metric_name in row:
        value = row[metric_name]
        if pd.isna(value) or (isinstance(value, str) and value.lower() == 'nan'):
            data['value'] = None
        else:
            data['value'] = value
    
    # For rate metrics, look for numerator/denominator patterns
    if '_rate' in metric_name or '_pct' in metric_name:
        base_metric = metric_name.replace('_rate', '').replace('_pct', '')
        
        # Enhanced numerator candidates for different metric patterns
        numerator_candidates = []
        
        if 'system_level_push_opt_out_pct' in metric_name:
            numerator_candidates = [f"{prefix}system_level_push_opt_out"]
        elif 'system_level_push_opt_in_pct' in metric_name:
            numerator_candidates = [f"{prefix}system_level_push_opt_in"]
        elif metric_name in ['explore_rate', 'store_rate', 'cart_rate', 'checkout_rate']:
            # Special pattern: explore_rate → explore_view, store_rate → store_view, etc.
            view_metric = base_metric + '_view'  # explore + _view = explore_view
            numerator_candidates = [f"{prefix}{view_metric}", f"{prefix}{base_metric}"]
        elif 'completion' in metric_name:
            # onboarding_completion → calculate from rate * denominator since count not available
            # Don't look for numerator - we'll calculate it below
            numerator_candidates = []
        else:
            # Standard patterns: try both singular and plural
            numerator_candidates = [f"{prefix}{base_metric}s", f"{prefix}{base_metric}"]
            
        for candidate in numerator_candidates:
            if candidate in row:
                data['numerator'] = row[candidate]
                break
        
        # Denominator: exposure, total_cx, etc.
        denominator_candidates = [
            f"{prefix}exposure_onboard", f"{prefix}exposure", f"{prefix}total_cx", 
            f"{prefix}sample_size", f"{prefix}total_devices", f"{prefix}total_users"
        ]
        for candidate in denominator_candidates:
            if candidate in row:
                data['denominator'] = row[candidate]
                break
                
        # If no denominator found, use the sample_size from the metric value itself
        if 'denominator' not in data or data['denominator'] is None:
            if f"{prefix}sample_size" in row and row[f"{prefix}sample_size"] is not None:
                data['denominator'] = row[f"{prefix}sample_size"]
                
        # This logic was inside the rate block but needs to be outside
    
    # Special handling for completion metrics - find denominator and calculate numerator
    # This needs to be OUTSIDE the rate block since completion doesn't contain '_rate'  
    if 'completion' in metric_name:
        # For completion metrics, denominator is usually exposure/sample_size
        if 'denominator' not in data or data['denominator'] is None:
            for denom_candidate in [f"{prefix}exposure", f"{prefix}sample_size", f"{prefix}total_cx"]:
                if denom_candidate in row and row[denom_candidate] is not None:
                    data['denominator'] = row[denom_candidate]
                    break
        
        # Calculate numerator from rate * denominator
        if 'denominator' in data and data['denominator'] is not None:
            rate_value = data.get('value')
            if rate_value is not None and rate_value != 'nan':
                try:
                    data['numerator'] = int(float(rate_value) * float(data['denominator']))
                except (ValueError, TypeError):
                    pass

    # Sample size
    sample_size_candidates = [
        f"{prefix}sample_size", f"{prefix}exposure", f"{prefix}total_cx",
        f"{prefix}n_{metric_name.replace('_rate', '')}", f"{prefix}n_orders_for_stats"
    ]
    for candidate in sample_size_candidates:
        if candidate in row:
            data['sample_size'] = row[candidate]
            break
    
    # Standard deviation (for continuous metrics)
    # Map metric names to actual SQL column names for std
    metric_name_mapping = {
        'vp': 'variable_profit',
        'vp_per_device': 'variable_profit',  # Both vp metrics use same std
        'gov': 'gov',
        'gov_per_device': 'gov'  # Both gov metrics use same std
    }
    
    # Get the actual column name for std lookup
    std_column_name = metric_name_mapping.get(metric_name, metric_name)
    
    std_candidates = [
        f"{prefix}std_{std_column_name}", 
        f"{prefix}{std_column_name}_std",
        f"{prefix}std_{metric_name}", 
        f"{prefix}{metric_name}_std"
    ]
    for candidate in std_candidates:
        if candidate in row:
            data['std'] = row[candidate]
            break
    
    return data
