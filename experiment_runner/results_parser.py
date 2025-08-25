"""
Results Parser - Parse SQL query results into experiment metrics
"""

from dataclasses import dataclass
from typing import List, Optional, Any

@dataclass
class ExperimentMetric:
    """Data class for individual experiment metric"""
    
    # Experiment identifiers
    experiment_name: str
    start_date: str
    end_date: str
    version: int
    
    # Metric details
    granularity: str  # 'consumer_id' or 'device_id'
    template_name: str
    metric_name: str
    treatment_arm: str  # 'treatment', 'variant_1', etc. (no control)
    metric_type: str  # 'rate' or 'continuous'
    dimension: Optional[str] = None  # e.g., 'app', 'app_clip', None
    
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
                version=config['version'],
                
                # Metric details
                granularity=config['bucket_key'],
                template_name=template_name,
                metric_name=metric_name,
                treatment_arm=treatment_arm_value,  # treatment, variant_1, etc.
                metric_type=metric_type,
                dimension=dimension_value,
                
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
    
    # Rate metrics typically end with '_rate' or have 'completion', 'pct'
    rate_indicators = ['_rate', '_pct', 'completion', 'signup', 'download']
    
    if any(indicator in metric_name.lower() for indicator in rate_indicators):
        return 'rate'
    
    # Continuous metrics typically have std columns
    if treatment_row and f"std_{metric_name}" in treatment_row:
        return 'continuous'
    
    if control_row and f"control_std_{metric_name}" in control_row:
        return 'continuous'
    
    # Additional heuristics
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
    
    # Try to find the base metric value
    if f"{prefix}{metric_name}" in row:
        data['value'] = row[f"{prefix}{metric_name}"]
    elif metric_name in row:
        data['value'] = row[metric_name]
    
    # For rate metrics, look for numerator/denominator patterns
    if '_rate' in metric_name:
        base_metric = metric_name.replace('_rate', '')
        
        # Numerator: e.g., 'orders' for 'order_rate'
        if f"{prefix}{base_metric}" in row:
            data['numerator'] = row[f"{prefix}{base_metric}"]
        
        # Denominator: exposure, total_cx, etc.
        denominator_candidates = [f"{prefix}exposure", f"{prefix}total_cx", f"{prefix}sample_size"]
        for candidate in denominator_candidates:
            if candidate in row:
                data['denominator'] = row[candidate]
                break
    
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
    std_candidates = [f"{prefix}std_{metric_name}", f"{prefix}{metric_name}_std"]
    for candidate in std_candidates:
        if candidate in row:
            data['std'] = row[candidate]
            break
    
    return data
