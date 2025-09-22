"""
Experiment Configuration - Load and parse experiment metadata from YAML
"""

import yaml
import os
from pathlib import Path
try:
    # Python 3.9+
    from importlib.resources import files
except ImportError:
    # Python 3.8 fallback
    from importlib_resources import files

def load_experiment_config(experiment_key: str) -> dict:
    """
    Load experiment configuration from manual_experiments.yaml
    
    Args:
        experiment_key: Key from the YAML file (e.g., 'should_pin_leaderboard_carousel')
    
    Returns:
        Dictionary with experiment configuration
    """
    
    try:
        # Try package resource access first
        package_files = files("nux_slack_bot.data_models")
        metadata_file = package_files / "manual_experiments.yaml"
        with metadata_file.open('r') as f:
            data = yaml.safe_load(f)
    except (ImportError, FileNotFoundError, ModuleNotFoundError):
        # Fallback to relative path (for development)
        yaml_path = Path(__file__).parent.parent / "data_models" / "manual_experiments.yaml"
        if not yaml_path.exists():
            # Original relative path as last resort
            yaml_path = os.path.join(os.path.dirname(__file__), '..', 'data_models', 'manual_experiments.yaml')
        
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
    
    if experiment_key not in data['experiments']:
        raise ValueError(f"Experiment '{experiment_key}' not found in manual_experiments.yaml")
    
    experiment_config = data['experiments'][experiment_key]
    
    # Validate required fields (make 'version' optional)
    required_fields = ['experiment_name', 'start_date', 'end_date', 'bucket_key', 'template']
    for field in required_fields:
        if field not in experiment_config:
            raise ValueError(f"Required field '{field}' missing from experiment config")
    
    return experiment_config

def get_templates_for_experiment(config: dict) -> list:
    """
    Discover template files based on experiment configuration
    
    Args:
        config: Experiment configuration dictionary
    
    Returns:
        List of template file information
    """
    import glob
    
    bucket_key = config['bucket_key']  # 'consumer_id' or 'device_id'
    template_type = config['template']  # 'onboarding', 'appclip', etc.
    
    # Find all matching template files
    template_dir = os.path.join(os.path.dirname(__file__), '..', 'sql_scripts', f'{bucket_key}_level')
    pattern = os.path.join(template_dir, f'{bucket_key}_{template_type}_*.sql')
    
    template_files = glob.glob(pattern)
    
    if not template_files:
        # Fallback: try to find any templates that start with the template type
        fallback_pattern = os.path.join(template_dir, f'{bucket_key}_*{template_type}*.sql')
        template_files = glob.glob(fallback_pattern)
    
    return [{
        'path': file_path,
        'name': os.path.basename(file_path).replace(f'{bucket_key}_', '').replace('.sql', ''),
        'bucket_key': bucket_key
    } for file_path in template_files]
