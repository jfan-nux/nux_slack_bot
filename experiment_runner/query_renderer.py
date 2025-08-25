"""
Query Renderer - Render Jinja2 SQL templates with experiment parameters
"""

import jinja2
import os
from .experiment_config import get_templates_for_experiment

def render_templates_for_experiment(config: dict) -> dict:
    """
    Render all SQL templates for an experiment with its configuration.
    Skip rendering files that already exist to preserve manual overrides.
    
    Args:
        config: Experiment configuration dictionary
    
    Returns:
        Dictionary mapping template_name -> rendered_query_path
    """
    
    # Get all template files for this experiment
    templates = get_templates_for_experiment(config)
    
    # Create rendered queries directory for this experiment
    experiment_name = config['experiment_name']
    rendered_dir = os.path.join(os.path.dirname(__file__), 'rendered_queries', experiment_name)
    os.makedirs(rendered_dir, exist_ok=True)
    
    rendered_queries = {}
    skipped_count = 0
    rendered_count = 0
    
    for template_info in templates:
        # Determine output path
        rendered_filename = f"{experiment_name}_{template_info['name']}.sql"
        rendered_path = os.path.join(rendered_dir, rendered_filename)
        
        # Check if file already exists
        if os.path.exists(rendered_path):
            print(f"      ⏭️  Skipping {template_info['name']} (file exists - preserving manual override)")
            skipped_count += 1
        else:
            # Render the template
            rendered_sql = render_template_file(template_info['path'], config)
            
            # Save rendered query
            with open(rendered_path, 'w') as f:
                f.write(rendered_sql)
            
            print(f"      ✨ Rendered {template_info['name']}")
            rendered_count += 1
        
        # Add to results regardless of whether it was rendered or skipped
        rendered_queries[template_info['name']] = rendered_path
    
    if skipped_count > 0 or rendered_count > 0:
        print(f"      📊 Summary: {rendered_count} rendered, {skipped_count} skipped (existing files)")
        
    return rendered_queries

def render_template_file(template_path: str, config: dict) -> str:
    """
    Render a single SQL template file with experiment parameters
    
    Args:
        template_path: Path to the SQL template file
        config: Experiment configuration dictionary
    
    Returns:
        Rendered SQL string
    """
    
    with open(template_path, 'r') as f:
        template_content = f.read()
    
    # Create Jinja2 template
    template = jinja2.Template(template_content)
    
    # Render with experiment parameters
    rendered = template.render(
        experiment_name=config['experiment_name'],
        start_date=config['start_date'],
        end_date=config['end_date'],
        version=config['version'],
        bucket_key=config['bucket_key'],
        segments=config.get('segments', [])  # Pass segments array, default to empty list
    )
    
    return rendered
