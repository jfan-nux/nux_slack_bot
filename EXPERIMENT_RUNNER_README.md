# Experiment Runner System

A metadata-driven experiment analysis system that renders SQL templates, executes queries, and stores metrics with statistical significance calculations.

## Architecture

```
nux_slack_bot/
├── experiment_runner/           # Core experiment runner
│   ├── __init__.py             # Main entry point
│   ├── experiment_config.py    # Load YAML config
│   ├── query_renderer.py       # Render Jinja2 templates  
│   ├── results_parser.py       # Parse SQL results
│   ├── analysis.py             # Statistical analysis
│   ├── metrics_storage.py      # Store to database
│   └── rendered_queries/       # Rendered SQL files (allows manual override)
├── sql_scripts/                # Jinja2 SQL templates
│   ├── consumer_id_level/      # Consumer ID experiments
│   └── device_id_level/        # Device ID experiments
└── data_models/
    └── manual_experiments.yaml # Experiment metadata
```

## Quick Start

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Run experiment analysis:**
```python
from experiment_runner import run_experiment_analysis

# Analyze experiment from YAML config
metrics = run_experiment_analysis("should_pin_leaderboard_carousel")
```

3. **Or use the demo script:**
```bash
python demo_experiment_runner.py
```

## How It Works

### 1. Configuration-Driven
Experiments are defined in `data_models/manual_experiments.yaml`:

```yaml
experiments:
  should_pin_leaderboard_carousel:
    experiment_name: "should_pin_leaderboard_carousel"
    start_date: "2025-08-21"
    end_date: "2025-09-31"
    bucket_key: "consumer_id"  # or "device_id"
    template: "onboarding"     # finds onboarding_* templates
    version: 1
```

### 2. Template Discovery
Based on `bucket_key` and `template`, the system finds SQL templates:
- `bucket_key: "consumer_id"` + `template: "onboarding"` 
- → Finds: `sql_scripts/consumer_id_level/consumer_id_onboarding_*.sql`

### 3. Jinja2 Template Rendering
SQL templates use Jinja2 variables:

```sql
{# 
Jinja2 Template Variables:
- experiment_name: {{ experiment_name }}
- start_date: {{ start_date }}  
- end_date: {{ end_date }}
- version: {{ version }}
#}

SELECT * FROM experiment_exposure 
WHERE experiment_name = '{{ experiment_name }}'
  AND exposure_time BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

### 4. Query Execution & Storage
- Rendered queries saved to: `experiment_runner/rendered_queries/{experiment_name}/`
- Executed from rendered files (allows manual override)
- Results parsed into structured metrics
- Statistical analysis applied (p-values, confidence intervals)
- Stored in `experiment_metrics_results` table

## Database Schema

```sql
CREATE TABLE experiment_metrics_results (
    -- Experiment Identifiers  
    experiment_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    version INT,
    
    -- Metric Details
    granularity VARCHAR(20),      -- 'consumer_id' or 'device_id'
    template_name VARCHAR(100),   -- e.g., 'onboarding_topline'
    metric_name VARCHAR(100),     -- e.g., 'order_rate'
    treatment_arm VARCHAR(50),    -- e.g., 'treatment' (NO control rows)
    metric_type VARCHAR(20),      -- 'rate' or 'continuous'
    dimension VARCHAR(50),        -- e.g., 'app', 'app_clip', NULL
    
    -- Treatment Values
    treatment_numerator FLOAT,
    treatment_denominator FLOAT,
    treatment_value FLOAT,
    treatment_sample_size INT,
    treatment_std FLOAT,
    
    -- Control Values (as columns)
    control_numerator FLOAT,
    control_denominator FLOAT, 
    control_value FLOAT,
    control_sample_size INT,
    control_std FLOAT,
    
    -- Statistical Analysis
    lift FLOAT,
    absolute_difference FLOAT,
    p_value FLOAT,
    confidence_interval_lower FLOAT,
    confidence_interval_upper FLOAT,
    statsig_string VARCHAR(50),   -- 'significant', 'not_significant', etc.
    
    -- Metadata
    insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    query_execution_timestamp TIMESTAMP,
    
    PRIMARY KEY (experiment_name, version, granularity, template_name, 
                 metric_name, treatment_arm, COALESCE(dimension, 'null'))
);
```

## Key Features

### ✅ Metadata-Driven
- No hardcoded template mappings
- Everything driven by YAML configuration  
- Automatic template discovery

### ✅ Manual Override Support
- Rendered queries saved before execution
- Edit rendered files for custom analysis
- Re-run without re-rendering

### ✅ Statistical Analysis  
- Two-proportion z-test for rates
- Two-sample t-test for continuous metrics
- Curie-style significance classification
- Confidence intervals

### ✅ Dimension Support
- Handles dimensional breakdowns (e.g., platform splits)
- Automatic control matching by dimension
- Extensible for new dimension types

### ✅ Clean Data Model
- No duplicate control rows
- Control data as columns
- Treatment arms only in rows

## SQL Template Requirements

Templates must:
1. Use Jinja2 variable syntax: `{{ experiment_name }}`
2. Include all required Lift_ columns in final SELECT
3. Have corresponding control_ prefixed statistical columns
4. Follow naming convention: `{granularity}_{template_type}_{specific}.sql`

Example metric columns expected:
```sql
SELECT 
    tag,                          -- treatment arm name
    order_rate,                   -- treatment metric
    Lift_order_rate,             -- calculated lift
    control_orders,              -- control numerator  
    control_exposure,            -- control denominator
    control_order_rate,          -- control metric
    -- ... other metrics
FROM ...
```

## Usage Examples

### Run Single Experiment
```python
from experiment_runner import run_experiment_analysis

metrics = run_experiment_analysis("cx_mobile_onboarding_preferences")

for metric in metrics:
    print(f"{metric.metric_name}: lift={metric.lift:.3f}, p={metric.p_value:.3f}")
```

### Create Database Table
```python
from experiment_runner.metrics_storage import create_metrics_table

create_metrics_table()  # Creates table if not exists
```

### Manual Query Override
1. Run experiment once to generate rendered queries
2. Edit files in `experiment_runner/rendered_queries/{experiment_name}/`
3. Re-run analysis - uses your edited queries

This system provides a complete, scalable solution for experiment analysis that maintains statistical rigor while offering flexibility for custom analysis needs.
