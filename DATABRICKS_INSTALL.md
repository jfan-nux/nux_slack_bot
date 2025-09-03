# Installing nux_slack_bot in Databricks

This guide shows multiple ways to install the nux_slack_bot package in Databricks environments.

## Method 1: Install from DoorDash Artifactory (Recommended)

```python
# In a Databricks cell - using DoorDash internal PyPI:
%pip install --index-url https://ddartifacts.jfrog.io/artifactory/api/pypi/pypi/simple/ nux_slack_bot

# Or configure the index permanently and then install:
%pip config set global.index-url https://ddartifacts.jfrog.io/artifactory/api/pypi/pypi/simple/
%pip install nux_slack_bot
```

## Method 2: Install Directly from GitHub (Alternative)

```python
# In a Databricks cell:
%pip install git+https://github.com/jfan-nux/nux_slack_bot.git

# Or for a specific branch:
%pip install git+https://github.com/jfan-nux/nux_slack_bot.git@main

# Or for a specific commit:
%pip install git+https://github.com/jfan-nux/nux_slack_bot.git@abc1234
```

## Method 3: Build and Upload Wheel to Databricks Volumes

### Step 1: Build the wheel locally
```bash
# From your project root directory:
pip install build
python -m build

# This creates:
# - dist/nux_slack_bot-0.1.0-py3-none-any.whl
# - dist/nux_slack_bot-0.1.0.tar.gz
```

### Step 2: Upload to Databricks Volume
```python
# Upload wheel to a volume in Databricks
dbutils.fs.cp("file:/path/to/nux_slack_bot-0.1.0-py3-none-any.whl", 
              "/Volumes/my_catalog/my_schema/my_volume/nux_slack_bot-0.1.0-py3-none-any.whl")
```

### Step 3: Install from Volume
```python
%pip install /Volumes/my_catalog/my_schema/my_volume/nux_slack_bot-0.1.0-py3-none-any.whl
```

## Method 4: Build and Install from Local Files

```python
# In Databricks, if you have the source files:
%pip install /path/to/nux_slack_bot/

# Or in editable mode for development:
%pip install -e /path/to/nux_slack_bot/
```

## Method 5: Cluster-Level Installation

### Via Cluster Libraries UI:
1. Go to your cluster configuration
2. Click "Libraries" tab
3. Click "Install New"
4. Choose "PyPI" and enter: `git+https://github.com/jfan-nux/nux_slack_bot.git`

### Via Init Script:
Create an init script that installs the package:
```bash
#!/bin/bash
pip install git+https://github.com/jfan-nux/nux_slack_bot.git
```

## Usage After Installation

```python
# Import the package
import nux_slack_bot

# Use specific modules
from nux_slack_bot.utils.snowflake_connection import SnowflakeHook
from nux_slack_bot.experiment_runner.analysis import run_analysis

# Use the Snowflake connection
with SnowflakeHook() as hook:
    df = hook.query_snowflake("SELECT * FROM my_table LIMIT 10")
    print(df.head())

# Run experiments
from nux_slack_bot import run_experiments
run_experiments.main()
```

## Environment Variables

Make sure to set these in your Databricks environment:
```python
# Set via Databricks secrets or environment
import os
os.environ['SNOWFLAKE_ACCOUNT'] = 'your_account'
os.environ['SNOWFLAKE_USER'] = 'your_user'
# ... etc
```

## Troubleshooting

### Common Issues:
1. **Permission errors**: Use the GitHub installation method instead of cloning
2. **Missing dependencies**: Install with `%pip install -r requirements.txt`
3. **Import errors**: Restart your Python kernel after installation

### Verify Installation:
```python
import nux_slack_bot
print(f"✅ nux_slack_bot version: {nux_slack_bot.__version__}")
print(f"📁 Package location: {nux_slack_bot.__file__}")
```

### Check Available Modules:
```python
import pkgutil
import nux_slack_bot

for importer, modname, ispkg in pkgutil.walk_packages(nux_slack_bot.__path__, 
                                                      nux_slack_bot.__name__ + "."):
    print(f"📦 {modname}")
```
