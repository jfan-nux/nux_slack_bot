"""
NUX Slack Bot - Experiment analysis and reporting automation.

This package provides tools for:
- Running and analyzing experiments
- Connecting to Snowflake for data retrieval
- Automating Slack reporting
- Managing experiment metadata and configurations
"""

__version__ = "0.1.0"
__author__ = "Fiona Fan"
__email__ = "fiona.fan@doordash.com"

# Import main modules for easy access
from . import utils
from . import config
from . import experiment_runner
from . import services
from . import integrations

__all__ = [
    "utils",
    "config", 
    "experiment_runner",
    "services",
    "integrations",
]
