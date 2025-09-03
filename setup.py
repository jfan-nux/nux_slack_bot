#!/usr/bin/env python3
"""
Setup configuration for nux_slack_bot package.
Makes the repository installable via pip as a wheel.
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "NUX Slack Bot - Experiment analysis and reporting automation"

# Read requirements from requirements.txt
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="nux_slack_bot",
    version="0.1.1",
    description="NUX Slack Bot - Experiment analysis and reporting automation",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="Fiona Fan",
    author_email="fiona.fan@doordash.com",
    url="https://github.com/jfan-nux/nux_slack_bot",
    
    # Package discovery - treat root directory as the package
    packages=find_packages(where="."),
    package_dir={"nux_slack_bot": "."},
    include_package_data=True,
    
    # Dependencies
    install_requires=read_requirements(),
    
    # Python version requirement
    python_requires=">=3.8",
    
    # Package data - include YAML files and SQL scripts
    package_data={
        "nux_slack_bot": [
            "data_models/*.yaml",
            "sql_scripts/*.sql",
            "sql_scripts/*/*.sql",
            "experiment_runner/rendered_queries/*/*.sql",
        ]
    },
    
    # Entry points for command-line tools
    entry_points={
        "console_scripts": [
            "run-experiments=run_experiments:main",
            "create-metrics-table=create_combined_metrics_table:main",
        ]
    },
    
    # Classifiers for PyPI
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    
    # Additional metadata
    keywords="databricks, experiments, analytics, slack, automation",
    project_urls={
        "Bug Reports": "https://github.com/jfan-nux/nux_slack_bot/issues",
        "Source": "https://github.com/jfan-nux/nux_slack_bot",
    },
)
