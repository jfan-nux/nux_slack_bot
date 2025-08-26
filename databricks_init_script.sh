#!/bin/bash

# Databricks Init Script to Export Environment Variables from Secrets
# Upload this file to DBFS and reference it in your cluster configuration

echo "Setting up environment variables from Databricks secrets..."

# Export sensitive variables from secrets
export CODA_API_KEY=$(databricks secrets get --scope nux-slack-bot --key CODA_API_KEY)
export SNOWFLAKE_PASSWORD=$(databricks secrets get --scope nux-slack-bot --key SNOWFLAKE_PASSWORD)
export PORTKEY_API_KEY=$(databricks secrets get --scope nux-slack-bot --key PORTKEY_API_KEY)
export PORTKEY_OPENAI_VIRTUAL_KEY=$(databricks secrets get --scope nux-slack-bot --key PORTKEY_OPENAI_VIRTUAL_KEY)
export OPENAI_VIRTUAL_KEY=$(databricks secrets get --scope nux-slack-bot --key OPENAI_VIRTUAL_KEY)

# Export non-sensitive variables directly
export MODE_WORKSPACE="doordash"
export SNOWFLAKE_ACCOUNT="doordash.snowflakecomputing.com"
export SNOWFLAKE_USER="fiona.fan"
export SNOWFLAKE_WAREHOUSE="TEAM_DATA_ANALYTICS"
export SNOWFLAKE_DATABASE="proddb"
export SNOWFLAKE_SCHEMA="fionafan"
export PORTKEY_BASE_URL="https://cybertron-service-gateway.doordash.team/v1"


echo "Environment variables configured successfully!"
