# Utility Scripts for AWS Data Lake

This directory contains modular utility scripts used by the main deployment scripts.

## Files

### create_glue_tables.py
- Creates Glue tables for the data lake
- Reads table schemas from `table_schemas.json`
- Requires environment variables: `AWS_REGION`, `GLUE_DATABASE_NAME`, `RAW_BUCKET_NAME`

### table_schemas.json
- Defines the schema for all data lake tables
- Contains column definitions for: customers, products, orders, order_items

### export_env_template.sh
- Template for generating environment variable export file
- Used by `setup-env.sh` to create `configs/env-vars.sh`

## Usage

These scripts are called automatically by the main deployment scripts.
Do not run them directly unless you know what you're doing.