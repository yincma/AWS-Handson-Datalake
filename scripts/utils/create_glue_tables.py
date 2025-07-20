#!/usr/bin/env python3
"""
Create Glue tables for AWS Data Lake
This script reads table schemas from JSON and creates Glue tables
"""

import boto3
import json
import os
import sys

def create_glue_tables():
    # Get environment variables
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    database_name = os.environ.get('GLUE_DATABASE_NAME')
    bucket_name = os.environ.get('RAW_BUCKET_NAME')
    
    if not database_name or not bucket_name:
        print("Error: Required environment variables not set")
        print("Please set GLUE_DATABASE_NAME and RAW_BUCKET_NAME")
        sys.exit(1)
    
    # Initialize Glue client
    glue = boto3.client('glue', region_name=aws_region)
    
    # Load table schemas
    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_file = os.path.join(script_dir, 'table_schemas.json')
    
    try:
        with open(schema_file, 'r') as f:
            schemas = json.load(f)
    except Exception as e:
        print(f"Error loading table schemas: {e}")
        sys.exit(1)
    
    # Create tables
    for table in schemas['tables']:
        table_input = {
            'Name': table['name'],
            'StorageDescriptor': {
                'Columns': table['columns'],
                'Location': f's3://{bucket_name}/ecommerce/{table["name"]}/',
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {
                        'field.delim': ',',
                        'skip.header.line.count': '1'
                    }
                }
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'csv',
                'skip.header.line.count': '1'
            }
        }
        
        try:
            glue.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"✓ Created table {table['name']}")
        except glue.exceptions.AlreadyExistsException:
            print(f"  Table {table['name']} already exists")
        except Exception as e:
            print(f"✗ Error creating table {table['name']}: {e}")

if __name__ == "__main__":
    create_glue_tables()