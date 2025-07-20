# CloudFormation Templates

This directory contains CloudFormation templates for deploying the AWS Data Lake infrastructure.

## Templates Overview

### Core Infrastructure
- **`s3-storage-layer.yaml`** - S3 buckets for Raw/Clean/Analytics layers
- **`iam-roles-policies.yaml`** - IAM roles and policies for all services
- **`glue-catalog.yaml`** - Glue Data Catalog configuration

### Lake Formation Templates
- **`lake-formation-simple.yaml`** ⭐ **[PRIMARY]** - Simplified setup using service-linked roles
- **`lake-formation.yaml`** - Complete setup with custom roles and detailed permissions

> **Note**: The setup script uses `lake-formation-simple.yaml` by default and falls back to the complete version if needed.

### Monitoring
- **`cost-monitoring.yaml`** - Cost alerts and monitoring with SNS notifications

## Usage

These templates are automatically deployed by the setup scripts. You don't need to deploy them manually.

## Template Selection

The deployment script automatically chooses the appropriate Lake Formation template:
1. **Primary**: Uses `lake-formation-simple.yaml` (easier deployment)
2. **Fallback**: Uses `lake-formation.yaml` if simple version fails