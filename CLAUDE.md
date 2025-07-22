# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an AWS Data Lake hands-on project that demonstrates enterprise-level data lake architecture using a multi-layered approach (Raw → Clean → Analytics). The project is written primarily in Bash scripts, Python (PySpark), SQL, and CloudFormation YAML templates.

## Key Development Commands

### Main Deployment Commands
```bash
# One-click deployment (basic infrastructure only)
./scripts/deploy-all.sh

# Full deployment with EMR cluster and analytics
./scripts/deploy-all.sh --with-emr --with-analytics

# Manual step-by-step deployment
./scripts/setup-env.sh

# Create EMR cluster separately
./scripts/create-emr-cluster.sh [--key-name KEY] [--subnet-id SUBNET]

# Submit PySpark analytics job
./scripts/submit_pyspark_job.sh
```

### Resource Management
```bash
# Check resource status and costs
./scripts/cost-optimization.sh

# Clean up all resources (recommended approach)
./scripts/cleanup.sh --force --deep-clean --retry-failed

# Check remaining resources
./scripts/utils/check-resources.sh
```

### Configuration Management
```bash
# Create local config (copy and customize)
cp configs/config.env configs/config.local.env

# Export environment variables (auto-generated)
source configs/env-vars.sh
```

## Architecture Overview

### Data Flow Pipeline
1. **Raw Layer** (Bronze): S3 bucket receives raw CSV data
2. **Clean Layer** (Silver): Processed via Glue Crawler and DataBrew
3. **Analytics Layer** (Gold): EMR PySpark creates aggregated datasets
4. **Query Layer**: Amazon Athena enables SQL analytics

### Core AWS Services
- **Storage**: Amazon S3 (3-layer hierarchy)
- **Catalog**: AWS Glue (Data Catalog, Crawlers, DataBrew)
- **Processing**: Amazon EMR (PySpark jobs)
- **Analytics**: Amazon Athena (serverless SQL)
- **Governance**: AWS Lake Formation (permissions, security)
- **Infrastructure**: CloudFormation (IaC templates)

### Key Components
- **Configuration**: `configs/config.env` - centralized project settings
- **Infrastructure**: `templates/*.yaml` - CloudFormation templates
- **Scripts**: `scripts/` - deployment automation and utilities
- **Analytics**: `scripts/pyspark_analytics.py` - PySpark data processing
- **Sample Data**: `sample-data/` - E-commerce dataset (customers, orders, products, order_items)

## Important File Locations

### Configuration Files
- `configs/config.env` - Main configuration template
- `configs/config.local.env` - Local overrides (created by user)
- `configs/env-vars.sh` - Auto-generated environment variables

### Core Scripts
- `scripts/deploy-all.sh` - Master deployment orchestrator
- `scripts/setup-env.sh` - Base infrastructure deployment
- `scripts/cleanup.sh` - Complete resource cleanup
- `scripts/cost-optimization.sh` - Cost monitoring and optimization
- `scripts/pyspark_analytics.py` - Main data processing logic

### Infrastructure Templates
- `templates/s3-storage-layer.yaml` - S3 buckets and lifecycle policies
- `templates/iam-roles-policies.yaml` - IAM roles and permissions
- `templates/lake-formation-simple.yaml` - Primary Lake Formation setup
- `templates/glue-catalog.yaml` - Glue database and crawlers

### Analytics Resources
- `scripts/athena_queries.sql` - Sample SQL queries and table definitions
- `scripts/utils/table_schemas.json` - Glue table schema definitions

## Development Practices

### Configuration Management
- Always use `configs/config.local.env` for local customizations
- Never commit sensitive information like AWS credentials
- The deploy script auto-discovers EC2 key pairs and VPC subnets when not specified

### Resource Naming Convention
- Format: `${PROJECT_PREFIX}-${RESOURCE_TYPE}-${ENVIRONMENT}`
- Example: `dl-handson-raw-dev` (S3 bucket)
- Default prefix: `dl-handson`, environment: `dev`

### Cost Optimization
- EMR clusters are the primary cost driver (~70-80% of costs)
- Always run cleanup script after experiments: `./scripts/cleanup.sh --force --deep-clean --retry-failed`
- Use Spot instances for EMR to reduce costs by 60-70%
- Enable `--deep-clean` to remove S3 versioned objects and delete markers

### Error Recovery
- Failed CloudFormation stacks can be retried with `--retry-failed` option
- Lake Formation resources may cause stack deletion failures - the retry mechanism handles these
- Check resource status with `./scripts/utils/check-resources.sh`

## Testing and Validation

### Data Quality Verification
```sql
-- Basic data validation in Athena
SELECT COUNT(*) FROM "dl-handson-db".customers;
SELECT * FROM "dl-handson-db".orders LIMIT 10;
```

### Resource Validation
```bash
# Verify S3 buckets
aws s3 ls | grep dl-handson

# Check Glue database
aws glue get-databases

# Verify EMR cluster
aws emr list-clusters --active
```

## Common Issues and Solutions

### EMR Access
- EC2 key pairs are auto-discovered or created
- Private key files (*.pem) are saved locally when created
- Default VPC subnets are used automatically

### Lake Formation Permissions
- The project uses service-linked roles for simplified setup
- Fallback to custom roles if service-linked roles fail
- `lake-formation-simple.yaml` is the primary template

### S3 Cleanup
- Always use `--deep-clean` to remove versioned objects
- S3 lifecycle policies automatically manage object transitions
- Delete markers require explicit cleanup

## Security Considerations

- IAM roles follow principle of least privilege
- Lake Formation provides fine-grained data access control
- S3 buckets have encryption enabled by default
- CloudTrail logging is integrated for audit trails

## Sample Data Schema

The project includes e-commerce sample data:
- **customers**: Customer profiles and segments
- **products**: Product catalog with categories and pricing
- **orders**: Order transactions with status and amounts
- **order_items**: Line-item details with quantities and pricing

This multi-table schema enables complex analytical queries and joins across the data lake layers.