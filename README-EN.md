# AWS Data Lake Comprehensive Hands-on Project v2.1

**Author: mayinchen**

## Project Overview

This project is a practical hands-on project for building an enterprise-level data lake platform from scratch based on AWS cloud services. **v2.1 provides an optimized modular architecture and unified CLI management system.**

The multi-tier architecture design (Raw → Clean → Analytics) implements a complete data processing pipeline for data collection, storage, transformation, and analysis.

## 🆕 v2.1 New Features

- **Unified CLI Management**: Centralized system management via `datalake` command
- **Modular Architecture**: Highly independent component design with parallel deployment
- **Simplified Configuration**: Simplified permission management with Lake Formation Simple
- **Enterprise-level Reliability**: Comprehensive error handling and retry logic
- **Advanced Monitoring**: CloudTrail integrated security monitoring and cost optimization
- **Automated Deployment**: Intelligent resource management considering dependencies

## Table of Contents
- [Technical Architecture](#technical-architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Unified CLI Usage Guide](#unified-cli-usage-guide)
- [System Configuration](#system-configuration)
- [Module Details](#module-details)
- [Operations Management](#operations-management)
- [Troubleshooting](#troubleshooting)

## Technical Architecture

![AWS Data Lake Architecture](./Arch.drawio.svg)

### Core Service Stack
- **Storage Layer**: Amazon S3 (3-tier storage + lifecycle management)
- **Data Catalog**: AWS Glue (Crawler + Data Catalog)
- **Data Governance**: AWS Lake Formation (simplified permission control)
- **Compute Engine**: Amazon EMR (Spark distributed processing)
- **Analytics Engine**: Amazon Athena (serverless SQL queries)
- **Monitoring & Cost Management**: CloudTrail + AWS Budgets + CloudWatch

### Data Flow Architecture

<div align="center">

#### 🌊 **Data Lake 3-Tier Architecture**

</div>

```mermaid
graph TB
    %% Style definitions
    classDef rawLayer fill:#FF6B6B,stroke:#C92A2A,stroke-width:2px,color:#FFF
    classDef cleanLayer fill:#51CF66,stroke:#2B8A3E,stroke-width:2px,color:#FFF
    classDef analyticsLayer fill:#339AF0,stroke:#1864AB,stroke-width:2px,color:#FFF
    classDef process fill:#845EF7,stroke:#5F3DC4,stroke-width:2px,color:#FFF
    
    %% Components
    Sources["📊 Data Sources<br/>(CSV Files)"]
    Raw["🗃️ Raw Layer<br/>(Raw Data Storage)"]
    Clean["🧹 Clean Layer<br/>(Cleansed Data)"]
    Analytics["📈 Analytics Layer<br/>(Analytics Results)"]
    
    %% Processing
    Crawl["🔍 Glue Crawler<br/>(Auto Schema Discovery)"]
    DataBrew["🔧 Glue DataBrew<br/>(Data Cleansing)"]
    EMR["⚡ EMR PySpark<br/>(Data Aggregation)"]
    Athena["🔍 Athena<br/>(SQL Analytics)"]
    
    %% Data flow
    Sources -->|"Upload"| Raw
    Raw -->|"Catalog"| Crawl
    Crawl -->|"Schema"| Clean
    Raw -->|"Clean"| DataBrew
    DataBrew -->|"Transform"| Clean
    Clean -->|"Process"| EMR
    EMR -->|"Aggregate"| Analytics
    Analytics -->|"Query"| Athena
    
    %% Apply styles
    class Raw rawLayer
    class Clean cleanLayer
    class Analytics analyticsLayer
    class Crawl,DataBrew,EMR,Athena process
```

## Prerequisites

### 1. Required Environment
- Python 3.8+
- AWS CLI v2+
- jq command line tool
- Bash 4.0+ (macOS users need to update)

### 2. AWS Account Requirements
- Administrator access or equivalent permissions
- Service quotas: S3, Glue, EMR, Lake Formation, CloudFormation

### 3. Local Environment Setup
```bash
# Clone the project
git clone https://github.com/yourusername/aws-datalake-handson.git
cd aws-datalake-handson

# Install dependencies
pip install -r requirements.txt

# AWS CLI configuration
aws configure
```

## Quick Start

### 1. Basic Deployment (Infrastructure Only)
```bash
# Execute basic deployment
./scripts/datalake deploy

# Check deployment status
./scripts/datalake status
```

### 2. Full Deployment (Including Analytics)
```bash
# Full deployment including EMR cluster and analytics jobs
./scripts/datalake deploy --with-emr --with-analytics

# Monitor deployment progress
./scripts/datalake monitor
```

### 3. Cleanup Resources
```bash
# Complete cleanup of all resources
./scripts/datalake destroy --force --deep-clean
```

## Unified CLI Usage Guide

The new unified CLI provides centralized management for all data lake operations:

### Basic Commands
```bash
# Display help
./scripts/datalake help

# Check system status
./scripts/datalake status

# View configuration
./scripts/datalake config
```

### Deployment Management
```bash
# Basic deployment
./scripts/datalake deploy

# Deployment with specific modules
./scripts/datalake deploy --modules s3,iam,glue

# Deployment with EMR
./scripts/datalake deploy --with-emr

# Execute analytics job
./scripts/datalake analytics run
```

### Monitoring and Management
```bash
# Real-time monitoring
./scripts/datalake monitor

# Cost report
./scripts/datalake cost-report

# Resource validation
./scripts/datalake validate
```

## System Configuration

### 1. Configuration File Structure
```
configs/
├── config.env          # Main configuration template
├── config.local.env    # Local overrides (gitignored)
└── env-vars.sh        # Auto-generated environment variables
```

### 2. Key Configuration Items
```bash
# Project settings
PROJECT_PREFIX=dl-handson    # Resource name prefix
ENVIRONMENT=dev              # Environment (dev/staging/prod)
AWS_REGION=us-east-1        # AWS region

# S3 configuration
S3_VERSIONING=Enabled       # Version control
S3_LIFECYCLE_ENABLED=true   # Lifecycle management

# EMR configuration
EMR_INSTANCE_TYPE=m5.xlarge # Instance type
EMR_INSTANCE_COUNT=3        # Number of instances
```

### 3. Environment Variable Management
```bash
# Load configuration
source configs/env-vars.sh

# Validate configuration
./scripts/lib/config/validator.sh
```

## Module Details

### 1. Core Infrastructure Module
- **S3 Storage**: Multi-layer bucket architecture with encryption
- **IAM Roles**: Least-privilege security policies
- **VPC Configuration**: Network isolation and security groups

### 2. Data Catalog Module
- **Glue Database**: Centralized metadata repository
- **Glue Crawlers**: Automatic schema discovery
- **Table Definitions**: Structured data catalog

### 3. Data Processing Module
- **EMR Cluster**: Managed Spark environment
- **PySpark Jobs**: Scalable data transformations
- **Job Orchestration**: Automated workflow management

### 4. Analytics Module
- **Athena Setup**: Serverless query engine
- **Query Optimization**: Partitioning and compression
- **Result Storage**: Query result management

### 5. Monitoring Module
- **CloudTrail**: Security audit logs
- **CloudWatch**: Performance metrics
- **Cost Monitoring**: Budget alerts and optimization

## Operations Management

### 1. Daily Operations
```bash
# Check system health
./scripts/datalake status --detailed

# View recent activities
./scripts/datalake monitor --last-24h

# Run scheduled analytics
./scripts/datalake analytics run --scheduled
```

### 2. Cost Optimization
```bash
# Generate cost report
./scripts/datalake cost-report --last-30-days

# Identify optimization opportunities
./scripts/datalake optimize suggest

# Apply optimizations
./scripts/datalake optimize apply
```

### 3. Security Management
```bash
# Security audit
./scripts/datalake security audit

# Update permissions
./scripts/datalake security update-permissions

# Rotate credentials
./scripts/datalake security rotate-keys
```

## Troubleshooting

### Common Issues

1. **Deployment Failures**
   ```bash
   # Check CloudFormation stack status
   ./scripts/datalake diagnose stacks
   
   # Retry failed deployments
   ./scripts/datalake deploy --retry-failed
   ```

2. **Permission Issues**
   ```bash
   # Verify IAM permissions
   ./scripts/datalake validate permissions
   
   # Fix Lake Formation permissions
   ./scripts/datalake fix-permissions
   ```

3. **EMR Cluster Issues**
   ```bash
   # Check cluster status
   ./scripts/datalake emr status
   
   # View cluster logs
   ./scripts/datalake emr logs
   ```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Run with verbose output
./scripts/datalake deploy -v
```

## Project Structure
```
aws-datalake-handson/
├── scripts/              # Deployment and management scripts
│   ├── datalake         # Unified CLI entry point
│   ├── core/           # Core modules
│   ├── lib/            # Shared libraries
│   └── utils/          # Utility scripts
├── templates/           # CloudFormation templates
├── configs/            # Configuration files
├── sample-data/        # Sample datasets
└── docs/              # Documentation
```

## Best Practices

1. **Security**
   - Use IAM roles instead of access keys
   - Enable S3 bucket encryption
   - Implement least-privilege access
   - Regular security audits

2. **Cost Optimization**
   - Use spot instances for EMR
   - Implement S3 lifecycle policies
   - Monitor and set budget alerts
   - Regular resource cleanup

3. **Performance**
   - Partition data appropriately
   - Use columnar formats (Parquet)
   - Optimize Spark configurations
   - Implement data compression

4. **Reliability**
   - Implement retry mechanisms
   - Use CloudFormation for IaC
   - Regular backups
   - Monitoring and alerting

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to our repository.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- GitHub Issues: [Project Issues](https://github.com/yourusername/aws-datalake-handson/issues)
- Documentation: [Project Wiki](https://github.com/yourusername/aws-datalake-handson/wiki)

---
**Author**: mayinchen  
**Version**: 2.1  
**Last Updated**: 2024