#!/bin/bash
# Unified Naming Convention for AWS Data Lake Project
# This file ensures consistent naming across all components

# Function to get stack name
get_stack_name() {
    local module="$1"
    local project_prefix="${PROJECT_PREFIX:-dl-handson-v2}"
    local environment="${ENVIRONMENT:-dev}"
    
    # Unified format: project-stack-module-env
    echo "${project_prefix}-stack-${module}-${environment}"
}

# Function to get export name
get_export_name() {
    local module="$1"
    local resource="$2"
    local project_prefix="${PROJECT_PREFIX:-dl-handson-v2}"
    local environment="${ENVIRONMENT:-dev}"
    
    # Unified format: project-stack-module-env-Resource
    echo "${project_prefix}-stack-${module}-${environment}-${resource}"
}

# Function to get resource name (buckets, roles, etc)
get_resource_name() {
    local resource_type="$1"
    local project_prefix="${PROJECT_PREFIX:-dl-handson-v2}"
    local environment="${ENVIRONMENT:-dev}"
    
    # Unified format: project-resource-env
    echo "${project_prefix}-${resource_type}-${environment}"
}

# Stack name constants for easy reference
export STACK_NAME_S3=$(get_stack_name "s3")
export STACK_NAME_IAM=$(get_stack_name "iam")
export STACK_NAME_GLUE=$(get_stack_name "glue")
export STACK_NAME_LAKEFORMATION=$(get_stack_name "lakeformation")
export STACK_NAME_MONITORING=$(get_stack_name "monitoring")

# Export name patterns
export S3_EXPORT_PREFIX=$(get_export_name "s3" "")
export IAM_EXPORT_PREFIX=$(get_export_name "iam" "")
export GLUE_EXPORT_PREFIX=$(get_export_name "glue" "")