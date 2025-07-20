#!/bin/bash

# AWS Data Lake Resource Check Script
# This script checks for any remaining data lake resources

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_step() { echo -e "\n${BLUE}[STEP]${NC} $1"; }
print_success() { echo -e "${GREEN}✅${NC} $1"; }

# Load configuration
if [[ -f "configs/env-vars.sh" ]]; then
    source configs/env-vars.sh
elif [[ -f "configs/config.env" ]]; then
    source configs/config.env
else
    PROJECT_PREFIX="dl-handson"
    ENVIRONMENT="dev"
    AWS_REGION="us-east-1"
fi

echo "=========================================="
echo "AWS Data Lake Resource Check"
echo "=========================================="
echo "Project: $PROJECT_PREFIX"
echo "Environment: $ENVIRONMENT"
echo "Region: $AWS_REGION"
echo "=========================================="

# Check S3 buckets
print_step "Checking S3 buckets..."
buckets=(
    "${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
    "${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
    "${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
    "${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}"
)

found_buckets=0
for bucket in "${buckets[@]}"; do
    if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        print_warning "Found bucket: $bucket"
        # Check for versioning
        versioning=$(aws s3api get-bucket-versioning --bucket "$bucket" --query 'Status' --output text 2>/dev/null)
        if [[ "$versioning" == "Enabled" ]]; then
            print_info "  Versioning: Enabled"
            # Count versions
            version_count=$(aws s3api list-object-versions --bucket "$bucket" --query 'length(Versions)' --output text 2>/dev/null || echo "0")
            delete_marker_count=$(aws s3api list-object-versions --bucket "$bucket" --query 'length(DeleteMarkers)' --output text 2>/dev/null || echo "0")
            print_info "  Object versions: $version_count"
            print_info "  Delete markers: $delete_marker_count"
        fi
        found_buckets=$((found_buckets + 1))
    fi
done

if [[ $found_buckets -eq 0 ]]; then
    print_success "No S3 buckets found"
fi

# Check CloudFormation stacks
print_step "Checking CloudFormation stacks..."
stacks=(
    "datalake-s3-storage-${ENVIRONMENT}"
    "datalake-iam-roles-${ENVIRONMENT}"
    "datalake-lake-formation-${ENVIRONMENT}"
    "datalake-cost-monitoring-${ENVIRONMENT}"
)

found_stacks=0
for stack in "${stacks[@]}"; do
    status=$(aws cloudformation describe-stacks --stack-name "$stack" --query 'Stacks[0].StackStatus' --output text 2>/dev/null)
    if [[ -n "$status" ]]; then
        if [[ "$status" == "DELETE_FAILED" ]]; then
            print_error "Found stack in DELETE_FAILED state: $stack"
            # Show failed resources
            failed_resources=$(aws cloudformation describe-stack-events \
                --stack-name "$stack" \
                --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[ResourceType, ResourceStatusReason]" \
                --output text 2>/dev/null)
            if [[ -n "$failed_resources" ]]; then
                echo "$failed_resources" | while IFS=$'\t' read -r resource_type reason; do
                    print_info "  Failed resource: $resource_type"
                    print_info "  Reason: $reason"
                done
            fi
        else
            print_warning "Found stack: $stack (Status: $status)"
        fi
        found_stacks=$((found_stacks + 1))
    fi
done

if [[ $found_stacks -eq 0 ]]; then
    print_success "No CloudFormation stacks found"
fi

# Check Glue resources
print_step "Checking Glue resources..."
database="${PROJECT_PREFIX}-db"
if aws glue get-database --name "$database" &>/dev/null; then
    print_warning "Found Glue database: $database"
    # Count tables
    table_count=$(aws glue get-tables --database-name "$database" --query 'length(TableList)' --output text 2>/dev/null || echo "0")
    print_info "  Tables: $table_count"
else
    print_success "No Glue database found"
fi

# Check EMR clusters
print_step "Checking EMR clusters..."
cluster_count=$(aws emr list-clusters --active --query "length(Clusters[?Name=='${PROJECT_PREFIX}-emr-cluster-${ENVIRONMENT}'])" --output text 2>/dev/null || echo "0")
if [[ $cluster_count -gt 0 ]]; then
    print_warning "Found $cluster_count active EMR cluster(s)"
else
    print_success "No active EMR clusters found"
fi

# Check EC2 key pairs
print_step "Checking EC2 key pairs..."
key_pairs=$(aws ec2 describe-key-pairs --query "KeyPairs[?contains(KeyName, '${PROJECT_PREFIX}-emr-key')].KeyName" --output text 2>/dev/null)
if [[ -n "$key_pairs" ]]; then
    print_warning "Found EC2 key pairs:"
    for key in $key_pairs; do
        print_info "  - $key"
    done
    
    # Check for local .pem files
    local_pem_files=$(ls ${PROJECT_PREFIX}-emr-key-*.pem 2>/dev/null || true)
    if [[ -n "$local_pem_files" ]]; then
        print_warning "Found local key files:"
        for pem in $local_pem_files; do
            print_info "  - $pem"
        done
    fi
else
    print_success "No project EC2 key pairs found"
fi

# Summary
echo
echo "=========================================="
echo "Resource Check Complete"
echo "=========================================="

if [[ $found_buckets -gt 0 ]] || [[ $found_stacks -gt 0 ]]; then
    print_warning "Found remaining resources. Run cleanup script to remove them:"
    echo "./scripts/cleanup.sh --force"
    echo
    echo "For stuck resources (DELETE_FAILED stacks), use:"
    echo "./scripts/cleanup.sh --force --retry-failed"
    echo
    echo "For deep clean (including S3 versions), use:"
    echo "./scripts/cleanup.sh --force --deep-clean"
else
    print_success "No data lake resources found. Environment is clean!"
fi