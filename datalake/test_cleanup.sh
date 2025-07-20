#!/bin/bash

# 测试改进后的清理脚本逻辑
# 仅用于验证，不执行实际删除

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "\n${BLUE}[STEP]${NC} $1"
}

# Load configuration
source configs/config.env 2>/dev/null || {
    print_error "Could not load config.env"
    exit 1
}

PROJECT_PREFIX=${PROJECT_PREFIX:-dl-handson}
AWS_REGION=${AWS_REGION:-us-east-1}
ENVIRONMENT=${ENVIRONMENT:-dev}

print_step "Testing cleanup script logic improvements"

print_info "Project: $PROJECT_PREFIX"
print_info "Region: $AWS_REGION"
print_info "Environment: $ENVIRONMENT"

# Test S3 bucket discovery
print_step "Testing S3 bucket discovery..."
discovered_buckets=$(aws s3api list-buckets \
    --query "Buckets[?contains(Name, '$PROJECT_PREFIX')].Name" --output text 2>/dev/null || true)

if [[ -n "$discovered_buckets" && "$discovered_buckets" != "None" ]]; then
    print_info "Discovered buckets: $discovered_buckets"
else
    print_info "No buckets found with prefix: $PROJECT_PREFIX"
fi

# Test CloudFormation stack discovery
print_step "Testing CloudFormation stack discovery..."
discovered_stacks=$(aws cloudformation list-stacks --region "$AWS_REGION" \
    --query "StackSummaries[?contains(StackName, '$PROJECT_PREFIX')].{Name:StackName,Status:StackStatus}" \
    --output text 2>/dev/null | grep -v "DELETE_COMPLETE" | awk '{print $1}' || true)

if [[ -n "$discovered_stacks" ]]; then
    print_info "Discovered stacks: $discovered_stacks"
else
    print_info "No active stacks found with prefix: $PROJECT_PREFIX"
fi

# Test specific stack existence
print_step "Testing stack existence checks..."
test_stacks=(
    "datalake-s3-storage-${ENVIRONMENT}"
    "datalake-iam-roles-${ENVIRONMENT}"
    "datalake-lake-formation-${ENVIRONMENT}"
)

for stack in "${test_stacks[@]}"; do
    if aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" &>/dev/null; then
        stack_status=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
            --query "Stacks[0].StackStatus" --output text 2>/dev/null)
        print_info "Stack $stack exists with status: $stack_status"
    else
        print_info "Stack $stack does not exist"
    fi
done

print_step "Cleanup logic test completed!"
print_info "The improved cleanup script should now handle missing resources correctly."