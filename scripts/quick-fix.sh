#!/bin/bash
# Quick fix script for immediate deployment issues

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

print_step "Quick Fix for CloudFormation Deployment Issues"

# Step 1: Clean up failed stacks
print_info "Cleaning up failed stacks..."
for stack in dl-handson-v2-stack-lakeformation-dev dl-handson-v2-stack-glue-dev dl-handson-v2-stack-iam-dev; do
    if aws cloudformation describe-stacks --stack-name "$stack" &>/dev/null; then
        status=$(aws cloudformation describe-stacks --stack-name "$stack" --query 'Stacks[0].StackStatus' --output text)
        if [[ "$status" == "ROLLBACK_COMPLETE" || "$status" == "CREATE_FAILED" ]]; then
            print_info "Deleting failed stack: $stack"
            aws cloudformation delete-stack --stack-name "$stack"
            aws cloudformation wait stack-delete-complete --stack-name "$stack" || true
        fi
    fi
done

# Step 2: Deploy with consistent naming
print_info "Deploying with new system..."

# Option 1: Use new CLI (recommended)
if [[ -f "$SCRIPT_DIR/cli/datalake" ]]; then
    print_success "Using new CLI system"
    
    # Clean environment and deploy fresh
    export USE_UNIFIED_NAMING=true
    "$SCRIPT_DIR/cli/datalake" deploy --full
else
    # Option 2: Fallback to traditional deployment
    print_warning "CLI not found, using traditional deployment"
    
    # Ensure we use consistent naming
    export PROJECT_PREFIX="dl-handson-v2"
    export ENVIRONMENT="dev"
    
    # Deploy in correct order
    "$SCRIPT_DIR/setup-env.sh"
fi

print_success "Quick fix completed!"
print_info "Check deployment status with: ./scripts/cli/datalake status"