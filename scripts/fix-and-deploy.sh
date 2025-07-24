#!/bin/bash
# Final fix and deployment script
# This script cleans up failed stacks and performs a fresh deployment with correct naming

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

print_step "AWS Data Lake - Clean Fix and Deploy"

# Step 1: Clean up all existing stacks (both successful and failed)
print_info "Cleaning up existing stacks..."

# Get all data lake related stacks
ALL_STACKS=$(aws cloudformation list-stacks \
    --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE CREATE_FAILED ROLLBACK_COMPLETE ROLLBACK_FAILED \
    --query "StackSummaries[?contains(StackName, 'dl-handson') || contains(StackName, 'datalake')].StackName" \
    --output text)

if [[ -n "$ALL_STACKS" ]]; then
    print_info "Found existing stacks:"
    for stack in $ALL_STACKS; do
        print_info "  - $stack"
    done
    
    if confirm_action "Delete all existing stacks and start fresh?"; then
        for stack in $ALL_STACKS; do
            print_info "Deleting stack: $stack"
            aws cloudformation delete-stack --stack-name "$stack" || true
        done
        
        # Wait for deletions to complete
        print_info "Waiting for stack deletions to complete..."
        for stack in $ALL_STACKS; do
            aws cloudformation wait stack-delete-complete --stack-name "$stack" 2>/dev/null || true
        done
        
        print_success "All stacks deleted"
    else
        print_error "Deployment cancelled"
        exit 1
    fi
else
    print_info "No existing stacks found"
fi

# Step 2: Verify naming configuration
print_step "Verifying naming configuration..."

# Run the naming test
./scripts/utils/test_dynamic_naming.sh | grep -E "(S3 Stack:|IAM Stack:|Glue Stack:|Lake Formation Stack:)" || true

# Step 3: Deploy with new system
print_step "Deploying with unified naming system..."

# Ensure we're using the new CLI
if [[ -f "$SCRIPT_DIR/cli/datalake" ]]; then
    print_success "Using new CLI system"
    
    # Deploy all modules
    "$SCRIPT_DIR/cli/datalake" deploy --full
    
    # Check final status
    print_step "Checking deployment status..."
    "$SCRIPT_DIR/cli/datalake" status
else
    print_error "New CLI not found. Please ensure the modular system is properly installed."
    exit 1
fi

print_success "Deployment completed successfully!"
print_info "Next steps:"
print_info "  1. Verify deployment: ./scripts/cli/datalake status"
print_info "  2. Run validation: ./scripts/cli/datalake validate"
print_info "  3. Check costs: ./scripts/cli/datalake cost"