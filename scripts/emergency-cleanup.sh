#!/bin/bash
# Emergency cleanup script for DELETE_FAILED stacks
# This script handles S3 buckets with versioned objects

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

print_step "Emergency Cleanup for DELETE_FAILED Stacks"

# Function to empty S3 bucket including all versions
empty_s3_bucket_all_versions() {
    local bucket_name="$1"
    
    if ! aws s3api head-bucket --bucket "$bucket_name" 2>/dev/null; then
        print_info "Bucket $bucket_name does not exist"
        return 0
    fi
    
    print_info "Emptying bucket: $bucket_name (including all versions)"
    
    # Delete all object versions
    print_info "Deleting all object versions..."
    local versions=$(aws s3api list-object-versions --bucket "$bucket_name" \
        --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
        --output json 2>/dev/null || echo '{"Objects": []}')
    
    if [[ $(echo "$versions" | jq '.Objects | length') -gt 0 ]]; then
        aws s3api delete-objects --bucket "$bucket_name" --delete "$versions" >/dev/null || true
    fi
    
    # Delete all delete markers
    print_info "Deleting all delete markers..."
    local markers=$(aws s3api list-object-versions --bucket "$bucket_name" \
        --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
        --output json 2>/dev/null || echo '{"Objects": []}')
    
    if [[ $(echo "$markers" | jq '.Objects | length') -gt 0 ]]; then
        aws s3api delete-objects --bucket "$bucket_name" --delete "$markers" >/dev/null || true
    fi
    
    # Delete any remaining current versions
    print_info "Deleting any remaining objects..."
    aws s3 rm "s3://${bucket_name}" --recursive --force 2>/dev/null || true
    
    print_success "Bucket $bucket_name emptied"
}

# Step 1: Check for DELETE_FAILED stacks
print_info "Checking for stacks in DELETE_FAILED state..."

DELETE_FAILED_STACKS=$(aws cloudformation list-stacks \
    --stack-status-filter DELETE_FAILED \
    --query "StackSummaries[?contains(StackName, 'dl-handson')].StackName" \
    --output text)

if [[ -z "$DELETE_FAILED_STACKS" ]]; then
    print_info "No stacks in DELETE_FAILED state found"
else
    print_warning "Found DELETE_FAILED stacks:"
    for stack in $DELETE_FAILED_STACKS; do
        print_info "  - $stack"
    done
    
    # Step 2: Handle S3 stack specifically
    if [[ "$DELETE_FAILED_STACKS" =~ "s3" ]]; then
        print_step "Cleaning up S3 resources..."
        
        # Get bucket names from the project prefix
        PROJECT_PREFIX="${PROJECT_PREFIX:-dl-handson-v2}"
        ENVIRONMENT="${ENVIRONMENT:-dev}"
        
        # List all buckets that match our pattern
        for bucket_type in raw clean analytics athena-results; do
            bucket_name="${PROJECT_PREFIX}-${bucket_type}-${ENVIRONMENT}"
            empty_s3_bucket_all_versions "$bucket_name"
        done
    fi
    
    # Step 3: Force delete stacks
    print_step "Force deleting DELETE_FAILED stacks..."
    
    for stack in $DELETE_FAILED_STACKS; do
        print_info "Attempting to delete stack: $stack"
        
        # First, try normal delete
        aws cloudformation delete-stack --stack-name "$stack" 2>/dev/null || true
        
        # If it's still in DELETE_FAILED, we may need to retain resources
        sleep 5
        status=$(aws cloudformation describe-stacks --stack-name "$stack" \
            --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "DELETED")
        
        if [[ "$status" == "DELETE_FAILED" ]]; then
            print_warning "Stack still in DELETE_FAILED state. Manual intervention may be required."
            print_info "Try running: aws cloudformation delete-stack --stack-name $stack --retain-resources"
        else
            print_success "Stack deletion initiated: $stack"
        fi
    done
    
    # Wait for deletions
    print_info "Waiting for stack deletions to complete..."
    for stack in $DELETE_FAILED_STACKS; do
        aws cloudformation wait stack-delete-complete --stack-name "$stack" 2>/dev/null || true
    done
fi

# Step 4: Clean up any ROLLBACK_COMPLETE stacks
print_info "Checking for ROLLBACK_COMPLETE stacks..."

ROLLBACK_STACKS=$(aws cloudformation list-stacks \
    --stack-status-filter ROLLBACK_COMPLETE \
    --query "StackSummaries[?contains(StackName, 'dl-handson')].StackName" \
    --output text)

if [[ -n "$ROLLBACK_STACKS" ]]; then
    print_warning "Found ROLLBACK_COMPLETE stacks:"
    for stack in $ROLLBACK_STACKS; do
        print_info "  - $stack"
        aws cloudformation delete-stack --stack-name "$stack"
    done
    
    # Wait for deletions
    for stack in $ROLLBACK_STACKS; do
        aws cloudformation wait stack-delete-complete --stack-name "$stack" 2>/dev/null || true
    done
fi

print_success "Emergency cleanup completed!"
print_info "You can now run: ./scripts/cli/datalake deploy --full"