#!/bin/bash

# =============================================================================
# S3 Storage Module
# Version: 1.0.0
# Description: Manage S3 storage infrastructure for the data lake
# =============================================================================

# Load common utility library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly S3_STORAGE_MODULE_VERSION="1.0.0"

# Initialize configuration
load_config || true

# =============================================================================
# Module Configuration
# =============================================================================

# Load compatibility layer (if exists)
if [[ -f "$SCRIPT_DIR/../../lib/compatibility.sh" ]]; then
    source "$SCRIPT_DIR/../../lib/compatibility.sh"
fi

# Module dependencies - S3 storage is a basic module with no dependencies
# Compatibility support: check if legacy stack exists
if [[ -n "${USE_LEGACY_NAMING:-}" ]] || [[ -n "${MIGRATION_MODE:-}" ]]; then
    S3_STACK_NAME=$(get_stack_name "s3_storage" "${USE_LEGACY_NAMING:-false}")
else
    # Check if stack exists with any naming convention
    if existing_stack=$(check_stack_exists_any "s3_storage" 2>/dev/null); then
        S3_STACK_NAME="$existing_stack"
        print_debug "Using existing stack: $S3_STACK_NAME"
    else
        S3_STACK_NAME=$(get_stack_name "s3_storage" false)
    fi
fi

S3_TEMPLATE_FILE="${PROJECT_ROOT}/templates/s3-storage-layer.yaml"

# =============================================================================
# Required function implementations
# =============================================================================

s3_storage_validate() {
    print_info "Validating S3 storage module configuration"
    
    local validation_errors=0
    
    # Check required environment variables
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "Missing required environment variable: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # Check CloudFormation template file
    if [[ ! -f "$S3_TEMPLATE_FILE" ]]; then
        print_error "S3 template file does not exist: $S3_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    else
        print_debug "S3 template file exists: $S3_TEMPLATE_FILE"
    fi
    
    # Validate template syntax
    if aws cloudformation validate-template --template-body "file://$S3_TEMPLATE_FILE" &>/dev/null; then
        print_debug "S3 template syntax validation passed"
    else
        print_error "S3 template syntax validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "S3 storage module validation passed"
        return 0
    else
        print_error "S3 storage module validation failed with $validation_errors errors"
        return 1
    fi
}

s3_storage_deploy() {
    print_info "Deploying S3 storage module"
    
    # First validate configuration
    if ! s3_storage_validate; then
        print_error "Validation failed, cannot deploy"
        return 1
    fi
    
    # Prepare parameters
    local stack_params=(
        "ParameterKey=ProjectPrefix,ParameterValue=$PROJECT_PREFIX"
        "ParameterKey=Environment,ParameterValue=$ENVIRONMENT"
    )
    
    # Check if stack exists and handle ROLLBACK_COMPLETE status
    if check_stack_exists "$S3_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$S3_STACK_NAME")
        
        # Check if status is ROLLBACK_COMPLETE or DELETE_FAILED, if so delete stack
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" || "$stack_status" == "DELETE_FAILED" ]]; then
            print_warning "S3 stack is in $stack_status state, need to delete first"
            print_info "Deleting S3 stack: $S3_STACK_NAME"
            
            # First empty S3 buckets
            s3_storage_empty_buckets
            
            if aws cloudformation delete-stack --stack-name "$S3_STACK_NAME"; then
                print_info "Waiting for stack deletion to complete..."
                
                # Wait for deletion to complete
                local timeout=900  # 15 minutes
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$S3_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "S3 stack deletion completed"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "S3 stack deletion failed"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "S3 stack deletion timed out"
                    return 1
                fi
            else
                print_error "Failed to start S3 stack deletion"
                return 1
            fi
            
            # After deletion is complete, create new stack
            print_info "Creating new S3 stack: $S3_STACK_NAME"
            
            if retry_aws_command aws cloudformation create-stack \
                --stack-name "$S3_STACK_NAME" \
                --template-body "file://$S3_TEMPLATE_FILE" \
                --parameters "${stack_params[@]}" \
                --capabilities CAPABILITY_NAMED_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
                
                print_info "Stack operation started, waiting for completion..."
                
                # Wait for stack operation to complete
                if wait_for_stack_completion "$S3_STACK_NAME"; then
                    print_success "S3 storage module deployment successful"
                    
                    # Get and display outputs
                    s3_storage_show_outputs
                    
                    return 0
                else
                    print_error "S3 storage module deployment failed"
                    return 1
                fi
            else
                print_error "Failed to start S3 stack creation"
                return 1
            fi
        else
            # Normal stack update
            print_info "S3 stack exists, will update"
            
            # Try to update stack
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$S3_STACK_NAME" \
                --template-body "file://$S3_TEMPLATE_FILE" \
                --parameters "${stack_params[@]}" \
                --capabilities CAPABILITY_NAMED_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            
            local update_exit_code=$?
            
            # Check if "no update needed" situation
            if [[ $update_exit_code -ne 0 ]] && echo "$update_output" | grep -q "No updates are to be performed"; then
                print_success "S3 stack is already up to date, no update needed"
                
                # Get and display outputs
                s3_storage_show_outputs
                
                return 0
            elif [[ $update_exit_code -eq 0 ]]; then
                print_info "Stack update started, waiting for completion..."
                
                # Wait for stack operation to complete
                if wait_for_stack_completion "$S3_STACK_NAME"; then
                    print_success "S3 storage module update successful"
                    
                    # Get and display outputs
                    s3_storage_show_outputs
                    
                    return 0
                else
                    print_error "S3 storage module update failed"
                    return 1
                fi
            else
                # Other update error
                print_error "Failed to start S3 stack update"
                echo "$update_output"
                return 1
            fi
        fi
    else
        # Creating new S3 stack
        print_info "Creating new S3 stack: $S3_STACK_NAME"
        
        if retry_aws_command aws cloudformation create-stack \
            --stack-name "$S3_STACK_NAME" \
            --template-body "file://$S3_TEMPLATE_FILE" \
            --parameters "${stack_params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            print_info "Stack operation started, waiting for completion..."
            
            # Wait for stack operation to complete
            if wait_for_stack_completion "$S3_STACK_NAME"; then
                print_success "S3 storage module deployment successful"
                
                # Get and display outputs
                s3_storage_show_outputs
                
                return 0
            else
                print_error "S3 storage module deployment failed"
                return 1
            fi
        else
            print_error "Failed to start S3 stack creation"
            return 1
        fi
    fi
}

s3_storage_status() {
    print_info "Checking S3 storage module status"
    
    # Check CloudFormation stack status
    local stack_status
    stack_status=$(get_stack_status "$S3_STACK_NAME")
    
    case "$stack_status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "S3 stack status normal: $stack_status"
            ;;
        CREATE_IN_PROGRESS|UPDATE_IN_PROGRESS)
            print_info "S3 stack operation in progress: $stack_status"
            ;;
        *FAILED*|*ROLLBACK*)
            print_error "S3 stack status abnormal: $stack_status"
            return 1
            ;;
        DOES_NOT_EXIST)
            print_warning "S3 stack does not exist"
            return 1
            ;;
        *)
            print_warning "S3 stack status unknown: $stack_status"
            return 1
            ;;
    esac
    
    # Check S3 bucket status
    s3_storage_check_buckets
    
    return 0
}

s3_storage_cleanup() {
    print_info "Cleaning up S3 storage module resources"
    
    # Warn user
    print_warning "This will delete all S3 buckets and data!"
    if ! confirm_action "Are you sure you want to clean up S3 storage resources?" "n"; then
        print_info "Cleanup operation cancelled"
        return 0
    fi
    
    # First empty S3 buckets
    s3_storage_empty_buckets
    
    # Delete CloudFormation stack
    if check_stack_exists "$S3_STACK_NAME"; then
        print_info "Deleting S3 stack: $S3_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$S3_STACK_NAME"; then
            print_info "Waiting for stack deletion completion..."
            
            # Wait for deletion completion
            local timeout=900  # 15 minutes
            local elapsed=0
            
            while [[ $elapsed -lt $timeout ]]; do
                local status
                status=$(get_stack_status "$S3_STACK_NAME")
                
                if [[ "$status" == "DOES_NOT_EXIST" ]]; then
                    print_success "S3 stack deletion completed"
                    return 0
                elif [[ "$status" == "DELETE_FAILED" ]]; then
                    print_error "S3 stack deletion failed"
                    return 1
                fi
                
                sleep 10
                elapsed=$((elapsed + 10))
            done
            
            print_error "S3 stack deletion timeout"
            return 1
        else
            print_error "Failed to start S3 stack deletion"
            return 1
        fi
    else
        print_info "S3 stack does not exist, no deletion needed"
        return 0
    fi
}

s3_storage_rollback() {
    print_info "Rolling back S3 storage module changes"
    
    # Check stack status
    local stack_status
    stack_status=$(get_stack_status "$S3_STACK_NAME")
    
    if [[ "$stack_status" == *"FAILED"* ]] || [[ "$stack_status" == *"ROLLBACK"* ]]; then
        print_info "Stack is already in failed or rollback state: $stack_status"
        
        # Try to cancel update if in progress
        if [[ "$stack_status" == "UPDATE_ROLLBACK_FAILED" ]]; then
            print_info "Trying to continue rollback..."
            
            if aws cloudformation continue-update-rollback --stack-name "$S3_STACK_NAME"; then
                wait_for_stack_completion "$S3_STACK_NAME"
            fi
        fi
        
        return 0
    fi
    
    # If stack is normal but needs rollback to previous state, complex logic can be implemented here
    print_info "Stack status is normal, no rollback needed"
    return 0
}

# =============================================================================
# Optional function implementations
# =============================================================================

s3_storage_test() {
    print_info "Testing S3 storage module functionality"
    
    # Get bucket names
    local buckets
    if ! buckets=$(s3_storage_get_bucket_names); then
        print_error "Cannot get S3 bucket names"
        return 1
    fi
    
    local test_errors=0
    
    # Test each bucket
    while IFS= read -r bucket; do
        if [[ -n "$bucket" ]]; then
            print_info "Testing bucket: $bucket"
            
            # Test bucket access permissions
            if aws s3 ls "s3://$bucket" &>/dev/null; then
                print_success "✓ Bucket access normal: $bucket"
            else
                print_error "✗ Bucket access failed: $bucket"
                test_errors=$((test_errors + 1))
            fi
            
            # Test uploading small file
            local test_file="/tmp/s3_test_${RANDOM}.txt"
            echo "S3 test file $(date)" > "$test_file"
            
            if aws s3 cp "$test_file" "s3://$bucket/test/" &>/dev/null; then
                print_success "✓ Bucket write normal: $bucket"
                
                # Clean up test file
                aws s3 rm "s3://$bucket/test/$(basename "$test_file")" &>/dev/null
            else
                print_error "✗ Bucket write failed: $bucket"
                test_errors=$((test_errors + 1))
            fi
            
            rm -f "$test_file"
        fi
    done <<< "$buckets"
    
    if [[ $test_errors -eq 0 ]]; then
        print_success "S3 storage module functionality test passed"
        return 0
    else
        print_error "S3 storage module functionality test failed, found $test_errors errors"
        return 1
    fi
}

# =============================================================================
# Module-specific helper functions
# =============================================================================

s3_storage_get_bucket_names() {
    if ! check_stack_exists "$S3_STACK_NAME"; then
        print_error "S3 stack does not exist"
        return 1
    fi
    
    # Get bucket names from stack outputs
    aws cloudformation describe-stacks \
        --stack-name "$S3_STACK_NAME" \
        --query 'Stacks[0].Outputs[?contains(OutputKey, `Bucket`)].OutputValue' \
        --output text | tr '\t' '\n'
}

s3_storage_check_buckets() {
    print_info "Check S3 bucket status"
    
    local buckets
    if buckets=$(s3_storage_get_bucket_names); then
        local healthy_buckets=0
        local total_buckets=0
        
        while IFS= read -r bucket; do
            if [[ -n "$bucket" ]]; then
                total_buckets=$((total_buckets + 1))
                
                if check_s3_bucket_exists "$bucket"; then
                    print_debug "✓ Bucket exists: $bucket"
                    healthy_buckets=$((healthy_buckets + 1))
                else
                    print_error "✗ Bucket does not exist: $bucket"
                fi
            fi
        done <<< "$buckets"
        
        print_info "Bucket status: $healthy_buckets/$total_buckets healthy"
        
        if [[ $healthy_buckets -eq $total_buckets ]]; then
            return 0
        else
            return 1
        fi
    else
        print_error "Cannot get bucket names"
        return 1
    fi
}

s3_storage_empty_buckets() {
    print_info "Emptying S3 bucket contents (including all versions)"
    
    local buckets
    if buckets=$(s3_storage_get_bucket_names); then
        while IFS= read -r bucket; do
            if [[ -n "$bucket" ]] && check_s3_bucket_exists "$bucket"; then
                print_info "Emptying bucket: $bucket"
                
                # Delete all object versions
                print_debug "Deleting all object versions..."
                local versions=$(aws s3api list-object-versions --bucket "$bucket" \
                    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
                    --output json 2>/dev/null || echo '{"Objects": []}')
                
                if [[ $(echo "$versions" | jq '.Objects | length') -gt 0 ]]; then
                    aws s3api delete-objects --bucket "$bucket" --delete "$versions" >/dev/null || true
                fi
                
                # Delete all delete markers
                print_debug "Deleting all delete markers..."
                local markers=$(aws s3api list-object-versions --bucket "$bucket" \
                    --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
                    --output json 2>/dev/null || echo '{"Objects": []}')
                
                if [[ $(echo "$markers" | jq '.Objects | length') -gt 0 ]]; then
                    aws s3api delete-objects --bucket "$bucket" --delete "$markers" >/dev/null || true
                fi
                
                # Delete any remaining current version objects
                print_debug "Deleting remaining objects..."
                aws s3 rm "s3://$bucket" --recursive --force 2>/dev/null || true
                
                print_success "Bucket emptied: $bucket"
            fi
        done <<< "$buckets"
    fi
}

s3_storage_show_outputs() {
    if ! check_stack_exists "$S3_STACK_NAME"; then
        print_warning "S3 stack does not exist, cannot display outputs"
        return 1
    fi
    
    print_info "S3 stack outputs:"
    
    aws cloudformation describe-stacks \
        --stack-name "$S3_STACK_NAME" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue,Description]' \
        --output table
}

# =============================================================================
# If this script is executed directly
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Load configuration
    load_config
    
    # Load module interface
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # Execute passed operation
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "s3_storage" "${@:2}"
    else
        echo "Usage: $0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback, test"
        echo
        echo "Examples:"
        echo "  $0 validate     # Validate configuration"
        echo "  $0 deploy       # Deploy S3 storage"
        echo "  $0 status       # Check status"
        echo "  $0 test         # Test functionality"
        echo "  $0 cleanup      # Clean up resources"
    fi
fi