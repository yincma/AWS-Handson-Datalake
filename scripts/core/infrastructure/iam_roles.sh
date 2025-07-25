#!/bin/bash

# =============================================================================
# IAM Roles and Policies Module
# Version: 1.0.0
# Description: Manage IAM roles and permission policies for the data lake
# =============================================================================

# Load common utility library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly IAM_ROLES_MODULE_VERSION="1.0.0"

# Initialize configuration
load_config || true

# =============================================================================
# Module Configuration
# =============================================================================

# Load compatibility layer (if exists)
if [[ -f "$SCRIPT_DIR/../../lib/compatibility.sh" ]]; then
    source "$SCRIPT_DIR/../../lib/compatibility.sh"
fi

# Compatibility support: check if legacy stack exists
if [[ -n "${USE_LEGACY_NAMING:-}" ]] || [[ -n "${MIGRATION_MODE:-}" ]]; then
    IAM_STACK_NAME=$(get_stack_name "iam_roles" "${USE_LEGACY_NAMING:-false}")
else
    # Check if stack exists with any naming convention
    if existing_stack=$(check_stack_exists_any "iam_roles" 2>/dev/null); then
        IAM_STACK_NAME="$existing_stack"
        print_debug "Using existing stack: $IAM_STACK_NAME"
    else
        IAM_STACK_NAME="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
    fi
fi

IAM_TEMPLATE_FILE="${PROJECT_ROOT}/templates/iam-roles-policies.yaml"

# =============================================================================
# Required Function Implementations
# =============================================================================

iam_roles_validate() {
    print_info "Validating IAM roles module configuration"
    
    local validation_errors=0
    
    # Check required environment variables
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION" "AWS_ACCOUNT_ID")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "Missing required environment variable: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # Check CloudFormation template file
    if [[ ! -f "$IAM_TEMPLATE_FILE" ]]; then
        print_error "IAM template file does not exist: $IAM_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Validate template syntax
    if ! aws cloudformation validate-template --template-body "file://$IAM_TEMPLATE_FILE" &>/dev/null; then
        print_error "IAM template file syntax is invalid"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Validate IAM permissions
    if ! aws iam get-user &>/dev/null && ! aws sts get-caller-identity &>/dev/null; then
        print_error "IAM permission validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "IAM roles module validation passed"
        return 0
    else
        print_error "IAM roles module validation failed: $validation_errors errors"
        return 1
    fi
}

iam_roles_deploy() {
    print_info "Deploying IAM roles module"
    
    # Get S3 stack name (using unified function)
    local s3_stack_name
    s3_stack_name=$(get_dependency_stack_name "s3_storage")
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=S3StackName,ParameterValue="$s3_stack_name"
    )
    
    if check_stack_exists "$IAM_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$IAM_STACK_NAME")
        
        # Check if it's in ROLLBACK_COMPLETE status, delete stack if so
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" ]]; then
            print_warning "IAM stack is in ROLLBACK_COMPLETE status, needs to be deleted first"
            print_info "Deleting IAM stack: $IAM_STACK_NAME"
            
            if aws cloudformation delete-stack --stack-name "$IAM_STACK_NAME"; then
                print_info "Waiting for stack deletion to complete..."
                
                # Wait for deletion to complete
                local timeout=900  # 15 minutes
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$IAM_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "IAM stack deletion completed"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "IAM stack deletion failed"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "IAM stack deletion timed out"
                    return 1
                fi
            else
                print_error "Failed to start IAM stack deletion"
                return 1
            fi
            
            # After deletion completes, continue creating new stack
        else
            # Normal stack update
            print_info "Updating existing IAM stack: $IAM_STACK_NAME"
            
            # Attempt to update stack
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$IAM_STACK_NAME" \
                --template-body "file://$IAM_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            
            local update_exit_code=$?
            
            # Check if it's a "no updates needed" situation
            if [[ $update_exit_code -ne 0 ]] && echo "$update_output" | grep -q "No updates are to be performed"; then
                print_success "IAM stack is already up to date, no updates needed"
                return 0
            elif [[ $update_exit_code -eq 0 ]]; then
                print_info "Stack update initiated, waiting for completion..."
                
                if wait_for_stack_completion "$IAM_STACK_NAME"; then
                    print_success "IAM roles module updated successfully"
                    return 0
                else
                    print_error "IAM roles module update failed"
                    return 1
                fi
            else
                # Other update errors, continue with new stack creation process
                print_warning "IAM stack update failed, attempting to create new stack"
                echo "$update_output"
            fi
        fi
    else
        print_info "Creating new IAM stack: $IAM_STACK_NAME"
    fi
    
    # Create new IAM stack (or recreate after deletion)
    if retry_aws_command aws cloudformation create-stack \
        --stack-name "$IAM_STACK_NAME" \
        --template-body "file://$IAM_TEMPLATE_FILE" \
        --parameters "${template_params[@]}" \
        --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
        --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
        
        print_info "Stack creation initiated, waiting for completion..."
        
        if wait_for_stack_completion "$IAM_STACK_NAME"; then
            print_success "IAM roles module deployed successfully"
            return 0
        else
            print_error "IAM roles module deployment failed"
            return 1
        fi
    else
        print_error "Failed to start IAM stack creation"
        return 1
    fi
    
    print_error "IAM roles module deployment failed"
    return 1
}

iam_roles_status() {
    print_info "Checking IAM roles module status"
    
    local status
    status=$(get_stack_status "$IAM_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "IAM roles module running normally: $status"
            
            # Check if key roles exist
            local admin_role="${PROJECT_PREFIX}-DataLakeAdmin-${ENVIRONMENT}"
            if aws iam get-role --role-name "$admin_role" &>/dev/null; then
                print_debug "✓ Admin role exists: $admin_role"
            else
                print_warning "⚠ Admin role does not exist: $admin_role"
            fi
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "IAM roles module not deployed"
            return 1
            ;;
        *)
            print_error "IAM roles module status abnormal: $status"
            return 1
            ;;
    esac
}

iam_roles_cleanup() {
    print_info "Cleaning up IAM roles module resources"
    
    if check_stack_exists "$IAM_STACK_NAME"; then
        print_info "Deleting IAM stack: $IAM_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$IAM_STACK_NAME"; then
            if wait_for_stack_deletion "$IAM_STACK_NAME"; then
                print_success "IAM roles module cleanup successful"
                return 0
            fi
        fi
        
        print_error "IAM roles module cleanup failed"
        return 1
    else
        print_info "IAM roles module not deployed, no cleanup needed"
        return 0
    fi
}

iam_roles_rollback() {
    print_info "Rolling back IAM roles module changes"
    
    if check_stack_exists "$IAM_STACK_NAME"; then
        local status
        status=$(get_stack_status "$IAM_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "Detected failure status, performing stack deletion"
            iam_roles_cleanup
        else
            print_info "IAM roles module status normal, no rollback needed"
            return 0
        fi
    else
        print_info "IAM roles module does not exist, no rollback needed"
        return 0
    fi
}

# =============================================================================
# If executing this script directly
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Load module interface
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # Execute the passed operation
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "iam_roles" "${@:2}"
    else
        echo "Usage: $0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback"
    fi
fi