#!/bin/bash

# =============================================================================
# Lake Formation Module
# Version: 1.0.0
# Description: Manage Lake Formation permissions and security for the data lake
# =============================================================================

# Initialize project environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load configuration
if [[ -f "$PROJECT_ROOT/configs/config.local.env" ]]; then
    set -a
    source "$PROJECT_ROOT/configs/config.local.env"
    set +a
elif [[ -f "$PROJECT_ROOT/configs/config.env" ]]; then
    set -a
    source "$PROJECT_ROOT/configs/config.env"
    set +a
fi

# Set default values
export PROJECT_PREFIX="${PROJECT_PREFIX:-dl-handson}"
export ENVIRONMENT="${ENVIRONMENT:-dev}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# Load common utility library
source "$SCRIPT_DIR/../../lib/common.sh"

# Load compatibility layer
if [[ -f "$SCRIPT_DIR/../../lib/compatibility.sh" ]]; then
    source "$SCRIPT_DIR/../../lib/compatibility.sh"
fi

readonly LAKE_FORMATION_MODULE_VERSION="1.0.0"

# =============================================================================
# Module Configuration
# =============================================================================

LAKE_FORMATION_STACK_NAME="${PROJECT_PREFIX}-stack-lakeformation-${ENVIRONMENT}"
LAKE_FORMATION_TEMPLATE_FILE="${PROJECT_ROOT}/templates/lake-formation-simple.yaml"

# =============================================================================
# Required Function Implementations
# =============================================================================

lake_formation_validate() {
    print_info "Validating Lake Formation module configuration"
    
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
    if [[ ! -f "$LAKE_FORMATION_TEMPLATE_FILE" ]]; then
        print_error "Lake Formation template file does not exist: $LAKE_FORMATION_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Validate Lake Formation permissions
    if ! aws lakeformation get-data-lake-settings &>/dev/null; then
        print_error "Lake Formation permission validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Check dependency modules
    local iam_stack_name="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
    if ! check_stack_exists "$iam_stack_name"; then
        print_error "Dependent IAM module not deployed"
        validation_errors=$((validation_errors + 1))
    fi
    
    local glue_stack_name="${PROJECT_PREFIX}-stack-glue-${ENVIRONMENT}"
    if ! check_stack_exists "$glue_stack_name"; then
        print_error "Dependent Glue data catalog module not deployed"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "Lake Formation module validation passed"
        return 0
    else
        print_error "Lake Formation module validation failed: $validation_errors errors"
        return 1
    fi
}

lake_formation_deploy() {
    print_info "Deploying Lake Formation module"
    
    # Set stack reference parameters (use unified function to get dependency stack names)
    local s3_stack_name=$(get_dependency_stack_name "s3_storage")
    local iam_stack_name=$(get_dependency_stack_name "iam_roles")
    local glue_stack_name=$(get_dependency_stack_name "glue_catalog")
    
    # Debug output of variable values
    print_info "Stack names: s3=$s3_stack_name, iam=$iam_stack_name, glue=$glue_stack_name"
    
    # Validate that dependency stacks exist
    print_info "Validating dependency stack existence..."
    
    local s3_exists iam_exists glue_exists
    s3_exists=$(check_stack_exists "$s3_stack_name" && echo "true" || echo "false")
    iam_exists=$(check_stack_exists "$iam_stack_name" && echo "true" || echo "false")
    glue_exists=$(check_stack_exists "$glue_stack_name" && echo "true" || echo "false")
    
    print_info "Stack existence check: S3=$s3_exists, IAM=$iam_exists, Glue=$glue_exists"
    
    if [[ "$s3_exists" != "true" ]]; then
        print_error "S3 stack does not exist: $s3_stack_name, please deploy S3 module first"
        return 1
    fi
    
    if [[ "$iam_exists" != "true" ]]; then
        print_error "IAM stack does not exist: $iam_stack_name, please deploy IAM module first"
        return 1
    fi
    
    if [[ "$glue_exists" != "true" ]]; then
        print_error "Glue stack does not exist: $glue_stack_name, please deploy Glue module first"
        return 1
    fi
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=S3StackName,ParameterValue="$s3_stack_name"
        ParameterKey=IAMStackName,ParameterValue="$iam_stack_name"
        ParameterKey=GlueStackName,ParameterValue="$glue_stack_name"
    )
    
    # Check if stack already exists and handle ROLLBACK_COMPLETE status
    if check_stack_exists "$LAKE_FORMATION_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
        
        # Check if it's in ROLLBACK_COMPLETE status, delete stack if so
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" ]]; then
            print_warning "Lake Formation stack is in ROLLBACK_COMPLETE status, needs to be deleted first"
            print_info "Deleting Lake Formation stack: $LAKE_FORMATION_STACK_NAME"
            
            # First clean up Lake Formation permissions
            print_info "Cleaning up Lake Formation permissions..."
            local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
            
            # Get all permissions and clean them up
            local permissions
            permissions=$(aws lakeformation list-permissions \
                --resource Database="{Name=\"$database_name\"}" \
                --query 'PrincipalResourcePermissions[].Principal.DataLakePrincipalIdentifier' \
                --output text 2>/dev/null || echo "")
            
            for principal in $permissions; do
                if [[ -n "$principal" && "$principal" != "None" ]]; then
                    aws lakeformation revoke-permissions \
                        --principal DataLakePrincipalIdentifier="$principal" \
                        --resource Database="{Name=\"$database_name\"}" \
                        --permissions ALL &>/dev/null || true
                    print_debug "Cleaning up permission: $principal"
                fi
            done
            
            if aws cloudformation delete-stack --stack-name "$LAKE_FORMATION_STACK_NAME"; then
                print_info "Waiting for stack deletion to complete..."
                
                # Wait for deletion to complete
                local timeout=900  # 15 minutes
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "Lake Formation stack deletion completed"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "Lake Formation stack deletion failed"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "Lake Formation stack deletion timed out"
                    return 1
                fi
            else
                print_error "Failed to start Lake Formation stack deletion"
                return 1
            fi
            
            # After deletion completes, create new stack
            print_info "Creating new Lake Formation stack: $LAKE_FORMATION_STACK_NAME"
            
            if retry_aws_command aws cloudformation create-stack \
                --stack-name "$LAKE_FORMATION_STACK_NAME" \
                --template-body "file://$LAKE_FORMATION_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
                
                print_info "Stack operation initiated, waiting for completion..."
                
                # Wait for stack operation to complete
                if wait_for_stack_completion "$LAKE_FORMATION_STACK_NAME"; then
                    configure_lake_formation_permissions
                    print_success "Lake Formation module deployed successfully"
                    return 0
                else
                    print_error "Lake Formation module deployment failed"
                    return 1
                fi
            else
                print_error "Failed to start Lake Formation stack creation"
                return 1
            fi
        else
            # Normal stack update
            print_info "Lake Formation stack already exists, will perform update"
            
            # Capture update command output to handle "No updates are to be performed" situation
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$LAKE_FORMATION_STACK_NAME" \
                --template-body "file://$LAKE_FORMATION_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            local update_exit_code=$?
            
            if [[ $update_exit_code -eq 0 ]]; then
                print_info "Stack update initiated, waiting for completion..."
                
                # Wait for stack operation to complete
                if wait_for_stack_completion "$LAKE_FORMATION_STACK_NAME"; then
                    configure_lake_formation_permissions
                    print_success "Lake Formation module updated successfully"
                    return 0
                else
                    print_error "Lake Formation module update failed"
                    return 1
                fi
            elif [[ "$update_output" == *"No updates are to be performed"* ]]; then
                # Stack has no changes, this is normal
                print_info "Stack has no changes, configuring permissions"
                configure_lake_formation_permissions
                print_success "Lake Formation module validation successful (no changes)"
                return 0
            else
                print_error "Failed to start Lake Formation stack update: $update_output"
                return 1
            fi
        fi
    else
        # Create new stack
        print_info "Creating new Lake Formation stack: $LAKE_FORMATION_STACK_NAME"
        
        if retry_aws_command aws cloudformation create-stack \
            --stack-name "$LAKE_FORMATION_STACK_NAME" \
            --template-body "file://$LAKE_FORMATION_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            print_info "Stack creation initiated, waiting for completion..."
            
            # Wait for stack operation to complete
            if wait_for_stack_completion "$LAKE_FORMATION_STACK_NAME"; then
                configure_lake_formation_permissions
                print_success "Lake Formation module deployed successfully"
                return 0
            else
                print_error "Lake Formation module deployment failed"
                return 1
            fi
        else
            print_error "Failed to start Lake Formation stack creation"
            return 1
        fi
    fi
    
    print_error "Lake Formation module deployment failed"
    return 1
}

configure_lake_formation_permissions() {
    print_info "Configuring Lake Formation permissions"
    
    local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
    local admin_role_arn data_engineer_role_arn analyst_role_arn
    
    # Get role ARNs
    local iam_stack_name=$(get_dependency_stack_name "iam_roles")
    
    admin_role_arn=$(aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataLakeAdminRoleArn`].OutputValue' \
        --output text 2>/dev/null)
    
    data_engineer_role_arn=$(aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataEngineerRoleArn`].OutputValue' \
        --output text 2>/dev/null)
    
    analyst_role_arn=$(aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataAnalystRoleArn`].OutputValue' \
        --output text 2>/dev/null)
    
    # Grant full database permissions to admin role
    if [[ -n "$admin_role_arn" ]]; then
        aws lakeformation grant-permissions \
            --principal DataLakePrincipalIdentifier="$admin_role_arn" \
            --resource Database="{Name=\"$database_name\"}" \
            --permissions ALL &>/dev/null || true
        print_debug "✓ Admin permissions configured"
    fi
    
    # Grant read-write permissions to data engineer role
    if [[ -n "$data_engineer_role_arn" ]]; then
        aws lakeformation grant-permissions \
            --principal DataLakePrincipalIdentifier="$data_engineer_role_arn" \
            --resource Database="{Name=\"$database_name\"}" \
            --permissions CREATE_TABLE ALTER DROP &>/dev/null || true
        print_debug "✓ Data engineer permissions configured"
    fi
    
    # Grant read-only permissions to analyst role
    if [[ -n "$analyst_role_arn" ]]; then
        aws lakeformation grant-permissions \
            --principal DataLakePrincipalIdentifier="$analyst_role_arn" \
            --resource Database="{Name=\"$database_name\"}" \
            --permissions DESCRIBE &>/dev/null || true
        print_debug "✓ Analyst permissions configured"
    fi
    
    print_success "Lake Formation permission configuration completed"
}

lake_formation_status() {
    print_info "Checking Lake Formation module status"
    
    local status
    status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "Lake Formation module running normally: $status"
            
            # Check data lake settings
            if aws lakeformation get-data-lake-settings &>/dev/null; then
                print_debug "✓ Lake Formation service available"
                
                # Check database permissions
                local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
                local permissions
                permissions=$(aws lakeformation list-permissions \
                    --resource Database="{Name=\"$database_name\"}" \
                    --query 'length(PrincipalResourcePermissions)' \
                    --output text 2>/dev/null || echo "0")
                
                print_debug "✓ Database permission count: $permissions"
            else
                print_warning "⚠ Lake Formation service not available"
            fi
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "Lake Formation module not deployed"
            return 1
            ;;
        *)
            print_error "Lake Formation module status abnormal: $status"
            return 1
            ;;
    esac
}

lake_formation_cleanup() {
    print_info "Cleaning up Lake Formation module resources"
    
    # Clean up permissions (to avoid dependency issues when deleting stack)
    print_info "Cleaning up Lake Formation permissions..."
    local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
    
    # Get all permissions and clean them up
    local permissions
    permissions=$(aws lakeformation list-permissions \
        --resource Database="{Name=\"$database_name\"}" \
        --query 'PrincipalResourcePermissions[].Principal.DataLakePrincipalIdentifier' \
        --output text 2>/dev/null || echo "")
    
    for principal in $permissions; do
        if [[ -n "$principal" && "$principal" != "None" ]]; then
            aws lakeformation revoke-permissions \
                --principal DataLakePrincipalIdentifier="$principal" \
                --resource Database="{Name=\"$database_name\"}" \
                --permissions ALL &>/dev/null || true
            print_debug "Cleaning up permission: $principal"
        fi
    done
    
    if check_stack_exists "$LAKE_FORMATION_STACK_NAME"; then
        print_info "Deleting Lake Formation stack: $LAKE_FORMATION_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$LAKE_FORMATION_STACK_NAME"; then
            if wait_for_stack_deletion "$LAKE_FORMATION_STACK_NAME"; then
                print_success "Lake Formation module cleanup successful"
                return 0
            fi
        fi
        
        print_error "Lake Formation module cleanup failed"
        return 1
    else
        print_info "Lake Formation module not deployed, no cleanup needed"
        return 0
    fi
}

lake_formation_rollback() {
    print_info "Rolling back Lake Formation module changes"
    
    if check_stack_exists "$LAKE_FORMATION_STACK_NAME"; then
        local status
        status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "Detected failure status, performing stack deletion"
            lake_formation_cleanup
        else
            print_info "Lake Formation module status normal, no rollback needed"
            return 0
        fi
    else
        print_info "Lake Formation module does not exist, no rollback needed"
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
        module_interface "$1" "lake_formation" "${@:2}"
    else
        echo "Usage: $0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback"
    fi
fi