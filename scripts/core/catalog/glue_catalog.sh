#!/bin/bash

# =============================================================================
# Glue Data Catalog Module
# Version: 1.0.0
# Description: Manage Glue databases and tables for the data lake
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

readonly GLUE_CATALOG_MODULE_VERSION="1.0.0"

# =============================================================================
# Module Configuration
# =============================================================================

GLUE_STACK_NAME="${PROJECT_PREFIX}-stack-glue-${ENVIRONMENT}"
GLUE_TEMPLATE_FILE="${PROJECT_ROOT}/templates/glue-catalog.yaml"
TABLE_SCHEMAS_FILE="${PROJECT_ROOT}/scripts/utils/table_schemas.json"

# =============================================================================
# Required Function Implementations
# =============================================================================

glue_catalog_validate() {
    print_info "Validating Glue data catalog module configuration"
    
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
    if [[ ! -f "$GLUE_TEMPLATE_FILE" ]]; then
        print_error "Glue template file does not exist: $GLUE_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Check table schema definition file
    if [[ ! -f "$TABLE_SCHEMAS_FILE" ]]; then
        print_error "Table schema file does not exist: $TABLE_SCHEMAS_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Validate Glue permissions
    if ! aws glue get-databases &>/dev/null; then
        print_error "Glue permission validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Check S3 storage module (dependency)
    local s3_stack_name="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
    if ! check_stack_exists "$s3_stack_name"; then
        print_error "Dependent S3 storage module not deployed"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "Glue data catalog module validation passed"
        return 0
    else
        print_error "Glue data catalog module validation failed: $validation_errors errors"
        return 1
    fi
}

glue_catalog_deploy() {
    print_info "Deploying Glue data catalog module"
    
    # Set stack reference parameters (use unified function to get dependency stack names)
    local s3_stack_name=$(get_dependency_stack_name "s3_storage")
    local iam_stack_name=$(get_dependency_stack_name "iam_roles")
    
    # Validate that dependency stacks exist
    if ! check_stack_exists "$s3_stack_name"; then
        print_error "S3 stack does not exist: $s3_stack_name, please deploy S3 module first"
        return 1
    fi
    
    if ! check_stack_exists "$iam_stack_name"; then
        print_error "IAM stack does not exist: $iam_stack_name, please deploy IAM module first"
        return 1
    fi
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=S3StackName,ParameterValue="$s3_stack_name"
        ParameterKey=IAMStackName,ParameterValue="$iam_stack_name"
    )
    
    # Check if stack already exists and handle ROLLBACK_COMPLETE status
    if check_stack_exists "$GLUE_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$GLUE_STACK_NAME")
        
        # Check if it's in ROLLBACK_COMPLETE status, delete stack if so
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" ]]; then
            print_warning "Glue stack is in ROLLBACK_COMPLETE status, needs to be deleted first"
            print_info "Deleting Glue stack: $GLUE_STACK_NAME"
            
            # First delete Glue tables (to avoid dependency issues)
            local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
            if aws glue get-database --name "$database_name" &>/dev/null; then
                print_info "Deleting existing Glue tables..."
                local tables
                tables=$(aws glue get-tables --database-name "$database_name" --query 'TableList[].Name' --output text 2>/dev/null || echo "")
                
                for table in $tables; do
                    if [[ -n "$table" ]]; then
                        aws glue delete-table --database-name "$database_name" --name "$table" &>/dev/null || true
                        print_debug "Deleting table: $table"
                    fi
                done
            fi
            
            if aws cloudformation delete-stack --stack-name "$GLUE_STACK_NAME"; then
                print_info "Waiting for stack deletion to complete..."
                
                # Wait for deletion to complete
                local timeout=900  # 15 minutes
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$GLUE_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "Glue stack deletion completed"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "Glue stack deletion failed"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "Glue stack deletion timed out"
                    return 1
                fi
            else
                print_error "Failed to start Glue stack deletion"
                return 1
            fi
            
            # After deletion completes, create new stack
            print_info "Creating new Glue stack: $GLUE_STACK_NAME"
            
            if retry_aws_command aws cloudformation create-stack \
                --stack-name "$GLUE_STACK_NAME" \
                --template-body "file://$GLUE_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
                
                print_info "Stack operation initiated, waiting for completion..."
                
                # Wait for stack operation to complete
                if wait_for_stack_completion "$GLUE_STACK_NAME"; then
                    # Create table structures
                    create_glue_tables
                    print_success "Glue data catalog module deployed successfully"
                    return 0
                else
                    print_error "Glue data catalog module deployment failed"
                    return 1
                fi
            else
                print_error "Failed to start Glue stack creation"
                return 1
            fi
        else
            # Normal stack update
            print_info "Glue stack already exists, will perform update"
            
            # Capture update command output to handle "No updates are to be performed" situation
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$GLUE_STACK_NAME" \
                --template-body "file://$GLUE_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            local update_exit_code=$?
            
            if [[ $update_exit_code -eq 0 ]]; then
                print_info "Stack update initiated, waiting for completion..."
                
                # Wait for stack operation to complete
                if wait_for_stack_completion "$GLUE_STACK_NAME"; then
                    # Create table structures
                    create_glue_tables
                    print_success "Glue data catalog module updated successfully"
                    return 0
                else
                    print_error "Glue data catalog module update failed"
                    return 1
                fi
            elif [[ "$update_output" == *"No updates are to be performed"* ]]; then
                # Stack has no changes, this is normal
                print_info "Stack has no changes, creating table structures"
                create_glue_tables
                print_success "Glue data catalog module validation successful (no changes)"
                return 0
            else
                print_error "Failed to start Glue stack update: $update_output"
                return 1
            fi
        fi
    else
        # Create new stack
        print_info "Creating new Glue stack: $GLUE_STACK_NAME"
        
        if retry_aws_command aws cloudformation create-stack \
            --stack-name "$GLUE_STACK_NAME" \
            --template-body "file://$GLUE_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            print_info "Stack creation initiated, waiting for completion..."
            
            # Wait for stack operation to complete
            if wait_for_stack_completion "$GLUE_STACK_NAME"; then
                # Create table structures
                create_glue_tables
                print_success "Glue data catalog module deployed successfully"
                return 0
            else
                print_error "Glue data catalog module deployment failed"
                return 1
            fi
        else
            print_error "Failed to start Glue stack creation"
            return 1
        fi
    fi
    
    print_error "Glue data catalog module deployment failed"
    return 1
}

create_glue_tables() {
    print_info "Creating Glue table structures"
    
    # Use Python data processing module to create tables
    if [[ -f "$PROJECT_ROOT/scripts/lib/data_processing.py" ]]; then
        if python3 "$PROJECT_ROOT/scripts/lib/data_processing.py" \
            --processor glue_tables \
            --input "$TABLE_SCHEMAS_FILE"; then
            print_success "Glue table structures created successfully"
        else
            print_warning "Glue table structure creation failed, but continuing"
        fi
    else
        print_warning "Python data processing module not available, skipping table structure creation"
    fi
}

glue_catalog_status() {
    print_info "Checking Glue data catalog module status"
    
    local status
    status=$(get_stack_status "$GLUE_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "Glue data catalog module running normally: $status"
            
            # Check if database exists
            local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
            if aws glue get-database --name "$database_name" &>/dev/null; then
                print_debug "✓ Database exists: $database_name"
                
                # Check table count
                local table_count
                table_count=$(aws glue get-tables --database-name "$database_name" --query 'length(TableList)' --output text 2>/dev/null || echo "0")
                print_debug "✓ Table count: $table_count"
            else
                print_warning "⚠ Database does not exist: $database_name"
            fi
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "Glue data catalog module not deployed"
            return 1
            ;;
        *)
            print_error "Glue data catalog module status abnormal: $status"
            return 1
            ;;
    esac
}

glue_catalog_cleanup() {
    print_info "Cleaning up Glue data catalog module resources"
    
    # First delete tables (to avoid dependency issues when deleting stack)
    local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
    if aws glue get-database --name "$database_name" &>/dev/null; then
        print_info "Deleting Glue tables..."
        
        local tables
        tables=$(aws glue get-tables --database-name "$database_name" --query 'TableList[].Name' --output text 2>/dev/null)
        
        for table in $tables; do
            if [[ -n "$table" ]]; then
                aws glue delete-table --database-name "$database_name" --name "$table" &>/dev/null
                print_debug "Deleting table: $table"
            fi
        done
    fi
    
    if check_stack_exists "$GLUE_STACK_NAME"; then
        print_info "Deleting Glue stack: $GLUE_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$GLUE_STACK_NAME"; then
            if wait_for_stack_deletion "$GLUE_STACK_NAME"; then
                print_success "Glue data catalog module cleanup successful"
                return 0
            fi
        fi
        
        print_error "Glue data catalog module cleanup failed"
        return 1
    else
        print_info "Glue data catalog module not deployed, no cleanup needed"
        return 0
    fi
}

glue_catalog_rollback() {
    print_info "Rolling back Glue data catalog module changes"
    
    if check_stack_exists "$GLUE_STACK_NAME"; then
        local status
        status=$(get_stack_status "$GLUE_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "Detected failure status, performing stack deletion"
            glue_catalog_cleanup
        else
            print_info "Glue data catalog module status normal, no rollback needed"
            return 0
        fi
    else
        print_info "Glue data catalog module does not exist, no rollback needed"
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
        module_interface "$1" "glue_catalog" "${@:2}"
    else
        echo "Usage: $0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback"
    fi
fi