#!/bin/bash

# =============================================================================
# Cost Monitoring Module
# Version: 1.0.0
# Description: Manage cost monitoring and budget alerts for the data lake
# =============================================================================

# Load common utility library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly COST_MONITORING_MODULE_VERSION="1.0.0"

# Load configuration (if not already loaded)
if [[ -z "${PROJECT_PREFIX:-}" ]]; then
    load_config "$PROJECT_ROOT/configs/config.env"
fi

# =============================================================================
# Module Configuration
# =============================================================================

COST_STACK_NAME="${PROJECT_PREFIX}-stack-cost-monitoring-${ENVIRONMENT}"
COST_TEMPLATE_FILE="${PROJECT_ROOT}/templates/cost-monitoring.yaml"
BUDGET_LIMIT="${BUDGET_LIMIT:-100}"  # Default budget limit $100

# =============================================================================
# Required Function Implementations
# =============================================================================

cost_monitoring_validate() {
    print_info "Validating cost monitoring module configuration"
    
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
    if [[ ! -f "$COST_TEMPLATE_FILE" ]]; then
        print_error "Cost monitoring template file does not exist: $COST_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Validate Budgets permissions
    if ! aws budgets describe-budgets --account-id "$AWS_ACCOUNT_ID" &>/dev/null; then
        print_error "AWS Budgets permission validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Validate Cost Explorer permissions (if available)
    if ! aws ce get-dimension-values --dimension SERVICE --time-period Start=2023-01-01,End=2023-01-02 &>/dev/null; then
        print_warning "Cost Explorer permissions not available, some features will be limited"
    fi
    
    # Check dependency modules (S3 storage)
    local s3_stack_name="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
    if ! check_stack_exists "$s3_stack_name"; then
        print_error "Dependent S3 storage module not deployed"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "Cost monitoring module validation passed"
        return 0
    else
        print_error "Cost monitoring module validation failed: $validation_errors errors"
        return 1
    fi
}

cost_monitoring_deploy() {
    print_info "Deploying cost monitoring module"
    
    # Generate budget notification email (using default format)
    local notification_email="admin@${PROJECT_PREFIX}.example.com"
    print_info "Using default email address: $notification_email"
    print_warning "Please confirm email subscription in SNS console to receive cost alerts"
    
    # Get AWS account ID
    local aws_account_id
    aws_account_id=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
    if [[ -z "$aws_account_id" ]]; then
        print_error "Unable to get AWS account ID"
        return 1
    fi
    print_debug "AWS Account ID: $aws_account_id"
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=MonthlyBudgetLimit,ParameterValue="$BUDGET_LIMIT"
        ParameterKey=DailySpendThreshold,ParameterValue="10"
        ParameterKey=AlertEmail,ParameterValue="$notification_email"
    )
    
    if check_stack_exists "$COST_STACK_NAME"; then
        print_info "Updating existing cost monitoring stack: $COST_STACK_NAME"
        
        if aws cloudformation update-stack \
            --stack-name "$COST_STACK_NAME" \
            --template-body "file://$COST_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            if wait_for_stack_completion "$COST_STACK_NAME"; then
                configure_cost_alerts
                print_success "Cost monitoring module updated successfully"
                return 0
            fi
        fi
    else
        print_info "Creating new cost monitoring stack: $COST_STACK_NAME"
        
        if aws cloudformation create-stack \
            --stack-name "$COST_STACK_NAME" \
            --template-body "file://$COST_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            if wait_for_stack_completion "$COST_STACK_NAME"; then
                configure_cost_alerts
                print_success "Cost monitoring module deployed successfully"
                return 0
            fi
        fi
    fi
    
    print_error "Cost monitoring module deployment failed"
    return 1
}

configure_cost_alerts() {
    print_info "Configuring cost alerts"
    
    # Create cost anomaly detector
    local anomaly_detector_arn
    anomaly_detector_arn=$(aws ce create-anomaly-detector \
        --anomaly-detector MonitorArn="arn:aws:ce::${AWS_ACCOUNT_ID}:monitor/${PROJECT_PREFIX}-${ENVIRONMENT}" \
        --query 'AnomalyDetectorArn' \
        --output text 2>/dev/null)
    
    if [[ -n "$anomaly_detector_arn" ]]; then
        print_debug "✓ Cost anomaly detector created: $anomaly_detector_arn"
    else
        print_warning "⚠ Cost anomaly detector creation failed or already exists"
    fi
    
    print_success "Cost alert configuration completed"
}

cost_monitoring_status() {
    print_info "Checking cost monitoring module status"
    
    local status
    status=$(get_stack_status "$COST_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "Cost monitoring module running normally: $status"
            
            # Check budget status
            local budget_name="${PROJECT_PREFIX}-Budget-${ENVIRONMENT}"
            if aws budgets describe-budget --account-id "$AWS_ACCOUNT_ID" --budget-name "$budget_name" &>/dev/null; then
                print_debug "✓ Budget exists: $budget_name"
                
                # Get budget usage
                get_budget_usage "$budget_name"
            else
                print_warning "⚠ Budget does not exist: $budget_name"
            fi
            
            # Show current costs
            show_current_costs
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "Cost monitoring module not deployed"
            return 1
            ;;
        *)
            print_error "Cost monitoring module status abnormal: $status"
            return 1
            ;;
    esac
}

get_budget_usage() {
    local budget_name="$1"
    
    print_info "Getting budget usage..."
    
    local budget_info
    budget_info=$(aws budgets describe-budget \
        --account-id "$AWS_ACCOUNT_ID" \
        --budget-name "$budget_name" \
        --query 'Budget.{Limit:BudgetLimit.Amount,Unit:BudgetLimit.Unit}' \
        --output text 2>/dev/null)
    
    if [[ -n "$budget_info" ]]; then
        print_debug "Budget limit: $budget_info"
    fi
}

show_current_costs() {
    print_info "Showing current cost information..."
    
    # Get current month costs
    local start_date end_date
    start_date=$(date +%Y-%m-01)
    end_date=$(date +%Y-%m-%d)
    
    local monthly_cost
    monthly_cost=$(aws ce get-cost-and-usage \
        --time-period Start="$start_date",End="$end_date" \
        --granularity MONTHLY \
        --metrics BlendedCost \
        --query 'ResultsByTime[0].Total.BlendedCost.Amount' \
        --output text 2>/dev/null)
    
    if [[ -n "$monthly_cost" && "$monthly_cost" != "0" ]]; then
        print_debug "Current month cumulative cost: \$${monthly_cost}"
    else
        print_debug "Current month cost data not available"
    fi
    
    # Get project-related costs (if filterable)
    local project_cost
    project_cost=$(aws ce get-cost-and-usage \
        --time-period Start="$start_date",End="$end_date" \
        --granularity MONTHLY \
        --metrics BlendedCost \
        --group-by Type=TAG,Key=Project \
        --filter "Dimensions={Key=LINKED_ACCOUNT,Values=[\"$AWS_ACCOUNT_ID\"]}" \
        --query "ResultsByTime[0].Groups[?Keys[0]=='$PROJECT_PREFIX'].Metrics.BlendedCost.Amount" \
        --output text 2>/dev/null)
    
    if [[ -n "$project_cost" && "$project_cost" != "0" ]]; then
        print_debug "Project-related cost: \$${project_cost}"
    fi
}

cost_monitoring_cleanup() {
    print_info "Cleaning up cost monitoring module resources"
    
    # Clean up cost anomaly detectors
    print_info "Cleaning up cost anomaly detectors..."
    local detectors
    detectors=$(aws ce get-anomaly-detectors \
        --query 'AnomalyDetectors[].AnomalyDetectorArn' \
        --output text 2>/dev/null)
    
    for detector in $detectors; do
        if [[ -n "$detector" && "$detector" == *"${PROJECT_PREFIX}-${ENVIRONMENT}"* ]]; then
            aws ce delete-anomaly-detector --anomaly-detector-arn "$detector" &>/dev/null
            print_debug "Deleting anomaly detector: $detector"
        fi
    done
    
    if check_stack_exists "$COST_STACK_NAME"; then
        print_info "Deleting cost monitoring stack: $COST_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$COST_STACK_NAME"; then
            if wait_for_stack_deletion "$COST_STACK_NAME"; then
                print_success "Cost monitoring module cleanup successful"
                return 0
            fi
        fi
        
        print_error "Cost monitoring module cleanup failed"
        return 1
    else
        print_info "Cost monitoring module not deployed, no cleanup needed"
        return 0
    fi
}

cost_monitoring_rollback() {
    print_info "Rolling back cost monitoring module changes"
    
    if check_stack_exists "$COST_STACK_NAME"; then
        local status
        status=$(get_stack_status "$COST_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "Detected failure status, performing stack deletion"
            cost_monitoring_cleanup
        else
            print_info "Cost monitoring module status normal, no rollback needed"
            return 0
        fi
    else
        print_info "Cost monitoring module does not exist, no rollback needed"
        return 0
    fi
}

# =============================================================================
# Utility Functions
# =============================================================================

show_cost_report() {
    print_step "Data Lake Cost Report"
    
    local start_date end_date
    start_date=$(date -d '30 days ago' +%Y-%m-%d)
    end_date=$(date +%Y-%m-%d)
    
    print_info "Time range: $start_date to $end_date"
    
    # Cost grouped by service
    echo
    print_info "Cost grouped by AWS service:"
    aws ce get-cost-and-usage \
        --time-period Start="$start_date",End="$end_date" \
        --granularity MONTHLY \
        --metrics BlendedCost \
        --group-by Type=DIMENSION,Key=SERVICE \
        --query 'ResultsByTime[0].Groups[?Metrics.BlendedCost.Amount>`0`].[Keys[0],Metrics.BlendedCost.Amount]' \
        --output table 2>/dev/null || print_warning "Unable to get service cost data"
    
    # Daily cost trends
    echo
    print_info "Cost trends for the past 7 days:"
    aws ce get-cost-and-usage \
        --time-period Start="$(date -d '7 days ago' +%Y-%m-%d)",End="$end_date" \
        --granularity DAILY \
        --metrics BlendedCost \
        --query 'ResultsByTime[].[TimePeriod.Start,Total.BlendedCost.Amount]' \
        --output table 2>/dev/null || print_warning "Unable to get daily cost data"
}

# =============================================================================
# If executing this script directly
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Load module interface
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # Support additional cost report command
    if [[ "${1:-}" == "report" ]]; then
        load_config
        show_cost_report
        exit 0
    fi
    
    # Execute the passed operation
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "cost_monitoring" "${@:2}"
    else
        echo "Usage: $0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback, report"
    fi
fi