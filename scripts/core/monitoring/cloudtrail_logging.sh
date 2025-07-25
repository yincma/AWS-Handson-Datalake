#!/bin/bash

# =============================================================================
# CloudTrail Logging Module
# Version: 1.0.0
# Description: Manage CloudTrail audit logs for the data lake
# =============================================================================

# Load common utility library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

# Load compatibility layer (if exists)
if [[ -f "$SCRIPT_DIR/../../lib/compatibility.sh" ]]; then
    source "$SCRIPT_DIR/../../lib/compatibility.sh"
fi

readonly CLOUDTRAIL_LOGGING_MODULE_VERSION="1.0.0"

# Load configuration (if not already loaded)
if [[ -z "${PROJECT_PREFIX:-}" ]]; then
    load_config "$PROJECT_ROOT/configs/config.env"
fi

# =============================================================================
# Module Configuration
# =============================================================================

CLOUDTRAIL_NAME="${PROJECT_PREFIX}-cloudtrail-${ENVIRONMENT}"
CLOUDTRAIL_S3_PREFIX="logs/cloudtrail"

# =============================================================================
# Required Function Implementations
# =============================================================================

cloudtrail_logging_validate() {
    print_info "Validating CloudTrail logging module configuration"
    
    local validation_errors=0
    
    # Check required environment variables
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION" "AWS_ACCOUNT_ID")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "Missing required environment variable: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # Validate CloudTrail permissions
    if ! aws cloudtrail describe-trails &>/dev/null; then
        print_error "CloudTrail permission validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Check dependency modules (S3 storage, IAM roles)
    local dependencies=("s3" "iam")
    for dep in "${dependencies[@]}"; do
        local dep_stack_name="${PROJECT_PREFIX}-stack-${dep}-${ENVIRONMENT}"
        if ! check_stack_exists "$dep_stack_name"; then
            print_error "Dependency module not deployed: $dep"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "CloudTrail logging module validation passed"
        return 0
    else
        print_error "CloudTrail logging module validation failed: $validation_errors errors"
        return 1
    fi
}

cloudtrail_logging_deploy() {
    print_info "Deploying CloudTrail logging module"
    
    # Get S3 bucket name - check multiple possible naming conventions
    local s3_stack_name=""
    local raw_bucket=""
    
    # Method 1: Check existing v2 stack
    local v2_stack_name="${PROJECT_PREFIX}-v2-stack-s3-storage-${ENVIRONMENT}"
    if check_stack_exists "$v2_stack_name"; then
        s3_stack_name="$v2_stack_name"
        print_debug "Using v2 stack: $s3_stack_name"
    fi
    
    # Method 2: Check standard naming convention
    if [[ -z "$s3_stack_name" ]]; then
        local standard_stack_name="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
        if check_stack_exists "$standard_stack_name"; then
            s3_stack_name="$standard_stack_name"
            print_debug "Using standard stack: $s3_stack_name"
        fi
    fi
    
    # Method 3: Use check_stack_exists_any function if available
    if [[ -z "$s3_stack_name" ]] && declare -F check_stack_exists_any >/dev/null 2>&1; then
        if existing_stack=$(check_stack_exists_any "s3_storage" 2>/dev/null); then
            s3_stack_name="$existing_stack"
            print_debug "Stack found by check_stack_exists_any: $s3_stack_name"
        fi
    fi
    
    # If stack not found
    if [[ -z "$s3_stack_name" ]]; then
        print_error "S3 stack not found"
        return 1
    fi
    
    # Get S3 bucket name
    raw_bucket=$(aws cloudformation describe-stacks \
        --stack-name "$s3_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`RawDataBucketName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$raw_bucket" ]]; then
        print_error "Unable to get S3 bucket name (stack: $s3_stack_name)"
        return 1
    fi
    
    # Prepare CloudTrail configuration
    print_info "Configuring CloudTrail: S3 bucket=$raw_bucket"
    
    # Check if CloudTrail already exists
    if aws cloudtrail describe-trails --trail-name-list "$CLOUDTRAIL_NAME" &>/dev/null; then
        print_info "Updating existing CloudTrail: $CLOUDTRAIL_NAME"
        
        aws cloudtrail put-event-selectors \
            --trail-name "$CLOUDTRAIL_NAME" \
            --event-selectors ReadWriteType=All,IncludeManagementEvents=true,DataResources='[{Type=AWS::S3::Object,Values=["'$raw_bucket'/*"]},{Type=AWS::Glue::Table,Values=["*"]}]' &>/dev/null
        
        print_success "CloudTrail configuration updated"
    else
        print_info "Creating new CloudTrail: $CLOUDTRAIL_NAME"
        
        # Create CloudTrail
        if aws cloudtrail create-trail \
            --name "$CLOUDTRAIL_NAME" \
            --s3-bucket-name "$raw_bucket" \
            --s3-key-prefix "$CLOUDTRAIL_S3_PREFIX" \
            --include-global-service-events \
            --is-multi-region-trail \
            --enable-log-file-validation \
            --tags-list Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            # Start logging
            aws cloudtrail start-logging --name "$CLOUDTRAIL_NAME"
            
            # Configure event selectors
            aws cloudtrail put-event-selectors \
                --trail-name "$CLOUDTRAIL_NAME" \
                --event-selectors ReadWriteType=All,IncludeManagementEvents=true,DataResources='[{Type=AWS::S3::Object,Values=["'$raw_bucket'/*"]},{Type=AWS::Glue::Table,Values=["*"]}]' &>/dev/null
            
            print_success "CloudTrail logging module deployment successful"
            return 0
        else
            print_error "CloudTrail creation failed"
            return 1
        fi
    fi
    
    return 0
}

cloudtrail_logging_status() {
    print_info "Checking CloudTrail logging module status"
    
    local trail_status
    trail_status=$(aws cloudtrail get-trail-status --name "$CLOUDTRAIL_NAME" --query 'IsLogging' --output text 2>/dev/null)
    
    if [[ "$trail_status" == "True" ]]; then
        print_success "CloudTrail logging module running normally: $CLOUDTRAIL_NAME"
        
        # Display additional information
        local latest_log
        latest_log=$(aws cloudtrail get-trail-status --name "$CLOUDTRAIL_NAME" --query 'LatestDeliveryTime' --output text 2>/dev/null)
        if [[ -n "$latest_log" && "$latest_log" != "None" ]]; then
            print_debug "✓ Latest log delivery time: $latest_log"
        fi
        
        # Check event selectors
        local data_events
        data_events=$(aws cloudtrail get-event-selectors --trail-name "$CLOUDTRAIL_NAME" --query 'length(EventSelectors[0].DataResources)' --output text 2>/dev/null)
        print_debug "✓ Data event configuration count: $data_events"
        
        return 0
    elif [[ "$trail_status" == "False" ]]; then
        print_warning "CloudTrail exists but logging is stopped"
        return 1
    else
        print_warning "CloudTrail logging module not deployed"
        return 1
    fi
}

cloudtrail_logging_cleanup() {
    print_info "Cleaning up CloudTrail logging module resources"
    
    if aws cloudtrail describe-trails --trail-name-list "$CLOUDTRAIL_NAME" &>/dev/null; then
        print_info "Deleting CloudTrail: $CLOUDTRAIL_NAME"
        
        # Stop logging
        aws cloudtrail stop-logging --name "$CLOUDTRAIL_NAME" &>/dev/null
        
        # Delete CloudTrail
        if aws cloudtrail delete-trail --name "$CLOUDTRAIL_NAME" 2>/dev/null; then
            print_success "CloudTrail logging module cleanup successful"
            return 0
        else
            # Treat TrailNotFoundException as success
            local error_output
            error_output=$(aws cloudtrail delete-trail --name "$CLOUDTRAIL_NAME" 2>&1 || true)
            if echo "$error_output" | grep -q "TrailNotFoundException"; then
                print_info "CloudTrail has already been deleted"
                return 0
            else
                print_error "CloudTrail deletion failed"
                return 1
            fi
        fi
    else
        print_info "CloudTrail logging module not deployed, no cleanup needed"
        return 0
    fi
}

cloudtrail_logging_rollback() {
    print_info "Rolling back CloudTrail logging module changes"
    
    # CloudTrail rollback is the same as deletion
    cloudtrail_logging_cleanup
}

# =============================================================================
# Utility Functions
# =============================================================================

show_cloudtrail_logs() {
    local hours="${1:-1}"
    
    print_info "Displaying CloudTrail logs for the past ${hours} hours"
    
    local start_time end_time
    start_time=$(date -d "${hours} hours ago" -Iseconds)
    end_time=$(date -Iseconds)
    
    aws logs filter-log-events \
        --log-group-name CloudTrail/DataLakeEvents \
        --start-time "$(date -d "$start_time" +%s)000" \
        --end-time "$(date -d "$end_time" +%s)000" \
        --query 'events[].[eventTime,eventName,sourceIPAddress,userIdentity.type]' \
        --output table 2>/dev/null || \
    print_warning "Failed to retrieve CloudTrail logs, or logs do not exist"
}

analyze_security_events() {
    print_info "Analyzing security-related events"
    
    local start_time
    start_time=$(date -d "24 hours ago" -Iseconds)
    
    # Check for suspicious activities
    local suspicious_events=(
        "ConsoleLogin"
        "CreateUser"
        "AttachUserPolicy"
        "PutBucketPolicy"
        "DeleteBucket"
    )
    
    for event in "${suspicious_events[@]}"; do
        local count
        count=$(aws logs filter-log-events \
            --log-group-name CloudTrail/DataLakeEvents \
            --start-time "$(date -d "$start_time" +%s)000" \
            --filter-pattern "{ \$.eventName = $event }" \
            --query 'length(events)' \
            --output text 2>/dev/null || echo "0")
        
        if [[ "$count" -gt 0 ]]; then
            print_warning "⚠ Security event detected: $event ($count times)"
        else
            print_debug "✓ $event: No events"
        fi
    done
}

# =============================================================================
# If executing this script directly
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Load module interface
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # Support additional analysis commands
    case "${1:-}" in
        logs)
            load_config
            show_cloudtrail_logs "${2:-1}"
            exit 0
            ;;
        security)
            load_config
            analyze_security_events
            exit 0
            ;;
    esac
    
    # Execute the passed operation
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "cloudtrail_logging" "${@:2}"
    else
        echo "Usage: $0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback, logs, security"
    fi
fi