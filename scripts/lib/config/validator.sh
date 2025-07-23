#!/bin/bash

# =============================================================================
# AWS Data Lake Project Configuration Validator
# Version: 1.0.0
# Description: Validates configuration files and environment variables
# =============================================================================

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load common utilities library
source "$SCRIPT_DIR/../common.sh"

readonly CONFIG_VALIDATOR_VERSION="1.0.0"

# =============================================================================
# Validation Rule Definitions
# =============================================================================

# AWS region pattern
readonly AWS_REGION_PATTERN="^[a-z]{2,3}-[a-z]+-[0-9]+$"

# S3 bucket naming pattern
readonly BUCKET_NAME_PATTERN="^[a-z0-9][a-z0-9\-]*[a-z0-9]$"

# Project prefix pattern
readonly PROJECT_PREFIX_PATTERN="^[a-z0-9][a-z0-9\-]*[a-z0-9]$"

# Environment pattern
readonly ENVIRONMENT_PATTERN="^(dev|development|staging|stage|prod|production)$"

# EMR instance type pattern
readonly EMR_INSTANCE_TYPE_PATTERN="^[a-z][0-9]+\.[a-z]+$"

# =============================================================================
# Validation Functions
# =============================================================================

validate_aws_region() {
    local region="$1"
    local field_name="$2"
    
    if [[ ! "$region" =~ $AWS_REGION_PATTERN ]]; then
        echo "❌ Invalid AWS region format: $field_name='$region'"
        echo "   Expected format: us-east-1, eu-west-1, ap-southeast-1, etc."
        return 1
    fi
    
    # Verify if region actually exists
    if ! aws ec2 describe-regions --region-names "$region" &>/dev/null; then
        echo "❌ AWS region does not exist or is unavailable: $field_name='$region'"
        return 1
    fi
    
    print_success "✓ AWS region validation passed: $field_name='$region'"
    return 0
}

validate_s3_bucket_name() {
    local bucket_name="$1"
    local field_name="$2"
    
    # Check length
    if [[ ${#bucket_name} -lt 3 || ${#bucket_name} -gt 63 ]]; then
        echo "❌ Invalid S3 bucket name length: $field_name='$bucket_name'"
        echo "   Length must be between 3-63 characters"
        return 1
    fi
    
    # Check format
    if [[ ! "$bucket_name" =~ $BUCKET_NAME_PATTERN ]]; then
        echo "❌ Invalid S3 bucket name format: $field_name='$bucket_name'"
        echo "   Can only contain lowercase letters, numbers, and hyphens, cannot start or end with hyphens"
        return 1
    fi
    
    # Check for consecutive hyphens
    if [[ "$bucket_name" == *"--"* ]]; then
        echo "❌ S3 bucket name cannot contain consecutive hyphens: $field_name='$bucket_name'"
        return 1
    fi
    
    # Check if it looks like an IP address
    if [[ "$bucket_name" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "❌ S3 bucket name cannot look like an IP address: $field_name='$bucket_name'"
        return 1
    fi
    
    print_success "✓ S3 bucket name validation passed: $field_name='$bucket_name'"
    return 0
}

validate_project_prefix() {
    local prefix="$1"
    local field_name="$2"
    
    # Check length
    if [[ ${#prefix} -lt 3 || ${#prefix} -gt 20 ]]; then
        echo "❌ Invalid project prefix length: $field_name='$prefix'"
        echo "   Length must be between 3-20 characters"
        return 1
    fi
    
    # Check format
    if [[ ! "$prefix" =~ $PROJECT_PREFIX_PATTERN ]]; then
        echo "❌ Invalid project prefix format: $field_name='$prefix'"
        echo "   Can only contain lowercase letters, numbers, and hyphens, cannot start or end with hyphens"
        return 1
    fi
    
    print_success "✓ Project prefix validation passed: $field_name='$prefix'"
    return 0
}

validate_environment() {
    local environment="$1"
    local field_name="$2"
    
    if [[ ! "$environment" =~ $ENVIRONMENT_PATTERN ]]; then
        echo "❌ Invalid environment type: $field_name='$environment'"
        echo "   Allowed values: dev, development, staging, stage, prod, production"
        return 1
    fi
    
    print_success "✓ Environment type validation passed: $field_name='$environment'"
    return 0
}

validate_emr_instance_type() {
    local instance_type="$1"
    local field_name="$2"
    
    if [[ ! "$instance_type" =~ $EMR_INSTANCE_TYPE_PATTERN ]]; then
        echo "❌ Invalid EMR instance type format: $field_name='$instance_type'"
        echo "   Expected format: m5.xlarge, c5.2xlarge, r5.large, etc."
        return 1
    fi
    
    # Verify if instance type is available in current region
    if ! aws ec2 describe-instance-types --instance-types "$instance_type" &>/dev/null; then
        echo "❌ EMR instance type not available in current region: $field_name='$instance_type'"
        return 1
    fi
    
    print_success "✓ EMR instance type validation passed: $field_name='$instance_type'"
    return 0
}

validate_positive_integer() {
    local value="$1"
    local field_name="$2"
    local min_value="${3:-1}"
    local max_value="${4:-1000}"
    
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        echo "❌ Not a valid positive integer: $field_name='$value'"
        return 1
    fi
    
    if [[ $value -lt $min_value || $value -gt $max_value ]]; then
        echo "❌ Value out of valid range: $field_name='$value'"
        echo "   Allowed range: $min_value - $max_value"
        return 1
    fi
    
    print_success "✓ Numeric value validation passed: $field_name='$value'"
    return 0
}

validate_boolean() {
    local value="$1"
    local field_name="$2"
    
    case "${value,,}" in
        true|false|yes|no|1|0|enabled|disabled)
            print_success "✓ Boolean value validation passed: $field_name='$value'"
            return 0
            ;;
        *)
            echo "❌ Invalid boolean value: $field_name='$value'"
            echo "   Allowed values: true, false, yes, no, 1, 0, enabled, disabled"
            return 1
            ;;
    esac
}

# =============================================================================
# Main Validation Functions
# =============================================================================

validate_required_variables() {
    local validation_errors=0
    
    print_step "Validating required environment variables..."
    
    # Required variables list
    local required_vars=(
        "PROJECT_PREFIX"
        "ENVIRONMENT" 
        "AWS_REGION"
    )
    
    # Check if required variables are set
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            echo "❌ Missing required environment variable: $var"
            validation_errors=$((validation_errors + 1))
        else
            print_debug "✓ Environment variable set: $var='${!var}'"
        fi
    done
    
    return $validation_errors
}

validate_configuration_values() {
    local validation_errors=0
    
    print_step "Validating configuration values..."
    
    # Validate project prefix
    if [[ -n "${PROJECT_PREFIX:-}" ]]; then
        if ! validate_project_prefix "$PROJECT_PREFIX" "PROJECT_PREFIX"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # Validate environment
    if [[ -n "${ENVIRONMENT:-}" ]]; then
        if ! validate_environment "$ENVIRONMENT" "ENVIRONMENT"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # Validate AWS region
    if [[ -n "${AWS_REGION:-}" ]]; then
        if ! validate_aws_region "$AWS_REGION" "AWS_REGION"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # Validate EMR configuration (if exists)
    if [[ -n "${EMR_INSTANCE_TYPE:-}" ]]; then
        if ! validate_emr_instance_type "$EMR_INSTANCE_TYPE" "EMR_INSTANCE_TYPE"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    if [[ -n "${EMR_INSTANCE_COUNT:-}" ]]; then
        if ! validate_positive_integer "$EMR_INSTANCE_COUNT" "EMR_INSTANCE_COUNT" 1 50; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # Validate S3 configuration (if exists)
    if [[ -n "${S3_VERSIONING:-}" ]]; then
        if ! validate_boolean "$S3_VERSIONING" "S3_VERSIONING"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    return $validation_errors
}

validate_aws_permissions() {
    print_step "Validating AWS permissions..."
    
    local validation_errors=0
    local required_permissions=(
        "sts:GetCallerIdentity"
        "cloudformation:DescribeStacks"
        "s3:ListAllMyBuckets"
        "iam:ListRoles"
    )
    
    # Test basic AWS connection
    if ! aws sts get-caller-identity &>/dev/null; then
        echo "❌ Cannot connect to AWS, please check credential configuration"
        validation_errors=$((validation_errors + 1))
        return $validation_errors
    fi
    
    print_success "✓ AWS credentials validation passed"
    
    # Test permissions (not exhaustive list, but check key permissions)
    print_debug "Testing key AWS permissions..."
    
    # Test CloudFormation permissions
    if ! aws cloudformation describe-stacks &>/dev/null; then
        echo "❌ Missing CloudFormation permissions"
        validation_errors=$((validation_errors + 1))
    else
        print_debug "✓ CloudFormation permissions available"
    fi
    
    # Test S3 permissions
    if ! aws s3 ls &>/dev/null; then
        echo "❌ Missing S3 permissions"
        validation_errors=$((validation_errors + 1))
    else
        print_debug "✓ S3 permissions available"
    fi
    
    return $validation_errors
}

validate_resource_quotas() {
    print_step "Checking AWS resource quotas..."
    
    local validation_errors=0
    
    # Check VPC quotas
    local vpc_limit
    vpc_limit=$(aws ec2 describe-account-attributes \
        --attribute-names max-instances \
        --query 'AccountAttributes[0].AttributeValues[0].AttributeValue' \
        --output text 2>/dev/null || echo "unknown")
    
    if [[ "$vpc_limit" != "unknown" ]]; then
        print_debug "✓ Account EC2 instance limit: $vpc_limit"
    fi
    
    # Check CloudFormation stack limit
    local stack_count
    stack_count=$(aws cloudformation list-stacks \
        --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
        --query 'length(StackSummaries)' \
        --output text 2>/dev/null || echo "0")
    
    if [[ $stack_count -gt 200 ]]; then
        echo "⚠️  High number of CloudFormation stacks ($stack_count), may affect deployment"
    else
        print_debug "✓ CloudFormation stack count: $stack_count"
    fi
    
    return $validation_errors
}

# =============================================================================
# Main Validation Function
# =============================================================================

validate_configuration() {
    local config_file="${1:-}"
    local total_errors=0
    
    print_step "Starting configuration validation..."
    print_info "Configuration validator version: $CONFIG_VALIDATOR_VERSION"
    
    # If a config file is provided, load it
    if [[ -n "$config_file" && -f "$config_file" ]]; then
        print_info "Loading configuration file: $config_file"
        set -a
        source "$config_file"
        set +a
    fi
    
    # Validate required variables
    if ! validate_required_variables; then
        total_errors=$((total_errors + $?))
    fi
    
    # Validate configuration values
    if ! validate_configuration_values; then
        total_errors=$((total_errors + $?))
    fi
    
    # Validate AWS permissions
    if ! validate_aws_permissions; then
        total_errors=$((total_errors + $?))
    fi
    
    # Validate resource quotas
    if ! validate_resource_quotas; then
        total_errors=$((total_errors + $?))
    fi
    
    # Output validation results
    echo
    if [[ $total_errors -eq 0 ]]; then
        print_success "🎉 All configuration validations passed!"
        return 0
    else
        print_error "❌ Found $total_errors configuration errors"
        echo
        echo "Please fix the above errors and run validation again."
        echo "Reference documentation: configs/README.md"
        return 1
    fi
}

# =============================================================================
# Configuration Suggestions and Fixes
# =============================================================================

suggest_configuration_fixes() {
    print_step "Configuration fix suggestions..."
    
    # Check if sample config file exists
    if [[ ! -f "configs/config.env" ]]; then
        echo "💡 Suggestion: Create base configuration file"
        echo "   cp configs/config.env.example configs/config.env"
    fi
    
    # Check if local config file exists
    if [[ ! -f "configs/config.local.env" ]]; then
        echo "💡 Suggestion: Create local configuration file for personal settings"
        echo "   cp configs/config.env configs/config.local.env"
        echo "   # Then edit configs/config.local.env for personal customization"
    fi
    
    # Check AWS CLI configuration
    if [[ ! -f "$HOME/.aws/config" ]]; then
        echo "💡 Suggestion: Configure AWS CLI"
        echo "   aws configure"
        echo "   # Or set environment variables:"
        echo "   export AWS_REGION=us-east-1"
        echo "   export AWS_ACCESS_KEY_ID=your_key_id"
        echo "   export AWS_SECRET_ACCESS_KEY=your_secret_key"
    fi
}

generate_sample_config() {
    local output_file="${1:-configs/config.sample.env}"
    
    print_step "Generating sample configuration file: $output_file"
    
    cat > "$output_file" << 'EOF'
# =============================================================================
# AWS Data Lake Project Configuration Sample
# Copy this file to config.local.env and modify as needed
# =============================================================================

# Basic project settings
PROJECT_PREFIX=dl-handson
ENVIRONMENT=dev
AWS_REGION=us-east-1

# S3 configuration
S3_ENCRYPTION=AES256
S3_VERSIONING=Enabled
S3_LIFECYCLE_ENABLED=true

# EMR configuration
EMR_INSTANCE_TYPE=m5.xlarge
EMR_INSTANCE_COUNT=3
EMR_USE_SPOT_INSTANCES=false

# Glue configuration
GLUE_DATABASE_NAME=${PROJECT_PREFIX}-db-${ENVIRONMENT}

# Lake Formation configuration
LAKE_FORMATION_ADMIN_ENABLED=true

# Monitoring and logging
ENABLE_CLOUDTRAIL=true
ENABLE_COST_MONITORING=true

# Security configuration
ENABLE_BUCKET_NOTIFICATIONS=false
FORCE_SSL=true

# Optional: Override default resource names
# RAW_BUCKET_NAME=${PROJECT_PREFIX}-raw-${ENVIRONMENT}
# CLEAN_BUCKET_NAME=${PROJECT_PREFIX}-clean-${ENVIRONMENT}
# ANALYTICS_BUCKET_NAME=${PROJECT_PREFIX}-analytics-${ENVIRONMENT}
EOF
    
    print_success "Sample configuration file generated: $output_file"
    echo
    echo "Next steps:"
    echo "1. Copy sample file: cp $output_file configs/config.local.env"
    echo "2. Edit local configuration: nano configs/config.local.env"
    echo "3. Run validation: $0 configs/config.local.env"
}

# =============================================================================
# Main Function and CLI Interface
# =============================================================================

show_help() {
    cat << EOF
AWS Data Lake Configuration Validator v$CONFIG_VALIDATOR_VERSION

Usage: $0 [options] [config-file]

Options:
    -h, --help              Display this help information
    -g, --generate-sample   Generate sample configuration file
    -s, --suggest-fixes     Show configuration fix suggestions
    -v, --verbose           Verbose output
    --validate-only         Validate only, don't show suggestions

Parameters:
    config-file             Path to configuration file to validate (optional)

Examples:
    $0                                    # Validate current environment variables
    $0 configs/config.local.env           # Validate specified configuration file
    $0 --generate-sample                  # Generate sample configuration file
    $0 --suggest-fixes                    # Show fix suggestions

EOF
}

main() {
    local config_file=""
    local generate_sample=false
    local suggest_fixes=false
    local validate_only=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -g|--generate-sample)
                generate_sample=true
                shift
                ;;
            -s|--suggest-fixes)
                suggest_fixes=true
                shift
                ;;
            -v|--verbose)
                LOG_LEVEL="DEBUG"
                shift
                ;;
            --validate-only)
                validate_only=true
                shift
                ;;
            -*)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
            *)
                config_file="$1"
                shift
                ;;
        esac
    done
    
    # Execute corresponding operation
    if [[ "$generate_sample" == true ]]; then
        generate_sample_config
        exit 0
    fi
    
    if [[ "$suggest_fixes" == true ]]; then
        suggest_configuration_fixes
        exit 0
    fi
    
    # Execute configuration validation
    if validate_configuration "$config_file"; then
        if [[ "$validate_only" != true ]]; then
            echo
            print_info "Configuration validation completed, ready to deploy!"
        fi
        exit 0
    else
        if [[ "$validate_only" != true ]]; then
            echo
            suggest_configuration_fixes
        fi
        exit 1
    fi
}

# If this script is executed directly, run the main function
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi