#!/bin/bash

# =============================================================================
# AWS Data Lake Project Common Utility Library
# Version: 2.0.0
# Description: Provides unified logging, error handling, retry mechanism and common utility functions
# =============================================================================

set -eu
# pipefailはBash 3.x互換性のため無効化

# Global variables
# Prevent duplicate loading
if [[ -z "${COMMON_LIB_VERSION:-}" ]]; then
    readonly COMMON_LIB_VERSION="2.0.0"
fi
LOG_LEVEL="${LOG_LEVEL:-INFO}"
LOG_FORMAT="${LOG_FORMAT:-console}"
MAX_RETRIES="${MAX_RETRIES:-3}"
RETRY_DELAY="${RETRY_DELAY:-2}"

# Color definitions - prevent duplicate loading
if [[ -z "${RED:-}" ]]; then
    readonly RED='\033[0;31m'
    readonly GREEN='\033[0;32m'
    readonly YELLOW='\033[1;33m'
    readonly BLUE='\033[0;34m'
    readonly PURPLE='\033[0;35m'
    readonly CYAN='\033[0;36m'
    readonly WHITE='\033[1;37m'
    readonly NC='\033[0m' # No Color
fi

# Log level enumeration - prevent duplicate loading  
if [[ -z "${LOG_LEVEL_DEBUG:-}" ]]; then
    readonly LOG_LEVEL_DEBUG=10
    readonly LOG_LEVEL_INFO=20
    readonly LOG_LEVEL_WARNING=30
    readonly LOG_LEVEL_ERROR=40
    readonly LOG_LEVEL_CRITICAL=50
fi

# =============================================================================
# Logging System
# =============================================================================

get_log_level_num() {
    case "${1:-INFO}" in
        DEBUG) echo $LOG_LEVEL_DEBUG ;;
        INFO) echo $LOG_LEVEL_INFO ;;
        WARNING|WARN) echo $LOG_LEVEL_WARNING ;;
        ERROR) echo $LOG_LEVEL_ERROR ;;
        CRITICAL) echo $LOG_LEVEL_CRITICAL ;;
        *) echo $LOG_LEVEL_INFO ;;
    esac
}

should_log() {
    local level="$1"
    local current_level_num=$(get_log_level_num "$LOG_LEVEL")
    local message_level_num=$(get_log_level_num "$level")
    
    [[ $message_level_num -ge $current_level_num ]]
}

format_log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local caller="${BASH_SOURCE[3]##*/}:${BASH_LINENO[2]}"
    
    case "$LOG_FORMAT" in
        json)
            jq -n \
                --arg timestamp "$timestamp" \
                --arg level "$level" \
                --arg message "$message" \
                --arg caller "$caller" \
                --arg service "${SERVICE_NAME:-datalake}" \
                '{timestamp: $timestamp, level: $level, message: $message, caller: $caller, service: $service}'
            ;;
        console|*)
            echo "[$timestamp] [$level] [$caller] $message"
            ;;
    esac
}

log_message() {
    local level="$1"
    local message="$2"
    local color="$3"
    
    if should_log "$level"; then
        local formatted_message
        formatted_message=$(format_log_message "$level" "$message")
        
        if [[ -t 1 && "$LOG_FORMAT" == "console" ]]; then
            echo -e "${color}${formatted_message}${NC}" >&2
        else
            echo "$formatted_message" >&2
        fi
        
        # Also write to log file (if LOG_FILE is set)
        if [[ -n "${LOG_FILE:-}" ]]; then
            echo "$formatted_message" >> "$LOG_FILE"
        fi
        
        # Send to syslog
        logger -t "datalake" "$level: $message" 2>/dev/null || true
    fi
}

print_debug() { log_message "DEBUG" "$1" "$CYAN"; }
print_info() { log_message "INFO" "$1" "$GREEN"; }
print_warning() { log_message "WARNING" "$1" "$YELLOW"; }
print_error() { log_message "ERROR" "$1" "$RED"; }
print_critical() { log_message "CRITICAL" "$1" "$PURPLE"; }

print_step() { 
    echo
    log_message "INFO" "▶ $1" "$BLUE"
    echo
}

print_success() { log_message "INFO" "✅ $1" "$GREEN"; }
print_failure() { log_message "ERROR" "❌ $1" "$RED"; }

# =============================================================================
# Error Handling System
# =============================================================================

# Bash 3.x互換性のため連想配列を無効化
# declare -A error_handlers
# declare -g cleanup_functions=()
error_handlers=""
cleanup_functions=""

register_error_handler() {
    # Bash 3.x互換性のため無効化
    print_debug "Error handler registration disabled (Bash 3.x compatibility)"
}

register_cleanup_function() {
    # Bash 3.x互換性のため無効化
    print_debug "Cleanup function registration disabled (Bash 3.x compatibility)"
}

cleanup_on_error() {
    # Bash 3.x互換性のため無効化
    print_warning "Cleanup functionality disabled (Bash 3.x compatibility)"
}

handle_error() {
    local exit_code="${1:-1}"
    local error_message="${2:-Unknown error}"
    local context="${3:-}"
    
    print_error "$error_message"
    
    if [[ -n "$context" ]]; then
        print_debug "Error context: $context"
    fi
    
    # Find matching error handler
    for pattern in "${!error_handlers[@]}"; do
        if [[ "$error_message" =~ $pattern ]]; then
            local handler="${error_handlers[$pattern]}"
            print_debug "Executing error handler: $handler"
            if declare -F "$handler" >/dev/null; then
                "$handler" "$error_message" "$context"
            fi
        fi
    done
    
    cleanup_on_error
    exit "$exit_code"
}

setup_error_trap() {
    trap 'handle_error $? "Script execution failed" "Line: $LINENO, Command: $BASH_COMMAND"' ERR
    trap 'cleanup_on_error; exit 130' INT TERM
}

# =============================================================================
# Retry Mechanism
# =============================================================================

retry_with_backoff() {
    local max_attempts="${1:-$MAX_RETRIES}"
    local initial_delay="${2:-$RETRY_DELAY}"
    local backoff_factor="${3:-2}"
    shift 3
    
    local attempt=1
    local delay="$initial_delay"
    
    while [[ $attempt -le $max_attempts ]]; do
        print_debug "Attempt $attempt/$max_attempts: $*"
        
        if "$@"; then
            if [[ $attempt -gt 1 ]]; then
                print_success "Command succeeded on attempt $attempt"
            fi
            return 0
        fi
        
        local exit_code=$?
        
        if [[ $attempt -lt $max_attempts ]]; then
            print_warning "Attempt $attempt failed (exit code: $exit_code), retrying in ${delay}s..."
            sleep "$delay"
            delay=$((delay * backoff_factor))
        else
            print_error "All $max_attempts attempts failed"
        fi
        
        attempt=$((attempt + 1))
    done
    
    return $exit_code
}

# AWS-specific retry logic
retry_aws_command() {
    # AWS command specific retry function with fixed retry parameters
    retry_with_backoff 5 2 2 "$@"
}

# =============================================================================
# System Validation and Prerequisite Checks
# =============================================================================

check_command() {
    local cmd="$1"
    local install_hint="${2:-}"
    
    if ! command -v "$cmd" &> /dev/null; then
        local error_msg="Command '$cmd' not found"
        if [[ -n "$install_hint" ]]; then
            error_msg="$error_msg. $install_hint"
        fi
        handle_error 1 "$error_msg"
    fi
    
    print_debug "✓ Command '$cmd' is available"
}

check_aws_cli_version() {
    check_command "aws" "Please install AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    
    local version
    version=$(aws --version 2>&1 | cut -d/ -f2 | cut -d' ' -f1)
    local major_version=${version%%.*}
    
    if [[ $major_version -lt 1 ]]; then
        handle_error 1 "AWS CLI v1.18+ or v2+ required, current version: $version"
    fi
    
    if [[ $major_version -eq 1 ]]; then
        print_warning "Using AWS CLI v1 ($version). Upgrade to v2 recommended"
        print_info "Update: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    fi
    
    print_debug "✓ AWS CLI version: $version"
}

validate_aws_credentials() {
    print_debug "Validating AWS credentials..."
    
    if ! aws sts get-caller-identity &>/dev/null; then
        handle_error 1 "AWS credentials invalid or not configured. Please run 'aws configure' or set appropriate environment variables"
    fi
    
    local account_id region
    account_id=$(aws sts get-caller-identity --query Account --output text)
    region=$(aws configure get region || echo "${AWS_REGION:-}")
    
    if [[ -z "$region" ]]; then
        handle_error 1 "AWS region not set. Please set AWS_REGION environment variable or run 'aws configure'"
    fi
    
    print_debug "✓ AWS Account: $account_id, Region: $region"
    
    # Export for use by other scripts
    export AWS_ACCOUNT_ID="$account_id"
    export AWS_REGION="$region"
}

verify_required_env_vars() {
    local required_vars=("$@")
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            missing_vars+=("$var")
        else
            print_debug "✓ Environment variable $var is set"
        fi
    done
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        local error_msg="Missing required environment variables: $(IFS=', '; echo "${missing_vars[*]}")"
        handle_error 1 "$error_msg"
    fi
}

validate_prerequisites() {
    print_step "Validating system prerequisites..."
    
    # Check required commands
    check_command "jq" "Please install jq: sudo apt-get install jq or brew install jq"
    check_command "curl"
    check_aws_cli_version
    
    # Validate AWS credentials
    validate_aws_credentials
    
    # Check required environment variables
    verify_required_env_vars "PROJECT_PREFIX" "ENVIRONMENT"
    
    print_success "All prerequisites validated successfully"
}

# =============================================================================
# Configuration Management
# =============================================================================

load_config() {
    local config_file="${1:-configs/config.env}"
    local local_config="${config_file%.*}.local.env"
    
    print_debug "Loading configuration files..."
    
    # Load main configuration file
    if [[ -f "$config_file" ]]; then
        print_debug "Loading main config: $config_file"
        set -a
        source "$config_file"
        set +a
    else
        print_warning "Main config file does not exist: $config_file"
    fi
    
    # Load local override config
    if [[ -f "$local_config" ]]; then
        print_debug "Loading local config: $local_config"
        set -a
        source "$local_config"
        set +a
    fi
    
    # Set default values
    export PROJECT_PREFIX="${PROJECT_PREFIX:-dl-handson}"
    export ENVIRONMENT="${ENVIRONMENT:-dev}"
    export AWS_REGION="${AWS_REGION:-us-east-1}"
    
    print_debug "✓ Configuration loaded successfully"
}

# =============================================================================
# AWS Resource Name Generation
# =============================================================================

generate_resource_name() {
    local resource_type="$1"
    local suffix="${2:-}"
    
    # 根据资源类型决定命名格式
    local name_parts=()
    if [[ -n "$suffix" ]]; then
        # 如果有后缀，格式为：PROJECT_PREFIX-resource_type-suffix-ENVIRONMENT
        name_parts=("$PROJECT_PREFIX" "$resource_type" "$suffix" "$ENVIRONMENT")
    else
        # 如果没有后缀，格式为：PROJECT_PREFIX-resource_type-ENVIRONMENT
        name_parts=("$PROJECT_PREFIX" "$resource_type" "$ENVIRONMENT")
    fi
    
    local resource_name
    IFS='-'; resource_name="${name_parts[*]}"; unset IFS
    echo "$resource_name"
}

get_s3_bucket_name() {
    local layer="$1"
    generate_resource_name "$layer"
}

get_stack_name() {
    local stack_type="$1"
    generate_resource_name "stack" "$stack_type"
}

# 获取依赖堆栈名称的统一函数
# 注意：如果加载了 compatibility.sh，check_stack_exists_any 函数会处理新旧命名约定
get_dependency_stack_name() {
    local module_type="$1"
    
    # 如果 check_stack_exists_any 函数可用（来自 compatibility.sh），使用它
    if declare -F check_stack_exists_any >/dev/null 2>&1; then
        local existing_stack
        existing_stack=$(check_stack_exists_any "$module_type" 2>/dev/null || echo "")
        
        if [[ -n "$existing_stack" ]]; then
            echo "$existing_stack"
            return 0
        fi
    fi
    
    # 否则，返回标准的堆栈名称
    get_stack_name "$module_type"
}

# =============================================================================
# Progress and Status Management
# =============================================================================

show_progress() {
    local current="$1"
    local total="$2"
    local description="${3:-}"
    
    local percentage=$((current * 100 / total))
    local completed=$((current * 50 / total))
    local remaining=$((50 - completed))
    
    printf "\r["
    printf "%*s" $completed | tr ' ' '█'
    printf "%*s" $remaining | tr ' ' '░'
    printf "] %3d%% (%d/%d) %s" $percentage $current $total "$description"
}

finish_progress() {
    echo
}

# =============================================================================
# AWS Resource Checks
# =============================================================================

check_stack_exists() {
    local stack_name="$1"
    aws cloudformation describe-stacks --stack-name "$stack_name" &>/dev/null
}

check_s3_bucket_exists() {
    local bucket_name="$1"
    aws s3api head-bucket --bucket "$bucket_name" &>/dev/null
}

get_stack_status() {
    local stack_name="$1"
    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "DOES_NOT_EXIST"
}

# =============================================================================
# Utility Functions
# =============================================================================

confirm_action() {
    local message="$1"
    local default="${2:-n}"
    
    local prompt="$message"
    case "$default" in
        [yY]) prompt="$prompt [Y/n]: " ;;
        [nN]) prompt="$prompt [y/N]: " ;;
        *) prompt="$prompt [y/n]: " ;;
    esac
    
    local response
    read -r -p "$prompt" response
    
    case "${response:-$default}" in
        [yY]|[yY][eE][sS]) return 0 ;;
        *) return 1 ;;
    esac
}

wait_for_stack_completion() {
    local stack_name="$1"
    local timeout="${2:-1800}" # 30 minutes default timeout
    local start_time
    start_time=$(date +%s)
    
    print_info "Waiting for stack operation to complete: $stack_name"
    
    while true; do
        local status
        status=$(get_stack_status "$stack_name")
        
        case "$status" in
            *COMPLETE)
                print_success "Stack operation completed successfully: $stack_name"
                return 0
                ;;
            *FAILED*|*ROLLBACK*)
                print_error "Stack operation failed: $stack_name (status: $status)"
                return 1
                ;;
            *IN_PROGRESS)
                local current_time
                current_time=$(date +%s)
                local elapsed=$((current_time - start_time))
                
                if [[ $elapsed -gt $timeout ]]; then
                    print_error "Stack operation timed out: $stack_name"
                    return 1
                fi
                
                print_debug "Stack status: $status (waited ${elapsed}s)"
                sleep 10
                ;;
            DOES_NOT_EXIST)
                print_error "Stack does not exist: $stack_name"
                return 1
                ;;
        esac
    done
}

wait_for_stack_deletion() {
    local stack_name="$1"
    local timeout="${2:-600}" # 10 minutes default timeout for deletion
    local start_time
    start_time=$(date +%s)
    
    print_info "Waiting for stack deletion to complete: $stack_name"
    
    while true; do
        local status
        status=$(get_stack_status "$stack_name")
        
        case "$status" in
            DELETE_COMPLETE|DOES_NOT_EXIST)
                print_success "Stack deleted successfully: $stack_name"
                return 0
                ;;
            DELETE_FAILED)
                print_error "Stack deletion failed: $stack_name"
                return 1
                ;;
            DELETE_IN_PROGRESS)
                local current_time
                current_time=$(date +%s)
                local elapsed=$((current_time - start_time))
                
                if [[ $elapsed -gt $timeout ]]; then
                    print_error "Stack deletion timed out: $stack_name"
                    return 1
                fi
                
                print_debug "Stack deletion in progress (waited ${elapsed}s)"
                sleep 10
                ;;
            *)
                print_warning "Unexpected status during deletion: $status"
                return 1
                ;;
        esac
    done
}

# =============================================================================
# Performance and Monitoring
# =============================================================================

start_timer() {
    local timer_name="$1"
    # Bash 3.x互換性のためグローバル変数宣言を無効化
    eval "timer_${timer_name}_start=$(date +%s)"
}

end_timer() {
    local timer_name="$1"
    # Bash 3.x互換性のため単純化
    local start_time
    start_time=$(eval "echo \$timer_${timer_name}_start")
    
    if [[ -z "$start_time" ]]; then
        print_warning "Timer '$timer_name' not started"
        return 1
    fi
    
    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    print_info "⏱️  Operation '$timer_name' took: ${duration}s"
    
    # Clean up timer variable - disabled for Bash 3.x compatibility
    # unset "$start_var"
}

# =============================================================================
# Initialization
# =============================================================================

init_common_lib() {
    print_debug "Initializing common utility library v$COMMON_LIB_VERSION"
    
    # Set up error trapping
    setup_error_trap
    
    # Check and install bc (for calculations)
    if ! command -v bc &>/dev/null && [[ "$OSTYPE" == "darwin"* ]]; then
        print_warning "bc command not found on macOS, some calculation features may not be available"
    fi
    
    print_debug "✓ Common utility library initialization complete"
}

# Auto-initialize
init_common_lib

# =============================================================================
# Export Public Functions
# =============================================================================

# Define which functions should be exported for use by other scripts
# Note: In bash, all functions are "global" by default, this is mainly for documentation purposes
export -f print_debug print_info print_warning print_error print_critical
export -f print_step print_success print_failure
export -f handle_error retry_with_backoff retry_aws_command
export -f validate_prerequisites load_config
export -f generate_resource_name get_s3_bucket_name get_stack_name get_dependency_stack_name
export -f check_stack_exists check_s3_bucket_exists get_stack_status
export -f confirm_action wait_for_stack_completion wait_for_stack_deletion
export -f start_timer end_timer