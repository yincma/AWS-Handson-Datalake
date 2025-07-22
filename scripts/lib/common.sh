#!/bin/bash

# =============================================================================
# AWS 数据湖项目通用工具库
# 版本: 2.0.0
# 描述: 提供统一的日志、错误处理、重试机制和通用工具函数
# =============================================================================

set -euo pipefail

# 全局变量
readonly COMMON_LIB_VERSION="2.0.0"
declare -g LOG_LEVEL="${LOG_LEVEL:-INFO}"
declare -g LOG_FORMAT="${LOG_FORMAT:-console}"
declare -g MAX_RETRIES="${MAX_RETRIES:-3}"
declare -g RETRY_DELAY="${RETRY_DELAY:-2}"

# 颜色定义
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly PURPLE='\033[0;35m'
readonly CYAN='\033[0;36m'
readonly WHITE='\033[1;37m'
readonly NC='\033[0m' # No Color

# 日志级别枚举
readonly LOG_LEVEL_DEBUG=10
readonly LOG_LEVEL_INFO=20
readonly LOG_LEVEL_WARNING=30
readonly LOG_LEVEL_ERROR=40
readonly LOG_LEVEL_CRITICAL=50

# =============================================================================
# 日志系统
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
        
        # 同时写入日志文件（如果设置了LOG_FILE）
        if [[ -n "${LOG_FILE:-}" ]]; then
            echo "$formatted_message" >> "$LOG_FILE"
        fi
        
        # 发送到syslog
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
# 错误处理系统
# =============================================================================

declare -A error_handlers
declare -g cleanup_functions=()

register_error_handler() {
    local error_pattern="$1"
    local handler_function="$2"
    error_handlers["$error_pattern"]="$handler_function"
}

register_cleanup_function() {
    local cleanup_function="$1"
    cleanup_functions+=("$cleanup_function")
}

cleanup_on_error() {
    print_warning "执行清理函数..."
    for cleanup_func in "${cleanup_functions[@]}"; do
        if declare -F "$cleanup_func" >/dev/null; then
            print_debug "执行清理函数: $cleanup_func"
            "$cleanup_func" || print_error "清理函数失败: $cleanup_func"
        fi
    done
}

handle_error() {
    local exit_code="${1:-1}"
    local error_message="${2:-未知错误}"
    local context="${3:-}"
    
    print_error "$error_message"
    
    if [[ -n "$context" ]]; then
        print_debug "错误上下文: $context"
    fi
    
    # 查找匹配的错误处理器
    for pattern in "${!error_handlers[@]}"; do
        if [[ "$error_message" =~ $pattern ]]; then
            local handler="${error_handlers[$pattern]}"
            print_debug "执行错误处理器: $handler"
            if declare -F "$handler" >/dev/null; then
                "$handler" "$error_message" "$context"
            fi
        fi
    done
    
    cleanup_on_error
    exit "$exit_code"
}

setup_error_trap() {
    trap 'handle_error $? "脚本执行失败" "Line: $LINENO, Command: $BASH_COMMAND"' ERR
    trap 'cleanup_on_error; exit 130' INT TERM
}

# =============================================================================
# 重试机制
# =============================================================================

retry_with_backoff() {
    local max_attempts="${1:-$MAX_RETRIES}"
    local initial_delay="${2:-$RETRY_DELAY}"
    local backoff_factor="${3:-2}"
    shift 3
    
    local attempt=1
    local delay="$initial_delay"
    
    while [[ $attempt -le $max_attempts ]]; do
        print_debug "尝试 $attempt/$max_attempts: $*"
        
        if "$@"; then
            if [[ $attempt -gt 1 ]]; then
                print_success "命令在第 $attempt 次尝试中成功执行"
            fi
            return 0
        fi
        
        local exit_code=$?
        
        if [[ $attempt -lt $max_attempts ]]; then
            print_warning "尝试 $attempt 失败（退出代码: $exit_code），${delay}秒后重试..."
            sleep "$delay"
            delay=$((delay * backoff_factor))
        else
            print_error "所有 $max_attempts 次尝试都失败了"
        fi
        
        attempt=$((attempt + 1))
    done
    
    return $exit_code
}

# 特定于AWS的重试逻辑
retry_aws_command() {
    local max_attempts="${1:-5}"
    shift
    
    retry_with_backoff "$max_attempts" 2 2 "$@"
}

# =============================================================================
# 系统验证和前置检查
# =============================================================================

check_command() {
    local cmd="$1"
    local install_hint="${2:-}"
    
    if ! command -v "$cmd" &> /dev/null; then
        local error_msg="命令 '$cmd' 未找到"
        if [[ -n "$install_hint" ]]; then
            error_msg="$error_msg. $install_hint"
        fi
        handle_error 1 "$error_msg"
    fi
    
    print_debug "✓ 命令 '$cmd' 可用"
}

check_aws_cli_version() {
    check_command "aws" "请安装 AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    
    local version
    version=$(aws --version 2>&1 | cut -d/ -f2 | cut -d' ' -f1)
    local major_version=${version%%.*}
    
    if [[ $major_version -lt 2 ]]; then
        handle_error 1 "需要 AWS CLI v2 或更高版本，当前版本: $version"
    fi
    
    print_debug "✓ AWS CLI 版本: $version"
}

validate_aws_credentials() {
    print_debug "验证 AWS 凭证..."
    
    if ! aws sts get-caller-identity &>/dev/null; then
        handle_error 1 "AWS 凭证无效或未配置。请运行 'aws configure' 或设置相关环境变量"
    fi
    
    local account_id region
    account_id=$(aws sts get-caller-identity --query Account --output text)
    region=$(aws configure get region || echo "${AWS_REGION:-}")
    
    if [[ -z "$region" ]]; then
        handle_error 1 "AWS 区域未设置。请设置 AWS_REGION 环境变量或运行 'aws configure'"
    fi
    
    print_debug "✓ AWS 账户: $account_id, 区域: $region"
    
    # 导出供其他脚本使用
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
            print_debug "✓ 环境变量 $var 已设置"
        fi
    done
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        local error_msg="缺少必需的环境变量: $(IFS=', '; echo "${missing_vars[*]}")"
        handle_error 1 "$error_msg"
    fi
}

validate_prerequisites() {
    print_step "验证系统前置条件..."
    
    # 检查必需的命令
    check_command "jq" "请安装 jq: sudo apt-get install jq 或 brew install jq"
    check_command "curl"
    check_aws_cli_version
    
    # 验证 AWS 凭证
    validate_aws_credentials
    
    # 检查必需的环境变量
    verify_required_env_vars "PROJECT_PREFIX" "ENVIRONMENT"
    
    print_success "所有前置条件验证通过"
}

# =============================================================================
# 配置管理
# =============================================================================

load_config() {
    local config_file="${1:-configs/config.env}"
    local local_config="${config_file%.*}.local.env"
    
    print_debug "加载配置文件..."
    
    # 加载主配置文件
    if [[ -f "$config_file" ]]; then
        print_debug "加载主配置: $config_file"
        set -a
        source "$config_file"
        set +a
    else
        print_warning "主配置文件不存在: $config_file"
    fi
    
    # 加载本地覆盖配置
    if [[ -f "$local_config" ]]; then
        print_debug "加载本地配置: $local_config"
        set -a
        source "$local_config"
        set +a
    fi
    
    # 设置默认值
    export PROJECT_PREFIX="${PROJECT_PREFIX:-dl-handson}"
    export ENVIRONMENT="${ENVIRONMENT:-dev}"
    export AWS_REGION="${AWS_REGION:-us-east-1}"
    
    print_debug "✓ 配置加载完成"
}

# =============================================================================
# AWS 资源名称生成
# =============================================================================

generate_resource_name() {
    local resource_type="$1"
    local suffix="${2:-}"
    
    local name_parts=("$PROJECT_PREFIX" "$resource_type" "$ENVIRONMENT")
    
    if [[ -n "$suffix" ]]; then
        name_parts+=("$suffix")
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

# =============================================================================
# 进度和状态管理
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
# AWS 资源检查
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
# 实用工具函数
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
    local timeout="${2:-1800}" # 30分钟默认超时
    local start_time
    start_time=$(date +%s)
    
    print_info "等待堆栈操作完成: $stack_name"
    
    while true; do
        local status
        status=$(get_stack_status "$stack_name")
        
        case "$status" in
            *COMPLETE)
                print_success "堆栈操作成功完成: $stack_name"
                return 0
                ;;
            *FAILED*|*ROLLBACK*)
                print_error "堆栈操作失败: $stack_name (状态: $status)"
                return 1
                ;;
            *IN_PROGRESS)
                local current_time
                current_time=$(date +%s)
                local elapsed=$((current_time - start_time))
                
                if [[ $elapsed -gt $timeout ]]; then
                    print_error "等待堆栈操作超时: $stack_name"
                    return 1
                fi
                
                print_debug "堆栈状态: $status (已等待 ${elapsed}s)"
                sleep 10
                ;;
            DOES_NOT_EXIST)
                print_error "堆栈不存在: $stack_name"
                return 1
                ;;
        esac
    done
}

# =============================================================================
# 性能和监控
# =============================================================================

start_timer() {
    local timer_name="$1"
    declare -g "timer_${timer_name}_start=$(date +%s.%N)"
}

end_timer() {
    local timer_name="$1"
    local start_var="timer_${timer_name}_start"
    local start_time="${!start_var:-}"
    
    if [[ -z "$start_time" ]]; then
        print_warning "计时器 '$timer_name' 未启动"
        return 1
    fi
    
    local end_time
    end_time=$(date +%s.%N)
    local duration
    duration=$(echo "$end_time - $start_time" | bc)
    
    print_info "⏱️  操作 '$timer_name' 耗时: ${duration}s"
    
    # 清理计时器变量
    unset "$start_var"
}

# =============================================================================
# 初始化
# =============================================================================

init_common_lib() {
    print_debug "初始化通用工具库 v$COMMON_LIB_VERSION"
    
    # 设置错误捕获
    setup_error_trap
    
    # 检查并安装 bc（用于计算）
    if ! command -v bc &>/dev/null && [[ "$OSTYPE" == "darwin"* ]]; then
        print_warning "在 macOS 上未找到 bc 命令，某些计算功能可能不可用"
    fi
    
    print_debug "✓ 通用工具库初始化完成"
}

# 自动初始化
init_common_lib

# =============================================================================
# 导出公共函数
# =============================================================================

# 这里可以定义哪些函数应该被导出给其他脚本使用
# 注意：在 bash 中，所有函数默认都是"全局"的，这里主要用于文档目的
export -f print_debug print_info print_warning print_error print_critical
export -f print_step print_success print_failure
export -f handle_error retry_with_backoff retry_aws_command
export -f validate_prerequisites load_config
export -f generate_resource_name get_s3_bucket_name get_stack_name
export -f check_stack_exists check_s3_bucket_exists get_stack_status
export -f confirm_action wait_for_stack_completion
export -f start_timer end_timer