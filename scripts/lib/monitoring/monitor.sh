#!/bin/bash

# =============================================================================
# 监控和追踪接口 (Bash版本)
# 版本: 1.0.0
# 描述: 为Bash脚本提供监控、追踪和指标收集功能
# =============================================================================

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# 加载通用工具库
source "$SCRIPT_DIR/../common.sh"

readonly MONITOR_VERSION="1.0.0"

# =============================================================================
# 监控配置
# =============================================================================

# 全局配置
MONITORING_ENABLED="${MONITORING_ENABLED:-true}"
SERVICE_NAME="${SERVICE_NAME:-datalake}"
TRACE_ID="${TRACE_ID:-}"
PARENT_SPAN_ID="${PARENT_SPAN_ID:-}"

# CloudWatch配置
METRICS_NAMESPACE="${METRICS_NAMESPACE:-DataLake/Operations}"
LOGS_GROUP_PREFIX="${LOGS_GROUP_PREFIX:-/aws/datalake}"

# 缓冲区配置
METRICS_BUFFER_FILE="/tmp/datalake_metrics_$$.json"
EVENTS_BUFFER_FILE="/tmp/datalake_events_$$.json"
MAX_BUFFER_SIZE=50

# 清理函数
cleanup_monitoring_files() {
    rm -f "$METRICS_BUFFER_FILE" "$EVENTS_BUFFER_FILE" 2>/dev/null || true
}

# 注册清理函数
register_cleanup_function cleanup_monitoring_files

# =============================================================================
# 追踪管理
# =============================================================================

generate_trace_id() {
    if command -v uuidgen &>/dev/null; then
        uuidgen | tr '[:upper:]' '[:lower:]'
    else
        # 备用方案：使用随机字符串
        echo "$(date +%s)-$$-$RANDOM" | md5sum 2>/dev/null | cut -d' ' -f1 || \
        echo "$(date +%s)-$$-$RANDOM"
    fi
}

generate_span_id() {
    generate_trace_id
}

init_trace() {
    local operation_name="$1"
    
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        return 0
    fi
    
    # 生成追踪ID（如果不存在）
    if [[ -z "$TRACE_ID" ]]; then
        TRACE_ID=$(generate_trace_id)
        export TRACE_ID
        print_debug "生成新的追踪ID: $TRACE_ID"
    fi
    
    # 记录追踪开始事件
    record_event "info" "$operation_name" "追踪开始" \
        "trace_id=$TRACE_ID" \
        "service=$SERVICE_NAME" \
        "operation=$operation_name"
}

# =============================================================================
# Span管理
# =============================================================================

declare -A ACTIVE_SPANS
declare -A SPAN_START_TIMES

start_span() {
    local operation_name="$1"
    local parent_span_id="${2:-$PARENT_SPAN_ID}"
    
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        echo "noop-span"
        return 0
    fi
    
    local span_id
    span_id=$(generate_span_id)
    local start_time
    start_time=$(date +%s.%N)
    
    # 存储span信息
    ACTIVE_SPANS["$span_id"]="$operation_name"
    SPAN_START_TIMES["$span_id"]="$start_time"
    
    # 设置当前span为新span的父span
    export PARENT_SPAN_ID="$span_id"
    
    print_debug "开始span: $operation_name (ID: $span_id)"
    
    # 记录span开始事件
    record_event "debug" "$operation_name" "Span开始" \
        "trace_id=$TRACE_ID" \
        "span_id=$span_id" \
        "parent_span_id=$parent_span_id"
    
    echo "$span_id"
}

finish_span() {
    local span_id="$1"
    local status="${2:-ok}"
    local error_message="${3:-}"
    
    if [[ "$MONITORING_ENABLED" != "true" || "$span_id" == "noop-span" ]]; then
        return 0
    fi
    
    local operation_name="${ACTIVE_SPANS[$span_id]:-unknown}"
    local start_time="${SPAN_START_TIMES[$span_id]:-}"
    
    if [[ -z "$start_time" ]]; then
        print_warning "Span开始时间未找到: $span_id"
        return 1
    fi
    
    # 计算持续时间
    local end_time
    end_time=$(date +%s.%N)
    local duration_ms
    duration_ms=$(echo "($end_time - $start_time) * 1000" | bc -l 2>/dev/null | cut -d. -f1)
    
    print_debug "结束span: $operation_name (持续时间: ${duration_ms}ms)"
    
    # 发送持续时间指标
    record_metric "OperationDuration" "$duration_ms" "Milliseconds" \
        "Service=$SERVICE_NAME" \
        "Operation=$operation_name" \
        "Status=$status"
    
    # 如果是错误，发送错误指标
    if [[ "$status" == "error" ]]; then
        record_metric "OperationErrors" "1" "Count" \
            "Service=$SERVICE_NAME" \
            "Operation=$operation_name"
        
        # 记录错误事件
        record_event "error" "$operation_name" "操作失败: ${error_message:-未知错误}" \
            "trace_id=$TRACE_ID" \
            "span_id=$span_id" \
            "duration_ms=$duration_ms"
    else
        # 记录成功事件
        record_event "info" "$operation_name" "操作成功" \
            "trace_id=$TRACE_ID" \
            "span_id=$span_id" \
            "duration_ms=$duration_ms"
    fi
    
    # 清理span信息
    unset ACTIVE_SPANS["$span_id"]
    unset SPAN_START_TIMES["$span_id"]
}

# Span包装器函数
with_span() {
    local operation_name="$1"
    shift
    
    local span_id
    span_id=$(start_span "$operation_name")
    
    local exit_code=0
    local error_message=""
    
    # 执行命令
    if ! "$@"; then
        exit_code=$?
        error_message="命令执行失败: $* (退出代码: $exit_code)"
    fi
    
    # 结束span
    if [[ $exit_code -eq 0 ]]; then
        finish_span "$span_id" "ok"
    else
        finish_span "$span_id" "error" "$error_message"
    fi
    
    return $exit_code
}

# =============================================================================
# 指标收集
# =============================================================================

record_metric() {
    local metric_name="$1"
    local value="$2"
    local unit="${3:-Count}"
    shift 3
    local dimensions=("$@")
    
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        return 0
    fi
    
    # 构建指标JSON
    local timestamp
    timestamp=$(date -Iseconds)
    
    local dimensions_json="["
    local first=true
    
    for dim in "${dimensions[@]}"; do
        if [[ "$dim" == *"="* ]]; then
            local key="${dim%%=*}"
            local value="${dim#*=}"
            
            if [[ "$first" == true ]]; then
                first=false
            else
                dimensions_json+=","
            fi
            
            dimensions_json+="{\"Name\":\"$key\",\"Value\":\"$value\"}"
        fi
    done
    
    dimensions_json+="]"
    
    # 创建指标条目
    local metric_json
    metric_json=$(cat << EOF
{
    "MetricName": "$metric_name",
    "Value": $value,
    "Unit": "$unit",
    "Timestamp": "$timestamp",
    "Dimensions": $dimensions_json
}
EOF
    )
    
    # 添加到缓冲区
    echo "$metric_json" >> "$METRICS_BUFFER_FILE"
    
    print_debug "记录指标: $metric_name = $value $unit"
    
    # 检查缓冲区大小
    if [[ -f "$METRICS_BUFFER_FILE" ]]; then
        local line_count
        line_count=$(wc -l < "$METRICS_BUFFER_FILE")
        if [[ $line_count -ge $MAX_BUFFER_SIZE ]]; then
            flush_metrics
        fi
    fi
}

flush_metrics() {
    if [[ "$MONITORING_ENABLED" != "true" || ! -f "$METRICS_BUFFER_FILE" ]]; then
        return 0
    fi
    
    print_debug "刷新指标缓冲区到CloudWatch"
    
    # 构建指标数据数组
    local metrics_json="["
    local first=true
    
    while IFS= read -r line; do
        if [[ -n "$line" ]]; then
            if [[ "$first" == true ]]; then
                first=false
            else
                metrics_json+=","
            fi
            metrics_json+="$line"
        fi
    done < "$METRICS_BUFFER_FILE"
    
    metrics_json+="]"
    
    # 发送到CloudWatch
    if aws cloudwatch put-metric-data \
        --namespace "$METRICS_NAMESPACE" \
        --metric-data "$metrics_json" >/dev/null 2>&1; then
        
        print_debug "成功发送 $(wc -l < "$METRICS_BUFFER_FILE") 个指标"
        > "$METRICS_BUFFER_FILE"  # 清空文件
    else
        print_warning "发送指标到CloudWatch失败"
    fi
}

# =============================================================================
# 事件记录
# =============================================================================

record_event() {
    local level="$1"
    local operation="$2"
    local message="$3"
    shift 3
    local metadata=("$@")
    
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        return 0
    fi
    
    # 构建元数据JSON
    local metadata_json="{"
    local first=true
    
    for meta in "${metadata[@]}"; do
        if [[ "$meta" == *"="* ]]; then
            local key="${meta%%=*}"
            local value="${meta#*=}"
            
            if [[ "$first" == true ]]; then
                first=false
            else
                metadata_json+=","
            fi
            
            metadata_json+="\"$key\":\"$value\""
        fi
    done
    
    metadata_json+="}"
    
    # 创建事件条目
    local timestamp
    timestamp=$(date -Iseconds)
    
    local event_json
    event_json=$(cat << EOF
{
    "timestamp": "$timestamp",
    "level": "$level",
    "service": "$SERVICE_NAME",
    "operation": "$operation",
    "message": "$message",
    "trace_id": "$TRACE_ID",
    "metadata": $metadata_json
}
EOF
    )
    
    # 添加到缓冲区
    echo "$event_json" >> "$EVENTS_BUFFER_FILE"
    
    print_debug "记录事件: [$level] $operation - $message"
    
    # 检查缓冲区大小
    if [[ -f "$EVENTS_BUFFER_FILE" ]]; then
        local line_count
        line_count=$(wc -l < "$EVENTS_BUFFER_FILE")
        if [[ $line_count -ge $MAX_BUFFER_SIZE ]]; then
            flush_events
        fi
    fi
}

flush_events() {
    if [[ "$MONITORING_ENABLED" != "true" || ! -f "$EVENTS_BUFFER_FILE" ]]; then
        return 0
    fi
    
    print_debug "刷新事件缓冲区到CloudWatch Logs"
    
    local log_group_name="${LOGS_GROUP_PREFIX}/${SERVICE_NAME}"
    local log_stream_name="events-$(date +%Y-%m-%d-%H)"
    
    # 确保日志组存在
    aws logs create-log-group --log-group-name "$log_group_name" 2>/dev/null || true
    
    # 确保日志流存在
    aws logs create-log-stream \
        --log-group-name "$log_group_name" \
        --log-stream-name "$log_stream_name" 2>/dev/null || true
    
    # 准备日志事件
    local log_events="["
    local first=true
    
    while IFS= read -r line; do
        if [[ -n "$line" ]]; then
            # 获取时间戳
            local timestamp
            timestamp=$(echo "$line" | jq -r '.timestamp' 2>/dev/null)
            local timestamp_ms
            timestamp_ms=$(date -d "$timestamp" +%s%3N 2>/dev/null || echo "$(date +%s)000")
            
            if [[ "$first" == true ]]; then
                first=false
            else
                log_events+=","
            fi
            
            log_events+="{\"timestamp\":$timestamp_ms,\"message\":$(echo "$line" | jq -c .)}"
        fi
    done < "$EVENTS_BUFFER_FILE"
    
    log_events+="]"
    
    # 发送到CloudWatch Logs
    if aws logs put-log-events \
        --log-group-name "$log_group_name" \
        --log-stream-name "$log_stream_name" \
        --log-events "$log_events" >/dev/null 2>&1; then
        
        print_debug "成功发送 $(wc -l < "$EVENTS_BUFFER_FILE") 个事件"
        > "$EVENTS_BUFFER_FILE"  # 清空文件
    else
        print_warning "发送事件到CloudWatch Logs失败"
    fi
}

# =============================================================================
# 性能监控
# =============================================================================

monitor_resource_usage() {
    local operation="$1"
    local interval="${2:-10}"
    local duration="${3:-60}"
    
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        return 0
    fi
    
    print_info "开始资源监控: $operation (间隔: ${interval}s, 持续: ${duration}s)"
    
    local start_time
    start_time=$(date +%s)
    local end_time=$((start_time + duration))
    
    while [[ $(date +%s) -lt $end_time ]]; do
        # CPU使用率
        local cpu_usage
        if command -v top &>/dev/null; then
            cpu_usage=$(top -l 1 -n 0 | grep "CPU usage" | awk '{print $3}' | sed 's/%//' 2>/dev/null || echo "0")
        else
            cpu_usage="0"
        fi
        
        # 内存使用率
        local memory_usage
        if [[ -f /proc/meminfo ]]; then
            memory_usage=$(free | awk 'FNR==2{printf "%.1f", $3/($2)*100}' 2>/dev/null || echo "0")
        elif command -v vm_stat &>/dev/null; then
            # macOS
            memory_usage=$(vm_stat | awk '/Pages free:/{free=$3} /Pages active:/{active=$3} /Pages inactive:/{inactive=$3} /Pages speculative:/{spec=$3} /Pages wired down:/{wired=$4} END{total=(free+active+inactive+spec+wired); if(total>0) printf "%.1f", (active+wired)/total*100; else print "0"}' 2>/dev/null || echo "0")
        else
            memory_usage="0"
        fi
        
        # 磁盘使用率
        local disk_usage
        disk_usage=$(df / | awk 'NR==2{print $5}' | sed 's/%//' 2>/dev/null || echo "0")
        
        # 记录资源指标
        record_metric "CPUUsage" "$cpu_usage" "Percent" "Operation=$operation"
        record_metric "MemoryUsage" "$memory_usage" "Percent" "Operation=$operation"
        record_metric "DiskUsage" "$disk_usage" "Percent" "Operation=$operation"
        
        sleep "$interval"
    done
    
    print_debug "资源监控完成: $operation"
}

# =============================================================================
# AWS资源监控
# =============================================================================

monitor_cloudformation_stack() {
    local stack_name="$1"
    
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        return 0
    fi
    
    local status
    status=$(aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "DOES_NOT_EXIST")
    
    # 记录堆栈状态指标
    local status_value
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE) status_value=1 ;;
        *IN_PROGRESS*) status_value=0.5 ;;
        *FAILED*|*ROLLBACK*) status_value=0 ;;
        DOES_NOT_EXIST) status_value=-1 ;;
        *) status_value=0 ;;
    esac
    
    record_metric "StackHealth" "$status_value" "None" \
        "StackName=$stack_name" \
        "Status=$status"
    
    record_event "info" "stack_monitor" "堆栈状态检查" \
        "stack_name=$stack_name" \
        "status=$status"
}

monitor_s3_bucket() {
    local bucket_name="$1"
    
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        return 0
    fi
    
    # 检查桶是否存在和可访问
    local bucket_exists=0
    if aws s3api head-bucket --bucket "$bucket_name" >/dev/null 2>&1; then
        bucket_exists=1
        
        # 获取对象数量（近似值）
        local object_count
        object_count=$(aws s3api list-objects-v2 \
            --bucket "$bucket_name" \
            --query 'length(Contents)' \
            --output text 2>/dev/null || echo "0")
        
        record_metric "ObjectCount" "$object_count" "Count" \
            "BucketName=$bucket_name"
    fi
    
    record_metric "BucketHealth" "$bucket_exists" "None" \
        "BucketName=$bucket_name"
    
    record_event "info" "bucket_monitor" "S3桶状态检查" \
        "bucket_name=$bucket_name" \
        "exists=$bucket_exists"
}

# =============================================================================
# 清理和刷新
# =============================================================================

flush_all_buffers() {
    if [[ "$MONITORING_ENABLED" != "true" ]]; then
        return 0
    fi
    
    print_debug "刷新所有监控缓冲区"
    flush_metrics
    flush_events
}

# 设置退出时自动刷新
trap 'flush_all_buffers' EXIT

# =============================================================================
# CLI接口
# =============================================================================

show_monitoring_help() {
    cat << EOF
监控和追踪工具 v$MONITOR_VERSION

用法: $0 <command> [options]

命令:
    init <operation>             初始化追踪
    start-span <operation>       开始span
    finish-span <span_id> [status] [error]  结束span
    metric <name> <value> [unit] [dimensions...]  记录指标
    event <level> <operation> <message> [metadata...]  记录事件
    flush                        立即刷新所有缓冲区
    monitor-stack <stack_name>   监控CloudFormation堆栈
    monitor-bucket <bucket_name> 监控S3桶
    
环境变量:
    MONITORING_ENABLED          启用/禁用监控 (true/false)
    SERVICE_NAME               服务名称
    TRACE_ID                   追踪ID
    METRICS_NAMESPACE          CloudWatch指标命名空间
    
示例:
    $0 init "deploy_infrastructure"
    $0 start-span "create_s3_bucket"
    $0 metric "DeploymentCount" 1 "Count" "Environment=dev"
    $0 event "info" "deployment" "部署开始" "version=1.0"
    $0 monitor-stack "my-stack"
    
EOF
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # 直接执行时的CLI处理
    case "${1:-}" in
        init)
            init_trace "$2"
            ;;
        start-span)
            start_span "$2" "$3"
            ;;
        finish-span)
            finish_span "$2" "$3" "$4"
            ;;
        metric)
            shift
            record_metric "$@"
            ;;
        event)
            shift
            record_event "$@"
            ;;
        flush)
            flush_all_buffers
            ;;
        monitor-stack)
            monitor_cloudformation_stack "$2"
            ;;
        monitor-bucket)
            monitor_s3_bucket "$2"
            ;;
        -h|--help|"")
            show_monitoring_help
            ;;
        *)
            print_error "未知命令: $1"
            show_monitoring_help
            exit 1
            ;;
    esac
fi