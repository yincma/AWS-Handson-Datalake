#!/bin/bash

# =============================================================================
# 并行化部署编排器
# 版本: 1.0.0
# 描述: 智能解析模块依赖关系并执行并行部署
# =============================================================================

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# 加载通用工具库
source "$SCRIPT_DIR/../../lib/common.sh"
source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"

readonly PARALLEL_ORCHESTRATOR_VERSION="1.0.0"

# =============================================================================
# 全局状态管理
# =============================================================================

# Bash 3.x互換性のため連想配列を無効化
# declare -A TASK_STATUS        # 任务状态映射
# declare -A TASK_PID           # 任务进程ID映射  
# declare -A TASK_START_TIME    # 任务开始时间
# declare -A TASK_LOG_FILE      # 任务日志文件
# declare -A TASK_RESULT        # 任务执行结果
# declare -A DEPLOYMENT_GROUPS  # 部署任务组
PARALLEL_JOBS=3    # 最大并行任务数
DEPLOYMENT_TIMEOUT=1800  # 30分钟超时
ENABLE_ROLLBACK_ON_FAILURE=true

# 任务状态常量
readonly STATUS_PENDING="pending"
readonly STATUS_RUNNING="running" 
readonly STATUS_COMPLETED="completed"
readonly STATUS_FAILED="failed"
readonly STATUS_SKIPPED="skipped"

# =============================================================================
# 依赖关系定义
# =============================================================================

setup_deployment_dependencies() {
    print_step "设置部署依赖关系"
    
    # Bash 3.x兼容的部署组定义 - 修复依赖关系
    INFRASTRUCTURE_GROUP="s3_storage iam_roles"
    CATALOG_GROUP="glue_catalog"  
    LAKE_FORMATION_GROUP="lake_formation"
    COMPUTE_GROUP="emr_cluster"
    MONITORING_GROUP="cost_monitoring cloudtrail_logging"
    
    print_debug "部署组配置："
    print_debug "  基础设施层: $INFRASTRUCTURE_GROUP"
    print_debug "  数据目录层: $CATALOG_GROUP"
    print_debug "  Lake Formation层: $LAKE_FORMATION_GROUP"
    print_debug "  计算层: $COMPUTE_GROUP"
    print_debug "  监控层: $MONITORING_GROUP"
    
    # 注册具体的模块依赖关系（虽然函数被禁用，但保留以备将来使用）
    register_module_dependency "glue_catalog" "s3_storage,iam_roles"
    register_module_dependency "lake_formation" "iam_roles,glue_catalog" 
    register_module_dependency "emr_cluster" "s3_storage,iam_roles,glue_catalog"
    register_module_dependency "cost_monitoring" "s3_storage"
    register_module_dependency "cloudtrail_logging" "s3_storage,iam_roles"
    
    print_success "部署依赖关系设置完成"
}

# Bash 3.x兼容的组任务获取函数
get_group_tasks() {
    local group="$1"
    case "$group" in
        infrastructure)
            echo "$INFRASTRUCTURE_GROUP"
            ;;
        catalog)
            echo "$CATALOG_GROUP"
            ;;
        lake_formation)
            echo "$LAKE_FORMATION_GROUP"
            ;;
        compute)
            echo "$COMPUTE_GROUP"
            ;;
        monitoring)
            echo "$MONITORING_GROUP"
            ;;
        *)
            echo ""
            ;;
    esac
}

# =============================================================================
# 任务管理
# =============================================================================

# Bash 3.x兼容的关联数组模拟函数
set_task_property() {
    local task="$1"
    local property="$2"
    local value="$3"
    
    # 对任务名进行编码以创建有效的变量名
    local encoded_task="$(echo "$task" | tr '-' '_' | tr '.' '_')"
    local var_name="TASK_${property}_${encoded_task}"
    
    # 动态设置变量
    eval "$var_name=\"\$value\""
}

get_task_property() {
    local task="$1"
    local property="$2"
    
    # 对任务名进行编码以创建有效的变量名
    local encoded_task="$(echo "$task" | tr '-' '_' | tr '.' '_')"
    local var_name="TASK_${property}_${encoded_task}"
    
    # 动态获取变量值
    eval "echo \"\$$var_name\""
}

# 获取所有已初始化的任务列表
get_all_tasks() {
    # 使用全局变量跟踪所有任务
    echo "$INITIALIZED_TASKS"
}

# 获取任务数量
get_task_count() {
    if [[ -n "$INITIALIZED_TASKS" ]]; then
        echo "$INITIALIZED_TASKS" | wc -w
    else
        echo "0"
    fi
}

# 添加任务到列表
add_task_to_list() {
    local task="$1"
    if [[ -z "$INITIALIZED_TASKS" ]]; then
        INITIALIZED_TASKS="$task"
    else
        # 检查任务是否已经在列表中
        if [[ ! " $INITIALIZED_TASKS " =~ " $task " ]]; then
            INITIALIZED_TASKS="$INITIALIZED_TASKS $task"
        fi
    fi
}

# 全局变量存储所有已初始化的任务
INITIALIZED_TASKS=""

initialize_task() {
    local task="$1"
    local log_dir="${PROJECT_ROOT}/logs/deployment"
    
    mkdir -p "$log_dir"
    
    # 使用兼容函数替代关联数组
    set_task_property "$task" STATUS "$STATUS_PENDING"
    set_task_property "$task" LOG_FILE "$log_dir/${task}_$(date +%Y%m%d_%H%M%S).log"
    set_task_property "$task" START_TIME ""
    set_task_property "$task" PID ""
    set_task_property "$task" RESULT ""
    
    # 将任务添加到全局任务列表
    add_task_to_list "$task"
    
    print_debug "任务初始化: $task -> $(get_task_property "$task" LOG_FILE)"
}

start_task() {
    local task="$1"
    local operation="${2:-deploy}"
    
    local current_status="$(get_task_property "$task" STATUS)"
    if [[ "$current_status" != "$STATUS_PENDING" ]]; then
        print_warning "任务状态不是pending，无法启动: $task ($current_status)"
        return 1
    fi
    
    print_info "启动任务: $task.$operation"
    
    # 更新任务状态
    set_task_property "$task" STATUS "$STATUS_RUNNING"
    set_task_property "$task" START_TIME "$(date +%s)"
    
    local log_file="$(get_task_property "$task" LOG_FILE)"
    
    # 在后台执行任务，重定向输出到日志文件
    (
        # 设置任务特定的环境
        export TASK_NAME="$task"
        export OPERATION="$operation"
        export LOG_FILE="$log_file"
        
        # 执行模块操作
        if module_interface "$operation" "$task"; then
            echo "TASK_SUCCESS:$task" >> "$log_file"
            exit 0
        else
            echo "TASK_FAILED:$task" >> "$log_file"
            exit 1
        fi
    ) > "$log_file" 2>&1 &
    
    # 记录进程ID
    set_task_property "$task" PID "$!"
    
    print_debug "任务已启动: $task (PID: $(get_task_property "$task" PID))"
}

wait_for_task() {
    local task="$1"
    local timeout="${2:-$DEPLOYMENT_TIMEOUT}"
    
    local pid="$(get_task_property "$task" PID)"
    local start_time="$(get_task_property "$task" START_TIME)"
    
    if [[ -z "$pid" || -z "$start_time" ]]; then
        print_error "任务信息不完整: $task"
        return 1
    fi
    
    print_debug "等待任务完成: $task (PID: $pid)"
    
    # 等待进程完成或超时
    local elapsed=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 5
        elapsed=$(($(date +%s) - start_time))
        
        if [[ $elapsed -gt $timeout ]]; then
            print_error "任务超时: $task (${elapsed}s > ${timeout}s)"
            
            # 杀死超时的任务
            kill -TERM "$pid" 2>/dev/null || true
            sleep 5
            kill -KILL "$pid" 2>/dev/null || true
            
            set_task_property "$task" STATUS "$STATUS_FAILED"
            set_task_property "$task" RESULT "TIMEOUT"
            return 1
        fi
    done
    
    # 获取任务退出状态
    local exit_code
    wait "$pid"
    exit_code=$?
    
    # 更新任务状态
    local duration=$(($(date +%s) - start_time))
    
    if [[ $exit_code -eq 0 ]]; then
        set_task_property "$task" STATUS "$STATUS_COMPLETED"
        set_task_property "$task" RESULT "SUCCESS:${duration}s"
        print_success "任务完成: $task (耗时: ${duration}s)"
        return 0
    else
        set_task_property "$task" STATUS "$STATUS_FAILED"
        set_task_property "$task" RESULT "FAILED:${duration}s:$exit_code"
        print_error "任务失败: $task (耗时: ${duration}s, 退出码: $exit_code)"
        return 1
    fi
}

get_task_logs() {
    local task="$1"
    local lines="${2:-50}"
    
    local log_file="$(get_task_property "$task" LOG_FILE)"
    
    if [[ -n "$log_file" && -f "$log_file" ]]; then
        echo "=== 任务日志: $task ==="
        tail -n "$lines" "$log_file"
        echo "========================"
    else
        print_warning "任务日志文件不存在: $task"
    fi
}

# =============================================================================
# 并行执行引擎
# =============================================================================

execute_group_parallel() {
    local group_name="$1"
    local tasks_string="$2"
    local operation="${3:-deploy}"
    
    print_step "并行执行任务组: $group_name"
    
    # 解析任务列表
    IFS=' ' read -ra tasks <<< "$tasks_string"
    
    if [[ ${#tasks[@]} -eq 0 ]]; then
        print_warning "任务组为空: $group_name"
        return 0
    fi
    
    print_info "任务组包含 ${#tasks[@]} 个任务: ${tasks[*]}"
    
    # 初始化所有任务
    for task in "${tasks[@]}"; do
        initialize_task "$task"
    done
    
    # 启动任务（考虑并行度限制）
    local running_tasks=()
    local pending_tasks=("${tasks[@]}")
    local completed_tasks=()
    local failed_tasks=()
    
    while [[ ${#pending_tasks[@]} -gt 0 || ${#running_tasks[@]} -gt 0 ]]; do
        
        # 启动新任务（在并行度限制内）
        while [[ ${#running_tasks[@]} -lt $PARALLEL_JOBS && ${#pending_tasks[@]} -gt 0 ]]; do
            local task="${pending_tasks[0]}"
            pending_tasks=("${pending_tasks[@]:1}")  # 移除第一个元素
            
            if start_task "$task" "$operation"; then
                running_tasks+=("$task")
                print_info "任务启动: $task (当前运行: ${#running_tasks[@]}/$PARALLEL_JOBS)"
            else
                failed_tasks+=("$task")
                print_error "任务启动失败: $task"
            fi
        done
        
        # 检查运行中的任务
        local still_running=()
        
        for task in "${running_tasks[@]}"; do
            local pid="$(get_task_property "$task" PID)"
            
            if ! kill -0 "$pid" 2>/dev/null; then
                # 任务已完成，等待获取结果
                if wait_for_task "$task" 0; then  # 0表示不等待，立即检查
                    completed_tasks+=("$task")
                    print_success "✓ 任务完成: $task"
                else
                    failed_tasks+=("$task")
                    print_error "✗ 任务失败: $task"
                fi
            else
                still_running+=("$task")
            fi
        done
        
        if [[ ${#still_running[@]} -gt 0 ]]; then
            running_tasks=("${still_running[@]}")
        else
            running_tasks=()
        fi
        
        # 显示进度
        local total_tasks=${#tasks[@]}
        local finished_tasks=$((${#completed_tasks[@]} + ${#failed_tasks[@]}))
        
        if [[ $finished_tasks -lt $total_tasks ]]; then
            show_progress $finished_tasks $total_tasks "组: $group_name"
            sleep 2
        fi
    done
    
    finish_progress
    
    # 输出任务组执行结果
    echo
    print_info "任务组 '$group_name' 执行结果:"
    print_success "  成功: ${#completed_tasks[@]} 个任务"
    
    if [[ ${#failed_tasks[@]} -gt 0 ]]; then
        print_error "  失败: ${#failed_tasks[@]} 个任务"
        
        for task in "${failed_tasks[@]}"; do
            print_error "    - $task: $(get_task_property "$task" RESULT)"
            
            # 显示失败任务的日志摘要
            if [[ "$LOG_LEVEL" == "DEBUG" ]]; then
                get_task_logs "$task" 10
            fi
        done
        
        return 1
    else
        print_success "  所有任务执行成功！"
        return 0
    fi
}

# =============================================================================
# 智能部署编排
# =============================================================================

deploy_all_parallel() {
    local operation="${1:-deploy}"
    local enable_rollback="${2:-$ENABLE_ROLLBACK_ON_FAILURE}"
    
    print_step "开始并行部署编排"
    print_info "操作: $operation"
    print_info "最大并行度: $PARALLEL_JOBS"
    print_info "任务超时: ${DEPLOYMENT_TIMEOUT}s"
    print_info "失败回滚: $enable_rollback"
    
    # 设置依赖关系
    setup_deployment_dependencies
    
    # 记录总开始时间
    local deployment_start_time
    deployment_start_time=$(date +%s)
    
    # 按组顺序执行（组内并行，组间串行）
    local successful_groups=""  # Bash 3.x兼容：使用字符串而非数组
    local failed_group=""
    
    for group in infrastructure catalog lake_formation compute monitoring; do
        local tasks=$(get_group_tasks "$group")
        
        if [[ -n "$tasks" ]]; then
            print_info "开始部署组: $group"
            
            if execute_group_parallel "$group" "$tasks" "$operation"; then
                # 添加到成功组列表
                if [[ -z "$successful_groups" ]]; then
                    successful_groups="$group"
                else
                    successful_groups="$successful_groups $group"
                fi
                print_success "部署组成功: $group"
            else
                failed_group="$group"
                print_error "部署组失败: $group"
                break
            fi
        else
            print_warning "跳过空任务组: $group"
        fi
        
        echo  # 组间分隔符
    done
    
    # 计算总耗时
    local total_duration=$(($(date +%s) - deployment_start_time))
    
    # 输出最终结果
    echo
    print_step "并行部署结果摘要"
    
    if [[ -z "$failed_group" ]]; then
        print_success "🎉 所有部署组执行成功！"
        print_info "成功的组: ${successful_groups[*]}"
        print_info "总耗时: ${total_duration}s"
        
        # 生成部署报告
        generate_deployment_report "SUCCESS" "$total_duration"
        
        return 0
    else
        print_error "❌ 部署失败在组: $failed_group"
        print_info "成功的组: $successful_groups"
        print_info "失败前耗时: ${total_duration}s"
        
        # 执行回滚（如果启用）
        if [[ "$enable_rollback" == true && -n "$successful_groups" ]]; then
            print_warning "执行自动回滚..."
            rollback_successful_groups $successful_groups
        fi
        
        # 生成故障报告
        generate_deployment_report "FAILED" "$total_duration" "$failed_group"
        
        return 1
    fi
}

rollback_successful_groups() {
    # Bash 3.x兼容的简单回滚逻辑
    print_step "回滚成功的部署组"
    
    # 简单的反向回滚顺序：monitoring -> compute -> lake_formation -> catalog -> infrastructure
    local rollback_order="monitoring compute lake_formation catalog infrastructure"
    
    for group in $rollback_order; do
        # 检查这个组是否需要回滚（通过参数传入的成功组中是否包含）
        local should_rollback=false
        for arg in "$@"; do
            if [[ "$arg" == "$group" ]]; then
                should_rollback=true
                break
            fi
        done
        
        if [[ "$should_rollback" == "true" ]]; then
            local tasks=$(get_group_tasks "$group")
            if [[ -n "$tasks" ]]; then
                print_info "回滚组: $group"
                execute_group_parallel "$group" "$tasks" "rollback"
            fi
        fi
    done
}

# =============================================================================
# 报告和监控
# =============================================================================

generate_deployment_report() {
    local status="$1"
    local duration="$2"
    local failed_group="${3:-}"
    
    local report_file="${PROJECT_ROOT}/logs/deployment_report_$(date +%Y%m%d_%H%M%S).json"
    
    # 收集任务统计信息
    local total_tasks=0
    local successful_tasks=0
    local failed_tasks=0
    local task_details=()
    
    for task in $(get_all_tasks); do
        total_tasks=$((total_tasks + 1))
        
        local task_status="$(get_task_property "$task" STATUS)"
        case "$task_status" in
            $STATUS_COMPLETED)
                successful_tasks=$((successful_tasks + 1))
                ;;
            $STATUS_FAILED)
                failed_tasks=$((failed_tasks + 1))
                ;;
        esac
        
        local task_result="$(get_task_property "$task" RESULT)"
        task_details+=("\"$task\": {\"status\": \"$task_status\", \"result\": \"${task_result:-}\"}")
    done
    
    # 生成JSON报告
    local task_details_json
    IFS=',' task_details_json="${task_details[*]}"
    
    cat > "$report_file" << EOF
{
    "deployment_report": {
        "timestamp": "$(date -Iseconds)",
        "version": "$PARALLEL_ORCHESTRATOR_VERSION",
        "status": "$status",
        "duration_seconds": $duration,
        "failed_group": "${failed_group:-null}",
        "statistics": {
            "total_tasks": $total_tasks,
            "successful_tasks": $successful_tasks,
            "failed_tasks": $failed_tasks,
            "success_rate": $(( total_tasks > 0 ? (successful_tasks * 100 / total_tasks) : 0 ))
        },
        "task_details": {
            $task_details_json
        },
        "configuration": {
            "parallel_jobs": $PARALLEL_JOBS,
            "timeout_seconds": $DEPLOYMENT_TIMEOUT,
            "rollback_enabled": $ENABLE_ROLLBACK_ON_FAILURE
        }
    }
}
EOF
    
    print_success "部署报告已生成: $report_file"
}

show_deployment_status() {
    print_step "实时部署状态"
    
    local task_count=$(get_task_count)
    if [[ $task_count -eq 0 ]]; then
        print_info "无活跃任务"
        return 0
    fi
    
    echo
    printf "%-20s %-12s %-15s %-s\n" "任务" "状态" "结果" "日志文件"
    printf "%-20s %-12s %-15s %-s\n" "----" "----" "----" "--------"
    
    for task in $(get_all_tasks); do
        local status="$(get_task_property "$task" STATUS)"
        local result="$(get_task_property "$task" RESULT)"
        local log_file="$(get_task_property "$task" LOG_FILE)"
        
        # 设置默认值
        status="${status:-N/A}"
        result="${result:-N/A}"
        log_file="${log_file:-N/A}"
        
        # 状态颜色
        case "$status" in
            $STATUS_COMPLETED) printf "\033[32m%-20s\033[0m" "$task" ;;
            $STATUS_FAILED) printf "\033[31m%-20s\033[0m" "$task" ;;
            $STATUS_RUNNING) printf "\033[33m%-20s\033[0m" "$task" ;;
            *) printf "%-20s" "$task" ;;
        esac
        
        printf " %-12s %-15s %-s\n" "$status" "$result" "$(basename "$log_file")"
    done
    
    echo
}

# =============================================================================
# 主函数和CLI
# =============================================================================

show_help() {
    cat << EOF
并行化部署编排器 v$PARALLEL_ORCHESTRATOR_VERSION

用法: $0 <command> [options]

命令:
    deploy                    执行并行部署
    rollback                  执行回滚操作
    status                    显示部署状态
    validate                  验证所有模块
    cleanup                   清理所有模块
    
选项:
    -j, --jobs N              设置最大并行任务数 (默认: 3)
    -t, --timeout N           设置任务超时时间(秒) (默认: 1800)
    --no-rollback            失败时不自动回滚
    -v, --verbose            详细输出
    -h, --help               显示帮助

示例:
    $0 deploy                           # 标准并行部署
    $0 deploy -j 5 -t 3600             # 5个并行任务，1小时超时
    $0 deploy --no-rollback             # 部署但不自动回滚
    $0 validate -v                      # 详细验证所有模块
    $0 status                           # 显示当前状态

EOF
}

main() {
    local command="${1:-}"
    
    # 解析选项
    while [[ $# -gt 0 ]]; do
        case $1 in
            deploy|rollback|status|validate|cleanup)
                if [[ -z "$command" ]]; then
                    command="$1"
                fi
                shift
                ;;
            -j|--jobs)
                PARALLEL_JOBS="$2"
                shift 2
                ;;
            -t|--timeout)
                DEPLOYMENT_TIMEOUT="$2"
                shift 2
                ;;
            --no-rollback)
                ENABLE_ROLLBACK_ON_FAILURE=false
                shift
                ;;
            -v|--verbose)
                LOG_LEVEL="DEBUG"
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                shift
                ;;
        esac
    done
    
    if [[ -z "$command" ]]; then
        show_help
        exit 1
    fi
    
    # 加载配置
    load_config
    
    # 验证前置条件
    validate_prerequisites
    
    # 执行命令
    case "$command" in
        deploy)
            deploy_all_parallel "deploy"
            ;;
        rollback)
            deploy_all_parallel "rollback" false
            ;;
        validate)
            deploy_all_parallel "validate" false
            ;;
        cleanup)
            print_warning "这将清理所有已部署的资源！"
            if confirm_action "确定要继续吗？" "n"; then
                deploy_all_parallel "cleanup" false
            fi
            ;;
        status)
            show_deployment_status
            ;;
        *)
            print_error "未知命令: $command"
            show_help
            exit 1
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi