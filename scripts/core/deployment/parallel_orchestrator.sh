#!/bin/bash

# =============================================================================
# Parallel Deployment Orchestrator
# Version: 1.0.0
# Description: Intelligently analyzes module dependencies and executes parallel deployment
# =============================================================================

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load common utility library
source "$SCRIPT_DIR/../../lib/common.sh"
source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"

readonly PARALLEL_ORCHESTRATOR_VERSION="1.0.0"

# =============================================================================
# Global State Management
# =============================================================================

# Disable associative arrays for Bash 3.x compatibility
# declare -A TASK_STATUS        # Task status mapping
# declare -A TASK_PID           # Task process ID mapping  
# declare -A TASK_START_TIME    # Task start time
# declare -A TASK_LOG_FILE      # Task log file
# declare -A TASK_RESULT        # Task execution result
# declare -A DEPLOYMENT_GROUPS  # Deployment task groups
PARALLEL_JOBS=3    # Maximum parallel tasks
DEPLOYMENT_TIMEOUT=1800  # 30 minute timeout
ENABLE_ROLLBACK_ON_FAILURE=true

# Task status constants
readonly STATUS_PENDING="pending"
readonly STATUS_RUNNING="running" 
readonly STATUS_COMPLETED="completed"
readonly STATUS_FAILED="failed"
readonly STATUS_SKIPPED="skipped"

# =============================================================================
# Dependency Relationship Definition
# =============================================================================

setup_deployment_dependencies() {
    print_step "Setting up deployment dependencies"
    
    # Bash 3.x compatible deployment group definition - fix dependencies
    INFRASTRUCTURE_GROUP="s3_storage"
    IAM_GROUP="iam_roles"
    CATALOG_GROUP="glue_catalog"  
    LAKE_FORMATION_GROUP="lake_formation"
    COMPUTE_GROUP="emr_cluster"
    MONITORING_GROUP="cost_monitoring cloudtrail_logging"
    
    print_debug "Deployment group configuration:"
    print_debug "  Infrastructure layer: $INFRASTRUCTURE_GROUP"
    print_debug "  IAM layer: $IAM_GROUP"
    print_debug "  Data catalog layer: $CATALOG_GROUP"
    print_debug "  Lake Formation layer: $LAKE_FORMATION_GROUP"
    print_debug "  Compute layer: $COMPUTE_GROUP"
    print_debug "  Monitoring layer: $MONITORING_GROUP"
    
    # Register specific module dependencies (although function is disabled, kept for future use)
    register_module_dependency "glue_catalog" "s3_storage,iam_roles"
    register_module_dependency "lake_formation" "iam_roles,glue_catalog" 
    register_module_dependency "emr_cluster" "s3_storage,iam_roles,glue_catalog"
    register_module_dependency "cost_monitoring" "s3_storage"
    register_module_dependency "cloudtrail_logging" "s3_storage,iam_roles"
    
    print_success "Deployment dependency setup completed"
}

# Bash 3.x compatible group task retrieval function
get_group_tasks() {
    local group="$1"
    case "$group" in
        infrastructure)
            echo "$INFRASTRUCTURE_GROUP"
            ;;
        iam)
            echo "$IAM_GROUP"
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
# Task Management
# =============================================================================

# Bash 3.x compatible associative array simulation function
set_task_property() {
    local task="$1"
    local property="$2"
    local value="$3"
    
    # Encode task name to create valid variable name
    local encoded_task="$(echo "$task" | tr '-' '_' | tr '.' '_')"
    local var_name="TASK_${property}_${encoded_task}"
    
    # Dynamically set variable
    eval "$var_name=\"\$value\""
}

get_task_property() {
    local task="$1"
    local property="$2"
    
    # Encode task name to create valid variable name
    local encoded_task="$(echo "$task" | tr '-' '_' | tr '.' '_')"
    local var_name="TASK_${property}_${encoded_task}"
    
    # Dynamically get variable value
    eval "echo \"\$$var_name\""
}

# Get list of all initialized tasks
get_all_tasks() {
    # Use global variable to track all tasks
    echo "$INITIALIZED_TASKS"
}

# Get task count
get_task_count() {
    if [[ -n "$INITIALIZED_TASKS" ]]; then
        echo "$INITIALIZED_TASKS" | wc -w
    else
        echo "0"
    fi
}

# Add task to list
add_task_to_list() {
    local task="$1"
    if [[ -z "$INITIALIZED_TASKS" ]]; then
        INITIALIZED_TASKS="$task"
    else
        # Check if task is already in list
        if [[ ! " $INITIALIZED_TASKS " =~ " $task " ]]; then
            INITIALIZED_TASKS="$INITIALIZED_TASKS $task"
        fi
    fi
}

# Global variable to store all initialized tasks
INITIALIZED_TASKS=""

initialize_task() {
    local task="$1"
    local log_dir="${PROJECT_ROOT}/logs/deployment"
    
    mkdir -p "$log_dir"
    
    # Use compatibility functions instead of associative arrays
    set_task_property "$task" STATUS "$STATUS_PENDING"
    set_task_property "$task" LOG_FILE "$log_dir/${task}_$(date +%Y%m%d_%H%M%S).log"
    set_task_property "$task" START_TIME ""
    set_task_property "$task" PID ""
    set_task_property "$task" RESULT ""
    
    # Add task to global task list
    add_task_to_list "$task"
    
    print_debug "Task initialized: $task -> $(get_task_property "$task" LOG_FILE)"
}

start_task() {
    local task="$1"
    local operation="${2:-deploy}"
    
    local current_status="$(get_task_property "$task" STATUS)"
    if [[ "$current_status" != "$STATUS_PENDING" ]]; then
        print_warning "Task status is not pending, cannot start: $task ($current_status)"
        return 1
    fi
    
    print_info "Starting task: $task.$operation"
    
    # Update task status
    set_task_property "$task" STATUS "$STATUS_RUNNING"
    set_task_property "$task" START_TIME "$(date +%s)"
    
    local log_file="$(get_task_property "$task" LOG_FILE)"
    
    # Execute task in background, redirect output to log file
    (
        # Set task-specific environment
        export TASK_NAME="$task"
        export OPERATION="$operation"
        export LOG_FILE="$log_file"
        
        # Execute module operation
        if module_interface "$operation" "$task"; then
            echo "TASK_SUCCESS:$task" >> "$log_file"
            exit 0
        else
            echo "TASK_FAILED:$task" >> "$log_file"
            exit 1
        fi
    ) > "$log_file" 2>&1 &
    
    # Record process ID
    set_task_property "$task" PID "$!"
    
    print_debug "Task started: $task (PID: $(get_task_property "$task" PID))"
}

wait_for_task() {
    local task="$1"
    local timeout="${2:-$DEPLOYMENT_TIMEOUT}"
    
    local pid="$(get_task_property "$task" PID)"
    local start_time="$(get_task_property "$task" START_TIME)"
    
    if [[ -z "$pid" || -z "$start_time" ]]; then
        print_error "Task information incomplete: $task"
        return 1
    fi
    
    print_debug "Waiting for task completion: $task (PID: $pid)"
    
    # Wait for process completion or timeout
    local elapsed=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 5
        elapsed=$(($(date +%s) - start_time))
        
        if [[ $elapsed -gt $timeout ]]; then
            print_error "Task timeout: $task (${elapsed}s > ${timeout}s)"
            
            # Kill timed out task
            kill -TERM "$pid" 2>/dev/null || true
            sleep 5
            kill -KILL "$pid" 2>/dev/null || true
            
            set_task_property "$task" STATUS "$STATUS_FAILED"
            set_task_property "$task" RESULT "TIMEOUT"
            return 1
        fi
    done
    
    # Get task exit status
    local exit_code
    wait "$pid"
    exit_code=$?
    
    # Update task status
    local duration=$(($(date +%s) - start_time))
    
    if [[ $exit_code -eq 0 ]]; then
        set_task_property "$task" STATUS "$STATUS_COMPLETED"
        set_task_property "$task" RESULT "SUCCESS:${duration}s"
        print_success "Task completed: $task (duration: ${duration}s)"
        return 0
    else
        set_task_property "$task" STATUS "$STATUS_FAILED"
        set_task_property "$task" RESULT "FAILED:${duration}s:$exit_code"
        print_error "Task failed: $task (duration: ${duration}s, exit code: $exit_code)"
        return 1
    fi
}

get_task_logs() {
    local task="$1"
    local lines="${2:-50}"
    
    local log_file="$(get_task_property "$task" LOG_FILE)"
    
    if [[ -n "$log_file" && -f "$log_file" ]]; then
        echo "=== Task logs: $task ==="
        tail -n "$lines" "$log_file"
        echo "========================"
    else
        print_warning "Task log file does not exist: $task"
    fi
}

# =============================================================================
# Parallel Execution Engine
# =============================================================================

execute_group_parallel() {
    local group_name="$1"
    local tasks_string="$2"
    local operation="${3:-deploy}"
    
    print_step "Executing task group in parallel: $group_name"
    
    # Parse task list
    IFS=' ' read -ra tasks <<< "$tasks_string"
    
    if [[ ${#tasks[@]} -eq 0 ]]; then
        print_warning "Task group is empty: $group_name"
        return 0
    fi
    
    print_info "Task group contains ${#tasks[@]} tasks: ${tasks[*]}"
    
    # Initialize all tasks
    for task in "${tasks[@]}"; do
        initialize_task "$task"
    done
    
    # Start tasks (considering parallelism limits)
    local running_tasks=()
    local pending_tasks=("${tasks[@]}")
    local completed_tasks=()
    local failed_tasks=()
    
    while [[ ${#pending_tasks[@]} -gt 0 || ${#running_tasks[@]} -gt 0 ]]; do
        
        # Start new tasks (within parallelism limits)
        while [[ ${#running_tasks[@]} -lt $PARALLEL_JOBS && ${#pending_tasks[@]} -gt 0 ]]; do
            local task="${pending_tasks[0]}"
            pending_tasks=("${pending_tasks[@]:1}")  # Remove first element
            
            if start_task "$task" "$operation"; then
                running_tasks+=("$task")
                print_info "Task started: $task (currently running: ${#running_tasks[@]}/$PARALLEL_JOBS)"
            else
                failed_tasks+=("$task")
                print_error "Task start failed: $task"
            fi
        done
        
        # Check running tasks
        local still_running=()
        
        for task in "${running_tasks[@]}"; do
            local pid="$(get_task_property "$task" PID)"
            
            if ! kill -0 "$pid" 2>/dev/null; then
                # Task completed, wait to get result
                if wait_for_task "$task" 0; then  # 0 means no wait, check immediately
                    completed_tasks+=("$task")
                    print_success "✓ Task completed: $task"
                else
                    failed_tasks+=("$task")
                    print_error "✗ Task failed: $task"
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
        
        # Show progress
        local total_tasks=${#tasks[@]}
        local finished_tasks=$((${#completed_tasks[@]} + ${#failed_tasks[@]}))
        
        if [[ $finished_tasks -lt $total_tasks ]]; then
            show_progress $finished_tasks $total_tasks "Group: $group_name"
            sleep 2
        fi
    done
    
    finish_progress
    
    # Output task group execution results
    echo
    print_info "Task group '$group_name' execution results:"
    print_success "  Successful: ${#completed_tasks[@]} tasks"
    
    if [[ ${#failed_tasks[@]} -gt 0 ]]; then
        print_error "  Failed: ${#failed_tasks[@]} tasks"
        
        for task in "${failed_tasks[@]}"; do
            print_error "    - $task: $(get_task_property "$task" RESULT)"
            
            # Show summary of failed task logs
            if [[ "$LOG_LEVEL" == "DEBUG" ]]; then
                get_task_logs "$task" 10
            fi
        done
        
        return 1
    else
        print_success "  All tasks executed successfully!"
        return 0
    fi
}

# =============================================================================
# 智能部署编排
# =============================================================================

deploy_all_parallel() {
    local operation="${1:-deploy}"
    local enable_rollback="${2:-$ENABLE_ROLLBACK_ON_FAILURE}"
    
    print_step "Starting parallel deployment orchestration"
    print_info "Operation: $operation"
    print_info "Maximum parallelism: $PARALLEL_JOBS"
    print_info "Task timeout: ${DEPLOYMENT_TIMEOUT}s"
    print_info "Failure rollback: $enable_rollback"
    
    # Set up dependencies
    setup_deployment_dependencies
    
    # Record total start time
    local deployment_start_time
    deployment_start_time=$(date +%s)
    
    # Execute by group order (parallel within groups, serial between groups)
    local successful_groups=""  # Bash 3.x compatible: use strings instead of arrays
    local failed_group=""
    
    for group in infrastructure iam catalog lake_formation compute monitoring; do
        local tasks=$(get_group_tasks "$group")
        
        if [[ -n "$tasks" ]]; then
            print_info "Starting deployment group: $group"
            
            if execute_group_parallel "$group" "$tasks" "$operation"; then
                # Add to successful groups list
                if [[ -z "$successful_groups" ]]; then
                    successful_groups="$group"
                else
                    successful_groups="$successful_groups $group"
                fi
                print_success "Deployment group successful: $group"
            else
                failed_group="$group"
                print_error "Deployment group failed: $group"
                break
            fi
        else
            print_warning "Skipping empty task group: $group"
        fi
        
        echo  # Inter-group separator
    done
    
    # Calculate total duration
    local total_duration=$(($(date +%s) - deployment_start_time))
    
    # Output final results
    echo
    print_step "Parallel deployment results summary"
    
    if [[ -z "$failed_group" ]]; then
        print_success "🎉 All deployment groups executed successfully!"
        print_info "Successful groups: ${successful_groups[*]}"
        print_info "Total duration: ${total_duration}s"
        
        # Generate deployment report
        generate_deployment_report "SUCCESS" "$total_duration"
        
        return 0
    else
        print_error "❌ Deployment failed at group: $failed_group"
        print_info "Successful groups: $successful_groups"
        print_info "Duration before failure: ${total_duration}s"
        
        # Execute rollback (if enabled)
        if [[ "$enable_rollback" == true && -n "$successful_groups" ]]; then
            print_warning "Executing automatic rollback..."
            rollback_successful_groups $successful_groups
        fi
        
        # Generate failure report
        generate_deployment_report "FAILED" "$total_duration" "$failed_group"
        
        return 1
    fi
}

rollback_successful_groups() {
    # Bash 3.x compatible simple rollback logic
    print_step "Rolling back successful deployment groups"
    
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
Parallel Deployment Orchestrator v$PARALLEL_ORCHESTRATOR_VERSION

Usage: $0 <command> [options]

Commands:
    deploy                    Execute parallel deployment
    rollback                  Execute rollback operation
    status                    Display deployment status
    validate                  Validate all modules
    cleanup                   Clean up all modules
    
Options:
    -j, --jobs N              Set maximum parallel tasks (default: 3)
    -t, --timeout N           Set task timeout in seconds (default: 1800)
    --no-rollback            Do not auto-rollback on failure
    -v, --verbose            Verbose output
    -h, --help               Display help

Examples:
    $0 deploy                           # Standard parallel deployment
    $0 deploy -j 5 -t 3600             # 5 parallel tasks, 1 hour timeout
    $0 deploy --no-rollback             # Deploy without auto-rollback
    $0 validate -v                      # Verbose validation of all modules
    $0 status                           # Display current status

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