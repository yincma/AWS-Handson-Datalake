#!/bin/bash

# =============================================================================
# 成本监控模块
# 版本: 1.0.0
# 描述: 管理数据湖的成本监控和预算告警
# =============================================================================

# 加载通用工具库
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly COST_MONITORING_MODULE_VERSION="1.0.0"

# 加载配置（如果尚未加载）
if [[ -z "${PROJECT_PREFIX:-}" ]]; then
    load_config "$PROJECT_ROOT/configs/config.env"
fi

# =============================================================================
# 模块配置
# =============================================================================

COST_STACK_NAME="${PROJECT_PREFIX}-stack-cost-monitoring-${ENVIRONMENT}"
COST_TEMPLATE_FILE="${PROJECT_ROOT}/templates/cost-monitoring.yaml"
BUDGET_LIMIT="${BUDGET_LIMIT:-100}"  # 默认预算限制100美元

# =============================================================================
# 必需函数实现
# =============================================================================

cost_monitoring_validate() {
    print_info "验证成本监控模块配置"
    
    local validation_errors=0
    
    # 检查必需的环境变量
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION" "AWS_ACCOUNT_ID")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "缺少必需的环境变量: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # 检查CloudFormation模板文件
    if [[ ! -f "$COST_TEMPLATE_FILE" ]]; then
        print_error "成本监控模板文件不存在: $COST_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 验证Budgets权限
    if ! aws budgets describe-budgets --account-id "$AWS_ACCOUNT_ID" &>/dev/null; then
        print_error "AWS Budgets权限验证失败"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 验证Cost Explorer权限（如果可用）
    if ! aws ce get-dimension-values --dimension SERVICE --time-period Start=2023-01-01,End=2023-01-02 &>/dev/null; then
        print_warning "Cost Explorer权限不可用，某些功能将受限"
    fi
    
    # 检查依赖模块（S3存储）
    local s3_stack_name="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
    if ! check_stack_exists "$s3_stack_name"; then
        print_error "依赖的S3存储模块未部署"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "成本监控模块验证通过"
        return 0
    else
        print_error "成本监控模块验证失败: $validation_errors 个错误"
        return 1
    fi
}

cost_monitoring_deploy() {
    print_info "部署成本监控模块"
    
    # 生成预算通知邮箱（使用默认格式）
    local notification_email="admin@${PROJECT_PREFIX}.example.com"
    print_info "使用默认邮箱地址: $notification_email"
    print_warning "请在SNS控制台中确认邮箱订阅以接收成本告警"
    
    # 获取AWS账户ID
    local aws_account_id
    aws_account_id=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
    if [[ -z "$aws_account_id" ]]; then
        print_error "无法获取AWS账户ID"
        return 1
    fi
    print_debug "AWS账户ID: $aws_account_id"
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=MonthlyBudgetLimit,ParameterValue="$BUDGET_LIMIT"
        ParameterKey=DailySpendThreshold,ParameterValue="10"
        ParameterKey=AlertEmail,ParameterValue="$notification_email"
    )
    
    if check_stack_exists "$COST_STACK_NAME"; then
        print_info "更新现有成本监控堆栈: $COST_STACK_NAME"
        
        if aws cloudformation update-stack \
            --stack-name "$COST_STACK_NAME" \
            --template-body "file://$COST_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            if wait_for_stack_completion "$COST_STACK_NAME"; then
                configure_cost_alerts
                print_success "成本监控模块更新成功"
                return 0
            fi
        fi
    else
        print_info "创建新的成本监控堆栈: $COST_STACK_NAME"
        
        if aws cloudformation create-stack \
            --stack-name "$COST_STACK_NAME" \
            --template-body "file://$COST_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            if wait_for_stack_completion "$COST_STACK_NAME"; then
                configure_cost_alerts
                print_success "成本监控模块部署成功"
                return 0
            fi
        fi
    fi
    
    print_error "成本监控模块部署失败"
    return 1
}

configure_cost_alerts() {
    print_info "配置成本告警"
    
    # 创建成本异常检测器
    local anomaly_detector_arn
    anomaly_detector_arn=$(aws ce create-anomaly-detector \
        --anomaly-detector MonitorArn="arn:aws:ce::${AWS_ACCOUNT_ID}:monitor/${PROJECT_PREFIX}-${ENVIRONMENT}" \
        --query 'AnomalyDetectorArn' \
        --output text 2>/dev/null)
    
    if [[ -n "$anomaly_detector_arn" ]]; then
        print_debug "✓ 成本异常检测器已创建: $anomaly_detector_arn"
    else
        print_warning "⚠ 成本异常检测器创建失败或已存在"
    fi
    
    print_success "成本告警配置完成"
}

cost_monitoring_status() {
    print_info "检查成本监控模块状态"
    
    local status
    status=$(get_stack_status "$COST_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "成本监控模块运行正常: $status"
            
            # 检查预算状态
            local budget_name="${PROJECT_PREFIX}-Budget-${ENVIRONMENT}"
            if aws budgets describe-budget --account-id "$AWS_ACCOUNT_ID" --budget-name "$budget_name" &>/dev/null; then
                print_debug "✓ 预算存在: $budget_name"
                
                # 获取预算使用情况
                get_budget_usage "$budget_name"
            else
                print_warning "⚠ 预算不存在: $budget_name"
            fi
            
            # 显示当前成本
            show_current_costs
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "成本监控模块未部署"
            return 1
            ;;
        *)
            print_error "成本监控模块状态异常: $status"
            return 1
            ;;
    esac
}

get_budget_usage() {
    local budget_name="$1"
    
    print_info "获取预算使用情况..."
    
    local budget_info
    budget_info=$(aws budgets describe-budget \
        --account-id "$AWS_ACCOUNT_ID" \
        --budget-name "$budget_name" \
        --query 'Budget.{Limit:BudgetLimit.Amount,Unit:BudgetLimit.Unit}' \
        --output text 2>/dev/null)
    
    if [[ -n "$budget_info" ]]; then
        print_debug "预算限制: $budget_info"
    fi
}

show_current_costs() {
    print_info "显示当前成本信息..."
    
    # 获取本月成本
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
        print_debug "本月累计成本: \$${monthly_cost}"
    else
        print_debug "本月成本数据暂不可用"
    fi
    
    # 获取项目相关的成本（如果可以过滤）
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
        print_debug "项目相关成本: \$${project_cost}"
    fi
}

cost_monitoring_cleanup() {
    print_info "清理成本监控模块资源"
    
    # 清理成本异常检测器
    print_info "清理成本异常检测器..."
    local detectors
    detectors=$(aws ce get-anomaly-detectors \
        --query 'AnomalyDetectors[].AnomalyDetectorArn' \
        --output text 2>/dev/null)
    
    for detector in $detectors; do
        if [[ -n "$detector" && "$detector" == *"${PROJECT_PREFIX}-${ENVIRONMENT}"* ]]; then
            aws ce delete-anomaly-detector --anomaly-detector-arn "$detector" &>/dev/null
            print_debug "删除异常检测器: $detector"
        fi
    done
    
    if check_stack_exists "$COST_STACK_NAME"; then
        print_info "删除成本监控堆栈: $COST_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$COST_STACK_NAME"; then
            if wait_for_stack_deletion "$COST_STACK_NAME"; then
                print_success "成本监控模块清理成功"
                return 0
            fi
        fi
        
        print_error "成本监控模块清理失败"
        return 1
    else
        print_info "成本监控模块未部署，无需清理"
        return 0
    fi
}

cost_monitoring_rollback() {
    print_info "回滚成本监控模块更改"
    
    if check_stack_exists "$COST_STACK_NAME"; then
        local status
        status=$(get_stack_status "$COST_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "检测到失败状态，执行堆栈删除"
            cost_monitoring_cleanup
        else
            print_info "成本监控模块状态正常，无需回滚"
            return 0
        fi
    else
        print_info "成本监控模块不存在，无需回滚"
        return 0
    fi
}

# =============================================================================
# 实用函数
# =============================================================================

show_cost_report() {
    print_step "数据湖成本报告"
    
    local start_date end_date
    start_date=$(date -d '30 days ago' +%Y-%m-%d)
    end_date=$(date +%Y-%m-%d)
    
    print_info "时间范围: $start_date 到 $end_date"
    
    # 按服务分组的成本
    echo
    print_info "按AWS服务分组的成本:"
    aws ce get-cost-and-usage \
        --time-period Start="$start_date",End="$end_date" \
        --granularity MONTHLY \
        --metrics BlendedCost \
        --group-by Type=DIMENSION,Key=SERVICE \
        --query 'ResultsByTime[0].Groups[?Metrics.BlendedCost.Amount>`0`].[Keys[0],Metrics.BlendedCost.Amount]' \
        --output table 2>/dev/null || print_warning "无法获取服务成本数据"
    
    # 每日成本趋势
    echo
    print_info "过去7天成本趋势:"
    aws ce get-cost-and-usage \
        --time-period Start="$(date -d '7 days ago' +%Y-%m-%d)",End="$end_date" \
        --granularity DAILY \
        --metrics BlendedCost \
        --query 'ResultsByTime[].[TimePeriod.Start,Total.BlendedCost.Amount]' \
        --output table 2>/dev/null || print_warning "无法获取每日成本数据"
}

# =============================================================================
# 如果直接执行此脚本
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # 加载模块接口
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # 支持额外的成本报告命令
    if [[ "${1:-}" == "report" ]]; then
        load_config
        show_cost_report
        exit 0
    fi
    
    # 执行传入的操作
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "cost_monitoring" "${@:2}"
    else
        echo "用法: $0 <action> [args...]"
        echo "可用操作: validate, deploy, status, cleanup, rollback, report"
    fi
fi