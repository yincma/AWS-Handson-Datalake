#!/bin/bash

# =============================================================================
# IAM角色和策略模块
# 版本: 1.0.0
# 描述: 管理数据湖的IAM角色和权限策略
# =============================================================================

# 加载通用工具库
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly IAM_ROLES_MODULE_VERSION="1.0.0"

# 设置を初期化
load_config || true

# =============================================================================
# 模块配置
# =============================================================================

IAM_STACK_NAME="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
IAM_TEMPLATE_FILE="${PROJECT_ROOT}/templates/iam-roles-policies.yaml"

# =============================================================================
# 必需函数实现
# =============================================================================

iam_roles_validate() {
    print_info "验证IAM角色模块配置"
    
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
    if [[ ! -f "$IAM_TEMPLATE_FILE" ]]; then
        print_error "IAM模板文件不存在: $IAM_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 验证模板语法
    if ! aws cloudformation validate-template --template-body "file://$IAM_TEMPLATE_FILE" &>/dev/null; then
        print_error "IAM模板文件语法无效"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 验证IAM权限
    if ! aws iam get-user &>/dev/null && ! aws sts get-caller-identity &>/dev/null; then
        print_error "IAM权限验证失败"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "IAM角色模块验证通过"
        return 0
    else
        print_error "IAM角色模块验证失败: $validation_errors 个错误"
        return 1
    fi
}

iam_roles_deploy() {
    print_info "部署IAM角色模块"
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=S3StackName,ParameterValue="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
    )
    
    if check_stack_exists "$IAM_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$IAM_STACK_NAME")
        
        # 检查是否为ROLLBACK_COMPLETE状态，如果是则删除栈
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" ]]; then
            print_warning "IAM堆栈处于ROLLBACK_COMPLETE状态，需要先删除"
            print_info "删除IAM堆栈: $IAM_STACK_NAME"
            
            if aws cloudformation delete-stack --stack-name "$IAM_STACK_NAME"; then
                print_info "等待堆栈删除完成..."
                
                # 等待删除完成
                local timeout=900  # 15分钟
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$IAM_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "IAM堆栈删除完成"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "IAM堆栈删除失败"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "IAM堆栈删除超时"
                    return 1
                fi
            else
                print_error "启动IAM堆栈删除失败"
                return 1
            fi
            
            # 删除完成后，继续创建新栈
        else
            # 正常更新堆栈
            print_info "更新现有IAM堆栈: $IAM_STACK_NAME"
            
            # 尝试更新堆栈
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$IAM_STACK_NAME" \
                --template-body "file://$IAM_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            
            local update_exit_code=$?
            
            # 检查是否是"无需更新"的情况
            if [[ $update_exit_code -ne 0 ]] && echo "$update_output" | grep -q "No updates are to be performed"; then
                print_success "IAM堆栈已是最新状态，无需更新"
                return 0
            elif [[ $update_exit_code -eq 0 ]]; then
                print_info "堆栈更新已启动，等待完成..."
                
                if wait_for_stack_completion "$IAM_STACK_NAME"; then
                    print_success "IAM角色模块更新成功"
                    return 0
                else
                    print_error "IAM角色模块更新失败"
                    return 1
                fi
            else
                # 其他更新错误，继续创建新栈的流程
                print_warning "IAM堆栈更新失败，尝试创建新栈"
                echo "$update_output"
            fi
        fi
    else
        print_info "创建新的IAM堆栈: $IAM_STACK_NAME"
    fi
    
    # 创建新的IAM堆栈（或删除后重建）
    if retry_aws_command aws cloudformation create-stack \
        --stack-name "$IAM_STACK_NAME" \
        --template-body "file://$IAM_TEMPLATE_FILE" \
        --parameters "${template_params[@]}" \
        --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
        --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
        
        print_info "堆栈创建已启动，等待完成..."
        
        if wait_for_stack_completion "$IAM_STACK_NAME"; then
            print_success "IAM角色模块部署成功"
            return 0
        else
            print_error "IAM角色模块部署失败"
            return 1
        fi
    else
        print_error "启动IAM堆栈创建失败"
        return 1
    fi
    
    print_error "IAM角色模块部署失败"
    return 1
}

iam_roles_status() {
    print_info "检查IAM角色模块状态"
    
    local status
    status=$(get_stack_status "$IAM_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "IAM角色模块运行正常: $status"
            
            # 检查关键角色是否存在
            local admin_role="${PROJECT_PREFIX}-DataLakeAdmin-${ENVIRONMENT}"
            if aws iam get-role --role-name "$admin_role" &>/dev/null; then
                print_debug "✓ 管理员角色存在: $admin_role"
            else
                print_warning "⚠ 管理员角色不存在: $admin_role"
            fi
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "IAM角色模块未部署"
            return 1
            ;;
        *)
            print_error "IAM角色模块状态异常: $status"
            return 1
            ;;
    esac
}

iam_roles_cleanup() {
    print_info "清理IAM角色模块资源"
    
    if check_stack_exists "$IAM_STACK_NAME"; then
        print_info "删除IAM堆栈: $IAM_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$IAM_STACK_NAME"; then
            if wait_for_stack_completion "$IAM_STACK_NAME"; then
                print_success "IAM角色模块清理成功"
                return 0
            fi
        fi
        
        print_error "IAM角色模块清理失败"
        return 1
    else
        print_info "IAM角色模块未部署，无需清理"
        return 0
    fi
}

iam_roles_rollback() {
    print_info "回滚IAM角色模块更改"
    
    if check_stack_exists "$IAM_STACK_NAME"; then
        local status
        status=$(get_stack_status "$IAM_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "检测到失败状态，执行堆栈删除"
            iam_roles_cleanup
        else
            print_info "IAM角色模块状态正常，无需回滚"
            return 0
        fi
    else
        print_info "IAM角色模块不存在，无需回滚"
        return 0
    fi
}

# =============================================================================
# 如果直接执行此脚本
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # 加载模块接口
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # 执行传入的操作
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "iam_roles" "${@:2}"
    else
        echo "用法: $0 <action> [args...]"
        echo "可用操作: validate, deploy, status, cleanup, rollback"
    fi
fi