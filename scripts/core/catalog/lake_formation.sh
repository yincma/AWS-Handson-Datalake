#!/bin/bash

# =============================================================================
# Lake Formation模块
# 版本: 1.0.0
# 描述: 管理数据湖的Lake Formation权限和安全
# =============================================================================

# 初始化项目环境
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# 加载配置
if [[ -f "$PROJECT_ROOT/configs/config.local.env" ]]; then
    set -a
    source "$PROJECT_ROOT/configs/config.local.env"
    set +a
elif [[ -f "$PROJECT_ROOT/configs/config.env" ]]; then
    set -a
    source "$PROJECT_ROOT/configs/config.env"
    set +a
fi

# 设置默认值
export PROJECT_PREFIX="${PROJECT_PREFIX:-dl-handson}"
export ENVIRONMENT="${ENVIRONMENT:-dev}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# 加载通用工具库
source "$SCRIPT_DIR/../../lib/common.sh"

# 加载兼容性层
if [[ -f "$SCRIPT_DIR/../../lib/compatibility.sh" ]]; then
    source "$SCRIPT_DIR/../../lib/compatibility.sh"
fi

readonly LAKE_FORMATION_MODULE_VERSION="1.0.0"

# =============================================================================
# 模块配置
# =============================================================================

LAKE_FORMATION_STACK_NAME="${PROJECT_PREFIX}-stack-lakeformation-${ENVIRONMENT}"
LAKE_FORMATION_TEMPLATE_FILE="${PROJECT_ROOT}/templates/lake-formation-simple.yaml"

# =============================================================================
# 必需函数实现
# =============================================================================

lake_formation_validate() {
    print_info "验证Lake Formation模块配置"
    
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
    if [[ ! -f "$LAKE_FORMATION_TEMPLATE_FILE" ]]; then
        print_error "Lake Formation模板文件不存在: $LAKE_FORMATION_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 验证Lake Formation权限
    if ! aws lakeformation get-data-lake-settings &>/dev/null; then
        print_error "Lake Formation权限验证失败"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 检查依赖模块
    local iam_stack_name="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
    if ! check_stack_exists "$iam_stack_name"; then
        print_error "依赖的IAM模块未部署"
        validation_errors=$((validation_errors + 1))
    fi
    
    local glue_stack_name="${PROJECT_PREFIX}-stack-glue-${ENVIRONMENT}"
    if ! check_stack_exists "$glue_stack_name"; then
        print_error "依赖的Glue数据目录模块未部署"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "Lake Formation模块验证通过"
        return 0
    else
        print_error "Lake Formation模块验证失败: $validation_errors 个错误"
        return 1
    fi
}

lake_formation_deploy() {
    print_info "部署Lake Formation模块"
    
    # 设置堆栈引用参数 (使用统一函数获取依赖堆栈名称)
    local s3_stack_name=$(get_dependency_stack_name "s3_storage")
    local iam_stack_name=$(get_dependency_stack_name "iam_roles")
    local glue_stack_name=$(get_dependency_stack_name "glue_catalog")
    
    # 调试输出变量值
    print_info "堆栈名称: s3=$s3_stack_name, iam=$iam_stack_name, glue=$glue_stack_name"
    
    # 验证依赖堆栈存在
    print_info "验证依赖堆栈存在性..."
    
    local s3_exists iam_exists glue_exists
    s3_exists=$(check_stack_exists "$s3_stack_name" && echo "true" || echo "false")
    iam_exists=$(check_stack_exists "$iam_stack_name" && echo "true" || echo "false")
    glue_exists=$(check_stack_exists "$glue_stack_name" && echo "true" || echo "false")
    
    print_info "堆栈存在性检查: S3=$s3_exists, IAM=$iam_exists, Glue=$glue_exists"
    
    if [[ "$s3_exists" != "true" ]]; then
        print_error "S3 stack does not exist: $s3_stack_name, please deploy S3 module first"
        return 1
    fi
    
    if [[ "$iam_exists" != "true" ]]; then
        print_error "IAM stack does not exist: $iam_stack_name, please deploy IAM module first"
        return 1
    fi
    
    if [[ "$glue_exists" != "true" ]]; then
        print_error "Glue stack does not exist: $glue_stack_name, please deploy Glue module first"
        return 1
    fi
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=S3StackName,ParameterValue="$s3_stack_name"
        ParameterKey=IAMStackName,ParameterValue="$iam_stack_name"
        ParameterKey=GlueStackName,ParameterValue="$glue_stack_name"
    )
    
    # 检查堆栈是否已存在并处理ROLLBACK_COMPLETE状态
    if check_stack_exists "$LAKE_FORMATION_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
        
        # 检查是否为ROLLBACK_COMPLETE状态，如果是则删除栈
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" ]]; then
            print_warning "Lake Formation堆栈处于ROLLBACK_COMPLETE状态，需要先删除"
            print_info "删除Lake Formation堆栈: $LAKE_FORMATION_STACK_NAME"
            
            # 首先清理Lake Formation权限
            print_info "清理Lake Formation权限..."
            local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
            
            # 获取所有权限并清理
            local permissions
            permissions=$(aws lakeformation list-permissions \
                --resource Database="{Name=\"$database_name\"}" \
                --query 'PrincipalResourcePermissions[].Principal.DataLakePrincipalIdentifier' \
                --output text 2>/dev/null || echo "")
            
            for principal in $permissions; do
                if [[ -n "$principal" && "$principal" != "None" ]]; then
                    aws lakeformation revoke-permissions \
                        --principal DataLakePrincipalIdentifier="$principal" \
                        --resource Database="{Name=\"$database_name\"}" \
                        --permissions ALL &>/dev/null || true
                    print_debug "清理权限: $principal"
                fi
            done
            
            if aws cloudformation delete-stack --stack-name "$LAKE_FORMATION_STACK_NAME"; then
                print_info "等待堆栈删除完成..."
                
                # 等待删除完成
                local timeout=900  # 15分钟
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "Lake Formation堆栈删除完成"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "Lake Formation堆栈删除失败"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "Lake Formation堆栈删除超时"
                    return 1
                fi
            else
                print_error "启动Lake Formation堆栈删除失败"
                return 1
            fi
            
            # 删除完成后，创建新栈
            print_info "创建新的Lake Formation堆栈: $LAKE_FORMATION_STACK_NAME"
            
            if retry_aws_command aws cloudformation create-stack \
                --stack-name "$LAKE_FORMATION_STACK_NAME" \
                --template-body "file://$LAKE_FORMATION_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
                
                print_info "堆栈操作已启动，等待完成..."
                
                # 等待堆栈操作完成
                if wait_for_stack_completion "$LAKE_FORMATION_STACK_NAME"; then
                    configure_lake_formation_permissions
                    print_success "Lake Formation模块部署成功"
                    return 0
                else
                    print_error "Lake Formation模块部署失败"
                    return 1
                fi
            else
                print_error "启动Lake Formation堆栈创建失败"
                return 1
            fi
        else
            # 正常更新堆栈
            print_info "Lake Formation堆栈已存在，将进行更新"
            
            # 捕获更新命令的输出来处理"No updates are to be performed"情况
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$LAKE_FORMATION_STACK_NAME" \
                --template-body "file://$LAKE_FORMATION_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            local update_exit_code=$?
            
            if [[ $update_exit_code -eq 0 ]]; then
                print_info "堆栈更新已启动，等待完成..."
                
                # 等待堆栈操作完成
                if wait_for_stack_completion "$LAKE_FORMATION_STACK_NAME"; then
                    configure_lake_formation_permissions
                    print_success "Lake Formation模块更新成功"
                    return 0
                else
                    print_error "Lake Formation模块更新失败"
                    return 1
                fi
            elif [[ "$update_output" == *"No updates are to be performed"* ]]; then
                # 堆栈无变化，这是正常情况
                print_info "堆栈无变化，配置权限"
                configure_lake_formation_permissions
                print_success "Lake Formation模块验证成功（无变化）"
                return 0
            else
                print_error "启动Lake Formation堆栈更新失败: $update_output"
                return 1
            fi
        fi
    else
        # 创建新堆栈
        print_info "创建新的Lake Formation堆栈: $LAKE_FORMATION_STACK_NAME"
        
        if retry_aws_command aws cloudformation create-stack \
            --stack-name "$LAKE_FORMATION_STACK_NAME" \
            --template-body "file://$LAKE_FORMATION_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            print_info "堆栈创建已启动，等待完成..."
            
            # 等待堆栈操作完成
            if wait_for_stack_completion "$LAKE_FORMATION_STACK_NAME"; then
                configure_lake_formation_permissions
                print_success "Lake Formation模块部署成功"
                return 0
            else
                print_error "Lake Formation模块部署失败"
                return 1
            fi
        else
            print_error "启动Lake Formation堆栈创建失败"
            return 1
        fi
    fi
    
    print_error "Lake Formation模块部署失败"
    return 1
}

configure_lake_formation_permissions() {
    print_info "配置Lake Formation权限"
    
    local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
    local admin_role_arn data_engineer_role_arn analyst_role_arn
    
    # 获取角色ARN
    local iam_stack_name=$(get_dependency_stack_name "iam_roles")
    
    admin_role_arn=$(aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataLakeAdminRoleArn`].OutputValue' \
        --output text 2>/dev/null)
    
    data_engineer_role_arn=$(aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataEngineerRoleArn`].OutputValue' \
        --output text 2>/dev/null)
    
    analyst_role_arn=$(aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`DataAnalystRoleArn`].OutputValue' \
        --output text 2>/dev/null)
    
    # 为管理员角色授予数据库的全部权限
    if [[ -n "$admin_role_arn" ]]; then
        aws lakeformation grant-permissions \
            --principal DataLakePrincipalIdentifier="$admin_role_arn" \
            --resource Database="{Name=\"$database_name\"}" \
            --permissions ALL &>/dev/null || true
        print_debug "✓ 管理员权限已配置"
    fi
    
    # 为数据工程师角色授予读写权限
    if [[ -n "$data_engineer_role_arn" ]]; then
        aws lakeformation grant-permissions \
            --principal DataLakePrincipalIdentifier="$data_engineer_role_arn" \
            --resource Database="{Name=\"$database_name\"}" \
            --permissions CREATE_TABLE ALTER DROP &>/dev/null || true
        print_debug "✓ 数据工程师权限已配置"
    fi
    
    # 为分析师角色授予只读权限
    if [[ -n "$analyst_role_arn" ]]; then
        aws lakeformation grant-permissions \
            --principal DataLakePrincipalIdentifier="$analyst_role_arn" \
            --resource Database="{Name=\"$database_name\"}" \
            --permissions DESCRIBE &>/dev/null || true
        print_debug "✓ 分析师权限已配置"
    fi
    
    print_success "Lake Formation权限配置完成"
}

lake_formation_status() {
    print_info "检查Lake Formation模块状态"
    
    local status
    status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "Lake Formation模块运行正常: $status"
            
            # 检查数据湖设置
            if aws lakeformation get-data-lake-settings &>/dev/null; then
                print_debug "✓ Lake Formation服务可用"
                
                # 检查数据库权限
                local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
                local permissions
                permissions=$(aws lakeformation list-permissions \
                    --resource Database="{Name=\"$database_name\"}" \
                    --query 'length(PrincipalResourcePermissions)' \
                    --output text 2>/dev/null || echo "0")
                
                print_debug "✓ 数据库权限数量: $permissions"
            else
                print_warning "⚠ Lake Formation服务不可用"
            fi
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "Lake Formation模块未部署"
            return 1
            ;;
        *)
            print_error "Lake Formation模块状态异常: $status"
            return 1
            ;;
    esac
}

lake_formation_cleanup() {
    print_info "清理Lake Formation模块资源"
    
    # 清理权限（为了避免删除堆栈时的依赖问题）
    print_info "清理Lake Formation权限..."
    local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
    
    # 获取所有权限并清理
    local permissions
    permissions=$(aws lakeformation list-permissions \
        --resource Database="{Name=\"$database_name\"}" \
        --query 'PrincipalResourcePermissions[].Principal.DataLakePrincipalIdentifier' \
        --output text 2>/dev/null || echo "")
    
    for principal in $permissions; do
        if [[ -n "$principal" && "$principal" != "None" ]]; then
            aws lakeformation revoke-permissions \
                --principal DataLakePrincipalIdentifier="$principal" \
                --resource Database="{Name=\"$database_name\"}" \
                --permissions ALL &>/dev/null || true
            print_debug "清理权限: $principal"
        fi
    done
    
    if check_stack_exists "$LAKE_FORMATION_STACK_NAME"; then
        print_info "删除Lake Formation堆栈: $LAKE_FORMATION_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$LAKE_FORMATION_STACK_NAME"; then
            if wait_for_stack_deletion "$LAKE_FORMATION_STACK_NAME"; then
                print_success "Lake Formation模块清理成功"
                return 0
            fi
        fi
        
        print_error "Lake Formation模块清理失败"
        return 1
    else
        print_info "Lake Formation模块未部署，无需清理"
        return 0
    fi
}

lake_formation_rollback() {
    print_info "回滚Lake Formation模块更改"
    
    if check_stack_exists "$LAKE_FORMATION_STACK_NAME"; then
        local status
        status=$(get_stack_status "$LAKE_FORMATION_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "检测到失败状态，执行堆栈删除"
            lake_formation_cleanup
        else
            print_info "Lake Formation模块状态正常，无需回滚"
            return 0
        fi
    else
        print_info "Lake Formation模块不存在，无需回滚"
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
        module_interface "$1" "lake_formation" "${@:2}"
    else
        echo "用法: $0 <action> [args...]"
        echo "可用操作: validate, deploy, status, cleanup, rollback"
    fi
fi