#!/bin/bash

# =============================================================================
# Glue数据目录模块
# 版本: 1.0.0
# 描述: 管理数据湖的Glue数据库和表
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

readonly GLUE_CATALOG_MODULE_VERSION="1.0.0"

# =============================================================================
# 模块配置
# =============================================================================

GLUE_STACK_NAME="${PROJECT_PREFIX}-stack-glue-${ENVIRONMENT}"
GLUE_TEMPLATE_FILE="${PROJECT_ROOT}/templates/glue-catalog.yaml"
TABLE_SCHEMAS_FILE="${PROJECT_ROOT}/scripts/utils/table_schemas.json"

# =============================================================================
# 必需函数实现
# =============================================================================

glue_catalog_validate() {
    print_info "验证Glue数据目录模块配置"
    
    local validation_errors=0
    
    # 检查必需的环境变量
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "缺少必需的环境变量: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # 检查CloudFormation模板文件
    if [[ ! -f "$GLUE_TEMPLATE_FILE" ]]; then
        print_error "Glue模板文件不存在: $GLUE_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 检查表结构定义文件
    if [[ ! -f "$TABLE_SCHEMAS_FILE" ]]; then
        print_error "表结构文件不存在: $TABLE_SCHEMAS_FILE"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 验证Glue权限
    if ! aws glue get-databases &>/dev/null; then
        print_error "Glue权限验证失败"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 检查S3存储模块（依赖）
    local s3_stack_name="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
    if ! check_stack_exists "$s3_stack_name"; then
        print_error "依赖的S3存储模块未部署"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "Glue数据目录模块验证通过"
        return 0
    else
        print_error "Glue数据目录模块验证失败: $validation_errors 个错误"
        return 1
    fi
}

glue_catalog_deploy() {
    print_info "部署Glue数据目录模块"
    
    # 设置堆栈引用参数 (使用统一函数获取依赖堆栈名称)
    local s3_stack_name=$(get_dependency_stack_name "s3_storage")
    local iam_stack_name=$(get_dependency_stack_name "iam_roles")
    
    # 验证依赖堆栈存在
    if ! check_stack_exists "$s3_stack_name"; then
        print_error "S3堆栈不存在: $s3_stack_name, 请先部署S3模块"
        return 1
    fi
    
    if ! check_stack_exists "$iam_stack_name"; then
        print_error "IAM堆栈不存在: $iam_stack_name, 请先部署IAM模块"
        return 1
    fi
    
    local template_params=(
        ParameterKey=ProjectPrefix,ParameterValue="$PROJECT_PREFIX"
        ParameterKey=Environment,ParameterValue="$ENVIRONMENT"
        ParameterKey=S3StackName,ParameterValue="$s3_stack_name"
        ParameterKey=IAMStackName,ParameterValue="$iam_stack_name"
    )
    
    # 检查堆栈是否已存在并处理ROLLBACK_COMPLETE状态
    if check_stack_exists "$GLUE_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$GLUE_STACK_NAME")
        
        # 检查是否为ROLLBACK_COMPLETE状态，如果是则删除栈
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" ]]; then
            print_warning "Glue堆栈处于ROLLBACK_COMPLETE状态，需要先删除"
            print_info "删除Glue堆栈: $GLUE_STACK_NAME"
            
            # 首先删除Glue表（避免依赖问题）
            local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
            if aws glue get-database --name "$database_name" &>/dev/null; then
                print_info "删除现有Glue表..."
                local tables
                tables=$(aws glue get-tables --database-name "$database_name" --query 'TableList[].Name' --output text 2>/dev/null || echo "")
                
                for table in $tables; do
                    if [[ -n "$table" ]]; then
                        aws glue delete-table --database-name "$database_name" --name "$table" &>/dev/null || true
                        print_debug "删除表: $table"
                    fi
                done
            fi
            
            if aws cloudformation delete-stack --stack-name "$GLUE_STACK_NAME"; then
                print_info "等待堆栈删除完成..."
                
                # 等待删除完成
                local timeout=900  # 15分钟
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$GLUE_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "Glue堆栈删除完成"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "Glue堆栈删除失败"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "Glue堆栈删除超时"
                    return 1
                fi
            else
                print_error "启动Glue堆栈删除失败"
                return 1
            fi
            
            # 删除完成后，创建新栈
            print_info "创建新的Glue堆栈: $GLUE_STACK_NAME"
            
            if retry_aws_command aws cloudformation create-stack \
                --stack-name "$GLUE_STACK_NAME" \
                --template-body "file://$GLUE_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
                
                print_info "堆栈操作已启动，等待完成..."
                
                # 等待堆栈操作完成
                if wait_for_stack_completion "$GLUE_STACK_NAME"; then
                    # 创建表结构
                    create_glue_tables
                    print_success "Glue数据目录模块部署成功"
                    return 0
                else
                    print_error "Glue数据目录模块部署失败"
                    return 1
                fi
            else
                print_error "启动Glue堆栈创建失败"
                return 1
            fi
        else
            # 正常更新堆栈
            print_info "Glue堆栈已存在，将进行更新"
            
            # 捕获更新命令的输出来处理"No updates are to be performed"情况
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$GLUE_STACK_NAME" \
                --template-body "file://$GLUE_TEMPLATE_FILE" \
                --parameters "${template_params[@]}" \
                --capabilities CAPABILITY_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            local update_exit_code=$?
            
            if [[ $update_exit_code -eq 0 ]]; then
                print_info "堆栈更新已启动，等待完成..."
                
                # 等待堆栈操作完成
                if wait_for_stack_completion "$GLUE_STACK_NAME"; then
                    # 创建表结构
                    create_glue_tables
                    print_success "Glue数据目录模块更新成功"
                    return 0
                else
                    print_error "Glue数据目录模块更新失败"
                    return 1
                fi
            elif [[ "$update_output" == *"No updates are to be performed"* ]]; then
                # 堆栈无变化，这是正常情况
                print_info "堆栈无变化，创建表结构"
                create_glue_tables
                print_success "Glue数据目录模块验证成功（无变化）"
                return 0
            else
                print_error "启动Glue堆栈更新失败: $update_output"
                return 1
            fi
        fi
    else
        # 创建新堆栈
        print_info "创建新的Glue堆栈: $GLUE_STACK_NAME"
        
        if retry_aws_command aws cloudformation create-stack \
            --stack-name "$GLUE_STACK_NAME" \
            --template-body "file://$GLUE_TEMPLATE_FILE" \
            --parameters "${template_params[@]}" \
            --capabilities CAPABILITY_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            print_info "堆栈创建已启动，等待完成..."
            
            # 等待堆栈操作完成
            if wait_for_stack_completion "$GLUE_STACK_NAME"; then
                # 创建表结构
                create_glue_tables
                print_success "Glue数据目录模块部署成功"
                return 0
            else
                print_error "Glue数据目录模块部署失败"
                return 1
            fi
        else
            print_error "启动Glue堆栈创建失败"
            return 1
        fi
    fi
    
    print_error "Glue数据目录模块部署失败"
    return 1
}

create_glue_tables() {
    print_info "创建Glue表结构"
    
    # 使用Python数据处理模块创建表
    if [[ -f "$PROJECT_ROOT/scripts/lib/data_processing.py" ]]; then
        if python3 "$PROJECT_ROOT/scripts/lib/data_processing.py" \
            --processor glue_tables \
            --input "$TABLE_SCHEMAS_FILE"; then
            print_success "Glue表结构创建成功"
        else
            print_warning "Glue表结构创建失败，但继续执行"
        fi
    else
        print_warning "Python数据处理模块不可用，跳过表结构创建"
    fi
}

glue_catalog_status() {
    print_info "检查Glue数据目录模块状态"
    
    local status
    status=$(get_stack_status "$GLUE_STACK_NAME")
    
    case "$status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "Glue数据目录模块运行正常: $status"
            
            # 检查数据库是否存在
            local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
            if aws glue get-database --name "$database_name" &>/dev/null; then
                print_debug "✓ 数据库存在: $database_name"
                
                # 检查表数量
                local table_count
                table_count=$(aws glue get-tables --database-name "$database_name" --query 'length(TableList)' --output text 2>/dev/null || echo "0")
                print_debug "✓ 表数量: $table_count"
            else
                print_warning "⚠ 数据库不存在: $database_name"
            fi
            
            return 0
            ;;
        DOES_NOT_EXIST)
            print_warning "Glue数据目录模块未部署"
            return 1
            ;;
        *)
            print_error "Glue数据目录模块状态异常: $status"
            return 1
            ;;
    esac
}

glue_catalog_cleanup() {
    print_info "清理Glue数据目录模块资源"
    
    # 首先删除表（避免删除堆栈时出现依赖问题）
    local database_name="${PROJECT_PREFIX}-db-${ENVIRONMENT}"
    if aws glue get-database --name "$database_name" &>/dev/null; then
        print_info "删除Glue表..."
        
        local tables
        tables=$(aws glue get-tables --database-name "$database_name" --query 'TableList[].Name' --output text 2>/dev/null)
        
        for table in $tables; do
            if [[ -n "$table" ]]; then
                aws glue delete-table --database-name "$database_name" --name "$table" &>/dev/null
                print_debug "删除表: $table"
            fi
        done
    fi
    
    if check_stack_exists "$GLUE_STACK_NAME"; then
        print_info "删除Glue堆栈: $GLUE_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$GLUE_STACK_NAME"; then
            if wait_for_stack_deletion "$GLUE_STACK_NAME"; then
                print_success "Glue数据目录模块清理成功"
                return 0
            fi
        fi
        
        print_error "Glue数据目录模块清理失败"
        return 1
    else
        print_info "Glue数据目录模块未部署，无需清理"
        return 0
    fi
}

glue_catalog_rollback() {
    print_info "回滚Glue数据目录模块更改"
    
    if check_stack_exists "$GLUE_STACK_NAME"; then
        local status
        status=$(get_stack_status "$GLUE_STACK_NAME")
        
        if [[ "$status" == *"FAILED"* || "$status" == *"ROLLBACK"* ]]; then
            print_info "检测到失败状态，执行堆栈删除"
            glue_catalog_cleanup
        else
            print_info "Glue数据目录模块状态正常，无需回滚"
            return 0
        fi
    else
        print_info "Glue数据目录模块不存在，无需回滚"
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
        module_interface "$1" "glue_catalog" "${@:2}"
    else
        echo "用法: $0 <action> [args...]"
        echo "可用操作: validate, deploy, status, cleanup, rollback"
    fi
fi