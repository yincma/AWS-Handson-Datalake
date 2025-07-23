#!/bin/bash

# =============================================================================
# S3存储模块
# 版本: 1.0.0
# 描述: 管理数据湖的S3存储基础设施
# =============================================================================

# 加载通用工具库
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly S3_STORAGE_MODULE_VERSION="1.0.0"

# 設定を初期化
load_config || true

# =============================================================================
# 模块配置
# =============================================================================

# 模块依赖 - S3存储是基础模块，没有依赖
S3_STACK_NAME="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
S3_TEMPLATE_FILE="${PROJECT_ROOT}/templates/s3-storage-layer.yaml"

# =============================================================================
# 必需函数实现
# =============================================================================

s3_storage_validate() {
    print_info "验证S3存储模块配置"
    
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
    if [[ ! -f "$S3_TEMPLATE_FILE" ]]; then
        print_error "S3模板文件不存在: $S3_TEMPLATE_FILE"
        validation_errors=$((validation_errors + 1))
    else
        print_debug "S3模板文件存在: $S3_TEMPLATE_FILE"
    fi
    
    # 验证模板语法
    if aws cloudformation validate-template --template-body "file://$S3_TEMPLATE_FILE" &>/dev/null; then
        print_debug "S3模板语法验证通过"
    else
        print_error "S3模板语法验证失败"
        validation_errors=$((validation_errors + 1))
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "S3存储模块验证通过"
        return 0
    else
        print_error "S3存储模块验证失败，发现 $validation_errors 个错误"
        return 1
    fi
}

s3_storage_deploy() {
    print_info "部署S3存储模块"
    
    # 首先验证配置
    if ! s3_storage_validate; then
        print_error "验证失败，无法部署"
        return 1
    fi
    
    # 准备参数
    local stack_params=(
        "ParameterKey=ProjectPrefix,ParameterValue=$PROJECT_PREFIX"
        "ParameterKey=Environment,ParameterValue=$ENVIRONMENT"
    )
    
    # 检查堆栈是否已存在并处理ROLLBACK_COMPLETE状态
    if check_stack_exists "$S3_STACK_NAME"; then
        local stack_status
        stack_status=$(get_stack_status "$S3_STACK_NAME")
        
        # 检查是否为ROLLBACK_COMPLETE状态，如果是则删除栈
        if [[ "$stack_status" == "ROLLBACK_COMPLETE" ]]; then
            print_warning "S3堆栈处于ROLLBACK_COMPLETE状态，需要先删除"
            print_info "删除S3堆栈: $S3_STACK_NAME"
            
            # 首先清空S3桶
            s3_storage_empty_buckets
            
            if aws cloudformation delete-stack --stack-name "$S3_STACK_NAME"; then
                print_info "等待堆栈删除完成..."
                
                # 等待删除完成
                local timeout=900  # 15分钟
                local elapsed=0
                
                while [[ $elapsed -lt $timeout ]]; do
                    local current_status
                    current_status=$(get_stack_status "$S3_STACK_NAME")
                    
                    if [[ "$current_status" == "DOES_NOT_EXIST" ]]; then
                        print_success "S3堆栈删除完成"
                        break
                    elif [[ "$current_status" == "DELETE_FAILED" ]]; then
                        print_error "S3堆栈删除失败"
                        return 1
                    fi
                    
                    sleep 10
                    elapsed=$((elapsed + 10))
                done
                
                if [[ $elapsed -ge $timeout ]]; then
                    print_error "S3堆栈删除超时"
                    return 1
                fi
            else
                print_error "启动S3堆栈删除失败"
                return 1
            fi
            
            # 删除完成后，创建新栈
            print_info "创建新的S3堆栈: $S3_STACK_NAME"
            
            if retry_aws_command aws cloudformation create-stack \
                --stack-name "$S3_STACK_NAME" \
                --template-body "file://$S3_TEMPLATE_FILE" \
                --parameters "${stack_params[@]}" \
                --capabilities CAPABILITY_NAMED_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
                
                print_info "堆栈操作已启动，等待完成..."
                
                # 等待堆栈操作完成
                if wait_for_stack_completion "$S3_STACK_NAME"; then
                    print_success "S3存储模块部署成功"
                    
                    # 获取并显示输出
                    s3_storage_show_outputs
                    
                    return 0
                else
                    print_error "S3存储模块部署失败"
                    return 1
                fi
            else
                print_error "启动S3堆栈创建失败"
                return 1
            fi
        else
            # 正常更新堆栈
            print_info "S3堆栈已存在，将进行更新"
            
            # 尝试更新堆栈
            local update_output
            update_output=$(aws cloudformation update-stack \
                --stack-name "$S3_STACK_NAME" \
                --template-body "file://$S3_TEMPLATE_FILE" \
                --parameters "${stack_params[@]}" \
                --capabilities CAPABILITY_NAMED_IAM \
                --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT" 2>&1)
            
            local update_exit_code=$?
            
            # 检查是否是"无需更新"的情况
            if [[ $update_exit_code -ne 0 ]] && echo "$update_output" | grep -q "No updates are to be performed"; then
                print_success "S3堆栈已是最新状态，无需更新"
                
                # 获取并显示输出
                s3_storage_show_outputs
                
                return 0
            elif [[ $update_exit_code -eq 0 ]]; then
                print_info "堆栈更新已启动，等待完成..."
                
                # 等待堆栈操作完成
                if wait_for_stack_completion "$S3_STACK_NAME"; then
                    print_success "S3存储模块更新成功"
                    
                    # 获取并显示输出
                    s3_storage_show_outputs
                    
                    return 0
                else
                    print_error "S3存储模块更新失败"
                    return 1
                fi
            else
                # 其他更新错误
                print_error "启动S3堆栈更新失败"
                echo "$update_output"
                return 1
            fi
        fi
    else
        # 创建新的S3堆栈
        print_info "创建新的S3堆栈: $S3_STACK_NAME"
        
        if retry_aws_command aws cloudformation create-stack \
            --stack-name "$S3_STACK_NAME" \
            --template-body "file://$S3_TEMPLATE_FILE" \
            --parameters "${stack_params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --tags Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            print_info "堆栈操作已启动，等待完成..."
            
            # 等待堆栈操作完成
            if wait_for_stack_completion "$S3_STACK_NAME"; then
                print_success "S3存储模块部署成功"
                
                # 获取并显示输出
                s3_storage_show_outputs
                
                return 0
            else
                print_error "S3存储模块部署失败"
                return 1
            fi
        else
            print_error "启动S3堆栈创建失败"
            return 1
        fi
    fi
}

s3_storage_status() {
    print_info "检查S3存储模块状态"
    
    # 检查CloudFormation堆栈状态
    local stack_status
    stack_status=$(get_stack_status "$S3_STACK_NAME")
    
    case "$stack_status" in
        CREATE_COMPLETE|UPDATE_COMPLETE)
            print_success "S3堆栈状态正常: $stack_status"
            ;;
        CREATE_IN_PROGRESS|UPDATE_IN_PROGRESS)
            print_info "S3堆栈操作进行中: $stack_status"
            ;;
        *FAILED*|*ROLLBACK*)
            print_error "S3堆栈状态异常: $stack_status"
            return 1
            ;;
        DOES_NOT_EXIST)
            print_warning "S3堆栈不存在"
            return 1
            ;;
        *)
            print_warning "S3堆栈状态未知: $stack_status"
            return 1
            ;;
    esac
    
    # 检查S3桶状态
    s3_storage_check_buckets
    
    return 0
}

s3_storage_cleanup() {
    print_info "清理S3存储模块资源"
    
    # 警告用户
    print_warning "这将删除所有S3桶和数据！"
    if ! confirm_action "确定要清理S3存储资源吗？" "n"; then
        print_info "取消清理操作"
        return 0
    fi
    
    # 首先清空S3桶
    s3_storage_empty_buckets
    
    # 删除CloudFormation堆栈
    if check_stack_exists "$S3_STACK_NAME"; then
        print_info "删除S3堆栈: $S3_STACK_NAME"
        
        if aws cloudformation delete-stack --stack-name "$S3_STACK_NAME"; then
            print_info "等待堆栈删除完成..."
            
            # 等待删除完成
            local timeout=900  # 15分钟
            local elapsed=0
            
            while [[ $elapsed -lt $timeout ]]; do
                local status
                status=$(get_stack_status "$S3_STACK_NAME")
                
                if [[ "$status" == "DOES_NOT_EXIST" ]]; then
                    print_success "S3堆栈删除完成"
                    return 0
                elif [[ "$status" == "DELETE_FAILED" ]]; then
                    print_error "S3堆栈删除失败"
                    return 1
                fi
                
                sleep 10
                elapsed=$((elapsed + 10))
            done
            
            print_error "S3堆栈删除超时"
            return 1
        else
            print_error "启动S3堆栈删除失败"
            return 1
        fi
    else
        print_info "S3堆栈不存在，无需删除"
        return 0
    fi
}

s3_storage_rollback() {
    print_info "回滚S3存储模块更改"
    
    # 检查堆栈状态
    local stack_status
    stack_status=$(get_stack_status "$S3_STACK_NAME")
    
    if [[ "$stack_status" == *"FAILED"* ]] || [[ "$stack_status" == *"ROLLBACK"* ]]; then
        print_info "堆栈已处于失败或回滚状态: $stack_status"
        
        # 尝试取消更新（如果正在进行中）
        if [[ "$stack_status" == "UPDATE_ROLLBACK_FAILED" ]]; then
            print_info "尝试继续回滚..."
            
            if aws cloudformation continue-update-rollback --stack-name "$S3_STACK_NAME"; then
                wait_for_stack_completion "$S3_STACK_NAME"
            fi
        fi
        
        return 0
    fi
    
    # 如果堆栈正常，但需要回滚到之前的状态，这里可以实现更复杂的逻辑
    print_info "堆栈状态正常，无需回滚"
    return 0
}

# =============================================================================
# 可选函数实现
# =============================================================================

s3_storage_test() {
    print_info "测试S3存储模块功能"
    
    # 获取桶名称
    local buckets
    if ! buckets=$(s3_storage_get_bucket_names); then
        print_error "无法获取S3桶名称"
        return 1
    fi
    
    local test_errors=0
    
    # 测试每个桶
    while IFS= read -r bucket; do
        if [[ -n "$bucket" ]]; then
            print_info "测试桶: $bucket"
            
            # 测试桶访问权限
            if aws s3 ls "s3://$bucket" &>/dev/null; then
                print_success "✓ 桶访问正常: $bucket"
            else
                print_error "✗ 桶访问失败: $bucket"
                test_errors=$((test_errors + 1))
            fi
            
            # 测试上传小文件
            local test_file="/tmp/s3_test_${RANDOM}.txt"
            echo "S3测试文件 $(date)" > "$test_file"
            
            if aws s3 cp "$test_file" "s3://$bucket/test/" &>/dev/null; then
                print_success "✓ 桶写入正常: $bucket"
                
                # 清理测试文件
                aws s3 rm "s3://$bucket/test/$(basename "$test_file")" &>/dev/null
            else
                print_error "✗ 桶写入失败: $bucket"
                test_errors=$((test_errors + 1))
            fi
            
            rm -f "$test_file"
        fi
    done <<< "$buckets"
    
    if [[ $test_errors -eq 0 ]]; then
        print_success "S3存储模块功能测试通过"
        return 0
    else
        print_error "S3存储模块功能测试失败，发现 $test_errors 个错误"
        return 1
    fi
}

# =============================================================================
# 模块特定的辅助函数
# =============================================================================

s3_storage_get_bucket_names() {
    if ! check_stack_exists "$S3_STACK_NAME"; then
        print_error "S3堆栈不存在"
        return 1
    fi
    
    # 从堆栈输出获取桶名称
    aws cloudformation describe-stacks \
        --stack-name "$S3_STACK_NAME" \
        --query 'Stacks[0].Outputs[?contains(OutputKey, `Bucket`)].OutputValue' \
        --output text | tr '\t' '\n'
}

s3_storage_check_buckets() {
    print_info "检查S3桶状态"
    
    local buckets
    if buckets=$(s3_storage_get_bucket_names); then
        local healthy_buckets=0
        local total_buckets=0
        
        while IFS= read -r bucket; do
            if [[ -n "$bucket" ]]; then
                total_buckets=$((total_buckets + 1))
                
                if check_s3_bucket_exists "$bucket"; then
                    print_debug "✓ 桶存在: $bucket"
                    healthy_buckets=$((healthy_buckets + 1))
                else
                    print_error "✗ 桶不存在: $bucket"
                fi
            fi
        done <<< "$buckets"
        
        print_info "桶状态: $healthy_buckets/$total_buckets 健康"
        
        if [[ $healthy_buckets -eq $total_buckets ]]; then
            return 0
        else
            return 1
        fi
    else
        print_error "无法获取桶名称"
        return 1
    fi
}

s3_storage_empty_buckets() {
    print_info "清空S3桶内容"
    
    local buckets
    if buckets=$(s3_storage_get_bucket_names); then
        while IFS= read -r bucket; do
            if [[ -n "$bucket" ]] && check_s3_bucket_exists "$bucket"; then
                print_info "清空桶: $bucket"
                
                # 删除所有对象版本和删除标记
                if command -v "$SCRIPT_DIR/../utils/delete-s3-versions.py" &>/dev/null; then
                    python3 "$SCRIPT_DIR/../utils/delete-s3-versions.py" --bucket "$bucket"
                else
                    # 简单清空（不处理版本）
                    aws s3 rm "s3://$bucket" --recursive
                fi
            fi
        done <<< "$buckets"
    fi
}

s3_storage_show_outputs() {
    if ! check_stack_exists "$S3_STACK_NAME"; then
        print_warning "S3堆栈不存在，无法显示输出"
        return 1
    fi
    
    print_info "S3堆栈输出:"
    
    aws cloudformation describe-stacks \
        --stack-name "$S3_STACK_NAME" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue,Description]' \
        --output table
}

# =============================================================================
# 如果直接执行此脚本
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # 加载配置
    load_config
    
    # 加载模块接口
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # 执行传入的操作
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "s3_storage" "${@:2}"
    else
        echo "用法: $0 <action> [args...]"
        echo "可用操作: validate, deploy, status, cleanup, rollback, test"
        echo
        echo "示例:"
        echo "  $0 validate     # 验证配置"
        echo "  $0 deploy       # 部署S3存储"
        echo "  $0 status       # 检查状态"
        echo "  $0 test         # 测试功能"
        echo "  $0 cleanup      # 清理资源"
    fi
fi