#!/bin/bash

# =============================================================================
# 兼容性层 - 支持新旧系统并存
# 版本: 1.0.0
# 描述: 提供命名约定转换和向后兼容性支持
# =============================================================================

# 获取脚本目录
# 如果SCRIPT_DIR未定义，则设置它
if [[ -z "${SCRIPT_DIR:-}" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
# 如果从其他脚本加载，使用相对路径
if [[ -f "$SCRIPT_DIR/common.sh" ]]; then
    source "$SCRIPT_DIR/common.sh"
elif [[ -f "$(dirname "${BASH_SOURCE[0]}")/common.sh" ]]; then
    source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
fi

readonly COMPATIBILITY_VERSION="1.0.0"

# =============================================================================
# 命名约定映射
# =============================================================================

# 获取堆栈名称（支持新旧命名约定）
get_stack_name() {
    local module_name="$1"
    local use_legacy="${2:-false}"
    
    if [[ "$use_legacy" == "true" || "${USE_LEGACY_NAMING:-false}" == "true" ]]; then
        # 旧命名约定: datalake-<module>-<env>
        case "$module_name" in
            s3_storage) echo "datalake-s3-storage-${ENVIRONMENT}" ;;
            iam_roles) echo "datalake-iam-roles-${ENVIRONMENT}" ;;
            glue_catalog) echo "datalake-glue-catalog-${ENVIRONMENT}" ;;
            lake_formation) echo "datalake-lake-formation-${ENVIRONMENT}" ;;
            cost_monitoring) echo "datalake-cost-monitoring-${ENVIRONMENT}" ;;
            cloudtrail_logging) echo "datalake-cloudtrail-${ENVIRONMENT}" ;;
            emr_cluster) echo "datalake-emr-cluster-${ENVIRONMENT}" ;;
            *) echo "datalake-${module_name}-${ENVIRONMENT}" ;;
        esac
    else
        # 新命名约定: ${PROJECT_PREFIX}-stack-<module>-<env>
        local module_slug="${module_name//_/-}"
        echo "${PROJECT_PREFIX}-stack-${module_slug}-${ENVIRONMENT}"
    fi
}

# 检查堆栈是否存在（尝试两种命名约定）
check_stack_exists_any() {
    local module_name="$1"
    
    # 先检查新命名约定
    local new_stack_name=$(get_stack_name "$module_name" false)
    if check_stack_exists "$new_stack_name"; then
        echo "$new_stack_name"
        return 0
    fi
    
    # 特殊处理：检查简短版本的堆栈名称（例如 s3 而不是 s3-storage）
    if [[ "$module_name" == "s3_storage" ]]; then
        local short_stack_name="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
        if check_stack_exists "$short_stack_name"; then
            echo "$short_stack_name"
            return 0
        fi
        # 检查带连字符的版本（s3-storage）
        local hyphen_stack_name="${PROJECT_PREFIX}-stack-s3-storage-${ENVIRONMENT}"
        if check_stack_exists "$hyphen_stack_name"; then
            echo "$hyphen_stack_name"
            return 0
        fi
    fi
    
    # 特殊处理：检查简短版本的IAM堆栈名称（例如 iam 而不是 iam-roles）
    if [[ "$module_name" == "iam_roles" ]]; then
        local short_stack_name="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
        if check_stack_exists "$short_stack_name"; then
            echo "$short_stack_name"
            return 0
        fi
    fi
    
    # 特殊处理：检查简短版本的Glue堆栈名称（例如 glue 而不是 glue-catalog）
    if [[ "$module_name" == "glue_catalog" ]]; then
        local short_stack_name="${PROJECT_PREFIX}-stack-glue-${ENVIRONMENT}"
        if check_stack_exists "$short_stack_name"; then
            echo "$short_stack_name"
            return 0
        fi
    fi
    
    # 再检查旧命名约定
    local legacy_stack_name=$(get_stack_name "$module_name" true)
    if check_stack_exists "$legacy_stack_name"; then
        echo "$legacy_stack_name"
        return 0
    fi
    
    return 1
}

# 获取导出值（支持新旧命名约定）
get_export_value() {
    local export_name="$1"
    local module_name="${2:-}"
    
    # 如果是直接的导出名称，先尝试获取
    if aws cloudformation describe-stacks --query "Stacks[].Outputs[?ExportName=='$export_name'].OutputValue" --output text 2>/dev/null | grep -v "None"; then
        return 0
    fi
    
    # 如果提供了模块名，尝试不同的命名约定
    if [[ -n "$module_name" ]]; then
        # 尝试新命名约定
        local new_stack_name=$(get_stack_name "$module_name" false)
        local new_export="${new_stack_name}-${export_name}"
        if aws cloudformation describe-stacks --query "Stacks[].Outputs[?ExportName=='$new_export'].OutputValue" --output text 2>/dev/null | grep -v "None"; then
            return 0
        fi
        
        # 尝试旧命名约定
        local legacy_stack_name=$(get_stack_name "$module_name" true)
        local legacy_export="${legacy_stack_name}-${export_name}"
        if aws cloudformation describe-stacks --query "Stacks[].Outputs[?ExportName=='$legacy_export'].OutputValue" --output text 2>/dev/null | grep -v "None"; then
            return 0
        fi
    fi
    
    return 1
}

# =============================================================================
# 迁移辅助函数
# =============================================================================

# 检查是否需要迁移
needs_migration() {
    local module_name="$1"
    
    # 检查是否存在旧堆栈
    local legacy_stack_name=$(get_stack_name "$module_name" true)
    if check_stack_exists "$legacy_stack_name"; then
        # 检查是否也存在新堆栈
        local new_stack_name=$(get_stack_name "$module_name" false)
        if ! check_stack_exists "$new_stack_name"; then
            return 0  # 需要迁移
        fi
    fi
    
    return 1  # 不需要迁移
}

# 获取旧堆栈的参数
get_legacy_stack_parameters() {
    local stack_name="$1"
    
    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --query 'Stacks[0].Parameters[].[ParameterKey,ParameterValue]' \
        --output text 2>/dev/null
}

# 迁移堆栈（保留数据）
migrate_stack() {
    local module_name="$1"
    local module_script="$2"
    
    print_step "迁移模块: $module_name"
    
    # 获取旧堆栈名称
    local legacy_stack_name=$(get_stack_name "$module_name" true)
    if ! check_stack_exists "$legacy_stack_name"; then
        print_info "旧堆栈不存在，无需迁移: $legacy_stack_name"
        return 0
    fi
    
    # 获取旧堆栈参数
    print_info "获取旧堆栈参数..."
    local old_params
    old_params=$(get_legacy_stack_parameters "$legacy_stack_name")
    
    # 创建参数映射文件
    local param_file="/tmp/${module_name}_migration_params.txt"
    echo "$old_params" > "$param_file"
    
    # 标记为迁移模式
    export MIGRATION_MODE=true
    export LEGACY_STACK_NAME="$legacy_stack_name"
    
    # 使用新系统部署（会自动处理资源导入）
    if "$module_script" deploy; then
        print_success "模块迁移成功: $module_name"
        
        # 询问是否删除旧堆栈
        if confirm_action "是否删除旧堆栈 $legacy_stack_name？" "n"; then
            print_info "删除旧堆栈..."
            aws cloudformation delete-stack --stack-name "$legacy_stack_name"
            wait_for_stack_delete "$legacy_stack_name"
        fi
    else
        print_error "模块迁移失败: $module_name"
        return 1
    fi
    
    unset MIGRATION_MODE
    unset LEGACY_STACK_NAME
    
    return 0
}

# =============================================================================
# 兼容性检查
# =============================================================================

check_compatibility() {
    print_step "检查系统兼容性"
    
    local issues=0
    
    # 检查是否存在旧堆栈
    local legacy_stacks=(
        "datalake-s3-storage-${ENVIRONMENT}"
        "datalake-iam-roles-${ENVIRONMENT}"
        "datalake-glue-catalog-${ENVIRONMENT}"
        "datalake-lake-formation-${ENVIRONMENT}"
        "datalake-cost-monitoring-${ENVIRONMENT}"
    )
    
    print_info "检查旧堆栈..."
    for stack in "${legacy_stacks[@]}"; do
        if check_stack_exists "$stack"; then
            print_warning "发现旧堆栈: $stack"
            issues=$((issues + 1))
        fi
    done
    
    # 检查命名冲突
    print_info "检查命名冲突..."
    local modules=(s3_storage iam_roles glue_catalog lake_formation cost_monitoring)
    for module in "${modules[@]}"; do
        local new_stack=$(get_stack_name "$module" false)
        local old_stack=$(get_stack_name "$module" true)
        
        if check_stack_exists "$new_stack" && check_stack_exists "$old_stack"; then
            print_error "发现命名冲突: $module (新旧堆栈同时存在)"
            issues=$((issues + 1))
        fi
    done
    
    if [[ $issues -eq 0 ]]; then
        print_success "未发现兼容性问题"
        return 0
    else
        print_warning "发现 $issues 个兼容性问题"
        return 1
    fi
}

# =============================================================================
# 导出兼容性函数
# =============================================================================

export -f get_stack_name check_stack_exists_any get_export_value
export -f needs_migration migrate_stack check_compatibility
export -f wait_for_stack_deletion