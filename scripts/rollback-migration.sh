#!/bin/bash

# =============================================================================
# 数据湖系统迁移回滚脚本
# 版本: 1.0.0
# 描述: 从备份恢复到迁移前状态
# =============================================================================

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"

readonly ROLLBACK_VERSION="1.0.0"

# =============================================================================
# 回滚函数
# =============================================================================

validate_backup_dir() {
    local backup_dir="$1"
    
    if [[ ! -d "$backup_dir" ]]; then
        print_error "备份目录不存在: $backup_dir"
        return 1
    fi
    
    # 检查必需的备份文件
    local required_files=(
        "s3_buckets.txt"
        "iam_roles.json"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$backup_dir/$file" ]]; then
            print_warning "备份文件缺失: $file"
        fi
    done
    
    return 0
}

restore_stack_from_backup() {
    local stack_name="$1"
    local backup_dir="$2"
    
    local backup_file="$backup_dir/${stack_name}_backup.json"
    local template_file="$backup_dir/${stack_name}_template.json"
    
    if [[ ! -f "$backup_file" ]]; then
        print_warning "堆栈备份文件不存在: $backup_file"
        return 1
    fi
    
    print_info "恢复堆栈: $stack_name"
    
    # 提取参数
    local params
    params=$(jq -r '.Stacks[0].Parameters[] | "--parameter-overrides " + .ParameterKey + "=" + .ParameterValue' "$backup_file" | tr '\n' ' ')
    
    # 提取模板
    local template_body
    if [[ -f "$template_file" ]]; then
        template_body=$(jq -r '.TemplateBody' "$template_file")
        
        # 创建临时模板文件
        local temp_template="/tmp/${stack_name}_template.yaml"
        echo "$template_body" > "$temp_template"
        
        # 部署堆栈
        if aws cloudformation deploy \
            --stack-name "$stack_name" \
            --template-file "$temp_template" \
            --capabilities CAPABILITY_NAMED_IAM \
            $params; then
            print_success "堆栈恢复成功: $stack_name"
            rm -f "$temp_template"
            return 0
        else
            print_error "堆栈恢复失败: $stack_name"
            rm -f "$temp_template"
            return 1
        fi
    else
        print_error "模板文件不存在: $template_file"
        return 1
    fi
}

# =============================================================================
# 主回滚流程
# =============================================================================

rollback_migration() {
    local backup_dir="$1"
    
    print_step "开始回滚迁移"
    
    # 删除新堆栈
    print_info "删除新创建的堆栈..."
    local new_stacks=(
        "${PROJECT_PREFIX}-stack-cost-monitoring-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-stack-lakeformation-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-stack-glue-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
    )
    
    for stack in "${new_stacks[@]}"; do
        if check_stack_exists "$stack"; then
            print_info "删除堆栈: $stack"
            aws cloudformation delete-stack --stack-name "$stack" || true
        fi
    done
    
    # 等待删除完成
    print_info "等待堆栈删除完成..."
    for stack in "${new_stacks[@]}"; do
        if aws cloudformation describe-stacks --stack-name "$stack" &>/dev/null; then
            wait_for_stack_delete "$stack" || true
        fi
    done
    
    # 恢复旧堆栈
    print_info "恢复旧堆栈..."
    local legacy_stacks=(
        "datalake-s3-storage-${ENVIRONMENT}"
        "datalake-iam-roles-${ENVIRONMENT}"
        "datalake-glue-catalog-${ENVIRONMENT}"
        "datalake-lake-formation-${ENVIRONMENT}"
        "datalake-cost-monitoring-${ENVIRONMENT}"
    )
    
    for stack in "${legacy_stacks[@]}"; do
        restore_stack_from_backup "$stack" "$backup_dir" || true
    done
    
    print_success "回滚完成"
}

# =============================================================================
# 主函数
# =============================================================================

show_help() {
    cat << EOF
数据湖系统迁移回滚工具 v$ROLLBACK_VERSION

用法: $0 <backup_directory>

参数:
  backup_directory   迁移时创建的备份目录路径

示例:
  $0 /path/to/backups/migration_20240115_120000

注意:
  - 需要提供迁移时创建的完整备份目录
  - 回滚会删除新创建的资源并恢复旧资源
  - S3桶中的数据不会受影响
EOF
}

main() {
    if [[ $# -ne 1 ]]; then
        print_error "请提供备份目录路径"
        show_help
        exit 1
    fi
    
    local backup_dir="$1"
    
    # 验证备份目录
    if ! validate_backup_dir "$backup_dir"; then
        print_error "备份目录验证失败"
        exit 1
    fi
    
    # 加载配置
    load_config
    
    # 确认回滚
    print_warning "此操作将回滚到迁移前的状态"
    print_warning "备份目录: $backup_dir"
    
    if ! confirm_action "确定要执行回滚吗？" "n"; then
        print_info "回滚已取消"
        exit 0
    fi
    
    # 执行回滚
    if rollback_migration "$backup_dir"; then
        print_success "回滚成功完成！"
        
        # 清理迁移标记
        if [[ -f "$PROJECT_ROOT/configs/config.env" ]]; then
            sed -i.bak '/MIGRATION_COMPLETED/d' "$PROJECT_ROOT/configs/config.env" || true
            sed -i.bak '/MIGRATION_DATE/d' "$PROJECT_ROOT/configs/config.env" || true
        fi
        
        exit 0
    else
        print_error "回滚过程中出现错误"
        exit 1
    fi
}

# 执行主函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi