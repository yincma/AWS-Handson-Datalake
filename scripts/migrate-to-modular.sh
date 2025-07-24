#!/bin/bash

# =============================================================================
# 数据湖系统迁移脚本
# 版本: 1.0.0
# 描述: 从旧系统安全迁移到新的模块化系统
# =============================================================================

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/compatibility.sh"

readonly MIGRATION_VERSION="1.0.0"

# =============================================================================
# 迁移配置
# =============================================================================

# 定义迁移顺序（考虑依赖关系）
readonly MIGRATION_ORDER=(
    "s3_storage"
    "iam_roles"
    "glue_catalog"
    "lake_formation"
    "cost_monitoring"
    "cloudtrail_logging"
    "emr_cluster"
)

# 模块脚本映射
declare -A MODULE_SCRIPTS
MODULE_SCRIPTS=(
    ["s3_storage"]="$PROJECT_ROOT/scripts/core/infrastructure/s3_storage.sh"
    ["iam_roles"]="$PROJECT_ROOT/scripts/core/infrastructure/iam_roles.sh"
    ["glue_catalog"]="$PROJECT_ROOT/scripts/core/catalog/glue_catalog.sh"
    ["lake_formation"]="$PROJECT_ROOT/scripts/core/catalog/lake_formation.sh"
    ["cost_monitoring"]="$PROJECT_ROOT/scripts/core/monitoring/cost_monitoring.sh"
    ["cloudtrail_logging"]="$PROJECT_ROOT/scripts/core/monitoring/cloudtrail_logging.sh"
    ["emr_cluster"]="$PROJECT_ROOT/scripts/core/compute/emr_cluster.sh"
)

# =============================================================================
# 迁移前检查
# =============================================================================

pre_migration_check() {
    print_step "执行迁移前检查"
    
    # 加载配置
    load_config
    
    # 检查AWS凭证
    if ! validate_aws_credentials; then
        print_error "AWS凭证无效"
        return 1
    fi
    
    # 检查兼容性
    if ! check_compatibility; then
        print_warning "发现兼容性问题，但可以继续迁移"
    fi
    
    # 创建备份目录
    local backup_dir="$PROJECT_ROOT/backups/migration_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$backup_dir"
    export MIGRATION_BACKUP_DIR="$backup_dir"
    
    # 备份当前状态
    print_info "备份当前系统状态..."
    backup_current_state "$backup_dir"
    
    print_success "迁移前检查完成"
    return 0
}

# =============================================================================
# 备份函数
# =============================================================================

backup_current_state() {
    local backup_dir="$1"
    
    # 备份堆栈信息
    print_info "备份CloudFormation堆栈信息..."
    
    local legacy_stacks=(
        "datalake-s3-storage-${ENVIRONMENT}"
        "datalake-iam-roles-${ENVIRONMENT}"
        "datalake-glue-catalog-${ENVIRONMENT}"
        "datalake-lake-formation-${ENVIRONMENT}"
        "datalake-cost-monitoring-${ENVIRONMENT}"
    )
    
    for stack in "${legacy_stacks[@]}"; do
        if check_stack_exists "$stack"; then
            print_debug "备份堆栈: $stack"
            aws cloudformation describe-stacks --stack-name "$stack" \
                > "$backup_dir/${stack}_backup.json" 2>/dev/null || true
            
            # 备份模板
            aws cloudformation get-template --stack-name "$stack" \
                > "$backup_dir/${stack}_template.json" 2>/dev/null || true
        fi
    done
    
    # 备份S3桶列表
    print_info "备份S3桶信息..."
    aws s3 ls | grep "${PROJECT_PREFIX}" > "$backup_dir/s3_buckets.txt" 2>/dev/null || true
    
    # 备份IAM角色
    print_info "备份IAM角色信息..."
    aws iam list-roles --query "Roles[?contains(RoleName, '${PROJECT_PREFIX}')]" \
        > "$backup_dir/iam_roles.json" 2>/dev/null || true
    
    print_success "备份完成: $backup_dir"
}

# =============================================================================
# 迁移执行
# =============================================================================

migrate_module_safe() {
    local module_name="$1"
    local module_script="${MODULE_SCRIPTS[$module_name]}"
    
    print_step "迁移模块: $module_name"
    
    # 检查模块脚本
    if [[ ! -f "$module_script" ]]; then
        print_error "模块脚本不存在: $module_script"
        return 1
    fi
    
    # 检查是否需要迁移
    if ! needs_migration "$module_name"; then
        print_info "模块不需要迁移: $module_name"
        return 0
    fi
    
    # 获取旧堆栈信息
    local legacy_stack_name=$(get_stack_name "$module_name" true)
    local legacy_status=""
    if check_stack_exists "$legacy_stack_name"; then
        legacy_status=$(get_stack_status "$legacy_stack_name")
        print_info "旧堆栈状态: $legacy_stack_name - $legacy_status"
    fi
    
    # 特殊处理S3存储模块（避免数据丢失）
    if [[ "$module_name" == "s3_storage" ]]; then
        print_warning "S3存储模块需要特殊处理以保留数据"
        if ! migrate_s3_storage_special; then
            print_error "S3存储模块迁移失败"
            return 1
        fi
        return 0
    fi
    
    # 执行标准迁移
    if migrate_stack "$module_name" "$module_script"; then
        print_success "模块迁移成功: $module_name"
        return 0
    else
        print_error "模块迁移失败: $module_name"
        return 1
    fi
}

# S3存储模块特殊迁移（保留数据）
migrate_s3_storage_special() {
    print_info "执行S3存储模块特殊迁移流程"
    
    # 1. 检查现有S3桶
    local buckets=(
        "${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}"
    )
    
    local existing_buckets=()
    for bucket in "${buckets[@]}"; do
        if aws s3 ls "s3://$bucket" &>/dev/null; then
            print_info "发现现有S3桶: $bucket"
            existing_buckets+=("$bucket")
        fi
    done
    
    if [[ ${#existing_buckets[@]} -eq 0 ]]; then
        print_info "没有现有S3桶，执行标准部署"
        "$PROJECT_ROOT/scripts/core/infrastructure/s3_storage.sh" deploy
        return $?
    fi
    
    # 2. 创建资源导入模板
    print_info "准备资源导入..."
    
    # 标记为导入模式
    export S3_IMPORT_MODE=true
    export S3_EXISTING_BUCKETS="${existing_buckets[*]}"
    
    # 使用新系统部署（会保留现有桶）
    if "$PROJECT_ROOT/scripts/core/infrastructure/s3_storage.sh" deploy; then
        print_success "S3存储模块迁移成功，数据已保留"
        
        # 删除旧堆栈（如果存在）
        local legacy_stack="datalake-s3-storage-${ENVIRONMENT}"
        if check_stack_exists "$legacy_stack"; then
            print_info "删除旧S3堆栈引用（桶已保留）..."
            # 使用retain deletion policy
            aws cloudformation delete-stack --stack-name "$legacy_stack" \
                --retain-resources "RawDataBucket" "CleanDataBucket" "AnalyticsDataBucket" "AthenaResultsBucket" \
                2>/dev/null || true
        fi
    else
        print_error "S3存储模块迁移失败"
        return 1
    fi
    
    unset S3_IMPORT_MODE
    unset S3_EXISTING_BUCKETS
    
    return 0
}

# =============================================================================
# 迁移流程
# =============================================================================

execute_migration() {
    print_step "开始执行迁移"
    
    local failed_modules=()
    local migrated_modules=()
    
    # 按顺序迁移模块
    for module in "${MIGRATION_ORDER[@]}"; do
        print_info "处理模块 (${#migrated_modules[@]}/${#MIGRATION_ORDER[@]}): $module"
        
        if migrate_module_safe "$module"; then
            migrated_modules+=("$module")
            print_success "✓ $module"
        else
            failed_modules+=("$module")
            print_error "✗ $module"
            
            # 询问是否继续
            if ! confirm_action "模块 $module 迁移失败，是否继续迁移其他模块？" "y"; then
                print_warning "用户中止迁移"
                break
            fi
        fi
        
        echo  # 空行分隔
    done
    
    # 迁移报告
    print_step "迁移报告"
    print_info "成功迁移: ${#migrated_modules[@]} 个模块"
    if [[ ${#migrated_modules[@]} -gt 0 ]]; then
        printf '%s\n' "${migrated_modules[@]}" | sed 's/^/  ✓ /'
    fi
    
    if [[ ${#failed_modules[@]} -gt 0 ]]; then
        print_error "失败: ${#failed_modules[@]} 个模块"
        printf '%s\n' "${failed_modules[@]}" | sed 's/^/  ✗ /'
        return 1
    fi
    
    return 0
}

# =============================================================================
# 后迁移任务
# =============================================================================

post_migration_tasks() {
    print_step "执行迁移后任务"
    
    # 1. 验证新系统
    print_info "验证新系统状态..."
    if "$PROJECT_ROOT/scripts/cli/datalake" status; then
        print_success "新系统状态正常"
    else
        print_warning "新系统状态检查失败"
    fi
    
    # 2. 更新配置文件
    print_info "更新配置文件..."
    if [[ -f "$PROJECT_ROOT/configs/config.env" ]]; then
        # 添加迁移完成标记
        echo "" >> "$PROJECT_ROOT/configs/config.env"
        echo "# Migration completed on $(date)" >> "$PROJECT_ROOT/configs/config.env"
        echo "MIGRATION_COMPLETED=true" >> "$PROJECT_ROOT/configs/config.env"
        echo "MIGRATION_DATE=$(date -Iseconds)" >> "$PROJECT_ROOT/configs/config.env"
    fi
    
    # 3. 清理临时文件
    print_info "清理临时文件..."
    rm -f /tmp/*_migration_params.txt
    
    # 4. 生成迁移报告
    generate_migration_report
    
    print_success "迁移后任务完成"
}

generate_migration_report() {
    local report_file="$MIGRATION_BACKUP_DIR/migration_report.md"
    
    cat > "$report_file" << EOF
# 数据湖系统迁移报告

**迁移日期**: $(date)
**迁移版本**: $MIGRATION_VERSION
**环境**: $ENVIRONMENT
**项目前缀**: $PROJECT_PREFIX

## 迁移摘要

### 旧系统信息
- 命名约定: datalake-<module>-<env>
- 部署方式: 串行脚本

### 新系统信息
- 命名约定: \${PROJECT_PREFIX}-stack-<module>-<env>
- 部署方式: 模块化并行部署

## 迁移结果

### 成功迁移的模块
EOF
    
    # 添加成功模块列表
    for module in "${migrated_modules[@]}"; do
        echo "- ✓ $module" >> "$report_file"
    done
    
    if [[ ${#failed_modules[@]} -gt 0 ]]; then
        echo "" >> "$report_file"
        echo "### 失败的模块" >> "$report_file"
        for module in "${failed_modules[@]}"; do
            echo "- ✗ $module" >> "$report_file"
        done
    fi
    
    cat >> "$report_file" << EOF

## 备份位置
$MIGRATION_BACKUP_DIR

## 后续步骤

1. 验证所有功能正常工作
2. 监控系统24小时
3. 如果一切正常，可以删除备份
4. 更新文档和运维手册

## 回滚说明

如需回滚，请执行：
\`\`\`bash
./scripts/rollback-migration.sh $MIGRATION_BACKUP_DIR
\`\`\`
EOF
    
    print_success "迁移报告已生成: $report_file"
}

# =============================================================================
# 主函数
# =============================================================================

show_help() {
    cat << EOF
数据湖系统迁移工具 v$MIGRATION_VERSION

用法: $0 [选项]

选项:
  --dry-run         模拟运行，不执行实际迁移
  --skip-backup     跳过备份步骤（不推荐）
  --force           强制迁移，不提示确认
  --module MODULE   只迁移指定模块
  -h, --help        显示帮助信息

示例:
  $0                    # 完整迁移
  $0 --dry-run         # 模拟迁移
  $0 --module s3_storage  # 只迁移S3存储模块

注意:
  - 迁移前会自动备份当前状态
  - 迁移过程中会保留所有数据
  - 建议在非生产环境先测试迁移流程
EOF
}

main() {
    local dry_run=false
    local skip_backup=false
    local force=false
    local specific_module=""
    
    # 解析参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                dry_run=true
                shift
                ;;
            --skip-backup)
                skip_backup=true
                shift
                ;;
            --force)
                force=true
                shift
                ;;
            --module)
                specific_module="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                print_error "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    print_step "AWS数据湖系统迁移工具 v$MIGRATION_VERSION"
    
    # 确认迁移
    if [[ "$force" != "true" ]]; then
        print_warning "此操作将把旧系统迁移到新的模块化系统"
        print_warning "迁移过程会保留所有数据，但建议先在测试环境验证"
        if ! confirm_action "确定要开始迁移吗？" "n"; then
            print_info "迁移已取消"
            exit 0
        fi
    fi
    
    # 迁移前检查
    if ! pre_migration_check; then
        print_error "迁移前检查失败"
        exit 1
    fi
    
    # 执行迁移
    if [[ "$dry_run" == "true" ]]; then
        print_info "模拟模式：不会执行实际迁移"
        # TODO: 实现模拟逻辑
    else
        if [[ -n "$specific_module" ]]; then
            # 迁移特定模块
            MIGRATION_ORDER=("$specific_module")
        fi
        
        if execute_migration; then
            print_success "迁移成功完成！"
            post_migration_tasks
            exit 0
        else
            print_error "迁移过程中出现错误"
            print_info "备份位置: $MIGRATION_BACKUP_DIR"
            exit 1
        fi
    fi
}

# 执行主函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi