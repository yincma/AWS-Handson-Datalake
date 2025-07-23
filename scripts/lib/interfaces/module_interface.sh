#!/bin/bash

# =============================================================================
# 模块化接口标准
# 版本: 1.0.0
# 描述: 定义统一的模块接口规范，确保所有模块遵循相同的API
# =============================================================================

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# 加载通用工具库
source "$SCRIPT_DIR/../common.sh"

readonly MODULE_INTERFACE_VERSION="1.0"

# =============================================================================
# 模块接口规范
# =============================================================================

# 每个模块必须实现的标准函数列表
readonly REQUIRED_MODULE_FUNCTIONS=(
    "validate"      # 验证模块配置和前置条件
    "deploy"        # 部署模块资源
    "status"        # 检查模块状态
    "cleanup"       # 清理模块资源
    "rollback"      # 回滚模块更改
)

# 可选的模块函数
readonly OPTIONAL_MODULE_FUNCTIONS=(
    "configure"     # 配置模块
    "test"          # 测试模块功能
    "monitor"       # 监控模块
    "backup"        # 备份模块数据
    "restore"       # 恢复模块数据
)

# =============================================================================
# 模块自动加载器
# =============================================================================

# 根据模块名查找并加载模块文件
load_module_if_needed() {
    local module_name="$1"
    local function_name="${module_name}_validate"  # 使用validate作为测试函数
    
    # 如果函数已经存在，则模块已加载
    if declare -F "$function_name" >/dev/null; then
        print_debug "模块已加载: $module_name"
        return 0
    fi
    
    print_debug "尝试加载模块: $module_name"
    
    # 定义可能的模块文件路径
    local module_file_candidates=(
        "$PROJECT_ROOT/scripts/core/infrastructure/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/catalog/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/compute/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/monitoring/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/data_processing/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/${module_name}.sh"
    )
    
    # 尝试加载模块文件
    for module_file in "${module_file_candidates[@]}"; do
        if [[ -f "$module_file" ]]; then
            print_debug "找到模块文件: $module_file"
            
            # 加载模块文件
            if source "$module_file"; then
                print_debug "成功加载模块: $module_name"
                
                # 验证模块是否正确加载（检查validate函数是否存在）
                if declare -F "$function_name" >/dev/null; then
                    print_debug "模块验证通过: $module_name"
                    return 0
                else
                    print_warning "模块文件已加载但验证函数不存在: $function_name"
                fi
            else
                print_error "加载模块文件失败: $module_file"
            fi
        fi
    done
    
    print_error "未找到模块文件: $module_name"
    print_debug "搜索路径: ${module_file_candidates[*]}"
    return 1
}

# =============================================================================
# 模块接口执行器
# =============================================================================

module_interface() {
    local action="$1"
    local module_name="$2"
    shift 2
    
    if [[ -z "$action" || -z "$module_name" ]]; then
        print_error "用法: module_interface <action> <module_name> [args...]"
        return 1
    fi
    
    local function_name="${module_name}_${action}"
    
    # 尝试加载模块（如果尚未加载）
    if ! load_module_if_needed "$module_name"; then
        print_error "无法加载模块: $module_name"
        return 1
    fi
    
    # 检查函数是否存在
    if ! declare -F "$function_name" >/dev/null; then
        print_error "模块函数不存在: $function_name"
        print_info "确保模块 '$module_name' 实现了 '$action' 函数"
        return 1
    fi
    
    # 记录操作开始
    print_debug "执行模块操作: $module_name.$action"
    start_timer "${module_name}_${action}"
    
    # 执行模块函数
    local exit_code=0
    if "$function_name" "$@"; then
        print_success "模块操作成功: $module_name.$action"
    else
        exit_code=$?
        print_error "模块操作失败: $module_name.$action (退出代码: $exit_code)"
    fi
    
    # 记录操作完成
    end_timer "${module_name}_${action}"
    
    return $exit_code
}

# =============================================================================
# 模块验证器
# =============================================================================

validate_module_implementation() {
    local module_name="$1"
    local module_file="$2"
    
    if [[ -z "$module_name" || -z "$module_file" ]]; then
        print_error "用法: validate_module_implementation <module_name> <module_file>"
        return 1
    fi
    
    if [[ ! -f "$module_file" ]]; then
        print_error "模块文件不存在: $module_file"
        return 1
    fi
    
    print_step "验证模块实现: $module_name"
    
    # 加载模块文件
    source "$module_file"
    
    local missing_functions=()
    local validation_errors=0
    
    # 检查必需函数
    for func in "${REQUIRED_MODULE_FUNCTIONS[@]}"; do
        local function_name="${module_name}_${func}"
        
        if ! declare -F "$function_name" >/dev/null; then
            missing_functions+=("$function_name")
            validation_errors=$((validation_errors + 1))
        else
            print_debug "✓ 找到必需函数: $function_name"
        fi
    done
    
    # 检查可选函数
    local optional_functions=()
    for func in "${OPTIONAL_MODULE_FUNCTIONS[@]}"; do
        local function_name="${module_name}_${func}"
        
        if declare -F "$function_name" >/dev/null; then
            optional_functions+=("$function_name")
            print_debug "✓ 找到可选函数: $function_name"
        fi
    done
    
    # 输出验证结果
    if [[ $validation_errors -eq 0 ]]; then
        print_success "模块 '$module_name' 实现验证通过"
        print_info "实现的可选函数: ${#optional_functions[@]}"
        
        if [[ ${#optional_functions[@]} -gt 0 ]]; then
            for func in "${optional_functions[@]}"; do
                print_debug "  - $func"
            done
        fi
        
        return 0
    else
        print_error "模块 '$module_name' 实现验证失败"
        print_error "缺少 $validation_errors 个必需函数:"
        
        for func in "${missing_functions[@]}"; do
            print_error "  - $func"
        done
        
        return 1
    fi
}

# =============================================================================
# 批量模块操作
# =============================================================================

execute_batch_operation() {
    local operation="$1"
    shift
    local modules=("$@")
    
    if [[ -z "$operation" || ${#modules[@]} -eq 0 ]]; then
        print_error "用法: execute_batch_operation <operation> <module1> [module2] ..."
        return 1
    fi
    
    print_step "批量执行操作: $operation"
    print_info "模块数量: ${#modules[@]}"
    
    local success_count=0
    local failed_modules=()
    
    for module in "${modules[@]}"; do
        print_info "处理模块: $module"
        
        if module_interface "$operation" "$module"; then
            success_count=$((success_count + 1))
            print_success "✓ $module.$operation 成功"
        else
            failed_modules+=("$module")
            print_error "✗ $module.$operation 失败"
        fi
        
        echo  # 分隔符
    done
    
    # 输出批量操作结果
    print_step "批量操作结果"
    print_info "成功: $success_count/${#modules[@]}"
    
    if [[ ${#failed_modules[@]} -gt 0 ]]; then
        print_error "失败的模块:"
        for module in "${failed_modules[@]}"; do
            print_error "  - $module"
        done
        return 1
    else
        print_success "所有模块操作都成功完成"
        return 0
    fi
}

# =============================================================================
# 依赖管理
# =============================================================================

# Bash 3.x互換性のため連想配列を無効化
# declare -A MODULE_DEPENDENCIES
MODULE_DEPENDENCIES=""

register_module_dependency() {
    # Bash 3.x互換性のため無効化
    print_debug "モジュール依存関係の登録は無効化されています (Bash 3.x互換性)"
}

get_module_dependencies() {
    # Bash 3.x互換性のため無効化
    print_debug "モジュール依存関係の取得は無効化されています (Bash 3.x互換性)"
}

resolve_deployment_order() {
    local modules=("$@")
    local resolved_order=()
    local processed=()
    
    print_debug "解析部署顺序，输入模块: ${modules[*]}"
    
    # 简单的依赖解析（拓扑排序的简化版本）
    # 注意：这是一个基础实现，实际项目中可能需要更复杂的算法
    
    local remaining_modules=("${modules[@]}")
    
    while [[ ${#remaining_modules[@]} -gt 0 ]]; do
        local progress_made=false
        local new_remaining=()
        
        for module in "${remaining_modules[@]}"; do
            local dependencies
            dependencies=$(get_module_dependencies "$module")
            
            if [[ -z "$dependencies" ]]; then
                # 没有依赖，可以直接处理
                resolved_order+=("$module")
                processed+=("$module")
                progress_made=true
                print_debug "添加无依赖模块: $module"
            else
                # 检查所有依赖是否已处理
                local all_deps_resolved=true
                IFS=',' read -ra deps <<< "$dependencies"
                
                for dep in "${deps[@]}"; do
                    if [[ ! " ${processed[*]} " =~ " $dep " ]]; then
                        all_deps_resolved=false
                        break
                    fi
                done
                
                if [[ "$all_deps_resolved" == true ]]; then
                    resolved_order+=("$module")
                    processed+=("$module")
                    progress_made=true
                    print_debug "添加依赖已满足的模块: $module"
                else
                    new_remaining+=("$module")
                fi
            fi
        done
        
        remaining_modules=("${new_remaining[@]}")
        
        # 检测循环依赖
        if [[ "$progress_made" == false && ${#remaining_modules[@]} -gt 0 ]]; then
            print_error "检测到循环依赖或未满足的依赖:"
            for module in "${remaining_modules[@]}"; do
                local deps
                deps=$(get_module_dependencies "$module")
                print_error "  $module -> $deps"
            done
            return 1
        fi
    done
    
    print_debug "解析的部署顺序: ${resolved_order[*]}"
    printf '%s\n' "${resolved_order[@]}"
}

# =============================================================================
# 模块生命周期管理
# =============================================================================

deploy_modules_in_order() {
    local modules=("$@")
    
    print_step "按依赖顺序部署模块"
    
    # 解析部署顺序
    local ordered_modules
    if ! ordered_modules=($(resolve_deployment_order "${modules[@]}")); then
        print_error "无法解析部署顺序"
        return 1
    fi
    
    print_info "部署顺序: ${ordered_modules[*]}"
    
    # 按顺序部署
    local deployed_modules=()
    
    for module in "${ordered_modules[@]}"; do
        print_info "部署模块: $module"
        
        # 验证模块
        if ! module_interface "validate" "$module"; then
            print_error "模块验证失败: $module"
            
            # 回滚已部署的模块
            if [[ ${#deployed_modules[@]} -gt 0 ]]; then
                print_warning "回滚已部署的模块..."
                rollback_modules "${deployed_modules[@]}"
            fi
            
            return 1
        fi
        
        # 部署模块
        if ! module_interface "deploy" "$module"; then
            print_error "模块部署失败: $module"
            
            # 回滚已部署的模块
            if [[ ${#deployed_modules[@]} -gt 0 ]]; then
                print_warning "回滚已部署的模块..."
                rollback_modules "${deployed_modules[@]}"
            fi
            
            return 1
        fi
        
        deployed_modules+=("$module")
        print_success "模块部署成功: $module"
    done
    
    print_success "所有模块部署完成"
    return 0
}

rollback_modules() {
    local modules=("$@")
    
    print_step "回滚模块"
    
    # 按相反顺序回滚
    local reversed_modules=()
    for ((i=${#modules[@]}-1; i>=0; i--)); do
        reversed_modules+=("${modules[i]}")
    done
    
    print_info "回滚顺序: ${reversed_modules[*]}"
    
    for module in "${reversed_modules[@]}"; do
        print_info "回滚模块: $module"
        
        if module_interface "rollback" "$module"; then
            print_success "模块回滚成功: $module"
        else
            print_error "模块回滚失败: $module"
        fi
    done
}

# =============================================================================
# 模块状态检查
# =============================================================================

check_all_modules_status() {
    local modules=("$@")
    
    print_step "检查所有模块状态"
    
    local healthy_count=0
    local unhealthy_modules=()
    
    for module in "${modules[@]}"; do
        if module_interface "status" "$module"; then
            healthy_count=$((healthy_count + 1))
            print_success "✓ $module: 健康"
        else
            unhealthy_modules+=("$module")
            print_error "✗ $module: 不健康"
        fi
    done
    
    # 输出状态摘要
    echo
    print_info "状态摘要:"
    print_info "健康模块: $healthy_count/${#modules[@]}"
    
    if [[ ${#unhealthy_modules[@]} -gt 0 ]]; then
        print_warning "不健康的模块:"
        for module in "${unhealthy_modules[@]}"; do
            print_warning "  - $module"
        done
        return 1
    else
        print_success "所有模块都健康"
        return 0
    fi
}

# =============================================================================
# 工具函数
# =============================================================================

list_available_modules() {
    local modules_dir="${1:-$PROJECT_ROOT/scripts/core}"
    
    print_info "可用模块:"
    
    if [[ -d "$modules_dir" ]]; then
        find "$modules_dir" -name "*.sh" -type f | while read -r module_file; do
            local module_name
            module_name=$(basename "$module_file" .sh)
            echo "  - $module_name"
        done
    else
        print_warning "模块目录不存在: $modules_dir"
    fi
}

generate_module_template() {
    local module_name="$1"
    local output_file="$2"
    
    if [[ -z "$module_name" ]]; then
        print_error "用法: generate_module_template <module_name> [output_file]"
        return 1
    fi
    
    output_file="${output_file:-${module_name}_module.sh}"
    
    cat > "$output_file" << EOF
#!/bin/bash

# =============================================================================
# ${module_name^} 模块
# 版本: 1.0.0
# 描述: ${module_name} 模块的实现
# =============================================================================

# 加载通用工具库
SCRIPT_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
source "\$SCRIPT_DIR/../lib/common.sh"

# =============================================================================
# 模块配置
# =============================================================================

readonly ${module_name^^}_MODULE_VERSION="1.0.0"

# =============================================================================
# 必需函数实现
# =============================================================================

${module_name}_validate() {
    print_info "验证 ${module_name} 模块配置"
    
    # 在此添加验证逻辑
    
    return 0
}

${module_name}_deploy() {
    print_info "部署 ${module_name} 模块"
    
    # 在此添加部署逻辑
    
    return 0
}

${module_name}_status() {
    print_info "检查 ${module_name} 模块状态"
    
    # 在此添加状态检查逻辑
    
    return 0
}

${module_name}_cleanup() {
    print_info "清理 ${module_name} 模块资源"
    
    # 在此添加清理逻辑
    
    return 0
}

${module_name}_rollback() {
    print_info "回滚 ${module_name} 模块更改"
    
    # 在此添加回滚逻辑
    
    return 0
}

# =============================================================================
# 可选函数实现
# =============================================================================

${module_name}_configure() {
    print_info "配置 ${module_name} 模块"
    
    # 在此添加配置逻辑
    
    return 0
}

${module_name}_test() {
    print_info "测试 ${module_name} 模块功能"
    
    # 在此添加测试逻辑
    
    return 0
}

# =============================================================================
# 模块特定的辅助函数
# =============================================================================

# 在此添加模块特定的函数

# =============================================================================
# 如果直接执行此脚本
# =============================================================================

if [[ "\${BASH_SOURCE[0]}" == "\${0}" ]]; then
    # 加载模块接口
    source "\$SCRIPT_DIR/../lib/interfaces/module_interface.sh"
    
    # 执行传入的操作
    if [[ \$# -gt 0 ]]; then
        module_interface "\$1" "${module_name}" "\${@:2}"
    else
        echo "用法: \$0 <action> [args...]"
        echo "可用操作: validate, deploy, status, cleanup, rollback"
    fi
fi
EOF
    
    chmod +x "$output_file"
    print_success "模块模板已生成: $output_file"
}

# =============================================================================
# 主函数
# =============================================================================

show_interface_help() {
    cat << EOF
模块化接口系统 v$MODULE_INTERFACE_VERSION

这个系统定义了标准化的模块接口，确保所有模块遵循统一的API。

标准接口函数：
    validate        验证模块配置和前置条件
    deploy          部署模块资源
    status          检查模块状态
    cleanup         清理模块资源
    rollback        回滚模块更改

可选接口函数：
    configure       配置模块
    test            测试模块功能
    monitor         监控模块
    backup          备份模块数据
    restore         恢复模块数据

使用示例：
    # 执行单个模块操作
    module_interface deploy s3_storage

    # 批量操作
    execute_batch_operation validate s3_storage iam_roles glue_catalog

    # 检查模块状态
    check_all_modules_status s3_storage iam_roles glue_catalog

    # 按依赖顺序部署
    deploy_modules_in_order s3_storage iam_roles glue_catalog

EOF
}

# 如果直接执行此脚本，显示帮助
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
        show_interface_help
    else
        echo "这是一个库文件，不应直接执行。"
        echo "使用 --help 查看使用说明。"
        exit 1
    fi
fi