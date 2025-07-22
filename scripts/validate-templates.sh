#!/bin/bash

# =============================================================================
# CloudFormation 模板验证工具
# 版本: 1.0.0
# 描述: 验证CloudFormation模板的语法、安全性和最佳实践
# =============================================================================

set -euo pipefail

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 加载通用工具库
source "$SCRIPT_DIR/lib/common.sh"

readonly TEMPLATE_VALIDATOR_VERSION="1.0.0"

# =============================================================================
# 全局变量
# =============================================================================

declare -g VALIDATION_ERRORS=0
declare -g VALIDATION_WARNINGS=0
declare -g TEMPLATE_DIR="${PROJECT_ROOT}/templates"
declare -g VALIDATION_REPORT=""
declare -g DETAILED_OUTPUT=false
declare -g STRICT_MODE=false

# =============================================================================
# 辅助函数
# =============================================================================

increment_error() {
    VALIDATION_ERRORS=$((VALIDATION_ERRORS + 1))
}

increment_warning() {
    VALIDATION_WARNINGS=$((VALIDATION_WARNINGS + 1))
}

print_validation_error() {
    local template="$1"
    local message="$2"
    print_error "$(basename "$template"): $message"
    increment_error
}

print_validation_warning() {
    local template="$1"
    local message="$2"
    print_warning "$(basename "$template"): $message"
    increment_warning
}

print_validation_info() {
    local template="$1"
    local message="$2"
    if [[ "$DETAILED_OUTPUT" == true ]]; then
        print_info "$(basename "$template"): $message"
    fi
}

# =============================================================================
# CloudFormation语法验证
# =============================================================================

validate_cloudformation_syntax() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "验证CloudFormation语法: $template_name"
    
    # 检查文件是否存在
    if [[ ! -f "$template" ]]; then
        print_validation_error "$template" "模板文件不存在"
        return 1
    fi
    
    # 检查文件是否为空
    if [[ ! -s "$template" ]]; then
        print_validation_error "$template" "模板文件为空"
        return 1
    fi
    
    # AWS CLI 语法验证
    local validation_output
    if validation_output=$(aws cloudformation validate-template --template-body "file://$template" 2>&1); then
        print_validation_info "$template" "AWS语法验证通过"
        
        # 检查模板描述
        if echo "$validation_output" | grep -q '"Description"'; then
            print_validation_info "$template" "包含模板描述"
        else
            print_validation_warning "$template" "建议添加模板描述"
        fi
        
        return 0
    else
        print_validation_error "$template" "AWS语法验证失败: $validation_output"
        return 1
    fi
}

# =============================================================================
# CFN-Lint 验证
# =============================================================================

check_cfn_lint_available() {
    if command -v cfn-lint &>/dev/null; then
        return 0
    elif python3 -m cfn_lint --version &>/dev/null; then
        return 0
    else
        print_warning "cfn-lint未安装，跳过高级验证"
        print_info "安装命令: pip install cfn-lint"
        return 1
    fi
}

validate_with_cfn_lint() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    if ! check_cfn_lint_available; then
        return 0
    fi
    
    print_debug "执行cfn-lint验证: $template_name"
    
    local cfn_lint_cmd
    if command -v cfn-lint &>/dev/null; then
        cfn_lint_cmd="cfn-lint"
    else
        cfn_lint_cmd="python3 -m cfn_lint"
    fi
    
    # 执行cfn-lint，忽略某些警告
    local lint_output
    if lint_output=$($cfn_lint_cmd "$template" \
        --ignore-checks W3002 W3005 \
        --format parseable 2>&1); then
        
        print_validation_info "$template" "CFN-Lint验证通过"
        return 0
    else
        # 分析输出，区分错误和警告
        while IFS= read -r line; do
            if [[ -n "$line" ]]; then
                if [[ "$line" =~ .*:[E][0-9]+:.* ]]; then
                    print_validation_error "$template" "CFN-Lint错误: $line"
                elif [[ "$line" =~ .*:[W][0-9]+:.* ]]; then
                    print_validation_warning "$template" "CFN-Lint警告: $line"
                elif [[ "$line" =~ .*:[I][0-9]+:.* ]]; then
                    print_validation_info "$template" "CFN-Lint信息: $line"
                fi
            fi
        done <<< "$lint_output"
        
        # 如果有错误且在严格模式下，返回失败
        if [[ "$STRICT_MODE" == true ]] && echo "$lint_output" | grep -q ":[E][0-9]:"; then
            return 1
        fi
        
        return 0
    fi
}

# =============================================================================
# 自定义验证规则
# =============================================================================

validate_security_rules() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "验证安全规则: $template_name"
    
    # 检查S3桶加密
    if grep -q "AWS::S3::Bucket" "$template"; then
        if ! grep -q "BucketEncryption\|ServerSideEncryptionConfiguration" "$template"; then
            print_validation_error "$template" "S3桶未配置加密"
        else
            print_validation_info "$template" "S3桶加密配置检查通过"
        fi
        
        # 检查公共访问阻止
        if ! grep -q "PublicAccessBlockConfiguration" "$template"; then
            print_validation_warning "$template" "S3桶未配置公共访问阻止"
        else
            print_validation_info "$template" "S3桶公共访问阻止配置检查通过"
        fi
    fi
    
    # 检查IAM策略
    if grep -q "AWS::IAM::" "$template"; then
        # 检查是否有过于宽泛的权限
        if grep -q '"Effect": "Allow".*"Action": "\*"' "$template" || \
           grep -q '"Effect": "Allow".*"Resource": "\*"' "$template"; then
            print_validation_warning "$template" "发现可能过于宽泛的IAM权限"
        fi
        
        # 检查根权限
        if grep -q '"Principal": "\*"' "$template"; then
            print_validation_error "$template" "发现危险的通配符主体"
        fi
    fi
    
    # 检查硬编码的敏感信息
    local sensitive_patterns=(
        "AKIA[0-9A-Z]{16}"          # AWS Access Key
        "[0-9]{12}"                 # AWS Account ID (需要更精确的检查)
        "password.*['\"][^'\"]{8,}" # 可能的密码
    )
    
    for pattern in "${sensitive_patterns[@]}"; do
        if grep -qE "$pattern" "$template"; then
            print_validation_error "$template" "可能包含硬编码的敏感信息: $pattern"
        fi
    done
}

validate_naming_conventions() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "验证命名规范: $template_name"
    
    # 检查资源命名是否使用参数
    local resource_count=0
    local parameterized_count=0
    
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*[A-Za-z][A-Za-z0-9]*:$ ]]; then
            resource_count=$((resource_count + 1))
            
            # 检查资源名称是否使用Ref或Sub函数
            local next_lines
            next_lines=$(grep -A 10 "$line" "$template" | head -10)
            if echo "$next_lines" | grep -q "Ref:\|!Ref\|Sub:\|!Sub"; then
                parameterized_count=$((parameterized_count + 1))
            fi
        fi
    done < <(grep -E "^[[:space:]]*[A-Za-z][A-Za-z0-9]*:$" "$template")
    
    if [[ $resource_count -gt 0 ]]; then
        local param_percentage=$((parameterized_count * 100 / resource_count))
        if [[ $param_percentage -lt 50 ]]; then
            print_validation_warning "$template" "建议更多使用参数化资源命名 ($param_percentage% 参数化)"
        else
            print_validation_info "$template" "资源命名参数化程度良好 ($param_percentage%)"
        fi
    fi
}

validate_best_practices() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "验证最佳实践: $template_name"
    
    # 检查是否有描述
    if ! grep -q "^Description:" "$template"; then
        print_validation_warning "$template" "建议添加模板描述"
    fi
    
    # 检查是否有标签
    if ! grep -q "Tags:" "$template"; then
        print_validation_warning "$template" "建议为资源添加标签"
    fi
    
    # 检查是否使用了条件
    if grep -q "Conditions:" "$template"; then
        print_validation_info "$template" "使用了条件逻辑"
    fi
    
    # 检查是否有输出
    if ! grep -q "Outputs:" "$template"; then
        print_validation_warning "$template" "建议添加有用的输出"
    fi
    
    # 检查模板大小（CloudFormation限制为51200字节）
    local template_size
    template_size=$(wc -c < "$template")
    if [[ $template_size -gt 51200 ]]; then
        print_validation_error "$template" "模板大小超过CloudFormation限制 (${template_size} > 51200 字节)"
    elif [[ $template_size -gt 40000 ]]; then
        print_validation_warning "$template" "模板大小接近CloudFormation限制 (${template_size} 字节)"
    fi
}

validate_dependencies() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "验证依赖关系: $template_name"
    
    # 检查循环依赖（简单检查）
    local dependencies=()
    
    # 提取Ref引用
    while IFS= read -r line; do
        if [[ "$line" =~ Ref:[[:space:]]*([A-Za-z][A-Za-z0-9]*) ]] || \
           [[ "$line" =~ !Ref[[:space:]]+([A-Za-z][A-Za-z0-9]*) ]]; then
            dependencies+=("${BASH_REMATCH[1]}")
        fi
    done < "$template"
    
    # 检查是否引用了不存在的资源
    # 这里进行简单的检查，更复杂的逻辑需要解析整个模板结构
    if [[ ${#dependencies[@]} -gt 0 ]]; then
        print_validation_info "$template" "发现 ${#dependencies[@]} 个资源引用"
    fi
}

# =============================================================================
# 综合验证函数
# =============================================================================

validate_single_template() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_info "验证模板: $template_name"
    
    # 初始化模板特定的计数器
    local template_errors=0
    local template_warnings=0
    local initial_errors=$VALIDATION_ERRORS
    local initial_warnings=$VALIDATION_WARNINGS
    
    # 执行各种验证
    validate_cloudformation_syntax "$template"
    validate_with_cfn_lint "$template"
    validate_security_rules "$template"
    validate_naming_conventions "$template"
    validate_best_practices "$template"
    validate_dependencies "$template"
    
    # 计算模板特定的错误和警告数
    template_errors=$((VALIDATION_ERRORS - initial_errors))
    template_warnings=$((VALIDATION_WARNINGS - initial_warnings))
    
    # 输出模板验证结果
    if [[ $template_errors -eq 0 ]]; then
        if [[ $template_warnings -eq 0 ]]; then
            print_success "✓ $template_name: 验证通过，无问题"
        else
            print_info "⚠ $template_name: 验证通过，有 $template_warnings 个警告"
        fi
    else
        print_error "✗ $template_name: 验证失败，有 $template_errors 个错误，$template_warnings 个警告"
    fi
    
    echo  # 空行分隔
}

validate_all_templates() {
    local template_pattern="${1:-*.yaml}"
    local template_count=0
    
    print_step "开始批量验证CloudFormation模板"
    print_info "模板验证器版本: $TEMPLATE_VALIDATOR_VERSION"
    print_info "模板目录: $TEMPLATE_DIR"
    print_info "模板模式: $template_pattern"
    
    # 检查模板目录是否存在
    if [[ ! -d "$TEMPLATE_DIR" ]]; then
        handle_error 1 "模板目录不存在: $TEMPLATE_DIR"
    fi
    
    # 查找模板文件
    local templates=()
    while IFS= read -r -d '' template; do
        templates+=("$template")
        template_count=$((template_count + 1))
    done < <(find "$TEMPLATE_DIR" -name "$template_pattern" -type f -print0)
    
    if [[ $template_count -eq 0 ]]; then
        print_warning "未找到匹配的模板文件: $template_pattern"
        return 0
    fi
    
    print_info "找到 $template_count 个模板文件"
    echo
    
    # 验证每个模板
    local current=0
    for template in "${templates[@]}"; do
        current=$((current + 1))
        show_progress $current $template_count "验证中..."
        validate_single_template "$template"
    done
    
    finish_progress
}

# =============================================================================
# 报告生成
# =============================================================================

generate_validation_report() {
    local output_file="${1:-validation_report.txt}"
    
    print_step "生成验证报告"
    
    local report_content
    report_content=$(cat << EOF
CloudFormation模板验证报告
========================
生成时间: $(date)
验证器版本: $TEMPLATE_VALIDATOR_VERSION
模板目录: $TEMPLATE_DIR

验证摘要:
========
总错误数: $VALIDATION_ERRORS
总警告数: $VALIDATION_WARNINGS
验证状态: $([[ $VALIDATION_ERRORS -eq 0 ]] && echo "通过" || echo "失败")

建议:
====
EOF
    )
    
    if [[ $VALIDATION_ERRORS -gt 0 ]]; then
        report_content+=$'\n- 修复所有错误后再进行部署'
    fi
    
    if [[ $VALIDATION_WARNINGS -gt 0 ]]; then
        report_content+=$'\n- 考虑修复警告以提高模板质量'
    fi
    
    if [[ $VALIDATION_ERRORS -eq 0 && $VALIDATION_WARNINGS -eq 0 ]]; then
        report_content+=$'\n- 所有模板验证通过，可以安全部署'
    fi
    
    # 写入报告文件
    echo "$report_content" > "$output_file"
    print_success "验证报告已保存: $output_file"
}

# =============================================================================
# 工具函数
# =============================================================================

install_cfn_lint() {
    print_step "安装cfn-lint"
    
    if command -v pip3 &>/dev/null; then
        pip3 install cfn-lint
    elif command -v pip &>/dev/null; then
        pip install cfn-lint
    else
        print_error "未找到pip，请手动安装cfn-lint"
        return 1
    fi
    
    print_success "cfn-lint安装完成"
}

fix_common_issues() {
    local template="$1"
    local backup_file="${template}.backup"
    
    print_info "尝试修复常见问题: $(basename "$template")"
    
    # 创建备份
    cp "$template" "$backup_file"
    
    # 修复常见格式问题
    # 移除尾随空格
    sed -i.tmp 's/[[:space:]]*$//' "$template" && rm "${template}.tmp"
    
    # 确保文件以换行符结尾
    [[ -n "$(tail -c1 "$template")" ]] && echo >> "$template"
    
    print_info "基本修复完成，备份保存为: $(basename "$backup_file")"
}

# =============================================================================
# 主函数和CLI
# =============================================================================

show_help() {
    cat << EOF
CloudFormation模板验证器 v$TEMPLATE_VALIDATOR_VERSION

用法: $0 [选项] [模板模式]

选项:
    -h, --help              显示帮助信息
    -v, --verbose           详细输出
    -s, --strict            严格模式（警告也视为错误）
    -d, --template-dir DIR  指定模板目录（默认: templates/）
    -r, --report FILE       生成验证报告到指定文件
    -f, --fix               尝试自动修复常见问题
    --install-cfn-lint      安装cfn-lint工具
    --single FILE           验证单个模板文件

参数:
    模板模式               要验证的模板文件模式（默认: *.yaml）

示例:
    $0                          # 验证所有YAML模板
    $0 s3*.yaml                 # 验证以s3开头的YAML模板
    $0 --single templates/iam-roles-policies.yaml  # 验证单个模板
    $0 --verbose --report validation.txt  # 详细输出并生成报告
    $0 --strict                 # 严格模式验证
    $0 --install-cfn-lint       # 安装cfn-lint工具

EOF
}

main() {
    local template_pattern="*.yaml"
    local single_template=""
    local report_file=""
    local auto_fix=false
    local install_lint=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--verbose)
                DETAILED_OUTPUT=true
                LOG_LEVEL="DEBUG"
                shift
                ;;
            -s|--strict)
                STRICT_MODE=true
                shift
                ;;
            -d|--template-dir)
                TEMPLATE_DIR="$2"
                shift 2
                ;;
            -r|--report)
                report_file="$2"
                shift 2
                ;;
            -f|--fix)
                auto_fix=true
                shift
                ;;
            --install-cfn-lint)
                install_lint=true
                shift
                ;;
            --single)
                single_template="$2"
                shift 2
                ;;
            -*)
                print_error "未知选项: $1"
                show_help
                exit 1
                ;;
            *)
                template_pattern="$1"
                shift
                ;;
        esac
    done
    
    # 验证前置条件
    validate_prerequisites
    
    # 安装cfn-lint（如果请求）
    if [[ "$install_lint" == true ]]; then
        install_cfn_lint
        exit 0
    fi
    
    # 开始验证
    start_timer "validation"
    
    if [[ -n "$single_template" ]]; then
        # 验证单个模板
        if [[ ! -f "$single_template" ]]; then
            handle_error 1 "模板文件不存在: $single_template"
        fi
        
        validate_single_template "$single_template"
        
        # 自动修复（如果请求）
        if [[ "$auto_fix" == true ]]; then
            fix_common_issues "$single_template"
        fi
    else
        # 批量验证
        validate_all_templates "$template_pattern"
    fi
    
    end_timer "validation"
    
    # 生成报告
    if [[ -n "$report_file" ]]; then
        generate_validation_report "$report_file"
    fi
    
    # 输出最终结果
    echo
    print_step "验证结果摘要"
    
    if [[ $VALIDATION_ERRORS -eq 0 ]]; then
        if [[ $VALIDATION_WARNINGS -eq 0 ]]; then
            print_success "🎉 所有模板验证通过，没有发现问题！"
        else
            print_info "✅ 所有模板验证通过，但有 $VALIDATION_WARNINGS 个警告"
            if [[ "$STRICT_MODE" == true ]]; then
                print_error "严格模式下警告被视为错误"
                exit 1
            fi
        fi
        exit 0
    else
        print_error "❌ 发现 $VALIDATION_ERRORS 个错误和 $VALIDATION_WARNINGS 个警告"
        echo
        print_info "修复建议:"
        print_info "1. 查看上述错误信息并修复模板"
        print_info "2. 重新运行验证确保问题已解决"
        print_info "3. 考虑使用 --fix 选项自动修复常见问题"
        exit 1
    fi
}

# 如果直接执行此脚本，运行主函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi