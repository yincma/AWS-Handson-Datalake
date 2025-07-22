#!/bin/bash

# =============================================================================
# AWS 数据湖项目配置验证器
# 版本: 1.0.0
# 描述: 验证配置文件和环境变量的有效性
# =============================================================================

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# 加载通用工具库
source "$SCRIPT_DIR/../common.sh"

readonly CONFIG_VALIDATOR_VERSION="1.0.0"

# =============================================================================
# 验证规则定义
# =============================================================================

# AWS 区域模式
readonly AWS_REGION_PATTERN="^[a-z]{2,3}-[a-z]+-[0-9]+$"

# S3 存储桶命名模式
readonly BUCKET_NAME_PATTERN="^[a-z0-9][a-z0-9\-]*[a-z0-9]$"

# 项目前缀模式
readonly PROJECT_PREFIX_PATTERN="^[a-z0-9][a-z0-9\-]*[a-z0-9]$"

# 环境模式
readonly ENVIRONMENT_PATTERN="^(dev|development|staging|stage|prod|production)$"

# EMR 实例类型模式
readonly EMR_INSTANCE_TYPE_PATTERN="^[a-z][0-9]+\.[a-z]+$"

# =============================================================================
# 验证函数
# =============================================================================

validate_aws_region() {
    local region="$1"
    local field_name="$2"
    
    if [[ ! "$region" =~ $AWS_REGION_PATTERN ]]; then
        echo "❌ 无效的AWS区域格式: $field_name='$region'"
        echo "   期望格式: us-east-1, eu-west-1, ap-southeast-1 等"
        return 1
    fi
    
    # 验证区域是否真实存在
    if ! aws ec2 describe-regions --region-names "$region" &>/dev/null; then
        echo "❌ AWS区域不存在或不可用: $field_name='$region'"
        return 1
    fi
    
    print_success "✓ AWS区域验证通过: $field_name='$region'"
    return 0
}

validate_s3_bucket_name() {
    local bucket_name="$1"
    local field_name="$2"
    
    # 检查长度
    if [[ ${#bucket_name} -lt 3 || ${#bucket_name} -gt 63 ]]; then
        echo "❌ S3存储桶名称长度无效: $field_name='$bucket_name'"
        echo "   长度必须在3-63个字符之间"
        return 1
    fi
    
    # 检查格式
    if [[ ! "$bucket_name" =~ $BUCKET_NAME_PATTERN ]]; then
        echo "❌ S3存储桶名称格式无效: $field_name='$bucket_name'"
        echo "   只能包含小写字母、数字和连字符，不能以连字符开头或结尾"
        return 1
    fi
    
    # 检查是否包含连续的连字符
    if [[ "$bucket_name" == *"--"* ]]; then
        echo "❌ S3存储桶名称不能包含连续的连字符: $field_name='$bucket_name'"
        return 1
    fi
    
    # 检查是否类似IP地址
    if [[ "$bucket_name" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "❌ S3存储桶名称不能类似IP地址: $field_name='$bucket_name'"
        return 1
    fi
    
    print_success "✓ S3存储桶名称验证通过: $field_name='$bucket_name'"
    return 0
}

validate_project_prefix() {
    local prefix="$1"
    local field_name="$2"
    
    # 检查长度
    if [[ ${#prefix} -lt 3 || ${#prefix} -gt 20 ]]; then
        echo "❌ 项目前缀长度无效: $field_name='$prefix'"
        echo "   长度必须在3-20个字符之间"
        return 1
    fi
    
    # 检查格式
    if [[ ! "$prefix" =~ $PROJECT_PREFIX_PATTERN ]]; then
        echo "❌ 项目前缀格式无效: $field_name='$prefix'"
        echo "   只能包含小写字母、数字和连字符，不能以连字符开头或结尾"
        return 1
    fi
    
    print_success "✓ 项目前缀验证通过: $field_name='$prefix'"
    return 0
}

validate_environment() {
    local environment="$1"
    local field_name="$2"
    
    if [[ ! "$environment" =~ $ENVIRONMENT_PATTERN ]]; then
        echo "❌ 环境类型无效: $field_name='$environment'"
        echo "   允许的值: dev, development, staging, stage, prod, production"
        return 1
    fi
    
    print_success "✓ 环境类型验证通过: $field_name='$environment'"
    return 0
}

validate_emr_instance_type() {
    local instance_type="$1"
    local field_name="$2"
    
    if [[ ! "$instance_type" =~ $EMR_INSTANCE_TYPE_PATTERN ]]; then
        echo "❌ EMR实例类型格式无效: $field_name='$instance_type'"
        echo "   期望格式: m5.xlarge, c5.2xlarge, r5.large 等"
        return 1
    fi
    
    # 验证实例类型是否在当前区域可用
    if ! aws ec2 describe-instance-types --instance-types "$instance_type" &>/dev/null; then
        echo "❌ EMR实例类型在当前区域不可用: $field_name='$instance_type'"
        return 1
    fi
    
    print_success "✓ EMR实例类型验证通过: $field_name='$instance_type'"
    return 0
}

validate_positive_integer() {
    local value="$1"
    local field_name="$2"
    local min_value="${3:-1}"
    local max_value="${4:-1000}"
    
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        echo "❌ 不是有效的正整数: $field_name='$value'"
        return 1
    fi
    
    if [[ $value -lt $min_value || $value -gt $max_value ]]; then
        echo "❌ 数值超出有效范围: $field_name='$value'"
        echo "   允许范围: $min_value - $max_value"
        return 1
    fi
    
    print_success "✓ 数值验证通过: $field_name='$value'"
    return 0
}

validate_boolean() {
    local value="$1"
    local field_name="$2"
    
    case "${value,,}" in
        true|false|yes|no|1|0|enabled|disabled)
            print_success "✓ 布尔值验证通过: $field_name='$value'"
            return 0
            ;;
        *)
            echo "❌ 无效的布尔值: $field_name='$value'"
            echo "   允许的值: true, false, yes, no, 1, 0, enabled, disabled"
            return 1
            ;;
    esac
}

# =============================================================================
# 主要验证函数
# =============================================================================

validate_required_variables() {
    local validation_errors=0
    
    print_step "验证必需的环境变量..."
    
    # 必需变量列表
    local required_vars=(
        "PROJECT_PREFIX"
        "ENVIRONMENT" 
        "AWS_REGION"
    )
    
    # 检查是否设置了必需变量
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            echo "❌ 缺少必需的环境变量: $var"
            validation_errors=$((validation_errors + 1))
        else
            print_debug "✓ 环境变量已设置: $var='${!var}'"
        fi
    done
    
    return $validation_errors
}

validate_configuration_values() {
    local validation_errors=0
    
    print_step "验证配置值..."
    
    # 验证项目前缀
    if [[ -n "${PROJECT_PREFIX:-}" ]]; then
        if ! validate_project_prefix "$PROJECT_PREFIX" "PROJECT_PREFIX"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # 验证环境
    if [[ -n "${ENVIRONMENT:-}" ]]; then
        if ! validate_environment "$ENVIRONMENT" "ENVIRONMENT"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # 验证AWS区域
    if [[ -n "${AWS_REGION:-}" ]]; then
        if ! validate_aws_region "$AWS_REGION" "AWS_REGION"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # 验证EMR配置（如果存在）
    if [[ -n "${EMR_INSTANCE_TYPE:-}" ]]; then
        if ! validate_emr_instance_type "$EMR_INSTANCE_TYPE" "EMR_INSTANCE_TYPE"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    if [[ -n "${EMR_INSTANCE_COUNT:-}" ]]; then
        if ! validate_positive_integer "$EMR_INSTANCE_COUNT" "EMR_INSTANCE_COUNT" 1 50; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    # 验证S3配置（如果存在）
    if [[ -n "${S3_VERSIONING:-}" ]]; then
        if ! validate_boolean "$S3_VERSIONING" "S3_VERSIONING"; then
            validation_errors=$((validation_errors + 1))
        fi
    fi
    
    return $validation_errors
}

validate_aws_permissions() {
    print_step "验证AWS权限..."
    
    local validation_errors=0
    local required_permissions=(
        "sts:GetCallerIdentity"
        "cloudformation:DescribeStacks"
        "s3:ListAllMyBuckets"
        "iam:ListRoles"
    )
    
    # 测试基本AWS连接
    if ! aws sts get-caller-identity &>/dev/null; then
        echo "❌ 无法连接到AWS，请检查凭证配置"
        validation_errors=$((validation_errors + 1))
        return $validation_errors
    fi
    
    print_success "✓ AWS凭证验证通过"
    
    # 测试权限（非详尽列表，但检查关键权限）
    print_debug "测试关键AWS权限..."
    
    # 测试CloudFormation权限
    if ! aws cloudformation describe-stacks &>/dev/null; then
        echo "❌ 缺少CloudFormation权限"
        validation_errors=$((validation_errors + 1))
    else
        print_debug "✓ CloudFormation权限可用"
    fi
    
    # 测试S3权限
    if ! aws s3 ls &>/dev/null; then
        echo "❌ 缺少S3权限"
        validation_errors=$((validation_errors + 1))
    else
        print_debug "✓ S3权限可用"
    fi
    
    return $validation_errors
}

validate_resource_quotas() {
    print_step "检查AWS资源配额..."
    
    local validation_errors=0
    
    # 检查VPC配额
    local vpc_limit
    vpc_limit=$(aws ec2 describe-account-attributes \
        --attribute-names max-instances \
        --query 'AccountAttributes[0].AttributeValues[0].AttributeValue' \
        --output text 2>/dev/null || echo "unknown")
    
    if [[ "$vpc_limit" != "unknown" ]]; then
        print_debug "✓ 账户EC2实例限制: $vpc_limit"
    fi
    
    # 检查CloudFormation堆栈限制
    local stack_count
    stack_count=$(aws cloudformation list-stacks \
        --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
        --query 'length(StackSummaries)' \
        --output text 2>/dev/null || echo "0")
    
    if [[ $stack_count -gt 200 ]]; then
        echo "⚠️  CloudFormation堆栈数量较多($stack_count)，可能影响部署"
    else
        print_debug "✓ CloudFormation堆栈数量: $stack_count"
    fi
    
    return $validation_errors
}

# =============================================================================
# 主验证函数
# =============================================================================

validate_configuration() {
    local config_file="${1:-}"
    local total_errors=0
    
    print_step "开始配置验证..."
    print_info "配置验证器版本: $CONFIG_VALIDATOR_VERSION"
    
    # 如果提供了配置文件，加载它
    if [[ -n "$config_file" && -f "$config_file" ]]; then
        print_info "加载配置文件: $config_file"
        set -a
        source "$config_file"
        set +a
    fi
    
    # 验证必需变量
    if ! validate_required_variables; then
        total_errors=$((total_errors + $?))
    fi
    
    # 验证配置值
    if ! validate_configuration_values; then
        total_errors=$((total_errors + $?))
    fi
    
    # 验证AWS权限
    if ! validate_aws_permissions; then
        total_errors=$((total_errors + $?))
    fi
    
    # 验证资源配额
    if ! validate_resource_quotas; then
        total_errors=$((total_errors + $?))
    fi
    
    # 输出验证结果
    echo
    if [[ $total_errors -eq 0 ]]; then
        print_success "🎉 所有配置验证通过！"
        return 0
    else
        print_error "❌ 发现 $total_errors 个配置错误"
        echo
        echo "请修复上述错误后再次运行验证。"
        echo "参考文档: configs/README.md"
        return 1
    fi
}

# =============================================================================
# 配置建议和修复
# =============================================================================

suggest_configuration_fixes() {
    print_step "配置修复建议..."
    
    # 检查是否存在示例配置文件
    if [[ ! -f "configs/config.env" ]]; then
        echo "💡 建议: 创建基础配置文件"
        echo "   cp configs/config.env.example configs/config.env"
    fi
    
    # 检查是否存在本地配置文件
    if [[ ! -f "configs/config.local.env" ]]; then
        echo "💡 建议: 创建本地配置文件用于个人设置"
        echo "   cp configs/config.env configs/config.local.env"
        echo "   # 然后编辑 configs/config.local.env 进行个人定制"
    fi
    
    # 检查AWS CLI配置
    if [[ ! -f "$HOME/.aws/config" ]]; then
        echo "💡 建议: 配置AWS CLI"
        echo "   aws configure"
        echo "   # 或者设置环境变量:"
        echo "   export AWS_REGION=us-east-1"
        echo "   export AWS_ACCESS_KEY_ID=your_key_id"
        echo "   export AWS_SECRET_ACCESS_KEY=your_secret_key"
    fi
}

generate_sample_config() {
    local output_file="${1:-configs/config.sample.env}"
    
    print_step "生成示例配置文件: $output_file"
    
    cat > "$output_file" << 'EOF'
# =============================================================================
# AWS 数据湖项目配置示例
# 复制此文件为 config.local.env 并根据需要修改
# =============================================================================

# 基础项目设置
PROJECT_PREFIX=dl-handson
ENVIRONMENT=dev
AWS_REGION=us-east-1

# S3 配置
S3_ENCRYPTION=AES256
S3_VERSIONING=Enabled
S3_LIFECYCLE_ENABLED=true

# EMR 配置
EMR_INSTANCE_TYPE=m5.xlarge
EMR_INSTANCE_COUNT=3
EMR_USE_SPOT_INSTANCES=false

# Glue 配置
GLUE_DATABASE_NAME=${PROJECT_PREFIX}-db-${ENVIRONMENT}

# Lake Formation 配置
LAKE_FORMATION_ADMIN_ENABLED=true

# 监控和日志
ENABLE_CLOUDTRAIL=true
ENABLE_COST_MONITORING=true

# 安全配置
ENABLE_BUCKET_NOTIFICATIONS=false
FORCE_SSL=true

# 可选：覆盖默认资源名称
# RAW_BUCKET_NAME=${PROJECT_PREFIX}-raw-${ENVIRONMENT}
# CLEAN_BUCKET_NAME=${PROJECT_PREFIX}-clean-${ENVIRONMENT}
# ANALYTICS_BUCKET_NAME=${PROJECT_PREFIX}-analytics-${ENVIRONMENT}
EOF
    
    print_success "示例配置文件已生成: $output_file"
    echo
    echo "下一步:"
    echo "1. 复制示例文件: cp $output_file configs/config.local.env"
    echo "2. 编辑本地配置: nano configs/config.local.env"
    echo "3. 运行验证: $0 configs/config.local.env"
}

# =============================================================================
# 主函数和CLI界面
# =============================================================================

show_help() {
    cat << EOF
AWS 数据湖配置验证器 v$CONFIG_VALIDATOR_VERSION

用法: $0 [选项] [配置文件]

选项:
    -h, --help              显示此帮助信息
    -g, --generate-sample   生成示例配置文件
    -s, --suggest-fixes     显示配置修复建议
    -v, --verbose           详细输出
    --validate-only         仅验证，不显示建议

参数:
    配置文件               要验证的配置文件路径（可选）

示例:
    $0                                    # 验证当前环境变量
    $0 configs/config.local.env           # 验证指定配置文件
    $0 --generate-sample                  # 生成示例配置文件
    $0 --suggest-fixes                    # 显示修复建议

EOF
}

main() {
    local config_file=""
    local generate_sample=false
    local suggest_fixes=false
    local validate_only=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -g|--generate-sample)
                generate_sample=true
                shift
                ;;
            -s|--suggest-fixes)
                suggest_fixes=true
                shift
                ;;
            -v|--verbose)
                LOG_LEVEL="DEBUG"
                shift
                ;;
            --validate-only)
                validate_only=true
                shift
                ;;
            -*)
                echo "未知选项: $1"
                show_help
                exit 1
                ;;
            *)
                config_file="$1"
                shift
                ;;
        esac
    done
    
    # 执行相应操作
    if [[ "$generate_sample" == true ]]; then
        generate_sample_config
        exit 0
    fi
    
    if [[ "$suggest_fixes" == true ]]; then
        suggest_configuration_fixes
        exit 0
    fi
    
    # 执行配置验证
    if validate_configuration "$config_file"; then
        if [[ "$validate_only" != true ]]; then
            echo
            print_info "配置验证完成，可以开始部署！"
        fi
        exit 0
    else
        if [[ "$validate_only" != true ]]; then
            echo
            suggest_configuration_fixes
        fi
        exit 1
    fi
}

# 如果直接执行此脚本，运行主函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi