#!/bin/bash

# =============================================================================
# 安全配置管理模块
# 版本: 1.0.0
# 描述: 管理敏感配置的加密存储和访问
# =============================================================================

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 加载通用工具库
source "$SCRIPT_DIR/../lib/common.sh"

readonly SECURE_CONFIG_VERSION="1.0.0"

# =============================================================================
# 配置变量
# =============================================================================

KMS_KEY_ALIAS="alias/datalake-${PROJECT_PREFIX}"
PARAMETER_STORE_PREFIX="/datalake/${PROJECT_PREFIX}"
SECURE_VAULT_DIR="${HOME}/.datalake/vault"

# =============================================================================
# KMS密钥管理
# =============================================================================

create_kms_key() {
    print_step "创建或获取KMS加密密钥"
    
    # 检查是否存在密钥别名
    local key_id
    key_id=$(aws kms describe-key --key-id "$KMS_KEY_ALIAS" --query 'KeyMetadata.KeyId' --output text 2>/dev/null)
    
    if [[ -n "$key_id" && "$key_id" != "None" ]]; then
        print_success "KMS密钥已存在: $KMS_KEY_ALIAS ($key_id)"
        echo "$key_id"
        return 0
    fi
    
    print_info "创建新的KMS密钥..."
    
    # 创建新密钥
    local policy_document
    policy_document=$(cat << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Enable IAM root permissions",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"
            },
            "Action": "kms:*",
            "Resource": "*"
        },
        {
            "Sid": "Allow DataLake services",
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "ssm.amazonaws.com",
                    "s3.amazonaws.com",
                    "glue.amazonaws.com"
                ]
            },
            "Action": [
                "kms:Decrypt",
                "kms:GenerateDataKey"
            ],
            "Resource": "*"
        }
    ]
}
EOF
    )
    
    # 替换账户ID
    policy_document=$(echo "$policy_document" | sed "s/\${AWS_ACCOUNT_ID}/$AWS_ACCOUNT_ID/g")
    
    key_id=$(aws kms create-key \
        --description "DataLake ${PROJECT_PREFIX} 加密密钥" \
        --key-usage ENCRYPT_DECRYPT \
        --key-spec SYMMETRIC_DEFAULT \
        --policy "$policy_document" \
        --tags TagKey=Project,TagValue="$PROJECT_PREFIX" TagKey=Environment,TagValue="$ENVIRONMENT" \
        --query 'KeyMetadata.KeyId' \
        --output text)
    
    if [[ -n "$key_id" ]]; then
        # 创建别名
        if aws kms create-alias --alias-name "$KMS_KEY_ALIAS" --target-key-id "$key_id"; then
            print_success "KMS密钥创建成功: $KMS_KEY_ALIAS ($key_id)"
            echo "$key_id"
            return 0
        else
            print_error "创建KMS密钥别名失败"
            return 1
        fi
    else
        print_error "创建KMS密钥失败"
        return 1
    fi
}

rotate_kms_key() {
    print_step "轮换KMS密钥"
    
    local key_id
    key_id=$(get_kms_key_id)
    
    if [[ -z "$key_id" ]]; then
        print_error "KMS密钥不存在"
        return 1
    fi
    
    # 启用自动轮换
    if aws kms enable-key-rotation --key-id "$key_id"; then
        print_success "KMS密钥自动轮换已启用"
        
        # 手动轮换（如果支持）
        if aws kms rotate-key-on-demand --key-id "$key_id" &>/dev/null; then
            print_success "KMS密钥手动轮换完成"
        else
            print_info "手动轮换不可用，将使用自动轮换"
        fi
        
        return 0
    else
        print_error "启用KMS密钥轮换失败"
        return 1
    fi
}

get_kms_key_id() {
    aws kms describe-key --key-id "$KMS_KEY_ALIAS" --query 'KeyMetadata.KeyId' --output text 2>/dev/null
}

# =============================================================================
# Parameter Store管理
# =============================================================================

store_secure_parameter() {
    local param_name="$1"
    local param_value="$2"
    local param_description="$3"
    local param_type="${4:-SecureString}"
    
    if [[ -z "$param_name" || -z "$param_value" ]]; then
        print_error "用法: store_secure_parameter <name> <value> [description] [type]"
        return 1
    fi
    
    local full_param_name="${PARAMETER_STORE_PREFIX}/${param_name}"
    local key_id
    key_id=$(get_kms_key_id)
    
    if [[ -z "$key_id" ]]; then
        print_error "KMS密钥不可用"
        return 1
    fi
    
    print_info "存储安全参数: $full_param_name"
    
    local put_params=(
        --name "$full_param_name"
        --value "$param_value"
        --type "$param_type"
        --key-id "$key_id"
        --overwrite
    )
    
    if [[ -n "$param_description" ]]; then
        put_params+=(--description "$param_description")
    fi
    
    # 添加标签
    put_params+=(--tags "Key=Project,Value=$PROJECT_PREFIX" "Key=Environment,Value=$ENVIRONMENT")
    
    if aws ssm put-parameter "${put_params[@]}"; then
        print_success "安全参数存储成功: $param_name"
        return 0
    else
        print_error "安全参数存储失败: $param_name"
        return 1
    fi
}

retrieve_secure_parameter() {
    local param_name="$1"
    local with_decryption="${2:-true}"
    
    if [[ -z "$param_name" ]]; then
        print_error "用法: retrieve_secure_parameter <name> [with_decryption]"
        return 1
    fi
    
    local full_param_name="${PARAMETER_STORE_PREFIX}/${param_name}"
    
    local get_params=(
        --name "$full_param_name"
        --query 'Parameter.Value'
        --output text
    )
    
    if [[ "$with_decryption" == true ]]; then
        get_params+=(--with-decryption)
    fi
    
    aws ssm get-parameter "${get_params[@]}" 2>/dev/null
}

list_secure_parameters() {
    print_info "列出安全参数"
    
    aws ssm describe-parameters \
        --filters "Key=Name,Option=BeginsWith,Values=$PARAMETER_STORE_PREFIX" \
        --query 'Parameters[*].[Name,Type,LastModifiedDate,Description]' \
        --output table
}

delete_secure_parameter() {
    local param_name="$1"
    
    if [[ -z "$param_name" ]]; then
        print_error "用法: delete_secure_parameter <name>"
        return 1
    fi
    
    local full_param_name="${PARAMETER_STORE_PREFIX}/${param_name}"
    
    if aws ssm delete-parameter --name "$full_param_name"; then
        print_success "安全参数删除成功: $param_name"
        return 0
    else
        print_error "安全参数删除失败: $param_name"
        return 1
    fi
}

# =============================================================================
# 配置文件加密
# =============================================================================

encrypt_config_file() {
    local config_file="$1"
    local encrypted_file="$2"
    
    if [[ -z "$config_file" || -z "$encrypted_file" ]]; then
        print_error "用法: encrypt_config_file <config_file> <encrypted_file>"
        return 1
    fi
    
    if [[ ! -f "$config_file" ]]; then
        print_error "配置文件不存在: $config_file"
        return 1
    fi
    
    local key_id
    key_id=$(get_kms_key_id)
    
    if [[ -z "$key_id" ]]; then
        print_error "KMS密钥不可用"
        return 1
    fi
    
    print_info "加密配置文件: $config_file -> $encrypted_file"
    
    # 读取配置文件内容
    local config_content
    config_content=$(cat "$config_file")
    
    # 使用KMS加密
    local encrypted_data
    encrypted_data=$(aws kms encrypt \
        --key-id "$key_id" \
        --plaintext "$config_content" \
        --query 'CiphertextBlob' \
        --output text)
    
    if [[ -n "$encrypted_data" ]]; then
        # 创建加密文件的元数据
        local metadata
        metadata=$(cat << EOF
{
    "version": "$SECURE_CONFIG_VERSION",
    "encrypted_at": "$(date -Iseconds)",
    "key_id": "$key_id",
    "original_file": "$(basename "$config_file")",
    "encrypted_data": "$encrypted_data"
}
EOF
        )
        
        # 写入加密文件
        echo "$metadata" > "$encrypted_file"
        
        # 设置严格权限
        chmod 600 "$encrypted_file"
        
        print_success "配置文件加密完成: $encrypted_file"
        return 0
    else
        print_error "配置文件加密失败"
        return 1
    fi
}

decrypt_config_file() {
    local encrypted_file="$1"
    local output_file="$2"
    
    if [[ -z "$encrypted_file" ]]; then
        print_error "用法: decrypt_config_file <encrypted_file> [output_file]"
        return 1
    fi
    
    if [[ ! -f "$encrypted_file" ]]; then
        print_error "加密文件不存在: $encrypted_file"
        return 1
    fi
    
    print_info "解密配置文件: $encrypted_file"
    
    # 读取加密文件
    local encrypted_data
    encrypted_data=$(jq -r '.encrypted_data' "$encrypted_file" 2>/dev/null)
    
    if [[ -z "$encrypted_data" || "$encrypted_data" == "null" ]]; then
        print_error "加密文件格式无效"
        return 1
    fi
    
    # 使用KMS解密
    local decrypted_content
    decrypted_content=$(aws kms decrypt \
        --ciphertext-blob "$encrypted_data" \
        --query 'Plaintext' \
        --output text | base64 --decode)
    
    if [[ -n "$decrypted_content" ]]; then
        if [[ -n "$output_file" ]]; then
            echo "$decrypted_content" > "$output_file"
            chmod 600 "$output_file"
            print_success "配置文件解密完成: $output_file"
        else
            echo "$decrypted_content"
        fi
        return 0
    else
        print_error "配置文件解密失败"
        return 1
    fi
}

# =============================================================================
# 安全密钥管理
# =============================================================================

setup_secure_vault() {
    print_step "设置安全密钥保管库"
    
    # 创建安全目录
    mkdir -p "$SECURE_VAULT_DIR"
    chmod 700 "$SECURE_VAULT_DIR"
    
    # 创建.gitignore文件确保不被提交
    echo "*" > "$SECURE_VAULT_DIR/.gitignore"
    chmod 600 "$SECURE_VAULT_DIR/.gitignore"
    
    print_success "安全保管库设置完成: $SECURE_VAULT_DIR"
}

store_ec2_key_securely() {
    local key_name="$1"
    local key_content="$2"
    
    if [[ -z "$key_name" ]]; then
        print_error "用法: store_ec2_key_securely <key_name> [key_content]"
        return 1
    fi
    
    setup_secure_vault
    
    # 如果没有提供密钥内容，从AWS获取或创建
    if [[ -z "$key_content" ]]; then
        print_info "创建新的EC2密钥对: $key_name"
        
        key_content=$(aws ec2 create-key-pair \
            --key-name "$key_name" \
            --query 'KeyMaterial' \
            --output text 2>/dev/null)
        
        if [[ -z "$key_content" ]]; then
            print_error "创建EC2密钥对失败"
            return 1
        fi
    fi
    
    # 将密钥存储到Parameter Store
    if store_secure_parameter "ec2-keys/${key_name}" "$key_content" "EC2密钥对: $key_name"; then
        print_success "EC2密钥安全存储完成: $key_name"
        
        # 可选：也保存到本地保管库（双重备份）
        local vault_key_file="$SECURE_VAULT_DIR/${key_name}.pem"
        echo "$key_content" > "$vault_key_file"
        chmod 400 "$vault_key_file"
        
        print_info "密钥也已备份到本地保管库: $vault_key_file"
        return 0
    else
        print_error "EC2密钥安全存储失败"
        return 1
    fi
}

retrieve_ec2_key() {
    local key_name="$1"
    local output_file="$2"
    
    if [[ -z "$key_name" ]]; then
        print_error "用法: retrieve_ec2_key <key_name> [output_file]"
        return 1
    fi
    
    local key_content
    key_content=$(retrieve_secure_parameter "ec2-keys/${key_name}")
    
    if [[ -n "$key_content" ]]; then
        if [[ -n "$output_file" ]]; then
            echo "$key_content" > "$output_file"
            chmod 400 "$output_file"
            print_success "EC2密钥已保存到: $output_file"
        else
            echo "$key_content"
        fi
        return 0
    else
        print_error "无法检索EC2密钥: $key_name"
        return 1
    fi
}

# =============================================================================
# 配置审计
# =============================================================================

audit_secure_configurations() {
    print_step "审计安全配置"
    
    local audit_errors=0
    
    # 检查KMS密钥
    print_info "检查KMS密钥..."
    local key_id
    key_id=$(get_kms_key_id)
    
    if [[ -n "$key_id" ]]; then
        print_success "✓ KMS密钥存在: $key_id"
        
        # 检查密钥轮换
        local rotation_enabled
        rotation_enabled=$(aws kms get-key-rotation-status --key-id "$key_id" --query 'KeyRotationEnabled' --output text)
        
        if [[ "$rotation_enabled" == "true" ]]; then
            print_success "✓ KMS密钥轮换已启用"
        else
            print_warning "⚠ KMS密钥轮换未启用"
            audit_errors=$((audit_errors + 1))
        fi
    else
        print_error "✗ KMS密钥不存在"
        audit_errors=$((audit_errors + 1))
    fi
    
    # 检查Parameter Store参数
    print_info "检查Parameter Store参数..."
    local param_count
    param_count=$(aws ssm describe-parameters \
        --filters "Key=Name,Option=BeginsWith,Values=$PARAMETER_STORE_PREFIX" \
        --query 'length(Parameters)' \
        --output text)
    
    print_info "发现 $param_count 个安全参数"
    
    # 检查保管库权限
    print_info "检查安全保管库..."
    if [[ -d "$SECURE_VAULT_DIR" ]]; then
        local vault_perms
        vault_perms=$(stat -f "%A" "$SECURE_VAULT_DIR" 2>/dev/null || stat -c "%a" "$SECURE_VAULT_DIR" 2>/dev/null)
        
        if [[ "$vault_perms" == "700" ]]; then
            print_success "✓ 保管库权限正确: $vault_perms"
        else
            print_error "✗ 保管库权限不安全: $vault_perms (应为700)"
            audit_errors=$((audit_errors + 1))
        fi
    else
        print_warning "⚠ 安全保管库不存在"
    fi
    
    # 输出审计结果
    echo
    if [[ $audit_errors -eq 0 ]]; then
        print_success "安全配置审计通过"
        return 0
    else
        print_error "安全配置审计发现 $audit_errors 个问题"
        return 1
    fi
}

# =============================================================================
# 主函数和CLI
# =============================================================================

show_help() {
    cat << EOF
安全配置管理器 v$SECURE_CONFIG_VERSION

用法: $0 <command> [options]

命令:
    init                          初始化安全基础设施
    create-key                    创建KMS加密密钥
    rotate-key                    轮换KMS密钥
    store <name> <value>          存储安全参数
    retrieve <name>               检索安全参数
    list                          列出所有安全参数
    delete <name>                 删除安全参数
    encrypt-file <input> <output> 加密配置文件
    decrypt-file <input> [output] 解密配置文件
    store-ec2-key <name>          安全存储EC2密钥对
    get-ec2-key <name> [output]   检索EC2密钥对
    audit                         审计安全配置
    cleanup                       清理安全资源

示例:
    $0 init                                     # 初始化
    $0 store db-password "secret123"            # 存储密码
    $0 retrieve db-password                     # 检索密码
    $0 encrypt-file config.env config.enc       # 加密配置
    $0 decrypt-file config.enc config.env       # 解密配置
    $0 store-ec2-key myproject-key              # 存储EC2密钥
    $0 audit                                    # 审计安全配置

EOF
}

main() {
    local command="${1:-}"
    
    if [[ -z "$command" ]]; then
        show_help
        exit 1
    fi
    
    # 加载配置
    load_config
    
    case "$command" in
        init)
            setup_secure_vault
            create_kms_key >/dev/null
            print_success "安全基础设施初始化完成"
            ;;
        create-key)
            create_kms_key
            ;;
        rotate-key)
            rotate_kms_key
            ;;
        store)
            if [[ $# -lt 3 ]]; then
                print_error "用法: $0 store <name> <value> [description]"
                exit 1
            fi
            store_secure_parameter "$2" "$3" "$4"
            ;;
        retrieve)
            if [[ $# -lt 2 ]]; then
                print_error "用法: $0 retrieve <name>"
                exit 1
            fi
            retrieve_secure_parameter "$2"
            ;;
        list)
            list_secure_parameters
            ;;
        delete)
            if [[ $# -lt 2 ]]; then
                print_error "用法: $0 delete <name>"
                exit 1
            fi
            delete_secure_parameter "$2"
            ;;
        encrypt-file)
            if [[ $# -lt 3 ]]; then
                print_error "用法: $0 encrypt-file <input> <output>"
                exit 1
            fi
            encrypt_config_file "$2" "$3"
            ;;
        decrypt-file)
            if [[ $# -lt 2 ]]; then
                print_error "用法: $0 decrypt-file <input> [output]"
                exit 1
            fi
            decrypt_config_file "$2" "$3"
            ;;
        store-ec2-key)
            if [[ $# -lt 2 ]]; then
                print_error "用法: $0 store-ec2-key <name>"
                exit 1
            fi
            store_ec2_key_securely "$2"
            ;;
        get-ec2-key)
            if [[ $# -lt 2 ]]; then
                print_error "用法: $0 get-ec2-key <name> [output]"
                exit 1
            fi
            retrieve_ec2_key "$2" "$3"
            ;;
        audit)
            audit_secure_configurations
            ;;
        cleanup)
            print_warning "这将删除所有安全配置！"
            if confirm_action "确定要清理安全资源吗？" "n"; then
                # 清理Parameter Store参数
                local params
                params=$(aws ssm describe-parameters \
                    --filters "Key=Name,Option=BeginsWith,Values=$PARAMETER_STORE_PREFIX" \
                    --query 'Parameters[].Name' \
                    --output text)
                
                for param in $params; do
                    aws ssm delete-parameter --name "$param" && \
                    print_info "删除参数: $param"
                done
                
                # 清理本地保管库
                if [[ -d "$SECURE_VAULT_DIR" ]]; then
                    rm -rf "$SECURE_VAULT_DIR"
                    print_info "删除本地保管库: $SECURE_VAULT_DIR"
                fi
                
                print_success "安全资源清理完成"
            fi
            ;;
        -h|--help)
            show_help
            ;;
        *)
            print_error "未知命令: $command"
            show_help
            exit 1
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi