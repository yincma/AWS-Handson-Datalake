#!/bin/bash

# =============================================================================
# EMR集群模块
# 版本: 1.0.0
# 描述: 管理数据湖的EMR计算集群
# =============================================================================

# 加载通用工具库
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly EMR_CLUSTER_MODULE_VERSION="1.0.0"

# 加载配置（如果尚未加载）
if [[ -z "${PROJECT_PREFIX:-}" ]]; then
    load_config "$PROJECT_ROOT/configs/config.env"
fi

# =============================================================================
# 模块配置
# =============================================================================

EMR_CLUSTER_NAME="${PROJECT_PREFIX}-emr-cluster-${ENVIRONMENT}"
EMR_LOG_URI="s3://${PROJECT_PREFIX}-raw-${ENVIRONMENT}/logs/emr/"

# 默认配置
DEFAULT_INSTANCE_TYPE="${EMR_INSTANCE_TYPE:-m5.xlarge}"
DEFAULT_INSTANCE_COUNT="${EMR_INSTANCE_COUNT:-3}"
DEFAULT_KEY_NAME="${EMR_KEY_NAME:-}"

# =============================================================================
# 必需函数实现
# =============================================================================

emr_cluster_validate() {
    print_info "验证EMR集群模块配置"
    
    local validation_errors=0
    
    # 检查必需的环境变量
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "缺少必需的环境变量: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # 验证EMR权限
    if ! aws emr list-clusters &>/dev/null; then
        print_error "EMR权限验证失败"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 验证EC2权限
    if ! aws ec2 describe-instances &>/dev/null; then
        print_error "EC2权限验证失败"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 检查依赖模块
    local dependencies=("s3_storage" "iam_roles" "glue_catalog")
    for dep in "${dependencies[@]}"; do
        local dep_stack_name="${PROJECT_PREFIX}-stack-${dep//_/-}-${ENVIRONMENT}"
        if ! check_stack_exists "$dep_stack_name"; then
            print_error "依赖的模块未部署: $dep"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # 验证实例类型
    if ! aws ec2 describe-instance-types --instance-types "$DEFAULT_INSTANCE_TYPE" &>/dev/null; then
        print_warning "实例类型可能在当前区域不可用: $DEFAULT_INSTANCE_TYPE"
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "EMR集群模块验证通过"
        return 0
    else
        print_error "EMR集群模块验证失败: $validation_errors 个错误"
        return 1
    fi
}

emr_cluster_deploy() {
    print_info "部署EMR集群模块"
    
    # 检查集群是否已存在且运行中
    if check_emr_cluster_exists && check_emr_cluster_running; then
        print_info "EMR集群已存在且运行中，跳过创建"
        return 0
    fi
    
    # 自动发现或创建EC2密钥对
    local key_name
    key_name=$(discover_or_create_key_pair)
    if [[ -z "$key_name" ]]; then
        print_error "无法获取EC2密钥对"
        return 1
    fi
    
    # 获取默认VPC和子网
    local subnet_id
    subnet_id=$(get_default_subnet)
    if [[ -z "$subnet_id" ]]; then
        print_error "无法获取默认子网"
        return 1
    fi
    
    # 获取IAM角色
    local emr_service_role emr_instance_profile
    emr_service_role=$(get_emr_service_role)
    emr_instance_profile=$(get_emr_instance_profile)
    
    if [[ -z "$emr_service_role" || -z "$emr_instance_profile" ]]; then
        print_error "无法获取EMR IAM角色"
        return 1
    fi
    
    print_info "创建EMR集群: $EMR_CLUSTER_NAME"
    print_info "  实例类型: $DEFAULT_INSTANCE_TYPE"
    print_info "  实例数量: $DEFAULT_INSTANCE_COUNT"
    print_info "  密钥对: $key_name"
    print_info "  子网: $subnet_id"
    print_debug "  服务角色: $emr_service_role"
    print_debug "  实例配置文件: $emr_instance_profile"
    print_debug "  日志URI: $EMR_LOG_URI"
    
    # 创建EMR集群
    local cluster_id
    local create_output
    local create_exit_code
    
    create_output=$(aws emr create-cluster \
        --name "$EMR_CLUSTER_NAME" \
        --release-label emr-6.9.0 \
        --applications Name=Spark Name=Hadoop Name=Hive \
        --instance-type "$DEFAULT_INSTANCE_TYPE" \
        --instance-count "$DEFAULT_INSTANCE_COUNT" \
        --ec2-attributes KeyName="$key_name",SubnetIds="$subnet_id" \
        --use-default-roles \
        --log-uri "$EMR_LOG_URI" \
        --enable-debugging \
        --tags Project="$PROJECT_PREFIX" Environment="$ENVIRONMENT" Purpose="DataLake" \
        --query 'ClusterId' \
        --output text 2>&1)
    create_exit_code=$?
    
    if [[ $create_exit_code -eq 0 && -n "$create_output" && "$create_output" != "None" ]]; then
        cluster_id="$create_output"
        print_success "EMR集群创建请求已提交: $cluster_id"
        
        # 等待集群准备就绪
        print_info "等待EMR集群启动..."
        if wait_for_emr_cluster "$cluster_id"; then
            print_success "EMR集群部署成功: $cluster_id"
            
            # 保存集群ID到临时文件
            echo "$cluster_id" > "/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
            
            return 0
        else
            print_error "EMR集群启动失败"
            return 1
        fi
    else
        print_error "EMR集群创建失败: $create_output"
        return 1
    fi
}

discover_or_create_key_pair() {
    local key_name="$DEFAULT_KEY_NAME"
    
    # 如果指定了密钥名，直接使用
    if [[ -n "$key_name" ]]; then
        if aws ec2 describe-key-pairs --key-names "$key_name" &>/dev/null; then
            echo "$key_name"
            return 0
        else
            print_error "指定的密钥对不存在: $key_name"
            return 1
        fi
    fi
    
    # 自动发现现有密钥对
    local existing_keys
    existing_keys=$(aws ec2 describe-key-pairs --query 'KeyPairs[0].KeyName' --output text 2>/dev/null)
    
    if [[ -n "$existing_keys" && "$existing_keys" != "None" ]]; then
        echo "$existing_keys"
        return 0
    fi
    
    # 创建新的密钥对
    key_name="${PROJECT_PREFIX}-emr-key-${ENVIRONMENT}"
    print_info "创建新的EC2密钥对: $key_name"
    
    local key_material
    key_material=$(aws ec2 create-key-pair --key-name "$key_name" --query 'KeyMaterial' --output text 2>/dev/null)
    
    if [[ -n "$key_material" ]]; then
        # 保存私钥到安全位置
        local key_file="${HOME}/.ssh/${key_name}.pem"
        echo "$key_material" > "$key_file"
        chmod 400 "$key_file"
        
        print_success "密钥对创建成功: $key_name"
        print_info "私钥已保存到: $key_file"
        
        echo "$key_name"
        return 0
    else
        print_error "创建密钥对失败"
        return 1
    fi
}

get_default_subnet() {
    # 获取默认VPC的第一个可用子网
    aws ec2 describe-subnets \
        --filters "Name=default-for-az,Values=true" \
        --query 'Subnets[0].SubnetId' \
        --output text 2>/dev/null
}

get_emr_service_role() {
    # 从IAM堆栈获取EMR服务角色
    local iam_stack_name="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
    aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`EMRServiceRoleArn`].OutputValue' \
        --output text 2>/dev/null | sed 's|arn:aws:iam::[0-9]*:role/||'
}

get_emr_instance_profile() {
    # 从IAM堆栈获取EMR实例配置文件
    local iam_stack_name="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
    aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`EMRInstanceProfileArn`].OutputValue' \
        --output text 2>/dev/null | sed 's|arn:aws:iam::[0-9]*:instance-profile/||'
}

check_emr_cluster_exists() {
    local cluster_id_file="/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
    [[ -f "$cluster_id_file" ]]
}

check_emr_cluster_running() {
    local cluster_id_file="/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
    
    if [[ -f "$cluster_id_file" ]]; then
        local cluster_id
        cluster_id=$(cat "$cluster_id_file")
        
        local state
        state=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.Status.State' --output text 2>/dev/null)
        
        [[ "$state" == "RUNNING" || "$state" == "WAITING" ]]
    else
        return 1
    fi
}

wait_for_emr_cluster() {
    local cluster_id="$1"
    local timeout=3600  # 1小时超时
    local start_time
    start_time=$(date +%s)
    
    while true; do
        local state
        state=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.Status.State' --output text 2>/dev/null)
        
        case "$state" in
            RUNNING|WAITING)
                return 0
                ;;
            TERMINATED|TERMINATED_WITH_ERRORS)
                print_error "EMR集群终止: $state"
                return 1
                ;;
            *)
                local elapsed=$(($(date +%s) - start_time))
                if [[ $elapsed -gt $timeout ]]; then
                    print_error "等待EMR集群超时"
                    return 1
                fi
                
                print_info "EMR集群状态: $state (已等待: ${elapsed}s)"
                sleep 30
                ;;
        esac
    done
}

emr_cluster_status() {
    print_info "检查EMR集群模块状态"
    
    local cluster_id_file="/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
    
    if [[ -f "$cluster_id_file" ]]; then
        local cluster_id
        cluster_id=$(cat "$cluster_id_file")
        
        local state
        state=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.Status.State' --output text 2>/dev/null)
        
        case "$state" in
            RUNNING|WAITING)
                print_success "EMR集群运行正常: $state ($cluster_id)"
                
                # 显示集群信息
                local master_dns
                master_dns=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.MasterPublicDnsName' --output text 2>/dev/null)
                if [[ -n "$master_dns" && "$master_dns" != "None" ]]; then
                    print_debug "✓ 主节点DNS: $master_dns"
                fi
                
                return 0
                ;;
            STARTING|BOOTSTRAPPING)
                print_warning "EMR集群启动中: $state ($cluster_id)"
                return 1
                ;;
            TERMINATED|TERMINATED_WITH_ERRORS)
                print_error "EMR集群已终止: $state ($cluster_id)"
                rm -f "$cluster_id_file"
                return 1
                ;;
            *)
                print_warning "EMR集群状态未知: $state ($cluster_id)"
                return 1
                ;;
        esac
    else
        print_warning "EMR集群未部署"
        return 1
    fi
}

emr_cluster_cleanup() {
    print_info "清理EMR集群模块资源"
    
    local cluster_id_file="/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
    
    if [[ -f "$cluster_id_file" ]]; then
        local cluster_id
        cluster_id=$(cat "$cluster_id_file")
        
        print_info "终止EMR集群: $cluster_id"
        
        if aws emr terminate-clusters --cluster-ids "$cluster_id"; then
            print_info "等待EMR集群终止..."
            
            # 等待集群终止
            local timeout=1800  # 30分钟超时
            local start_time
            start_time=$(date +%s)
            
            while true; do
                local state
                state=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.Status.State' --output text 2>/dev/null)
                
                if [[ "$state" == "TERMINATED" || "$state" == "TERMINATED_WITH_ERRORS" ]]; then
                    rm -f "$cluster_id_file"
                    print_success "EMR集群清理成功"
                    return 0
                fi
                
                local elapsed=$(($(date +%s) - start_time))
                if [[ $elapsed -gt $timeout ]]; then
                    print_warning "等待EMR集群终止超时，但继续清理"
                    rm -f "$cluster_id_file"
                    return 0
                fi
                
                print_debug "EMR集群状态: $state (已等待: ${elapsed}s)"
                sleep 30
            done
        else
            print_error "终止EMR集群失败"
            return 1
        fi
    else
        print_info "EMR集群未部署，无需清理"
        return 0
    fi
}

emr_cluster_rollback() {
    print_info "回滚EMR集群模块更改"
    
    # EMR集群的回滚就是删除集群
    emr_cluster_cleanup
}

# =============================================================================
# 如果直接执行此脚本
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # 加载模块接口
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # 执行传入的操作
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "emr_cluster" "${@:2}"
    else
        echo "用法: $0 <action> [args...]"
        echo "可用操作: validate, deploy, status, cleanup, rollback"
    fi
fi