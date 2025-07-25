#!/bin/bash

# =============================================================================
# EMR Cluster Module
# Version: 1.0.0
# Description: Manage EMR compute cluster for data lake
# =============================================================================

# Load common utility library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly EMR_CLUSTER_MODULE_VERSION="1.0.0"

# Load configuration (if not already loaded)
if [[ -z "${PROJECT_PREFIX:-}" ]]; then
    load_config "$PROJECT_ROOT/configs/config.env"
fi

# =============================================================================
# Module configuration
# =============================================================================

EMR_CLUSTER_NAME="${PROJECT_PREFIX}-emr-cluster-${ENVIRONMENT}"
EMR_LOG_URI="s3://${PROJECT_PREFIX}-raw-${ENVIRONMENT}/logs/emr/"

# Default configuration
DEFAULT_INSTANCE_TYPE="${EMR_INSTANCE_TYPE:-m5.xlarge}"
DEFAULT_INSTANCE_COUNT="${EMR_INSTANCE_COUNT:-3}"
DEFAULT_KEY_NAME="${EMR_KEY_NAME:-}"

# =============================================================================
# Required function implementations
# =============================================================================

emr_cluster_validate() {
    print_info "Validating EMR cluster module configuration"
    
    local validation_errors=0
    
    # Check required environment variables
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "Missing required environment variable: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # Validate EMR permissions
    if ! aws emr list-clusters &>/dev/null; then
        print_error "EMR permission validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Validate EC2 permissions
    if ! aws ec2 describe-instances &>/dev/null; then
        print_error "EC2 permission validation failed"
        validation_errors=$((validation_errors + 1))
    fi
    
    # Check dependency modules
    local dependencies=("s3_storage" "iam_roles" "glue_catalog")
    for dep in "${dependencies[@]}"; do
        local dep_stack_name="${PROJECT_PREFIX}-stack-${dep//_/-}-${ENVIRONMENT}"
        if ! check_stack_exists "$dep_stack_name"; then
            print_error "Dependency module not deployed: $dep"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # Validate instance type
    if ! aws ec2 describe-instance-types --instance-types "$DEFAULT_INSTANCE_TYPE" &>/dev/null; then
        print_warning "Instance type may not be available in current region: $DEFAULT_INSTANCE_TYPE"
    fi
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "EMR cluster module validation passed"
        return 0
    else
        print_error "EMR cluster module validation failed: $validation_errors errors"
        return 1
    fi
}

emr_cluster_deploy() {
    print_info "Deploying EMR cluster module"
    
    # Check if cluster already exists and is running
    if check_emr_cluster_exists && check_emr_cluster_running; then
        print_info "EMR cluster already exists and is running, skipping creation"
        return 0
    fi
    
    # Auto-discover or create EC2 key pair
    local key_name
    key_name=$(discover_or_create_key_pair)
    if [[ -z "$key_name" ]]; then
        print_error "Unable to get EC2 key pair"
        return 1
    fi
    
    # Get default VPC and subnet
    local subnet_id
    subnet_id=$(get_default_subnet)
    if [[ -z "$subnet_id" ]]; then
        print_error "Unable to get default subnet"
        return 1
    fi
    
    # Get IAM roles
    local emr_service_role emr_instance_profile
    emr_service_role=$(get_emr_service_role)
    emr_instance_profile=$(get_emr_instance_profile)
    
    if [[ -z "$emr_service_role" || -z "$emr_instance_profile" ]]; then
        print_error "Unable to get EMR IAM roles"
        return 1
    fi
    
    print_info "Creating EMR cluster: $EMR_CLUSTER_NAME"
    print_info "  Instance type: $DEFAULT_INSTANCE_TYPE"
    print_info "  Instance count: $DEFAULT_INSTANCE_COUNT"
    print_info "  Key pair: $key_name"
    print_info "  Subnet: $subnet_id"
    print_debug "  Service role: $emr_service_role"
    print_debug "  Instance profile: $emr_instance_profile"
    print_debug "  Log URI: $EMR_LOG_URI"
    
    # Create EMR cluster
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
        print_success "EMR cluster creation request submitted: $cluster_id"
        
        # Wait for cluster to be ready
        print_info "Waiting for EMR cluster startup..."
        if wait_for_emr_cluster "$cluster_id"; then
            print_success "EMR cluster deployment successful: $cluster_id"
            
            # Save cluster ID to temporary file
            echo "$cluster_id" > "/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
            
            return 0
        else
            print_error "EMR cluster startup failed"
            return 1
        fi
    else
        print_error "EMR cluster creation failed: $create_output"
        return 1
    fi
}

discover_or_create_key_pair() {
    local key_name="$DEFAULT_KEY_NAME"
    
    # If key name is specified, use it directly
    if [[ -n "$key_name" ]]; then
        if aws ec2 describe-key-pairs --key-names "$key_name" &>/dev/null; then
            echo "$key_name"
            return 0
        else
            print_error "Specified key pair does not exist: $key_name"
            return 1
        fi
    fi
    
    # Auto-discover existing key pairs
    local existing_keys
    existing_keys=$(aws ec2 describe-key-pairs --query 'KeyPairs[0].KeyName' --output text 2>/dev/null)
    
    if [[ -n "$existing_keys" && "$existing_keys" != "None" ]]; then
        echo "$existing_keys"
        return 0
    fi
    
    # Create new key pair
    key_name="${PROJECT_PREFIX}-emr-key-${ENVIRONMENT}"
    print_info "Creating new EC2 key pair: $key_name"
    
    local key_material
    key_material=$(aws ec2 create-key-pair --key-name "$key_name" --query 'KeyMaterial' --output text 2>/dev/null)
    
    if [[ -n "$key_material" ]]; then
        # Save private key to secure location
        local key_file="${HOME}/.ssh/${key_name}.pem"
        echo "$key_material" > "$key_file"
        chmod 400 "$key_file"
        
        print_success "Key pair created successfully: $key_name"
        print_info "Private key saved to: $key_file"
        
        echo "$key_name"
        return 0
    else
        print_error "Failed to create key pair"
        return 1
    fi
}

get_default_subnet() {
    # Get first available subnet of default VPC
    aws ec2 describe-subnets \
        --filters "Name=default-for-az,Values=true" \
        --query 'Subnets[0].SubnetId' \
        --output text 2>/dev/null
}

get_emr_service_role() {
    # Get EMR service role from IAM stack
    local iam_stack_name="${PROJECT_PREFIX}-stack-iam-${ENVIRONMENT}"
    aws cloudformation describe-stacks \
        --stack-name "$iam_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`EMRServiceRoleArn`].OutputValue' \
        --output text 2>/dev/null | sed 's|arn:aws:iam::[0-9]*:role/||'
}

get_emr_instance_profile() {
    # Get EMR instance profile from IAM stack
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
    local timeout=3600  # 1 hour timeout
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
                print_error "EMR cluster terminated: $state"
                return 1
                ;;
            *)
                local elapsed=$(($(date +%s) - start_time))
                if [[ $elapsed -gt $timeout ]]; then
                    print_error "EMR cluster wait timeout"
                    return 1
                fi
                
                print_info "EMR cluster status: $state (waited: ${elapsed}s)"
                sleep 30
                ;;
        esac
    done
}

emr_cluster_status() {
    print_info "Checking EMR cluster module status"
    
    local cluster_id_file="/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
    
    if [[ -f "$cluster_id_file" ]]; then
        local cluster_id
        cluster_id=$(cat "$cluster_id_file")
        
        local state
        state=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.Status.State' --output text 2>/dev/null)
        
        case "$state" in
            RUNNING|WAITING)
                print_success "EMR cluster running normally: $state ($cluster_id)"
                
                # Display cluster information
                local master_dns
                master_dns=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.MasterPublicDnsName' --output text 2>/dev/null)
                if [[ -n "$master_dns" && "$master_dns" != "None" ]]; then
                    print_debug "✓ Master node DNS: $master_dns"
                fi
                
                return 0
                ;;
            STARTING|BOOTSTRAPPING)
                print_warning "EMR cluster starting: $state ($cluster_id)"
                return 1
                ;;
            TERMINATED|TERMINATED_WITH_ERRORS)
                print_error "EMR cluster terminated: $state ($cluster_id)"
                rm -f "$cluster_id_file"
                return 1
                ;;
            *)
                print_warning "EMR cluster status unknown: $state ($cluster_id)"
                return 1
                ;;
        esac
    else
        print_warning "EMR cluster not deployed"
        return 1
    fi
}

emr_cluster_cleanup() {
    print_info "Cleaning up EMR cluster module resources"
    
    local cluster_id_file="/tmp/emr_cluster_id_${PROJECT_PREFIX}_${ENVIRONMENT}"
    
    if [[ -f "$cluster_id_file" ]]; then
        local cluster_id
        cluster_id=$(cat "$cluster_id_file")
        
        print_info "Terminating EMR cluster: $cluster_id"
        
        if aws emr terminate-clusters --cluster-ids "$cluster_id"; then
            print_info "Waiting for EMR cluster termination..."
            
            # Wait for cluster termination
            local timeout=1800  # 30 minute timeout
            local start_time
            start_time=$(date +%s)
            
            while true; do
                local state
                state=$(aws emr describe-cluster --cluster-id "$cluster_id" --query 'Cluster.Status.State' --output text 2>/dev/null)
                
                if [[ "$state" == "TERMINATED" || "$state" == "TERMINATED_WITH_ERRORS" ]]; then
                    rm -f "$cluster_id_file"
                    print_success "EMR cluster cleanup successful"
                    return 0
                fi
                
                local elapsed=$(($(date +%s) - start_time))
                if [[ $elapsed -gt $timeout ]]; then
                    print_warning "EMR cluster termination timeout, but continuing cleanup"
                    rm -f "$cluster_id_file"
                    return 0
                fi
                
                print_debug "EMR cluster status: $state (waited: ${elapsed}s)"
                sleep 30
            done
        else
            print_error "Failed to terminate EMR cluster"
            return 1
        fi
    else
        print_info "EMR cluster not deployed, no cleanup needed"
        return 0
    fi
}

emr_cluster_rollback() {
    print_info "Rolling back EMR cluster module changes"
    
    # EMR cluster rollback is cluster deletion
    emr_cluster_cleanup
}

# =============================================================================
# If this script is executed directly
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # Load module interface
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # Execute passed operation
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "emr_cluster" "${@:2}"
    else
        echo "Usage: $0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback"
    fi
fi