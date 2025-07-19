#!/bin/bash

# AWS Data Lake EMR Cluster Creation Script
# This script creates an EMR cluster for processing data lake workloads

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "\n${BLUE}[STEP]${NC} $1"
}

print_header() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

# Default values
USE_SPOT=false
KEY_NAME=""
SUBNET_ID=""
INSTANCE_TYPE=""
INSTANCE_COUNT=""
AUTO_TERMINATE=false

# Function to show usage
show_usage() {
    cat << EOF
AWS Data Lake EMR Cluster Creation Script

Usage: $0 [OPTIONS]

Options:
    --key-name KEY        EC2 key pair name for SSH access (required)
    --subnet-id SUBNET    Subnet ID for EMR cluster (required)
    --instance-type TYPE  Instance type (default: from config)
    --instance-count N    Number of instances (default: from config)
    --use-spot           Use spot instances for core nodes
    --auto-terminate     Auto-terminate cluster when idle
    --help, -h           Show this help message

Examples:
    $0 --key-name my-key --subnet-id subnet-12345
    $0 --key-name my-key --subnet-id subnet-12345 --use-spot --auto-terminate
    $0 --key-name my-key --subnet-id subnet-12345 --instance-type m5.2xlarge --instance-count 5

EOF
}

# Function to parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --key-name)
                KEY_NAME="$2"
                shift 2
                ;;
            --subnet-id)
                SUBNET_ID="$2"
                shift 2
                ;;
            --instance-type)
                INSTANCE_TYPE="$2"
                shift 2
                ;;
            --instance-count)
                INSTANCE_COUNT="$2"
                shift 2
                ;;
            --use-spot)
                USE_SPOT=true
                shift
                ;;
            --auto-terminate)
                AUTO_TERMINATE=true
                shift
                ;;
            --help|-h)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown argument: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Validate required arguments
    if [[ -z "$KEY_NAME" ]]; then
        print_error "EC2 key pair name is required (--key-name)"
        exit 1
    fi
    
    if [[ -z "$SUBNET_ID" ]]; then
        print_error "Subnet ID is required (--subnet-id)"
        exit 1
    fi
}

# Function to load configuration
load_config() {
    local config_file=""
    
    if [[ -f "configs/config.local.env" ]]; then
        config_file="configs/config.local.env"
        print_info "Using local configuration: configs/config.local.env"
    elif [[ -f "configs/config.env" ]]; then
        config_file="configs/config.env"
        print_info "Using default configuration: configs/config.env"
    else
        print_error "No configuration file found. Please run setup-env.sh first."
        exit 1
    fi
    
    # Load configuration
    set -a
    source "$config_file"
    set +a
    
    # Set defaults if not configured
    PROJECT_PREFIX=${PROJECT_PREFIX:-dl-handson}
    AWS_REGION=${AWS_REGION:-us-east-1}
    ENVIRONMENT=${ENVIRONMENT:-dev}
    EMR_CLUSTER_NAME=${EMR_CLUSTER_NAME:-${PROJECT_PREFIX}-emr-cluster}
    EMR_INSTANCE_TYPE=${INSTANCE_TYPE:-${EMR_INSTANCE_TYPE:-m5.xlarge}}
    EMR_INSTANCE_COUNT=${INSTANCE_COUNT:-${EMR_INSTANCE_COUNT:-3}}
    
    # S3 bucket for logs
    EMR_LOG_BUCKET="${PROJECT_PREFIX}-logs-${ENVIRONMENT}"
    
    print_info "Project: $PROJECT_PREFIX"
    print_info "Region: $AWS_REGION"
    print_info "Environment: $ENVIRONMENT"
    print_info "Cluster Name: $EMR_CLUSTER_NAME"
    print_info "Instance Type: $EMR_INSTANCE_TYPE"
    print_info "Instance Count: $EMR_INSTANCE_COUNT"
}

# Function to check if log bucket exists
check_log_bucket() {
    print_step "Checking EMR log bucket..."
    
    if ! aws s3api head-bucket --bucket "$EMR_LOG_BUCKET" --region "$AWS_REGION" 2>/dev/null; then
        print_info "Creating EMR log bucket: $EMR_LOG_BUCKET"
        aws s3api create-bucket \
            --bucket "$EMR_LOG_BUCKET" \
            --region "$AWS_REGION" \
            $(if [[ "$AWS_REGION" != "us-east-1" ]]; then echo "--create-bucket-configuration LocationConstraint=$AWS_REGION"; fi)
        
        # Enable encryption
        aws s3api put-bucket-encryption \
            --bucket "$EMR_LOG_BUCKET" \
            --server-side-encryption-configuration '{
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }]
            }' \
            --region "$AWS_REGION"
    else
        print_info "EMR log bucket already exists: $EMR_LOG_BUCKET"
    fi
}

# Function to check if cluster already exists
check_existing_cluster() {
    print_step "Checking for existing EMR clusters..."
    
    local existing_clusters=$(aws emr list-clusters --region "$AWS_REGION" --active \
        --query "Clusters[?Name=='$EMR_CLUSTER_NAME'].Id" --output text 2>/dev/null || true)
    
    if [[ -n "$existing_clusters" ]]; then
        print_warning "Active EMR cluster already exists with name: $EMR_CLUSTER_NAME"
        print_info "Cluster IDs: $existing_clusters"
        read -p "Do you want to create another cluster? (y/N): " confirm
        if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
            print_info "Cluster creation cancelled."
            exit 0
        fi
        # Append timestamp to cluster name to make it unique
        EMR_CLUSTER_NAME="${EMR_CLUSTER_NAME}-$(date +%Y%m%d-%H%M%S)"
        print_info "Creating cluster with name: $EMR_CLUSTER_NAME"
    fi
}

# Function to create EMR cluster
create_emr_cluster() {
    print_step "Creating EMR cluster..."
    
    # Build instance groups configuration
    local master_config='{
        "Name":"Master",
        "Market":"ON_DEMAND",
        "InstanceRole":"MASTER",
        "InstanceType":"'$EMR_INSTANCE_TYPE'",
        "InstanceCount":1
    }'
    
    local core_config
    if [[ "$USE_SPOT" == "true" ]]; then
        core_config='{
            "Name":"Core",
            "Market":"SPOT",
            "BidPrice":"0.10",
            "InstanceRole":"CORE",
            "InstanceType":"'$EMR_INSTANCE_TYPE'",
            "InstanceCount":'$(($EMR_INSTANCE_COUNT - 1))'
        }'
        print_info "Using Spot instances for Core nodes"
    else
        core_config='{
            "Name":"Core",
            "Market":"ON_DEMAND",
            "InstanceRole":"CORE",
            "InstanceType":"'$EMR_INSTANCE_TYPE'",
            "InstanceCount":'$(($EMR_INSTANCE_COUNT - 1))'
        }'
    fi
    
    # Build auto-termination configuration
    local auto_term_config=""
    if [[ "$AUTO_TERMINATE" == "true" ]]; then
        auto_term_config="--auto-termination-policy '{\"IdleTimeout\":3600}'"
        print_info "Auto-termination enabled (1 hour idle timeout)"
    fi
    
    # Create the cluster
    local cluster_id=$(aws emr create-cluster \
        --name "$EMR_CLUSTER_NAME" \
        --release-label emr-6.15.0 \
        --instance-groups "[$master_config,$core_config]" \
        --applications Name=Spark Name=Hadoop Name=Hive Name=JupyterHub \
        --ec2-attributes '{
            "KeyName":"'$KEY_NAME'",
            "SubnetId":"'$SUBNET_ID'",
            "EmrManagedMasterSecurityGroup":"",
            "EmrManagedSlaveSecurityGroup":"",
            "ServiceAccessSecurityGroup":""
        }' \
        --service-role EMR_DefaultRole \
        --log-uri "s3://$EMR_LOG_BUCKET/emr-logs/" \
        --enable-debugging \
        --configurations '[
            {
                "Classification":"spark-defaults",
                "Properties":{
                    "spark.sql.adaptive.enabled":"true",
                    "spark.sql.adaptive.coalescePartitions.enabled":"true",
                    "spark.sql.adaptive.skewJoin.enabled":"true",
                    "spark.serializer":"org.apache.spark.serializer.KryoSerializer",
                    "spark.dynamicAllocation.enabled":"true"
                }
            },
            {
                "Classification":"spark-env",
                "Configurations":[{
                    "Classification":"export",
                    "Properties":{
                        "PYSPARK_PYTHON":"/usr/bin/python3"
                    }
                }]
            }
        ]' \
        --ebs-root-volume-size 30 \
        --tags "Project=$PROJECT_PREFIX" "Environment=$ENVIRONMENT" "Name=$EMR_CLUSTER_NAME" \
        --region "$AWS_REGION" \
        $auto_term_config \
        --query 'ClusterId' --output text)
    
    if [[ -z "$cluster_id" ]]; then
        print_error "Failed to create EMR cluster"
        exit 1
    fi
    
    print_info "EMR cluster created successfully!"
    print_info "Cluster ID: $cluster_id"
    echo "$cluster_id"
}

# Function to wait for cluster to be ready
wait_for_cluster() {
    local cluster_id="$1"
    
    print_step "Waiting for cluster to be ready..."
    print_info "This may take 10-15 minutes..."
    
    aws emr wait cluster-running --cluster-id "$cluster_id" --region "$AWS_REGION"
    
    print_info "Cluster is ready!"
}

# Function to show cluster information
show_cluster_info() {
    local cluster_id="$1"
    
    print_step "Cluster Information"
    
    # Get cluster details
    local cluster_info=$(aws emr describe-cluster --cluster-id "$cluster_id" --region "$AWS_REGION")
    
    # Extract key information
    local master_dns=$(echo "$cluster_info" | jq -r '.Cluster.MasterPublicDnsName // "Not available yet"')
    local state=$(echo "$cluster_info" | jq -r '.Cluster.Status.State')
    
    cat << EOF

========================================
Cluster Details:
========================================
Cluster ID: $cluster_id
Cluster Name: $EMR_CLUSTER_NAME
State: $state
Master DNS: $master_dns
Region: $AWS_REGION

Useful Commands:
----------------
# SSH to master node (once DNS is available):
ssh -i ~/.ssh/$KEY_NAME.pem hadoop@$master_dns

# Submit PySpark job:
./scripts/submit_pyspark_job.sh --cluster-name $EMR_CLUSTER_NAME

# View cluster in console:
https://console.aws.amazon.com/elasticmapreduce/home?region=${AWS_REGION}#cluster-details:$cluster_id

# Terminate cluster:
aws emr terminate-clusters --cluster-ids $cluster_id --region $AWS_REGION

# Access JupyterHub (once cluster is ready):
https://$master_dns:9443 (use 'jovyan' as username)

Cost Optimization Tips:
----------------------
- This cluster costs approximately \$0.50-1.50 per hour
- Remember to terminate when not in use
- Use auto-termination for automatic shutdown
- Monitor usage in AWS Cost Explorer

EOF
}

# Main function
main() {
    print_header "AWS Data Lake EMR Cluster Creation"
    
    parse_arguments "$@"
    load_config
    check_log_bucket
    check_existing_cluster
    
    # Create cluster
    cluster_id=$(create_emr_cluster)
    
    # Wait for cluster to be ready
    wait_for_cluster "$cluster_id"
    
    # Show cluster information
    show_cluster_info "$cluster_id"
    
    print_header "Cluster Creation Complete!"
}

# Run main function
main "$@"