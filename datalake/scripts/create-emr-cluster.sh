#!/bin/bash

# Create EMR Cluster Script
# This script creates an EMR cluster for data processing

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_step() { echo -e "\n${BLUE}[STEP]${NC} $1"; }

# Default values (can be overridden by environment variables)
KEY_NAME="${EMR_KEY_NAME:-}"
SUBNET_ID="${EMR_SUBNET_ID:-}"
INSTANCE_TYPE="${EMR_INSTANCE_TYPE:-m5.xlarge}"
INSTANCE_COUNT="${EMR_INSTANCE_COUNT:-3}"

# Function to show usage
show_usage() {
    cat << EOF
EMR Cluster Creation Script

Usage: $0 --key-name <key> --subnet-id <subnet> [OPTIONS]

Required:
    --key-name        EC2 key pair name for SSH access
    --subnet-id       VPC subnet ID for the cluster

Optional:
    --instance-type   EC2 instance type (default: m5.xlarge)
    --instance-count  Number of instances (default: 3)
    --help, -h        Show this help message

Environment Variables:
    EMR_KEY_NAME      Default key pair name
    EMR_SUBNET_ID     Default subnet ID
    EMR_INSTANCE_TYPE Default instance type
    EMR_INSTANCE_COUNT Default instance count

Examples:
    # Using command line arguments
    $0 --key-name my-key --subnet-id subnet-12345
    
    # Using environment variables
    export EMR_KEY_NAME=my-key
    export EMR_SUBNET_ID=subnet-12345
    $0

EOF
}

# Parse arguments
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
if [[ -z "$KEY_NAME" || -z "$SUBNET_ID" ]]; then
    print_error "Missing required arguments"
    show_usage
    exit 1
fi

# Load configuration
load_configuration() {
    print_step "Loading configuration..."
    
    if [[ -f "configs/env-vars.sh" ]]; then
        source configs/env-vars.sh
    elif [[ -f "configs/config.env" ]]; then
        source configs/config.env
    else
        print_error "Configuration file not found!"
        exit 1
    fi
    
    # Set defaults
    PROJECT_PREFIX=${PROJECT_PREFIX:-dl-handson}
    AWS_REGION=${AWS_REGION:-us-east-1}
    ENVIRONMENT=${ENVIRONMENT:-dev}
    
    print_info "Project: $PROJECT_PREFIX"
    print_info "Region: $AWS_REGION"
}

# Get EMR service role
get_emr_roles() {
    print_step "Getting EMR roles..."
    
    # Get role ARNs from CloudFormation stack
    local stack_name="datalake-iam-roles-${ENVIRONMENT}"
    
    EMR_ROLE=$(aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`EMRServiceRoleArn`].OutputValue' \
        --output text 2>/dev/null || echo "")
    
    EMR_EC2_ROLE=$(aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`EMREC2InstanceProfileArn`].OutputValue' \
        --output text 2>/dev/null || echo "")
    
    if [[ -z "$EMR_ROLE" || -z "$EMR_EC2_ROLE" ]]; then
        print_warning "Using default EMR roles"
        EMR_ROLE="EMR_DefaultRole"
        EMR_EC2_ROLE="EMR_EC2_DefaultRole"
    else
        print_info "EMR Service Role: $EMR_ROLE"
        print_info "EMR EC2 Role: $EMR_EC2_ROLE"
    fi
}

# Create EMR cluster
create_cluster() {
    print_step "Creating EMR cluster..."
    
    CLUSTER_NAME="${PROJECT_PREFIX}-emr-cluster-${ENVIRONMENT}"
    LOG_URI="s3://${PROJECT_PREFIX}-raw-${ENVIRONMENT}/emr-logs/"
    
    # Create cluster
    CLUSTER_ID=$(aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label "emr-6.10.0" \
        --applications Name=Spark Name=Hadoop Name=Hive \
        --ec2-attributes KeyName="$KEY_NAME",SubnetId="$SUBNET_ID",InstanceProfile="$EMR_EC2_ROLE" \
        --service-role "$EMR_ROLE" \
        --instance-groups \
            InstanceGroupType=MASTER,InstanceCount=1,InstanceType="$INSTANCE_TYPE" \
            InstanceGroupType=CORE,InstanceCount=$((INSTANCE_COUNT-1)),InstanceType="$INSTANCE_TYPE" \
        --log-uri "$LOG_URI" \
        --region "$AWS_REGION" \
        --query 'ClusterId' \
        --output text)
    
    if [[ -z "$CLUSTER_ID" ]]; then
        print_error "Failed to create EMR cluster"
        exit 1
    fi
    
    print_info "EMR Cluster ID: $CLUSTER_ID"
    
    # Wait for cluster to be ready
    print_info "Waiting for cluster to be ready (this may take 5-10 minutes)..."
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID" --region "$AWS_REGION"
    
    # Get cluster details
    MASTER_DNS=$(aws emr describe-cluster \
        --cluster-id "$CLUSTER_ID" \
        --region "$AWS_REGION" \
        --query 'Cluster.MasterPublicDnsName' \
        --output text)
    
    # Save cluster information
    cat > configs/emr-cluster.env << EOF
#!/bin/bash
export EMR_CLUSTER_ID=$CLUSTER_ID
export EMR_MASTER_DNS=$MASTER_DNS
export EMR_CLUSTER_NAME=$CLUSTER_NAME
EOF
    
    print_step "EMR Cluster Created Successfully!"
    cat << EOF

✅ Cluster Information:
   • Cluster ID: $CLUSTER_ID
   • Master DNS: $MASTER_DNS
   • Region: $AWS_REGION

📋 Next Steps:
   1. Submit Spark jobs: ./scripts/submit_pyspark_job.sh
   2. SSH to master: ssh -i ~/.ssh/$KEY_NAME hadoop@$MASTER_DNS
   3. View Spark UI: http://$MASTER_DNS:8088

💰 Remember to terminate the cluster when done:
   aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $AWS_REGION

EOF
}

# Main function
main() {
    echo "=========================================="
    echo "EMR Cluster Creation"
    echo "=========================================="
    
    load_configuration
    get_emr_roles
    create_cluster
}

main