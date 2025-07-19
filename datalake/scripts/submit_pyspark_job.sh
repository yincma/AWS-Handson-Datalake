#!/bin/bash

# EMR PySpark Job Submission Script
# This script submits the PySpark analytics job to an EMR cluster

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
    
    # Set bucket names
    CLEAN_BUCKET="${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
    ANALYTICS_BUCKET="${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
    
    print_info "Project: $PROJECT_PREFIX"
    print_info "Region: $AWS_REGION"
    print_info "Environment: $ENVIRONMENT"
    print_info "EMR Cluster: $EMR_CLUSTER_NAME"
    print_info "Clean Bucket: $CLEAN_BUCKET"
    print_info "Analytics Bucket: $ANALYTICS_BUCKET"
}

# Function to check if EMR cluster exists and is running
check_emr_cluster() {
    print_step "Checking EMR cluster status..."
    
    local cluster_id=$(aws emr list-clusters --region "$AWS_REGION" --active \
        --query "Clusters[?Name=='$EMR_CLUSTER_NAME'].Id" --output text 2>/dev/null || true)
    
    if [[ -z "$cluster_id" ]]; then
        print_error "No active EMR cluster found with name: $EMR_CLUSTER_NAME"
        print_info "Please create an EMR cluster first or use a different cluster name."
        exit 1
    fi
    
    local cluster_state=$(aws emr describe-cluster --cluster-id "$cluster_id" --region "$AWS_REGION" \
        --query "Cluster.Status.State" --output text)
    
    if [[ "$cluster_state" != "WAITING" ]]; then
        print_error "EMR cluster is not in WAITING state. Current state: $cluster_state"
        exit 1
    fi
    
    print_info "EMR cluster found and ready: $cluster_id"
    echo "$cluster_id"
}

# Function to upload PySpark script to S3
upload_script_to_s3() {
    print_step "Uploading PySpark script to S3..."
    
    local script_s3_path="s3://${ANALYTICS_BUCKET}/scripts/pyspark_analytics.py"
    
    # Check if script exists locally
    if [[ ! -f "scripts/pyspark_analytics.py" ]]; then
        print_error "PySpark script not found: scripts/pyspark_analytics.py"
        exit 1
    fi
    
    # Upload script to S3
    aws s3 cp scripts/pyspark_analytics.py "$script_s3_path" --region "$AWS_REGION"
    print_info "Script uploaded to: $script_s3_path"
    
    echo "$script_s3_path"
}

# Function to submit PySpark job
submit_pyspark_job() {
    local cluster_id="$1"
    local script_s3_path="$2"
    
    print_step "Submitting PySpark job to EMR cluster..."
    
    # Submit the step
    local step_id=$(aws emr add-steps --cluster-id "$cluster_id" --region "$AWS_REGION" \
        --steps '[
            {
                "Name": "DataLake Analytics Processing",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "--master", "yarn",
                        "--conf", "spark.sql.adaptive.enabled=true",
                        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
                        "--conf", "spark.sql.parquet.compression.codec=snappy",
                        "--py-files", "'$script_s3_path'",
                        "'$script_s3_path'",
                        "--clean-bucket", "'$CLEAN_BUCKET'",
                        "--analytics-bucket", "'$ANALYTICS_BUCKET'",
                        "--app-name", "DataLakeAnalytics-'$(date +%Y%m%d-%H%M%S)'"
                    ]
                }
            }
        ]' --query 'StepIds[0]' --output text)
    
    print_info "PySpark job submitted with step ID: $step_id"
    echo "$step_id"
}

# Function to monitor job progress
monitor_job() {
    local cluster_id="$1"
    local step_id="$2"
    
    print_step "Monitoring job progress..."
    print_info "You can also monitor the job in the AWS EMR console:"
    print_info "https://console.aws.amazon.com/elasticmapreduce/home?region=${AWS_REGION}#cluster-details:${cluster_id}"
    
    while true; do
        local step_state=$(aws emr describe-step --cluster-id "$cluster_id" --step-id "$step_id" \
            --region "$AWS_REGION" --query "Step.Status.State" --output text)
        
        case "$step_state" in
            "PENDING")
                print_info "Job status: PENDING - waiting to start..."
                ;;
            "RUNNING")
                print_info "Job status: RUNNING - processing data..."
                ;;
            "COMPLETED")
                print_info "Job status: COMPLETED - job finished successfully!"
                break
                ;;
            "FAILED"|"CANCELLED"|"INTERRUPTED")
                print_error "Job status: $step_state"
                
                # Get failure details
                local failure_details=$(aws emr describe-step --cluster-id "$cluster_id" --step-id "$step_id" \
                    --region "$AWS_REGION" --query "Step.Status.FailureDetails" --output text 2>/dev/null || echo "No details available")
                
                print_error "Failure details: $failure_details"
                print_info "Check EMR cluster logs for more information."
                exit 1
                ;;
            *)
                print_warning "Unknown job status: $step_state"
                ;;
        esac
        
        sleep 30
    done
}

# Function to show results
show_results() {
    print_step "Checking analytics results..."
    
    local analytics_path="s3://${ANALYTICS_BUCKET}/analytics/"
    
    print_info "Analytics data written to: $analytics_path"
    print_info "Available datasets:"
    
    # List generated datasets
    aws s3 ls "$analytics_path" --recursive --region "$AWS_REGION" | head -20
    
    print_info "\nTo query the data with Athena, create external tables pointing to these S3 locations."
}

# Main function
main() {
    print_info "Starting PySpark job submission for AWS Data Lake analytics..."
    
    load_config
    
    # Check EMR cluster
    cluster_id=$(check_emr_cluster)
    
    # Upload script
    script_s3_path=$(upload_script_to_s3)
    
    # Submit job
    step_id=$(submit_pyspark_job "$cluster_id" "$script_s3_path")
    
    # Monitor progress
    monitor_job "$cluster_id" "$step_id"
    
    # Show results
    show_results
    
    print_info "PySpark analytics job completed successfully!"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster-name)
            EMR_CLUSTER_NAME="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--cluster-name <cluster-name>] [--help]"
            echo ""
            echo "Options:"
            echo "  --cluster-name  EMR cluster name (default: from config)"
            echo "  --help         Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown argument: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

# Run main function
main "$@"