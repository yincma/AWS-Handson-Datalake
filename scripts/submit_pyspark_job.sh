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
    EMR_CLUSTER_NAME=${EMR_CLUSTER_NAME:-${PROJECT_PREFIX}-emr-cluster-${ENVIRONMENT}}
    
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
    
    # Get cluster ID and clean any whitespace
    local cluster_id=$(aws emr list-clusters --region "$AWS_REGION" --active \
        --query "Clusters[?Name=='$EMR_CLUSTER_NAME'].Id" --output text 2>/dev/null | tr -d '\n\r\t ' || true)
    
    if [[ -z "$cluster_id" || "$cluster_id" == "None" ]]; then
        print_error "No active EMR cluster found with name: $EMR_CLUSTER_NAME"
        print_info "Please create an EMR cluster first or use a different cluster name."
        print_info "You can list active clusters with: aws emr list-clusters --active --region $AWS_REGION"
        exit 1
    fi
    
    # Validate cluster ID format (should start with j-)
    if [[ ! "$cluster_id" =~ ^j-[A-Z0-9]+$ ]]; then
        print_error "Invalid cluster ID format: '$cluster_id'"
        print_info "Expected format: j-XXXXXXXXX"
        exit 1
    fi
    
    # Check cluster state
    local cluster_state=$(aws emr describe-cluster --cluster-id "$cluster_id" --region "$AWS_REGION" \
        --query "Cluster.Status.State" --output text 2>/dev/null)
    
    if [[ -z "$cluster_state" ]]; then
        print_error "Failed to get cluster state for ID: $cluster_id"
        exit 1
    fi
    
    if [[ "$cluster_state" != "WAITING" ]]; then
        print_error "EMR cluster is not in WAITING state. Current state: $cluster_state"
        print_info "The cluster must be in WAITING state to accept new steps."
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
        print_info "Please ensure the script exists in the scripts directory"
        exit 1
    fi
    
    # Check if analytics bucket exists
    if ! aws s3 ls "s3://${ANALYTICS_BUCKET}/" --region "$AWS_REGION" &>/dev/null; then
        print_error "Analytics bucket does not exist: s3://${ANALYTICS_BUCKET}/"
        print_info "Please ensure the S3 bucket is created first"
        exit 1
    fi
    
    # Upload script to S3
    if aws s3 cp scripts/pyspark_analytics.py "$script_s3_path" --region "$AWS_REGION" &>/dev/null; then
        print_info "Script uploaded to: $script_s3_path"
        
        # Verify upload
        if aws s3 ls "$script_s3_path" --region "$AWS_REGION" &>/dev/null; then
            print_info "Upload verified successfully"
        else
            print_error "Upload verification failed"
            exit 1
        fi
    else
        print_error "Failed to upload script to S3"
        print_info "Check your AWS permissions for S3 access"
        exit 1
    fi
    
    echo "$script_s3_path"
}

# Function to submit PySpark job
submit_pyspark_job() {
    local cluster_id="$1"
    local script_s3_path="$2"
    
    print_step "Submitting PySpark job to EMR cluster..."
    
    # Generate timestamp for unique job naming
    local timestamp=$(date +%Y%m%d-%H%M%S)
    local app_name="DataLakeAnalytics-${timestamp}"
    
    print_info "Job details:"
    print_info "  Script: $script_s3_path"
    print_info "  Clean Bucket: $CLEAN_BUCKET"
    print_info "  Analytics Bucket: $ANALYTICS_BUCKET"
    print_info "  App Name: $app_name"
    
    # Create step configuration with proper JSON formatting
    local step_config=$(cat <<EOF
[
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
                "$script_s3_path",
                "--clean-bucket", "$CLEAN_BUCKET",
                "--analytics-bucket", "$ANALYTICS_BUCKET",
                "--app-name", "$app_name"
            ]
        }
    }
]
EOF
    )
    
    # Submit the step
    local step_id=$(aws emr add-steps --cluster-id "$cluster_id" --region "$AWS_REGION" \
        --steps "$step_config" --query 'StepIds[0]' --output text)
    
    if [[ -z "$step_id" || "$step_id" == "None" ]]; then
        print_error "Failed to submit PySpark job - no step ID returned"
        exit 1
    fi
    
    print_info "PySpark job submitted with step ID: $step_id"
    echo "$step_id"
}

# Function to monitor job progress
monitor_job() {
    local cluster_id="$1"
    local step_id="$2"
    
    print_step "Monitoring job progress..."
    print_info "Cluster ID: $cluster_id"
    print_info "Step ID: $step_id"
    print_info "You can also monitor the job in the AWS EMR console:"
    print_info "https://console.aws.amazon.com/elasticmapreduce/home?region=${AWS_REGION}#cluster-details:${cluster_id}"
    
    local max_attempts=120  # 60 minutes maximum (30s * 120)
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        # Get step state with error handling
        local step_state=$(aws emr describe-step --cluster-id "$cluster_id" --step-id "$step_id" \
            --region "$AWS_REGION" --query "Step.Status.State" --output text 2>/dev/null)
        
        local describe_result=$?
        
        if [[ $describe_result -ne 0 ]]; then
            print_error "Failed to describe step $step_id for cluster $cluster_id"
            print_info "Attempting to check cluster status..."
            
            local cluster_state=$(aws emr describe-cluster --cluster-id "$cluster_id" --region "$AWS_REGION" \
                --query "Cluster.Status.State" --output text 2>/dev/null)
            
            if [[ -n "$cluster_state" ]]; then
                print_info "Cluster state: $cluster_state"
            else
                print_error "Unable to connect to cluster. Check your AWS credentials and cluster ID."
                exit 1
            fi
            
            sleep 30
            ((attempt++))
            continue
        fi
        
        # Clean the step state
        step_state=$(echo "$step_state" | tr -d '\n\r\t ')
        
        case "$step_state" in
            "PENDING")
                print_info "Job status: PENDING - waiting to start... (${attempt}/120)"
                ;;
            "RUNNING")
                print_info "Job status: RUNNING - processing data... (${attempt}/120)"
                ;;
            "COMPLETED")
                print_info "Job status: COMPLETED - job finished successfully!"
                return 0
                ;;
            "FAILED"|"CANCELLED"|"INTERRUPTED")
                print_error "Job status: $step_state"
                
                # Get failure details
                local failure_details=$(aws emr describe-step --cluster-id "$cluster_id" --step-id "$step_id" \
                    --region "$AWS_REGION" --query "Step.Status.FailureDetails" --output text 2>/dev/null || echo "No details available")
                
                local state_change_reason=$(aws emr describe-step --cluster-id "$cluster_id" --step-id "$step_id" \
                    --region "$AWS_REGION" --query "Step.Status.StateChangeReason" --output text 2>/dev/null || echo "No reason available")
                
                print_error "Failure details: $failure_details"
                print_error "State change reason: $state_change_reason"
                print_info "Check EMR cluster logs for more information."
                print_info "Log location: s3://aws-logs-$(aws sts get-caller-identity --query Account --output text)-${AWS_REGION}/elasticmapreduce/"
                exit 1
                ;;
            "")
                print_warning "Empty step status received (attempt ${attempt}/120)"
                ;;
            *)
                print_warning "Unknown job status: '$step_state' (attempt ${attempt}/120)"
                ;;
        esac
        
        sleep 30
        ((attempt++))
    done
    
    print_error "Job monitoring timed out after 60 minutes"
    print_info "The job may still be running. Check the EMR console for updates."
    exit 1
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
    
    # Load and validate configuration
    load_config
    
    # Validate required variables
    if [[ -z "$PROJECT_PREFIX" || -z "$AWS_REGION" || -z "$ENVIRONMENT" ]]; then
        print_error "Missing required configuration variables"
        exit 1
    fi
    
    # Check EMR cluster
    print_info "Looking for EMR cluster: $EMR_CLUSTER_NAME"
    cluster_id=$(check_emr_cluster)
    if [[ -z "$cluster_id" ]]; then
        print_error "Failed to get cluster ID"
        exit 1
    fi
    
    # Upload script
    script_s3_path=$(upload_script_to_s3)
    if [[ -z "$script_s3_path" ]]; then
        print_error "Failed to upload script to S3"
        exit 1
    fi
    
    # Submit job
    step_id=$(submit_pyspark_job "$cluster_id" "$script_s3_path")
    if [[ -z "$step_id" ]]; then
        print_error "Failed to submit PySpark job"
        exit 1
    fi
    
    # Monitor progress
    print_info "Starting job monitoring..."
    if monitor_job "$cluster_id" "$step_id"; then
        # Show results only if job completed successfully
        show_results
        print_info "PySpark analytics job completed successfully!"
    else
        print_error "PySpark job failed or was interrupted"
        exit 1
    fi
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