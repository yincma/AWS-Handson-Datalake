#!/bin/bash

# AWS Data Lake Cleanup Script
# This script removes all resources created for the data lake project
# WARNING: This will delete all data and resources. Use with caution!

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

# Function to check if AWS CLI is configured
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS CLI is not configured or credentials are invalid."
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
    
    print_info "Project: $PROJECT_PREFIX"
    print_info "Region: $AWS_REGION"
    print_info "Environment: $ENVIRONMENT"
}

# Function to confirm deletion
confirm_deletion() {
    echo -e "\n${RED}WARNING: This will delete ALL resources for the data lake project!${NC}"
    echo "The following resources will be removed:"
    echo "  - All S3 buckets and their contents"
    echo "  - EMR clusters"
    echo "  - Glue crawlers, databases, and tables"
    echo "  - Glue DataBrew projects and jobs"
    echo "  - Lake Formation permissions and databases"
    echo "  - IAM roles and policies"
    echo "  - CloudFormation stacks"
    echo ""
    
    read -p "Are you sure you want to continue? Type 'DELETE' to confirm: " confirmation
    
    if [[ "$confirmation" != "DELETE" ]]; then
        print_info "Cleanup cancelled."
        exit 0
    fi
}

# Function to terminate EMR clusters
cleanup_emr_clusters() {
    print_step "Terminating EMR clusters..."
    
    local cluster_name="${PROJECT_PREFIX}-emr-cluster"
    local clusters=$(aws emr list-clusters --region "$AWS_REGION" --active \
        --query "Clusters[?Name=='$cluster_name'].Id" --output text 2>/dev/null || true)
    
    if [[ -n "$clusters" ]]; then
        for cluster_id in $clusters; do
            print_info "Terminating EMR cluster: $cluster_id"
            aws emr terminate-clusters --cluster-ids "$cluster_id" --region "$AWS_REGION"
        done
        
        print_info "Waiting for EMR clusters to terminate..."
        for cluster_id in $clusters; do
            aws emr wait cluster-terminated --cluster-id "$cluster_id" --region "$AWS_REGION"
        done
    else
        print_info "No active EMR clusters found."
    fi
}

# Function to cleanup Glue DataBrew
cleanup_databrew() {
    print_step "Cleaning up Glue DataBrew resources..."
    
    # List and delete jobs
    local jobs=$(aws databrew list-jobs --region "$AWS_REGION" \
        --query "Jobs[?starts_with(Name, '$PROJECT_PREFIX')].Name" --output text 2>/dev/null || true)
    
    if [[ -n "$jobs" ]]; then
        for job in $jobs; do
            print_info "Deleting DataBrew job: $job"
            aws databrew delete-job --name "$job" --region "$AWS_REGION" || true
        done
    fi
    
    # List and delete projects
    local projects=$(aws databrew list-projects --region "$AWS_REGION" \
        --query "Projects[?starts_with(Name, '$PROJECT_PREFIX')].Name" --output text 2>/dev/null || true)
    
    if [[ -n "$projects" ]]; then
        for project in $projects; do
            print_info "Deleting DataBrew project: $project"
            aws databrew delete-project --name "$project" --region "$AWS_REGION" || true
        done
    fi
    
    # List and delete datasets
    local datasets=$(aws databrew list-datasets --region "$AWS_REGION" \
        --query "Datasets[?starts_with(Name, '$PROJECT_PREFIX')].Name" --output text 2>/dev/null || true)
    
    if [[ -n "$datasets" ]]; then
        for dataset in $datasets; do
            print_info "Deleting DataBrew dataset: $dataset"
            aws databrew delete-dataset --name "$dataset" --region "$AWS_REGION" || true
        done
    fi
}

# Function to cleanup Glue resources
cleanup_glue() {
    print_step "Cleaning up Glue resources..."
    
    local database_name="${PROJECT_PREFIX}_db"
    
    # Stop and delete crawlers
    local crawlers=$(aws glue get-crawlers --region "$AWS_REGION" \
        --query "CrawlerList[?starts_with(Name, '$PROJECT_PREFIX')].Name" --output text 2>/dev/null || true)
    
    if [[ -n "$crawlers" ]]; then
        for crawler in $crawlers; do
            print_info "Stopping crawler: $crawler"
            aws glue stop-crawler --name "$crawler" --region "$AWS_REGION" 2>/dev/null || true
            
            print_info "Deleting crawler: $crawler"
            aws glue delete-crawler --name "$crawler" --region "$AWS_REGION" || true
        done
    fi
    
    # Delete tables from database
    local tables=$(aws glue get-tables --database-name "$database_name" --region "$AWS_REGION" \
        --query "TableList[].Name" --output text 2>/dev/null || true)
    
    if [[ -n "$tables" ]]; then
        for table in $tables; do
            print_info "Deleting table: $table"
            aws glue delete-table --database-name "$database_name" --name "$table" --region "$AWS_REGION" || true
        done
    fi
    
    # Delete database
    print_info "Deleting Glue database: $database_name"
    aws glue delete-database --name "$database_name" --region "$AWS_REGION" 2>/dev/null || true
}

# Function to cleanup Lake Formation
cleanup_lake_formation() {
    print_step "Cleaning up Lake Formation resources..."
    
    local database_name="${PROJECT_PREFIX}_db"
    
    # Revoke all permissions
    print_info "Revoking Lake Formation permissions..."
    aws lakeformation batch-revoke-permissions --region "$AWS_REGION" \
        --entries '[
            {
                "Id": "1",
                "Principal": {"DataLakePrincipalIdentifier": "*"},
                "Resource": {"Database": {"Name": "'$database_name'"}},
                "Permissions": ["ALL"]
            }
        ]' 2>/dev/null || true
    
    # Deregister S3 locations
    local raw_bucket="${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
    local clean_bucket="${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
    local analytics_bucket="${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
    
    for bucket in "$raw_bucket" "$clean_bucket" "$analytics_bucket"; do
        print_info "Deregistering S3 location: s3://$bucket"
        aws lakeformation deregister-resource --resource-arn "arn:aws:s3:::$bucket" --region "$AWS_REGION" 2>/dev/null || true
    done
}

# Function to empty and delete S3 buckets
cleanup_s3_buckets() {
    print_step "Cleaning up S3 buckets..."
    
    local buckets=(
        "${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}"
    )
    
    for bucket in "${buckets[@]}"; do
        if aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
            print_info "Emptying S3 bucket: $bucket"
            aws s3 rm "s3://$bucket" --recursive --region "$AWS_REGION" 2>/dev/null || true
            
            # Delete all versions and delete markers
            aws s3api list-object-versions --bucket "$bucket" --region "$AWS_REGION" \
                --query 'Versions[].{Key:Key,VersionId:VersionId}' --output text 2>/dev/null | \
                while read key version; do
                    if [[ -n "$key" && -n "$version" ]]; then
                        aws s3api delete-object --bucket "$bucket" --key "$key" --version-id "$version" --region "$AWS_REGION" 2>/dev/null || true
                    fi
                done
            
            aws s3api list-object-versions --bucket "$bucket" --region "$AWS_REGION" \
                --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' --output text 2>/dev/null | \
                while read key version; do
                    if [[ -n "$key" && -n "$version" ]]; then
                        aws s3api delete-object --bucket "$bucket" --key "$key" --version-id "$version" --region "$AWS_REGION" 2>/dev/null || true
                    fi
                done
            
            print_info "Deleting S3 bucket: $bucket"
            aws s3api delete-bucket --bucket "$bucket" --region "$AWS_REGION" || true
        else
            print_info "S3 bucket $bucket does not exist or not accessible."
        fi
    done
}

# Function to delete CloudFormation stacks
cleanup_cloudformation() {
    print_step "Deleting CloudFormation stacks..."
    
    local stacks=(
        "datalake-lake-formation-${ENVIRONMENT}"
        "datalake-iam-roles-${ENVIRONMENT}"
        "datalake-s3-storage-${ENVIRONMENT}"
    )
    
    # Check for optional cost-monitoring stack
    local cost_stack="datalake-cost-monitoring-${ENVIRONMENT}"
    if aws cloudformation describe-stacks --stack-name "$cost_stack" --region "$AWS_REGION" &>/dev/null; then
        print_info "Found optional cost monitoring stack, adding to deletion list"
        stacks+=("$cost_stack")
    fi
    
    for stack in "${stacks[@]}"; do
        if aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" &>/dev/null; then
            print_info "Deleting CloudFormation stack: $stack"
            aws cloudformation delete-stack --stack-name "$stack" --region "$AWS_REGION"
        else
            print_info "CloudFormation stack $stack does not exist."
        fi
    done
    
    # Wait for stacks to be deleted
    for stack in "${stacks[@]}"; do
        if aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" &>/dev/null; then
            print_info "Waiting for stack deletion: $stack"
            aws cloudformation wait stack-delete-complete --stack-name "$stack" --region "$AWS_REGION" || true
        fi
    done
}

# Function to cleanup CloudWatch logs
cleanup_cloudwatch_logs() {
    print_step "Cleaning up CloudWatch logs..."
    
    local log_groups=$(aws logs describe-log-groups --region "$AWS_REGION" \
        --query "logGroups[?contains(logGroupName, '$PROJECT_PREFIX')].logGroupName" --output text 2>/dev/null || true)
    
    if [[ -n "$log_groups" ]]; then
        for log_group in $log_groups; do
            print_info "Deleting log group: $log_group"
            aws logs delete-log-group --log-group-name "$log_group" --region "$AWS_REGION" || true
        done
    fi
}

# Main cleanup function
main() {
    print_info "Starting AWS Data Lake cleanup process..."
    
    check_aws_cli
    load_config
    confirm_deletion
    
    # Execute cleanup in order
    cleanup_emr_clusters
    cleanup_databrew
    cleanup_glue
    cleanup_lake_formation
    cleanup_s3_buckets
    cleanup_cloudformation
    cleanup_cloudwatch_logs
    
    print_info "Cleanup completed successfully!"
    print_warning "Please verify in the AWS console that all resources have been removed."
    print_warning "Check for any remaining charges in your AWS billing dashboard."
}

# Run the main function
main "$@"