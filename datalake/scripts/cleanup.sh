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
    
    # Auto-discover all buckets related to this project
    print_info "Discovering all S3 buckets for project: $PROJECT_PREFIX"
    local discovered_buckets=$(aws s3api list-buckets \
        --query "Buckets[?contains(Name, '$PROJECT_PREFIX')].Name" --output text 2>/dev/null || true)
    
    local buckets=()
    if [[ -n "$discovered_buckets" && "$discovered_buckets" != "None" ]]; then
        # Add discovered buckets
        for bucket in $discovered_buckets; do
            buckets+=("$bucket")
        done
        print_info "Found ${#buckets[@]} S3 buckets to delete"
    else
        # Fallback to predefined bucket names (only check if they exist)
        print_info "No buckets discovered automatically, checking predefined bucket names"
        local predefined_buckets=(
            "${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
            "${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
            "${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
            "${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}"
            "${PROJECT_PREFIX}-logs-${ENVIRONMENT}"
        )
        
        for bucket in "${predefined_buckets[@]}"; do
            if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
                buckets+=("$bucket")
            fi
        done
        
        if [[ ${#buckets[@]} -eq 0 ]]; then
            print_info "No S3 buckets found to delete"
            return
        else
            print_info "Found ${#buckets[@]} predefined S3 buckets to delete"
        fi
    fi
    
    for bucket in "${buckets[@]}"; do
        if aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
            print_info "Processing S3 bucket: $bucket"
            
            # First, remove all current objects
            print_info "Removing current objects from $bucket"
            aws s3 rm "s3://$bucket" --recursive --region "$AWS_REGION" 2>/dev/null || true
            
            # Check if versioning is enabled
            local versioning_status=$(aws s3api get-bucket-versioning --bucket "$bucket" --region "$AWS_REGION" \
                --query 'Status' --output text 2>/dev/null || echo "None")
            
            if [[ "$versioning_status" == "Enabled" ]]; then
                print_info "Bucket $bucket has versioning enabled, cleaning up all versions..."
                
                # Delete all object versions in batches
                local max_retries=3
                local retry_count=0
                
                while [[ $retry_count -lt $max_retries ]]; do
                    # Get object versions
                    local versions=$(aws s3api list-object-versions --bucket "$bucket" --region "$AWS_REGION" \
                        --query 'Versions[].{Key:Key,VersionId:VersionId}' --output json 2>/dev/null || echo "[]")
                    
                    local delete_markers=$(aws s3api list-object-versions --bucket "$bucket" --region "$AWS_REGION" \
                        --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' --output json 2>/dev/null || echo "[]")
                    
                    # Count total objects to delete
                    local version_count=$(echo "$versions" | jq '. | length' 2>/dev/null || echo "0")
                    local marker_count=$(echo "$delete_markers" | jq '. | length' 2>/dev/null || echo "0")
                    local total_count=$((version_count + marker_count))
                    
                    if [[ $total_count -eq 0 ]]; then
                        print_info "All versions cleaned from bucket $bucket"
                        break
                    fi
                    
                    print_info "Deleting $version_count versions and $marker_count delete markers from $bucket (attempt $((retry_count + 1)))"
                    
                    # Delete object versions in batches
                    if [[ $version_count -gt 0 ]]; then
                        echo "$versions" | jq -r '.[] | "\(.Key) \(.VersionId)"' | while read -r key version_id; do
                            if [[ -n "$key" && -n "$version_id" ]]; then
                                aws s3api delete-object --bucket "$bucket" --key "$key" --version-id "$version_id" --region "$AWS_REGION" 2>/dev/null || true
                            fi
                        done
                    fi
                    
                    # Delete delete markers in batches
                    if [[ $marker_count -gt 0 ]]; then
                        echo "$delete_markers" | jq -r '.[] | "\(.Key) \(.VersionId)"' | while read -r key version_id; do
                            if [[ -n "$key" && -n "$version_id" ]]; then
                                aws s3api delete-object --bucket "$bucket" --key "$key" --version-id "$version_id" --region "$AWS_REGION" 2>/dev/null || true
                            fi
                        done
                    fi
                    
                    retry_count=$((retry_count + 1))
                    sleep 5  # Wait a bit before next attempt
                done
                
                if [[ $retry_count -eq $max_retries ]]; then
                    print_warning "Could not clean all versions from $bucket after $max_retries attempts"
                fi
            fi
            
            # Try to delete the bucket
            print_info "Attempting to delete S3 bucket: $bucket"
            local delete_attempts=0
            local max_delete_attempts=5
            
            while [[ $delete_attempts -lt $max_delete_attempts ]]; do
                if aws s3api delete-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
                    print_info "Successfully deleted bucket: $bucket"
                    break
                else
                    delete_attempts=$((delete_attempts + 1))
                    if [[ $delete_attempts -lt $max_delete_attempts ]]; then
                        print_warning "Failed to delete bucket $bucket, retrying in 10 seconds... (attempt $delete_attempts)"
                        sleep 10
                        
                        # Try to empty again before retry
                        aws s3 rm "s3://$bucket" --recursive --region "$AWS_REGION" 2>/dev/null || true
                    else
                        print_error "Failed to delete bucket $bucket after $max_delete_attempts attempts"
                        print_warning "Please manually delete this bucket in the AWS Console"
                    fi
                fi
            done
        else
            print_info "S3 bucket $bucket does not exist or is not accessible."
        fi
    done
    
    # Final verification
    print_info "Verifying S3 bucket deletions..."
    local remaining_buckets=$(aws s3api list-buckets --region "$AWS_REGION" \
        --query "Buckets[?contains(Name, '$PROJECT_PREFIX')].Name" --output text 2>/dev/null || true)
    
    if [[ -n "$remaining_buckets" ]]; then
        print_warning "Some S3 buckets are still present:"
        for bucket in $remaining_buckets; do
            print_warning "  - $bucket"
        done
        print_warning "Please check these manually in the AWS Console"
    else
        print_info "All S3 buckets have been successfully deleted!"
    fi
}

# Function to delete CloudFormation stacks
cleanup_cloudformation() {
    print_step "Deleting CloudFormation stacks..."
    
    # Auto-discover all stacks related to this project
    print_info "Discovering all CloudFormation stacks for project: $PROJECT_PREFIX"
    local discovered_stacks=$(aws cloudformation list-stacks --region "$AWS_REGION" \
        --query "StackSummaries[?contains(StackName, '$PROJECT_PREFIX')].{Name:StackName,Status:StackStatus}" \
        --output text 2>/dev/null | grep -v "DELETE_COMPLETE" | awk '{print $1}' || true)
    
    # Convert to array and add known stacks as fallback
    local stacks=()
    if [[ -n "$discovered_stacks" ]]; then
        # Add discovered stacks
        for stack in $discovered_stacks; do
            stacks+=("$stack")
        done
        print_info "Found ${#stacks[@]} CloudFormation stacks to delete"
    else
        # Fallback to predefined stack names
        print_warning "No stacks discovered automatically, using predefined list"
        stacks=(
            "datalake-lake-formation-${ENVIRONMENT}"
            "datalake-glue-catalog-${ENVIRONMENT}"
            "datalake-iam-roles-${ENVIRONMENT}"
            "datalake-s3-storage-${ENVIRONMENT}"
            "datalake-cost-monitoring-${ENVIRONMENT}"
        )
    fi
    
    # Delete stacks in dependency order (reverse of creation order)
    print_info "Deleting stacks in correct dependency order..."
    local deletion_order=()
    
    # Add stacks in deletion order (dependencies first)
    for stack in "${stacks[@]}"; do
        case "$stack" in
            *"lake-formation"*|*"glue-catalog"*)
                deletion_order=("$stack" "${deletion_order[@]}")  # Add to front
                ;;
            *"iam-roles"*)
                deletion_order+=("$stack")  # Add to end
                ;;
            *"s3-storage"*)
                deletion_order+=("$stack")  # Add to end (last)
                ;;
            *)
                deletion_order=("$stack" "${deletion_order[@]}")  # Other stacks go first
                ;;
        esac
    done
    
    # Delete stacks
    for stack in "${deletion_order[@]}"; do
        if aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" &>/dev/null; then
            local stack_status=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
                --query "Stacks[0].StackStatus" --output text 2>/dev/null)
            
            if [[ "$stack_status" == "DELETE_IN_PROGRESS" ]]; then
                print_info "Stack $stack is already being deleted"
            elif [[ "$stack_status" != "DELETE_COMPLETE" ]]; then
                print_info "Deleting CloudFormation stack: $stack (status: $stack_status)"
                aws cloudformation delete-stack --stack-name "$stack" --region "$AWS_REGION"
            fi
        else
            print_info "CloudFormation stack $stack does not exist or already deleted."
        fi
    done
    
    # Wait for all stacks to be deleted with timeout
    print_info "Waiting for stack deletions to complete..."
    local timeout=1800  # 30 minutes timeout
    local start_time=$(date +%s)
    
    for stack in "${deletion_order[@]}"; do
        # Check if stack exists before waiting
        if ! aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" &>/dev/null; then
            print_info "Stack $stack does not exist or already deleted"
            continue
        fi
        
        print_info "Waiting for stack deletion: $stack"
        local waited=0
        while aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" &>/dev/null; do
            local current_time=$(date +%s)
            local elapsed=$((current_time - start_time))
            
            if [[ $elapsed -gt $timeout ]]; then
                print_warning "Timeout waiting for stack $stack deletion after ${timeout}s"
                break
            fi
            
            local stack_status=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
                --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "NOT_FOUND")
            
            if [[ "$stack_status" == "DELETE_FAILED" ]]; then
                print_error "Stack $stack deletion failed. Status: $stack_status"
                print_warning "Please check AWS Console for details and manually delete if needed"
                break
            fi
            
            if [[ "$stack_status" == "NOT_FOUND" ]]; then
                print_info "Stack $stack has been successfully deleted"
                break
            fi
            
            if [[ $((waited % 30)) -eq 0 ]]; then  # Print status every 30 seconds
                print_info "Stack $stack status: $stack_status (${elapsed}s elapsed)"
            fi
            
            sleep 10
            waited=$((waited + 10))
        done
        
        # Final check
        if ! aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" &>/dev/null; then
            print_info "Stack $stack deletion completed successfully"
        fi
    done
    
    # Final verification
    print_info "Verifying stack deletions..."
    local remaining_stacks=$(aws cloudformation list-stacks --region "$AWS_REGION" \
        --query "StackSummaries[?contains(StackName, '$PROJECT_PREFIX')].{Name:StackName,Status:StackStatus}" \
        --output text 2>/dev/null | grep -v "DELETE_COMPLETE" | awk '{print $1}' || true)
    
    if [[ -n "$remaining_stacks" ]]; then
        print_warning "Some stacks are still present:"
        for stack in $remaining_stacks; do
            local status=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
                --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "UNKNOWN")
            print_warning "  - $stack: $status"
        done
        print_warning "Please check these manually in the AWS Console"
    else
        print_info "All CloudFormation stacks have been successfully deleted!"
    fi
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