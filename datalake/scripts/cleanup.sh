#!/bin/bash

# AWS Data Lake Cleanup Script
# This script removes all resources created by the setup script

# DO NOT use set -e to allow complete cleanup even with errors

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_step() { echo -e "\n${BLUE}[STEP]${NC} $1"; }
print_success() { echo -e "${GREEN}✅${NC} $1"; }

# Default values
FORCE=false
DELETE_LOGS=false
RETRY_FAILED=false
DEEP_CLEAN=false

# Cleanup report arrays
declare -a SUCCESSFUL_DELETIONS=()
declare -a FAILED_DELETIONS=()

# Function to show usage
show_usage() {
    cat << EOF
AWS Data Lake Cleanup Script

Usage: $0 [OPTIONS]

Options:
    --force          Skip confirmation prompts
    --delete-logs    Also delete CloudWatch logs
    --retry-failed   Retry failed CloudFormation stack deletions
    --deep-clean     Deep clean including all S3 versions and delete markers
    --help, -h       Show this help message

This script will remove:
    • S3 buckets and all data (including versioned objects)
    • CloudFormation stacks
    • Glue resources (database and tables)
    • EMR clusters (if running)
    • EC2 key pairs (project-created)
    • Lake Formation resources

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE=true
            shift
            ;;
        --delete-logs)
            DELETE_LOGS=true
            shift
            ;;
        --retry-failed)
            RETRY_FAILED=true
            shift
            ;;
        --deep-clean)
            DEEP_CLEAN=true
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

# Load configuration
load_configuration() {
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
    
    print_info "Configuration loaded:"
    print_info "  Project: $PROJECT_PREFIX"
    print_info "  Region: $AWS_REGION"
    print_info "  Environment: $ENVIRONMENT"
}

# Track successful and failed operations
track_deletion() {
    local resource="$1"
    local status="$2"
    
    if [[ "$status" == "success" ]]; then
        SUCCESSFUL_DELETIONS+=("$resource")
    else
        FAILED_DELETIONS+=("$resource")
    fi
}

# Confirm deletion
confirm_deletion() {
    if [[ "$FORCE" == "false" ]]; then
        echo -e "${YELLOW}⚠️  WARNING: This will delete all data lake resources!${NC}"
        echo "Resources to be deleted:"
        echo "  • All S3 buckets starting with ${PROJECT_PREFIX}"
        echo "  • All CloudFormation stacks"
        echo "  • All Glue databases and tables"
        echo "  • All EC2 key pairs matching ${PROJECT_PREFIX}-emr-key*"
        echo "  • All Lake Formation resources"
        echo ""
        read -p "Are you sure you want to continue? (yes/no): " confirmation
        
        if [[ "$confirmation" != "yes" ]]; then
            print_info "Cleanup cancelled"
            exit 0
        fi
    fi
}

# Delete S3 bucket with retry logic
delete_s3_bucket() {
    local bucket="$1"
    local max_retries=3
    local retry_count=0
    
    while [[ $retry_count -lt $max_retries ]]; do
        if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            print_info "Processing bucket: $bucket"
            
            # If deep clean, remove all versions and delete markers
            if [[ "$DEEP_CLEAN" == "true" ]]; then
                print_info "Deep cleaning bucket (removing all versions)..."
                
                # List and delete all object versions
                aws s3api list-object-versions --bucket "$bucket" --output json | \
                jq -r '.Versions[]? | "--key \"\(.Key)\" --version-id \(.VersionId)"' | \
                while read -r line; do
                    eval "aws s3api delete-object --bucket \"$bucket\" $line" 2>/dev/null || true
                done
                
                # List and delete all delete markers
                aws s3api list-object-versions --bucket "$bucket" --output json | \
                jq -r '.DeleteMarkers[]? | "--key \"\(.Key)\" --version-id \(.VersionId)"' | \
                while read -r line; do
                    eval "aws s3api delete-object --bucket \"$bucket\" $line" 2>/dev/null || true
                done
            else
                # Standard deletion
                print_info "Emptying bucket..."
                aws s3 rm "s3://${bucket}" --recursive 2>/dev/null || true
            fi
            
            # Delete the bucket
            print_info "Deleting bucket..."
            if aws s3api delete-bucket --bucket "$bucket" 2>/dev/null; then
                print_success "Deleted bucket: $bucket"
                track_deletion "S3:$bucket" "success"
                return 0
            else
                print_error "Failed to delete bucket: $bucket (attempt $((retry_count + 1))/$max_retries)"
            fi
        else
            print_info "Bucket $bucket not found"
            return 0
        fi
        
        retry_count=$((retry_count + 1))
        if [[ $retry_count -lt $max_retries ]]; then
            sleep 5
        fi
    done
    
    track_deletion "S3:$bucket" "failed"
    return 1
}

# Delete S3 buckets
delete_s3_buckets() {
    print_step "Deleting S3 buckets..."
    
    local buckets=(
        "${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}"
    )
    
    for bucket in "${buckets[@]}"; do
        delete_s3_bucket "$bucket"
    done
}

# Terminate EMR clusters
terminate_emr_clusters() {
    print_step "Checking for EMR clusters..."
    
    local cluster_ids=$(aws emr list-clusters \
        --active \
        --query "Clusters[?Name=='${PROJECT_PREFIX}-emr-cluster-${ENVIRONMENT}'].Id" \
        --output text 2>/dev/null)
    
    if [[ -n "$cluster_ids" ]]; then
        for cluster_id in $cluster_ids; do
            print_info "Terminating EMR cluster: $cluster_id"
            if aws emr terminate-clusters --cluster-ids "$cluster_id" 2>/dev/null; then
                print_success "Terminated EMR cluster: $cluster_id"
                track_deletion "EMR:$cluster_id" "success"
            else
                print_error "Failed to terminate EMR cluster: $cluster_id"
                track_deletion "EMR:$cluster_id" "failed"
            fi
        done
    else
        print_info "No active EMR clusters found"
    fi
}

# Delete EC2 key pairs
delete_ec2_key_pairs() {
    print_step "Checking for EC2 key pairs..."
    
    # Pattern to match project-created key pairs
    local key_pattern="${PROJECT_PREFIX}-emr-key"
    
    # Get all key pairs matching the pattern
    local key_pairs=$(aws ec2 describe-key-pairs \
        --query "KeyPairs[?contains(KeyName, '${key_pattern}')].KeyName" \
        --output text 2>/dev/null)
    
    if [[ -n "$key_pairs" ]]; then
        for key_name in $key_pairs; do
            print_info "Deleting EC2 key pair: $key_name"
            if aws ec2 delete-key-pair --key-name "$key_name" 2>/dev/null; then
                print_success "Deleted EC2 key pair: $key_name"
                track_deletion "EC2:KeyPair:$key_name" "success"
                
                # Also delete local .pem file if exists
                if [[ -f "${key_name}.pem" ]]; then
                    rm -f "${key_name}.pem"
                    print_info "Removed local key file: ${key_name}.pem"
                fi
            else
                print_error "Failed to delete EC2 key pair: $key_name"
                track_deletion "EC2:KeyPair:$key_name" "failed"
            fi
        done
    else
        print_info "No project EC2 key pairs found"
    fi
    
    # Check for any remaining .pem files
    local pem_files=$(ls ${PROJECT_PREFIX}-emr-key-*.pem 2>/dev/null)
    if [[ -n "$pem_files" ]]; then
        print_info "Cleaning up remaining .pem files..."
        for pem_file in $pem_files; do
            rm -f "$pem_file"
            print_info "Removed: $pem_file"
        done
    fi
}

# Delete Glue resources
delete_glue_resources() {
    print_step "Deleting Glue resources..."
    
    # Delete crawlers
    for crawler in "${PROJECT_PREFIX}-raw-crawler" "${PROJECT_PREFIX}-clean-crawler" "${PROJECT_PREFIX}-raw-crawler-simple"; do
        if aws glue get-crawler --name "$crawler" &>/dev/null; then
            print_info "Deleting crawler: $crawler"
            if aws glue delete-crawler --name "$crawler" 2>/dev/null; then
                print_success "Deleted crawler: $crawler"
                track_deletion "Glue:Crawler:$crawler" "success"
            else
                print_error "Failed to delete crawler: $crawler"
                track_deletion "Glue:Crawler:$crawler" "failed"
            fi
        fi
    done
    
    # Delete tables - use correct database name with hyphen
    local database="${PROJECT_PREFIX}-db"
    if aws glue get-database --name "$database" &>/dev/null; then
        # Get all tables in the database
        local tables=$(aws glue get-tables --database-name "$database" --query 'TableList[].Name' --output text 2>/dev/null)
        if [[ -n "$tables" ]]; then
            for table in $tables; do
                print_info "Deleting table: $table"
                if aws glue delete-table --database-name "$database" --name "$table" 2>/dev/null; then
                    print_success "Deleted table: $table"
                    track_deletion "Glue:Table:$table" "success"
                else
                    print_error "Failed to delete table: $table"
                    track_deletion "Glue:Table:$table" "failed"
                fi
            done
        fi
        
        # Delete the database
        print_info "Deleting database: $database"
        if aws glue delete-database --name "$database" 2>/dev/null; then
            print_success "Deleted database: $database"
            track_deletion "Glue:Database:$database" "success"
        else
            print_error "Failed to delete database: $database"
            track_deletion "Glue:Database:$database" "failed"
        fi
    else
        print_info "Database $database not found"
    fi
}

# Delete CloudFormation stack with retry logic
delete_cloudformation_stack() {
    local stack="$1"
    local max_wait_time=300  # 5 minutes
    
    # Check if stack exists
    local stack_status=$(aws cloudformation describe-stacks \
        --stack-name "$stack" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null)
    
    if [[ -z "$stack_status" ]]; then
        print_info "Stack $stack not found"
        return 0
    fi
    
    # Handle DELETE_FAILED stacks
    if [[ "$stack_status" == "DELETE_FAILED" ]]; then
        if [[ "$RETRY_FAILED" == "true" ]]; then
            print_warning "Stack $stack is in DELETE_FAILED state, retrying deletion..."
            
            # Get failed resources
            local failed_resources=$(aws cloudformation describe-stack-events \
                --stack-name "$stack" \
                --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].ResourceType" \
                --output text 2>/dev/null)
            
            if [[ -n "$failed_resources" ]]; then
                print_info "Failed resources: $failed_resources"
            fi
        else
            print_warning "Stack $stack is in DELETE_FAILED state. Use --retry-failed to force deletion"
            track_deletion "CF:$stack" "failed"
            return 1
        fi
    fi
    
    # Delete the stack
    print_info "Deleting stack: $stack"
    if aws cloudformation delete-stack --stack-name "$stack" 2>/dev/null; then
        print_info "Waiting for stack deletion (timeout: ${max_wait_time}s)..."
        
        # Custom wait with timeout
        local elapsed=0
        while [[ $elapsed -lt $max_wait_time ]]; do
            local status=$(aws cloudformation describe-stacks \
                --stack-name "$stack" \
                --query 'Stacks[0].StackStatus' \
                --output text 2>/dev/null)
            
            if [[ -z "$status" ]]; then
                print_success "Stack deleted: $stack"
                track_deletion "CF:$stack" "success"
                return 0
            elif [[ "$status" == "DELETE_FAILED" ]]; then
                print_error "Stack deletion failed: $stack"
                track_deletion "CF:$stack" "failed"
                return 1
            fi
            
            sleep 10
            elapsed=$((elapsed + 10))
        done
        
        print_warning "Stack deletion timed out: $stack"
        track_deletion "CF:$stack" "timeout"
        return 1
    else
        print_error "Failed to initiate stack deletion: $stack"
        track_deletion "CF:$stack" "failed"
        return 1
    fi
}

# Delete CloudFormation stacks
delete_cloudformation_stacks() {
    print_step "Deleting CloudFormation stacks..."
    
    # Delete in dependency order
    local stacks=(
        "datalake-lake-formation-${ENVIRONMENT}"
        "datalake-iam-roles-${ENVIRONMENT}"
        "datalake-s3-storage-${ENVIRONMENT}"
        "datalake-cost-monitoring-${ENVIRONMENT}"
    )
    
    for stack in "${stacks[@]}"; do
        delete_cloudformation_stack "$stack"
    done
}

# Delete CloudWatch logs
delete_cloudwatch_logs() {
    if [[ "$DELETE_LOGS" == "true" ]]; then
        print_step "Deleting CloudWatch logs..."
        
        # Find all log groups related to the project
        local log_groups=$(aws logs describe-log-groups \
            --log-group-name-prefix "/aws-glue/" \
            --query "logGroups[?contains(logGroupName, '${PROJECT_PREFIX}')].logGroupName" \
            --output text 2>/dev/null)
        
        if [[ -n "$log_groups" ]]; then
            for log_group in $log_groups; do
                print_info "Deleting log group: $log_group"
                if aws logs delete-log-group --log-group-name "$log_group" 2>/dev/null; then
                    print_success "Deleted log group: $log_group"
                    track_deletion "Logs:$log_group" "success"
                else
                    print_error "Failed to delete log group: $log_group"
                    track_deletion "Logs:$log_group" "failed"
                fi
            done
        else
            print_info "No log groups found"
        fi
    fi
}

# Clean up local files
cleanup_local_files() {
    print_step "Cleaning up local files..."
    
    local files=(
        "configs/env-vars.sh"
        "configs/emr-cluster.env"
        "deployment.log"
        "deployment-summary.txt"
        "DEPLOYMENT_FIXES_SUMMARY.md"
    )
    
    for file in "${files[@]}"; do
        if [[ -f "$file" ]]; then
            rm -f "$file"
            print_info "Removed: $file"
        fi
    done
}

# Generate cleanup report
generate_cleanup_report() {
    print_step "Generating cleanup report..."
    
    local report_file="cleanup-report-$(date +%Y%m%d-%H%M%S).txt"
    
    cat > "$report_file" << EOF
AWS Data Lake Cleanup Report
Generated: $(date)
=====================================

Configuration:
- Project: $PROJECT_PREFIX
- Environment: $ENVIRONMENT
- Region: $AWS_REGION

Options Used:
- Force: $FORCE
- Delete Logs: $DELETE_LOGS
- Retry Failed: $RETRY_FAILED
- Deep Clean: $DEEP_CLEAN

SUCCESSFUL DELETIONS (${#SUCCESSFUL_DELETIONS[@]}):
EOF
    
    for item in "${SUCCESSFUL_DELETIONS[@]}"; do
        echo "✅ $item" >> "$report_file"
    done
    
    echo -e "\nFAILED DELETIONS (${#FAILED_DELETIONS[@]}):" >> "$report_file"
    
    for item in "${FAILED_DELETIONS[@]}"; do
        echo "❌ $item" >> "$report_file"
    done
    
    echo -e "\n=====================================\n" >> "$report_file"
    
    print_info "Cleanup report saved to: $report_file"
    
    # Display summary
    echo
    print_info "Cleanup Summary:"
    print_success "Successful deletions: ${#SUCCESSFUL_DELETIONS[@]}"
    if [[ ${#FAILED_DELETIONS[@]} -gt 0 ]]; then
        print_error "Failed deletions: ${#FAILED_DELETIONS[@]}"
        print_warning "Check $report_file for details"
    fi
}

# Main function
main() {
    echo "=========================================="
    echo "AWS Data Lake Cleanup"
    echo "=========================================="
    
    load_configuration
    confirm_deletion
    
    # Record start time
    START_TIME=$(date +%s)
    
    # Perform cleanup
    terminate_emr_clusters
    delete_ec2_key_pairs
    delete_glue_resources
    delete_s3_buckets
    delete_cloudformation_stacks
    delete_cloudwatch_logs
    cleanup_local_files
    
    # Calculate duration
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    # Generate report
    generate_cleanup_report
    
    print_step "Cleanup Complete!"
    echo -e "\n${GREEN}✅ Cleanup process finished in ${DURATION} seconds${NC}"
    
    if [[ ${#FAILED_DELETIONS[@]} -gt 0 ]]; then
        echo -e "${YELLOW}⚠️  Some resources failed to delete. Run with --retry-failed to retry${NC}"
    else
        echo -e "${GREEN}🎉 All resources have been successfully deleted${NC}"
    fi
}

# Run main function
main