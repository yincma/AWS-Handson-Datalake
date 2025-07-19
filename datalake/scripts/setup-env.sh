#!/bin/bash

# AWS Data Lake Project One-Click Deployment Script
# This script sets up the entire data lake infrastructure and loads sample data

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Global variables
DEPLOYMENT_MODE="full"
SKIP_COST_MONITORING=false
SKIP_SAMPLE_DATA=false
EMAIL_ADDRESS=""

# Function to show usage
show_usage() {
    cat << EOF
AWS Data Lake One-Click Deployment Script

Usage: $0 [OPTIONS]

Options:
    --mode MODE           Deployment mode: full, infrastructure-only, sample-data-only (default: full)
    --email EMAIL         Email address for cost alerts (required for full deployment)
    --skip-cost-monitoring Skip cost monitoring setup
    --skip-sample-data    Skip sample data upload
    --help, -h            Show this help message

Examples:
    $0 --email user@example.com                    # Full deployment with cost alerts
    $0 --mode infrastructure-only --email user@example.com  # Only infrastructure
    $0 --mode sample-data-only                     # Only upload sample data
    $0 --skip-cost-monitoring --email user@example.com      # Skip cost monitoring

Deployment Modes:
    full                  Complete setup: infrastructure + sample data + cost monitoring
    infrastructure-only   CloudFormation stacks only
    sample-data-only      Upload sample data to existing buckets

EOF
}

# Function to parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --mode)
                DEPLOYMENT_MODE="$2"
                shift 2
                ;;
            --email)
                EMAIL_ADDRESS="$2"
                shift 2
                ;;
            --skip-cost-monitoring)
                SKIP_COST_MONITORING=true
                shift
                ;;
            --skip-sample-data)
                SKIP_SAMPLE_DATA=true
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
    
    # Validate arguments
    if [[ "$DEPLOYMENT_MODE" == "full" && -z "$EMAIL_ADDRESS" && "$SKIP_COST_MONITORING" == "false" ]]; then
        print_error "Email address is required for full deployment with cost monitoring"
        print_info "Use --email flag or --skip-cost-monitoring"
        exit 1
    fi
    
    if [[ ! "$DEPLOYMENT_MODE" =~ ^(full|infrastructure-only|sample-data-only)$ ]]; then
        print_error "Invalid deployment mode: $DEPLOYMENT_MODE"
        show_usage
        exit 1
    fi
}

# Function to check prerequisites
check_prerequisites() {
    print_step "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check Python3
    if ! command -v python3 &> /dev/null; then
        print_warning "Python3 not found. Some features may not work properly."
    fi
    
    # Check required files
    local required_files=(
        "configs/config.env"
        "templates/s3-storage-layer.yaml"
        "templates/iam-roles-policies.yaml"
        "templates/lake-formation.yaml"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            print_error "Required file not found: $file"
            exit 1
        fi
    done
    
    print_info "Prerequisites check passed"
}

# Function to load configuration
load_configuration() {
    print_step "Loading configuration..."
    
    CONFIG_FILE="configs/config.env"
    LOCAL_CONFIG_FILE="configs/config.local.env"
    
    if [[ -f "$LOCAL_CONFIG_FILE" ]]; then
        print_info "Loading local configuration from $LOCAL_CONFIG_FILE"
        source "$LOCAL_CONFIG_FILE"
    elif [[ -f "$CONFIG_FILE" ]]; then
        print_warning "Using default configuration. Consider copying $CONFIG_FILE to $LOCAL_CONFIG_FILE for customization."
        source "$CONFIG_FILE"
    else
        print_error "Configuration file not found!"
        exit 1
    fi
    
    # Set defaults
    PROJECT_PREFIX=${PROJECT_PREFIX:-dl-handson}
    AWS_REGION=${AWS_REGION:-us-east-1}
    ENVIRONMENT=${ENVIRONMENT:-dev}
    AWS_PROFILE=${AWS_PROFILE:-default}
    
    print_info "Project: $PROJECT_PREFIX"
    print_info "Region: $AWS_REGION"
    print_info "Environment: $ENVIRONMENT"
    print_info "AWS Profile: $AWS_PROFILE"
}

# Function to verify AWS credentials
verify_aws_credentials() {
    print_step "Verifying AWS credentials..."
    
    # Build AWS CLI profile argument
    local profile_arg=""
    if [[ -n "$AWS_PROFILE" && "$AWS_PROFILE" != "default" ]]; then
        profile_arg="--profile $AWS_PROFILE"
    fi
    
    if ! aws sts get-caller-identity $profile_arg &> /dev/null; then
        print_error "AWS credentials not configured or invalid."
        print_info "Please run: aws configure $profile_arg"
        exit 1
    fi
    
    # Display current AWS identity
    local identity=$(aws sts get-caller-identity $profile_arg --output text --query 'Arn')
    print_info "Current AWS identity: $identity"
    
    # Check permissions (basic check)
    if ! aws iam list-roles --max-items 1 $profile_arg &> /dev/null; then
        print_warning "Limited IAM permissions detected. Some operations may fail."
    fi
    
    # Set default region
    print_info "Setting default region to $AWS_REGION"
    aws configure set region "$AWS_REGION" $profile_arg
}

# Function to export environment variables
export_environment_variables() {
    print_step "Exporting environment variables..."
    
    cat > configs/env-vars.sh << EOF
#!/bin/bash
# Auto-generated environment variables - $(date)
export AWS_REGION=$AWS_REGION
export AWS_PROFILE=$AWS_PROFILE
export PROJECT_NAME=$PROJECT_NAME
export PROJECT_PREFIX=$PROJECT_PREFIX
export ENVIRONMENT=$ENVIRONMENT
export RAW_BUCKET_NAME=${PROJECT_PREFIX}-${RAW_BUCKET_SUFFIX}-${ENVIRONMENT}
export CLEAN_BUCKET_NAME=${PROJECT_PREFIX}-${CLEAN_BUCKET_SUFFIX}-${ENVIRONMENT}
export ANALYTICS_BUCKET_NAME=${PROJECT_PREFIX}-${ANALYTICS_BUCKET_SUFFIX}-${ENVIRONMENT}
export ATHENA_RESULTS_BUCKET_NAME=${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}
export LAB_ADMIN_ROLE=$LAB_ADMIN_ROLE
export DATA_ENGINEER_ROLE=$DATA_ENGINEER_ROLE
export ANALYST_ROLE=$ANALYST_ROLE
export GLUE_CRAWLER_ROLE=$GLUE_CRAWLER_ROLE
export GLUE_DATABASE_NAME=$GLUE_DATABASE_NAME
export GLUE_CRAWLER_NAME=$GLUE_CRAWLER_NAME
export EMR_CLUSTER_NAME=$EMR_CLUSTER_NAME
export EMR_INSTANCE_TYPE=$EMR_INSTANCE_TYPE
export EMR_INSTANCE_COUNT=$EMR_INSTANCE_COUNT
export ATHENA_WORKGROUP=$ATHENA_WORKGROUP
export EMAIL_ADDRESS=$EMAIL_ADDRESS
EOF
    
    # Source the variables for current session
    source configs/env-vars.sh
    
    print_info "Environment variables exported to configs/env-vars.sh"
}

# Function to deploy CloudFormation stack
deploy_cloudformation_stack() {
    local stack_name="$1"
    local template_file="$2"
    local parameters="$3"
    
    print_info "Deploying CloudFormation stack: $stack_name"
    
    # Check if stack already exists
    if aws cloudformation describe-stacks --stack-name "$stack_name" --region "$AWS_REGION" &> /dev/null; then
        print_warning "Stack $stack_name already exists. Updating..."
        aws cloudformation update-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "$parameters" \
            --capabilities CAPABILITY_NAMED_IAM \
            --region "$AWS_REGION" || true
        
        aws cloudformation wait stack-update-complete \
            --stack-name "$stack_name" \
            --region "$AWS_REGION"
    else
        print_info "Creating new stack: $stack_name"
        aws cloudformation create-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "$parameters" \
            --capabilities CAPABILITY_NAMED_IAM \
            --region "$AWS_REGION"
        
        aws cloudformation wait stack-create-complete \
            --stack-name "$stack_name" \
            --region "$AWS_REGION"
    fi
    
    print_info "Stack $stack_name deployed successfully"
}

# Function to deploy infrastructure
deploy_infrastructure() {
    print_step "Deploying AWS infrastructure..."
    
    # Deploy S3 storage layer
    local s3_stack_name="datalake-s3-storage-${ENVIRONMENT}"
    local s3_parameters="ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX} ParameterKey=Environment,ParameterValue=${ENVIRONMENT}"
    deploy_cloudformation_stack "$s3_stack_name" "templates/s3-storage-layer.yaml" "$s3_parameters"
    
    # Deploy IAM roles
    local iam_stack_name="datalake-iam-roles-${ENVIRONMENT}"
    local iam_parameters="ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX} ParameterKey=Environment,ParameterValue=${ENVIRONMENT} ParameterKey=S3StackName,ParameterValue=${s3_stack_name}"
    deploy_cloudformation_stack "$iam_stack_name" "templates/iam-roles-policies.yaml" "$iam_parameters"
    
    # Deploy Lake Formation
    local lf_stack_name="datalake-lake-formation-${ENVIRONMENT}"
    local lf_parameters="ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX} ParameterKey=Environment,ParameterValue=${ENVIRONMENT} ParameterKey=S3StackName,ParameterValue=${s3_stack_name} ParameterKey=IAMStackName,ParameterValue=${iam_stack_name}"
    deploy_cloudformation_stack "$lf_stack_name" "templates/lake-formation.yaml" "$lf_parameters"
    
    print_info "Infrastructure deployment completed"
}

# Function to deploy cost monitoring
deploy_cost_monitoring() {
    if [[ "$SKIP_COST_MONITORING" == "true" ]]; then
        print_info "Skipping cost monitoring setup"
        return
    fi
    
    print_step "Deploying cost monitoring..."
    
    local cost_stack_name="datalake-cost-monitoring-${ENVIRONMENT}"
    local cost_parameters="ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX} ParameterKey=Environment,ParameterValue=${ENVIRONMENT} ParameterKey=AlertEmail,ParameterValue=${EMAIL_ADDRESS}"
    deploy_cloudformation_stack "$cost_stack_name" "templates/cost-monitoring.yaml" "$cost_parameters"
    
    print_info "Cost monitoring setup completed"
    print_warning "Please check your email and confirm the SNS subscription"
}

# Function to upload sample data
upload_sample_data() {
    if [[ "$SKIP_SAMPLE_DATA" == "true" ]]; then
        print_info "Skipping sample data upload"
        return
    fi
    
    print_step "Uploading sample data..."
    
    local raw_bucket="${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
    
    # Check if bucket exists
    if ! aws s3api head-bucket --bucket "$raw_bucket" --region "$AWS_REGION" 2>/dev/null; then
        print_error "Raw bucket $raw_bucket does not exist. Please deploy infrastructure first."
        return
    fi
    
    # Upload sample data files
    local data_files=(
        "sample-data/customers.csv"
        "sample-data/orders.csv"
        "sample-data/products.csv"
        "sample-data/order_items.csv"
    )
    
    for file in "${data_files[@]}"; do
        if [[ -f "$file" ]]; then
            local filename=$(basename "$file")
            local s3_path="s3://${raw_bucket}/ecommerce/${filename%.*}/${filename}"
            print_info "Uploading $file to $s3_path"
            aws s3 cp "$file" "$s3_path" --region "$AWS_REGION"
        else
            print_warning "Sample data file not found: $file"
        fi
    done
    
    print_info "Sample data upload completed"
}

# Function to verify deployment
verify_deployment() {
    print_step "Verifying deployment..."
    
    # Check CloudFormation stacks
    local stacks=(
        "datalake-s3-storage-${ENVIRONMENT}"
        "datalake-iam-roles-${ENVIRONMENT}"
        "datalake-lake-formation-${ENVIRONMENT}"
    )
    
    if [[ "$SKIP_COST_MONITORING" == "false" ]]; then
        stacks+=("datalake-cost-monitoring-${ENVIRONMENT}")
    fi
    
    for stack in "${stacks[@]}"; do
        local stack_status=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
            --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")
        
        if [[ "$stack_status" == *"COMPLETE" ]]; then
            print_info "✓ Stack $stack: $stack_status"
        else
            print_error "✗ Stack $stack: $stack_status"
        fi
    done
    
    # Check S3 buckets
    local buckets=(
        "${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}"
    )
    
    for bucket in "${buckets[@]}"; do
        if aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
            print_info "✓ S3 Bucket: $bucket"
        else
            print_warning "✗ S3 Bucket: $bucket (not accessible)"
        fi
    done
    
    print_info "Deployment verification completed"
}

# Function to show next steps
show_next_steps() {
    print_step "Next Steps"
    
    cat << EOF

🎉 Data Lake deployment completed successfully!

📋 What's been set up:
   • S3 storage layers (Raw, Clean, Analytics)
   • IAM roles and policies
   • Lake Formation data governance
   • Glue crawlers (scheduled)
   • Sample e-commerce data
$(if [[ "$SKIP_COST_MONITORING" == "false" ]]; then echo "   • Cost monitoring and alerts"; fi)

🚀 Next steps:

1. Start the Glue crawler to catalog your data:
   aws glue start-crawler --name ${PROJECT_PREFIX}-raw-crawler --region ${AWS_REGION}

2. Process data with EMR/PySpark:
   ./scripts/submit_pyspark_job.sh

3. Query data with Athena:
   Use the queries in scripts/athena_queries.sql

4. Monitor costs:
   ./scripts/cost-optimization.sh

5. Clean up when done:
   ./scripts/cleanup.sh

📊 Useful Links:
   • AWS Console: https://console.aws.amazon.com/
   • S3 Buckets: https://${AWS_REGION}.console.aws.amazon.com/s3/
   • Glue: https://${AWS_REGION}.console.aws.amazon.com/glue/
   • Athena: https://${AWS_REGION}.console.aws.amazon.com/athena/
$(if [[ "$SKIP_COST_MONITORING" == "false" ]]; then echo "   • Billing: https://console.aws.amazon.com/billing/"; fi)

💡 Pro Tips:
   • Load environment variables: source configs/env-vars.sh
   • Monitor costs regularly with ./scripts/cost-optimization.sh
   • Use the cleanup script when you're done to avoid charges

EOF
}

# Main function
main() {
    print_header "AWS Data Lake One-Click Deployment"
    
    parse_arguments "$@"
    check_prerequisites
    load_configuration
    verify_aws_credentials
    export_environment_variables
    
    case "$DEPLOYMENT_MODE" in
        "full")
            deploy_infrastructure
            deploy_cost_monitoring
            upload_sample_data
            verify_deployment
            show_next_steps
            ;;
        "infrastructure-only")
            deploy_infrastructure
            deploy_cost_monitoring
            verify_deployment
            print_info "Infrastructure deployment completed. Use --mode sample-data-only to upload data."
            ;;
        "sample-data-only")
            upload_sample_data
            print_info "Sample data upload completed."
            ;;
    esac
    
    print_header "Deployment Complete!"
}

# Run main function
main "$@"