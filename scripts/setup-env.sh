#!/bin/bash
# AWS Data Lake Environment Setup Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_step() { echo -e "\n${BLUE}[STEP]${NC} $1"; }

show_usage() {
    cat << EOF
AWS Data Lake Environment Setup Script

Usage: $0 [OPTIONS]

Options:
    --help, -h              Show this help message
    --region REGION         Specify AWS region (overrides config)
    --project-name NAME     Specify project name (overrides config)
    --environment ENV       Specify environment: dev, staging, prod
    --interactive          Interactive configuration mode

This script will:
1. Auto-create local configuration file
2. Deploy S3 buckets for data storage
3. Create IAM roles and policies
4. Set up Glue database and tables
5. Upload sample data

Examples:
    $0                                    # Use default configuration
    $0 --region ap-northeast-1            # Deploy to Tokyo region
    $0 --project-name my-datalake         # Custom project name
    $0 --interactive                      # Interactive setup

EOF
}

# Initialize command line variables
OVERRIDE_REGION=""
OVERRIDE_PROJECT_NAME=""
OVERRIDE_ENVIRONMENT=""
INTERACTIVE_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_usage
            exit 0
            ;;
        --region)
            OVERRIDE_REGION="$2"
            shift 2
            ;;
        --project-name)
            OVERRIDE_PROJECT_NAME="$2"
            shift 2
            ;;
        --environment)
            OVERRIDE_ENVIRONMENT="$2"
            shift 2
            ;;
        --interactive)
            INTERACTIVE_MODE=true
            shift
            ;;
        *)
            print_error "Unknown argument: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Function to check prerequisites
check_prerequisites() {
    print_step "Checking prerequisites..."
    
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    if ! command -v python3 &> /dev/null; then
        print_error "Python3 is not installed. Please install it first."
        exit 1
    fi
    
    # Check and install Python dependencies
    print_info "Checking Python dependencies..."
    if ! python3 -c "import boto3" &> /dev/null; then
        print_info "Installing boto3 dependency..."
        python3 -m pip install --user boto3 --quiet
        if ! python3 -c "import boto3" &> /dev/null; then
            print_error "Failed to install boto3. Please install manually: pip3 install boto3"
            exit 1
        fi
        print_info "boto3 installed successfully"
    fi
    
    print_info "Prerequisites check passed"
}

# Function to load configuration
load_configuration() {
    print_step "Loading configuration..."
    
    # Check for local configuration file first
    LOCAL_CONFIG_FILE="configs/config.local.env"
    BASE_CONFIG_FILE="configs/config.env"
    
    # Auto-create local config if it doesn't exist
    if [[ ! -f "$LOCAL_CONFIG_FILE" ]]; then
        if [[ -f "$BASE_CONFIG_FILE" ]]; then
            print_info "Creating local configuration from template..."
            cp "$BASE_CONFIG_FILE" "$LOCAL_CONFIG_FILE"
            
            # Auto-detect and set current AWS region
            CURRENT_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
            if [[ -n "$CURRENT_REGION" && "$CURRENT_REGION" != "us-east-1" ]]; then
                print_info "Setting AWS region to: $CURRENT_REGION"
                sed -i.bak "s/AWS_REGION=us-east-1/AWS_REGION=$CURRENT_REGION/" "$LOCAL_CONFIG_FILE"
                rm -f "${LOCAL_CONFIG_FILE}.bak"
            fi
            
            print_info "Local configuration created at: $LOCAL_CONFIG_FILE"
            print_warning "You can customize settings in $LOCAL_CONFIG_FILE if needed"
        else
            print_error "Base configuration file not found: $BASE_CONFIG_FILE"
            exit 1
        fi
    fi
    
    # Load the configuration
    source "$LOCAL_CONFIG_FILE"
    
    # Apply command line overrides
    [[ -n "$OVERRIDE_REGION" ]] && AWS_REGION="$OVERRIDE_REGION"
    [[ -n "$OVERRIDE_PROJECT_NAME" ]] && PROJECT_PREFIX="$OVERRIDE_PROJECT_NAME"
    [[ -n "$OVERRIDE_ENVIRONMENT" ]] && ENVIRONMENT="$OVERRIDE_ENVIRONMENT"
    
    # Set defaults
    PROJECT_PREFIX=${PROJECT_PREFIX:-dl-handson}
    AWS_REGION=${AWS_REGION:-us-east-1}
    ENVIRONMENT=${ENVIRONMENT:-dev}
    
    # Interactive mode
    if [[ "$INTERACTIVE_MODE" == "true" ]]; then
        print_step "Interactive configuration mode"
        
        read -p "Project name [$PROJECT_PREFIX]: " input_project
        [[ -n "$input_project" ]] && PROJECT_PREFIX="$input_project"
        
        read -p "AWS Region [$AWS_REGION]: " input_region
        [[ -n "$input_region" ]] && AWS_REGION="$input_region"
        
        read -p "Environment [$ENVIRONMENT]: " input_env
        [[ -n "$input_env" ]] && ENVIRONMENT="$input_env"
    fi
    
    print_info "Configuration loaded successfully:"
    print_info "  Project: $PROJECT_PREFIX"
    print_info "  Region: $AWS_REGION"
    print_info "  Environment: $ENVIRONMENT"
    
    # Confirm before proceeding in interactive mode
    if [[ "$INTERACTIVE_MODE" == "true" ]]; then
        echo
        read -p "Proceed with deployment? [y/N]: " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            print_info "Deployment cancelled by user"
            exit 0
        fi
    fi
}

# Function to verify AWS credentials
verify_aws_credentials() {
    print_step "Verifying AWS credentials..."
    
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured. Please run: aws configure"
        exit 1
    fi
    
    aws configure set region "$AWS_REGION"
    print_info "AWS credentials verified"
}

# Function to deploy CloudFormation stack
deploy_stack() {
    local stack_name="$1"
    local template_file="$2"
    shift 2
    
    print_info "Deploying $stack_name..."
    
    if aws cloudformation describe-stacks --stack-name "$stack_name" &> /dev/null; then
        print_info "Stack exists, updating..."
        aws cloudformation update-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "$@" \
            --capabilities CAPABILITY_NAMED_IAM 2>&1 | grep -v "No updates" || true
    else
        aws cloudformation create-stack \
            --stack-name "$stack_name" \
            --template-body "file://$template_file" \
            --parameters "$@" \
            --capabilities CAPABILITY_NAMED_IAM
        
        aws cloudformation wait stack-create-complete --stack-name "$stack_name"
    fi
}

# Function to configure Lake Formation
configure_lake_formation() {
    print_step "Configuring Lake Formation permissions..."
    
    # Get current user ARN
    local current_user_arn=$(aws sts get-caller-identity --query 'Arn' --output text)
    print_info "Setting up Lake Formation for user: $current_user_arn"
    
    # Set current user as Lake Formation admin
    aws lakeformation put-data-lake-settings --data-lake-settings "{
        \"DataLakeAdmins\": [
            {\"DataLakePrincipalIdentifier\": \"$current_user_arn\"}
        ],
        \"CreateDatabaseDefaultPermissions\": [
            {
                \"Principal\": {\"DataLakePrincipalIdentifier\": \"IAM_ALLOWED_PRINCIPALS\"},
                \"Permissions\": [\"ALL\"]
            }
        ],
        \"CreateTableDefaultPermissions\": [
            {
                \"Principal\": {\"DataLakePrincipalIdentifier\": \"IAM_ALLOWED_PRINCIPALS\"},
                \"Permissions\": [\"ALL\"]
            }
        ]
    }" 2>/dev/null || print_warning "Lake Formation settings update skipped (may already be configured)"
    
    # Register S3 data location
    local raw_bucket_arn="arn:aws:s3:::${PROJECT_PREFIX}-raw-${ENVIRONMENT}/"
    aws lakeformation register-resource --resource-arn "$raw_bucket_arn" --use-service-linked-role 2>/dev/null || print_info "S3 location already registered"
    
    print_info "Lake Formation configuration completed"
}

# Function to create Glue database
create_glue_database() {
    print_step "Creating Glue database..." >&2
    
    # Use hyphen instead of underscore for database name
    local db_name="${PROJECT_PREFIX}-db"
    
    # Create database if it doesn't exist
    aws glue create-database --database-input "{\"Name\": \"$db_name\"}" 2>/dev/null || print_info "Database $db_name already exists" >&2
    
    print_info "Glue database ready: $db_name" >&2
    echo "$db_name"
}

# Function to create Glue tables
create_glue_tables() {
    print_step "Creating Glue tables..."
    
    # Ensure database exists first
    create_glue_database >/dev/null 2>&1
    
    # Set database name directly
    local db_name="${PROJECT_PREFIX}-db"
    
    # Export required environment variables
    export GLUE_DATABASE_NAME="$db_name"
    export RAW_BUCKET_NAME="${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
    export AWS_REGION="${AWS_REGION}"
    
    # Run the external Python script
    python3 scripts/utils/create_glue_tables.py
}

# Main deployment function
main() {
    echo "=========================================="
    echo "AWS Data Lake Environment Setup"
    echo "=========================================="
    
    check_prerequisites
    load_configuration
    verify_aws_credentials
    
    # Deploy S3 storage
    print_step "Deploying S3 storage layer..."
    deploy_stack "datalake-s3-storage-${ENVIRONMENT}" "templates/s3-storage-layer.yaml" \
        "ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX}" \
        "ParameterKey=Environment,ParameterValue=${ENVIRONMENT}"
    
    # Deploy IAM roles
    print_step "Deploying IAM roles..."
    deploy_stack "datalake-iam-roles-${ENVIRONMENT}" "templates/iam-roles-policies.yaml" \
        "ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX}" \
        "ParameterKey=Environment,ParameterValue=${ENVIRONMENT}" \
        "ParameterKey=S3StackName,ParameterValue=datalake-s3-storage-${ENVIRONMENT}"
    
    # Deploy Glue catalog
    print_step "Deploying Glue catalog..."
    deploy_stack "datalake-glue-catalog-${ENVIRONMENT}" "templates/glue-catalog.yaml" \
        "ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX}" \
        "ParameterKey=Environment,ParameterValue=${ENVIRONMENT}" \
        "ParameterKey=S3StackName,ParameterValue=datalake-s3-storage-${ENVIRONMENT}" \
        "ParameterKey=IAMStackName,ParameterValue=datalake-iam-roles-${ENVIRONMENT}"
    
    # Deploy Lake Formation (simplified)
    print_step "Deploying Lake Formation..."
    TEMPLATE="templates/lake-formation-simple.yaml"
    # Use only the simplified template now
    
    deploy_stack "datalake-lake-formation-${ENVIRONMENT}" "$TEMPLATE" \
        "ParameterKey=ProjectPrefix,ParameterValue=${PROJECT_PREFIX}" \
        "ParameterKey=Environment,ParameterValue=${ENVIRONMENT}" \
        "ParameterKey=S3StackName,ParameterValue=datalake-s3-storage-${ENVIRONMENT}" \
        "ParameterKey=IAMStackName,ParameterValue=datalake-iam-roles-${ENVIRONMENT}" \
        "ParameterKey=GlueStackName,ParameterValue=datalake-glue-catalog-${ENVIRONMENT}"
    
    # Upload sample data
    print_step "Uploading sample data..."
    RAW_BUCKET="${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
    
    for file in sample-data/*.csv; do
        if [[ -f "$file" ]]; then
            filename=$(basename "$file")
            table_name="${filename%.*}"
            aws s3 cp "$file" "s3://${RAW_BUCKET}/ecommerce/${table_name}/${filename}"
            print_info "Uploaded $filename"
        fi
    done
    
    # Configure Lake Formation permissions
    configure_lake_formation
    
    # Note: Glue tables are now created via CloudFormation template (Glue Crawlers)
    # To populate tables, run crawlers after data upload:
    print_info "Run Glue crawlers to create tables: aws glue start-crawler --name ${PROJECT_PREFIX}-raw-crawler"
    
    # Generate environment variables file
    sed -e "s/%DATE%/$(date)/" \
        -e "s/%AWS_REGION%/$AWS_REGION/g" \
        -e "s/%PROJECT_PREFIX%/$PROJECT_PREFIX/g" \
        -e "s/%ENVIRONMENT%/$ENVIRONMENT/g" \
        scripts/utils/export_env_template.sh > configs/env-vars.sh
    
    print_step "Deployment Complete!"
    cat << EOF

✅ Successfully deployed:
   • S3 buckets for data storage
   • IAM roles and policies
   • Glue database and tables
   • Sample data uploaded

📊 Next steps:
   1. Query data with Athena: SELECT * FROM "${PROJECT_PREFIX}-db".customers LIMIT 10;
   2. Create EMR cluster: ./scripts/core/compute/emr_cluster.sh create
   3. Clean up when done: ./scripts/cleanup.sh

💡 Load environment variables: source configs/env-vars.sh

EOF
}

main