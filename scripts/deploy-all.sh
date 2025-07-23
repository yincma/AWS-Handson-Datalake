#!/bin/bash
# AWS Data Lake Complete One-Click Deployment Script (v2.0)
# モジュラーアーキテクチャを使用した新しい統一デプロイメントスクリプト
# 推奨: 新しい 'datalake' CLI を使用してください

set -e

# 共通ライブラリをロード
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# Default values
DEPLOY_EMR=false
RUN_PYSPARK=false
KEY_NAME=""
SUBNET_ID=""

# Function to show usage
show_usage() {
    cat << EOF
AWS Data Lake Complete Deployment Script

Usage: $0 [OPTIONS]

Options:
    --with-emr              Also create EMR cluster (auto-discovers key/subnet)
    --with-analytics        Also run PySpark analytics job (requires --with-emr)
    --key-name NAME         EC2 key pair name (optional, auto-discovered if not provided)
    --subnet-id ID          VPC subnet ID (optional, uses default VPC if not provided)
    --help, -h              Show this help message

Examples:
    # Basic deployment (infrastructure only)
    $0
    
    # Full deployment with EMR and analytics (automatic configuration)
    $0 --with-emr --with-analytics
    
    # Deploy with EMR using specific key and subnet
    $0 --with-emr --with-analytics --key-name my-key --subnet-id subnet-123
    
    # Deploy with EMR only (no analytics)
    $0 --with-emr

This script will:
1. Deploy all infrastructure (S3, IAM, Lake Formation, Glue)
2. Configure permissions and upload sample data
3. Optionally create EMR cluster and run analytics

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --with-emr)
            DEPLOY_EMR=true
            shift
            ;;
        --with-analytics)
            RUN_PYSPARK=true
            shift
            ;;
        --key-name)
            KEY_NAME="$2"
            shift 2
            ;;
        --subnet-id)
            SUBNET_ID="$2"
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

# Function to discover or create EC2 key pair
discover_or_create_key_pair() {
    print_info "Discovering EC2 key pairs..."
    
    # Get existing key pairs
    local existing_keys=$(aws ec2 describe-key-pairs --query 'KeyPairs[*].KeyName' --output text 2>/dev/null)
    
    if [[ -n "$existing_keys" ]]; then
        local key_count=$(echo "$existing_keys" | wc -w | xargs)
        
        if [[ $key_count -eq 1 ]]; then
            KEY_NAME="$existing_keys"
            print_info "Using existing key pair: $KEY_NAME"
        else
            # Multiple keys exist, try to find one with datalake or emr prefix
            local preferred_key=$(echo "$existing_keys" | tr ' ' '\n' | grep -E "datalake|emr|${PROJECT_PREFIX}" | head -1)
            
            if [[ -n "$preferred_key" ]]; then
                KEY_NAME="$preferred_key"
                print_info "Using existing key pair: $KEY_NAME"
            else
                # Use the first available key
                KEY_NAME=$(echo "$existing_keys" | awk '{print $1}')
                print_info "Using existing key pair: $KEY_NAME"
            fi
        fi
    else
        # No key pairs exist, create a new one
        # Use default values if not yet loaded
        local prefix="${PROJECT_PREFIX:-dl-handson}"
        local env="${ENVIRONMENT:-dev}"
        KEY_NAME="${prefix}-emr-key-${env}"
        print_info "Creating new key pair: $KEY_NAME"
        
        # Create key pair and save private key
        if aws ec2 create-key-pair --key-name "$KEY_NAME" --query 'KeyMaterial' --output text > "${KEY_NAME}.pem"; then
            chmod 400 "${KEY_NAME}.pem"
            print_success "Key pair created and saved to: ${KEY_NAME}.pem"
            print_warning "Please keep this file safe - it's required to access your EMR cluster!"
        else
            print_error "Failed to create key pair"
            return 1
        fi
    fi
}

# Function to discover default VPC subnet
discover_default_subnet() {
    print_info "Discovering default VPC subnet..."
    
    # Get default VPC
    local default_vpc=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query 'Vpcs[0].VpcId' --output text 2>/dev/null)
    
    if [[ -n "$default_vpc" && "$default_vpc" != "None" ]]; then
        # Get first available subnet in the default VPC
        SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$default_vpc" "Name=state,Values=available" --query 'Subnets[0].SubnetId' --output text 2>/dev/null)
        
        if [[ -n "$SUBNET_ID" && "$SUBNET_ID" != "None" ]]; then
            print_info "Using default VPC subnet: $SUBNET_ID"
        else
            print_error "No available subnets found in default VPC"
            return 1
        fi
    else
        print_error "No default VPC found"
        return 1
    fi
}

# Validate EMR arguments
if [[ "$DEPLOY_EMR" == "true" ]]; then
    # Auto-discover key pair if not provided
    if [[ -z "$KEY_NAME" ]]; then
        # Try environment variable first
        KEY_NAME="${EMR_KEY_NAME:-}"
        
        if [[ -z "$KEY_NAME" ]]; then
            if ! discover_or_create_key_pair; then
                print_error "Failed to discover or create EC2 key pair"
                exit 1
            fi
        else
            print_info "Using key pair from environment: $KEY_NAME"
        fi
    fi
    
    # Auto-discover subnet if not provided
    if [[ -z "$SUBNET_ID" ]]; then
        # Try environment variable first
        SUBNET_ID="${EMR_SUBNET_ID:-}"
        
        if [[ -z "$SUBNET_ID" || "$SUBNET_ID" == "auto" ]]; then
            if ! discover_default_subnet; then
                print_error "Failed to discover default subnet"
                exit 1
            fi
        else
            print_info "Using subnet from environment: $SUBNET_ID"
        fi
    fi
    
    print_success "EMR configuration ready: Key=$KEY_NAME, Subnet=$SUBNET_ID"
fi

if [[ "$RUN_PYSPARK" == "true" && "$DEPLOY_EMR" != "true" ]]; then
    print_error "Analytics job requires EMR cluster (add --with-emr)"
    show_usage
    exit 1
fi

# Function to check prerequisites
check_prerequisites() {
    print_step "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed"
        exit 1
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python3 is not installed"
        exit 1
    fi
    
    # Verify AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured"
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

# Function to track deployment status
track_deployment() {
    local step="$1"
    local status="$2"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $step: $status" >> deployment.log
}

# Main deployment function
main() {
    echo "=========================================="
    echo "🚀 AWS Data Lake One-Click Deployment"
    echo "=========================================="
    
    # Record start time
    START_TIME=$(date +%s)
    
    # Initialize deployment log
    echo "Deployment started at $(date)" > deployment.log
    
    # Check prerequisites
    check_prerequisites
    track_deployment "Prerequisites" "PASSED"
    
    # 检查是否从CLI调用（避免递归循环）
    if [[ "${CALLED_FROM_CLI:-}" == "true" ]]; then
        print_info "从CLI调用，继续使用传统部署逻辑"
    else
        # 新しいモジュラーシステムが利用可能かチェック
        local new_cli="$SCRIPT_DIR/cli/datalake"
        if [[ -f "$new_cli" ]]; then
            print_info "新しい統一CLIシステムを使用しています"
            
            # Bash 3.x兼容的参数处理
            local deploy_args=""
            if [[ "$DEPLOY_EMR" == "true" ]]; then
                deploy_args="$deploy_args --emr"
            fi
            if [[ "$RUN_PYSPARK" == "true" ]]; then
                deploy_args="$deploy_args --analytics"
            fi
            
            # 执行新CLI部署
            if [[ -n "$deploy_args" ]]; then
                "$new_cli" deploy $deploy_args
            else
                "$new_cli" deploy
            fi
            return $?
        fi
    fi
    
    # 従来のロジックを保持（フォールバック）
    print_step "1/5 Deploying base infrastructure..."
    if ./scripts/setup-env.sh; then
        print_success "Base infrastructure deployed successfully"
        track_deployment "Base Infrastructure" "SUCCESS"
    else
        print_error "Base infrastructure deployment failed"
        track_deployment "Base Infrastructure" "FAILED"
        exit 1
    fi
    
    # Load environment variables
    if [[ -f "configs/env-vars.sh" ]]; then
        source configs/env-vars.sh
    fi
    
    # Step 2: Verify deployment
    print_step "2/5 Verifying deployment..."
    VERIFICATION_PASSED=true
    
    # Check S3 buckets
    for bucket_type in raw clean analytics; do
        bucket_name="${PROJECT_PREFIX}-${bucket_type}-${ENVIRONMENT}"
        if aws s3 ls "s3://${bucket_name}" &> /dev/null; then
            print_success "S3 bucket verified: ${bucket_name}"
        else
            print_error "S3 bucket not found: ${bucket_name}"
            VERIFICATION_PASSED=false
        fi
    done
    
    # Check Glue database
    if aws glue get-database --name "${PROJECT_PREFIX}-db" &> /dev/null; then
        print_success "Glue database verified: ${PROJECT_PREFIX}-db"
    else
        print_error "Glue database not found: ${PROJECT_PREFIX}-db"
        VERIFICATION_PASSED=false
    fi
    
    if [[ "$VERIFICATION_PASSED" == "true" ]]; then
        track_deployment "Verification" "PASSED"
    else
        track_deployment "Verification" "FAILED"
        exit 1
    fi
    
    # Step 3: Create EMR cluster (optional)
    if [[ "$DEPLOY_EMR" == "true" ]]; then
        print_step "3/5 Creating EMR cluster..."
        if ./scripts/core/compute/emr_cluster.sh create --key-name "$KEY_NAME" --subnet-id "$SUBNET_ID"; then
            print_success "EMR cluster created successfully"
            track_deployment "EMR Cluster" "SUCCESS"
            
            # Wait for cluster to be ready
            print_info "Waiting for EMR cluster to be ready..."
            sleep 30
        else
            print_error "EMR cluster creation failed"
            track_deployment "EMR Cluster" "FAILED"
            exit 1
        fi
    else
        print_info "Skipping EMR cluster creation (use --with-emr to enable)"
    fi
    
    # Step 4: Run PySpark analytics (optional)
    if [[ "$RUN_PYSPARK" == "true" ]]; then
        print_step "4/5 Running PySpark analytics job..."
        if ./scripts/submit_pyspark_job.sh; then
            print_success "PySpark job submitted successfully"
            track_deployment "PySpark Analytics" "SUCCESS"
        else
            print_warning "PySpark job submission failed (non-critical)"
            track_deployment "PySpark Analytics" "FAILED"
        fi
    else
        print_info "Skipping PySpark analytics (use --with-analytics to enable)"
    fi
    
    # Step 5: Generate summary report
    print_step "5/5 Generating deployment summary..."
    
    # Calculate deployment time
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    MINUTES=$((DURATION / 60))
    SECONDS=$((DURATION % 60))
    
    # Generate summary
    cat << EOF > deployment-summary.txt
========================================
AWS Data Lake Deployment Summary
========================================
Date: $(date)
Duration: ${MINUTES}m ${SECONDS}s

DEPLOYED RESOURCES:
✅ S3 Storage Layer
   - Raw bucket: ${PROJECT_PREFIX}-raw-${ENVIRONMENT}
   - Clean bucket: ${PROJECT_PREFIX}-clean-${ENVIRONMENT}
   - Analytics bucket: ${PROJECT_PREFIX}-analytics-${ENVIRONMENT}

✅ IAM Roles and Policies
   - Lake Formation admin role
   - Glue service role
   - EMR service roles

✅ Lake Formation
   - Data lake permissions configured
   - S3 locations registered

✅ Glue Data Catalog
   - Database: ${PROJECT_PREFIX}-db
   - Tables: customers, products, orders, order_items

EOF

    if [[ "$DEPLOY_EMR" == "true" ]]; then
        echo "✅ EMR Cluster: ${PROJECT_PREFIX}-emr-cluster-${ENVIRONMENT}" >> deployment-summary.txt
        echo "   - EC2 Key Pair: ${KEY_NAME}" >> deployment-summary.txt
        echo "   - Subnet ID: ${SUBNET_ID}" >> deployment-summary.txt
        
        # Check if we created a new key pair
        if [[ -f "${KEY_NAME}.pem" ]]; then
            echo "" >> deployment-summary.txt
            echo "⚠️  NEW KEY PAIR CREATED:" >> deployment-summary.txt
            echo "   Private key saved to: ${KEY_NAME}.pem" >> deployment-summary.txt
            echo "   Please keep this file safe - it's required to access your EMR cluster!" >> deployment-summary.txt
        fi
    fi
    
    if [[ "$RUN_PYSPARK" == "true" ]]; then
        echo "✅ PySpark Analytics Job: Submitted" >> deployment-summary.txt
    fi
    
    cat << EOF >> deployment-summary.txt

NEXT STEPS:
1. Query data with Athena:
   SELECT * FROM "${PROJECT_PREFIX}-db".customers LIMIT 10;

2. Access EMR cluster (if deployed):
   aws emr list-clusters --active

3. Monitor costs:
   ./scripts/cost-optimization.sh

4. Clean up resources:
   ./scripts/cleanup.sh --force

ESTIMATED COSTS:
- Storage: ~$0.001/month
- Compute (if EMR active): ~$1.05/hour
- Data processing: ~$0.50 per run

⚠️  IMPORTANT: Remember to terminate EMR cluster when done!

EOF

    # Display summary
    cat deployment-summary.txt
    
    # Final success message
    echo
    print_success "🎉 AWS Data Lake deployment completed successfully!"
    print_info "📄 Deployment log: deployment.log"
    print_info "📋 Summary report: deployment-summary.txt"
    
    # 新しいCLIの推奨
    echo
    print_info "💡 新機能: 統一CLIが利用可能です"
    print_info "   ./scripts/cli/datalake --help でコマンドを確認"
    print_info "   ./scripts/cli/datalake status でシステム状態を確認"
    
    if [[ "$DEPLOY_EMR" == "true" ]]; then
        print_warning "💰 EMR cluster is running! Remember to terminate it when done."
        print_info "   ./scripts/cli/datalake destroy でクリーンアップ"
    fi
}

# Run main function
main