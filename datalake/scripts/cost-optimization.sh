#!/bin/bash

# AWS Data Lake Cost Optimization Script
# This script helps optimize costs by managing resources and providing recommendations

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
    
    print_info "Project: $PROJECT_PREFIX"
    print_info "Region: $AWS_REGION"
    print_info "Environment: $ENVIRONMENT"
}

# Function to check current costs
check_current_costs() {
    print_step "Checking current AWS costs..."
    
    local start_date=$(date -d "7 days ago" +%Y-%m-%d)
    local end_date=$(date +%Y-%m-%d)
    
    print_info "Getting cost data from $start_date to $end_date"
    
    # Get cost and usage data
    local cost_response=$(aws ce get-cost-and-usage \
        --time-period Start="$start_date",End="$end_date" \
        --granularity DAILY \
        --metrics BlendedCost \
        --group-by Type=DIMENSION,Key=SERVICE \
        --region us-east-1 \
        --output json 2>/dev/null || echo '{"ResultsByTime": []}')
    
    if [[ "$cost_response" == '{"ResultsByTime": []}' ]]; then
        print_warning "No cost data available or insufficient permissions"
        return
    fi
    
    # Parse and display cost data
    echo "$cost_response" | python3 -c "
import json
import sys
from decimal import Decimal

data = json.load(sys.stdin)
total_cost = 0
service_costs = {}

for result in data['ResultsByTime']:
    for group in result['Groups']:
        service = group['Keys'][0]
        cost = float(group['Metrics']['BlendedCost']['Amount'])
        if service not in service_costs:
            service_costs[service] = 0
        service_costs[service] += cost
        total_cost += cost

print(f'Total cost (last 7 days): \${total_cost:.2f}')
print(f'Daily average: \${total_cost/7:.2f}')
print('')
print('Top services by cost:')
for service, cost in sorted(service_costs.items(), key=lambda x: x[1], reverse=True)[:10]:
    if cost > 0.01:
        print(f'  {service}: \${cost:.2f}')
"
}

# Function to check EMR cluster status and costs
check_emr_clusters() {
    print_step "Checking EMR clusters..."
    
    local clusters=$(aws emr list-clusters --region "$AWS_REGION" --active \
        --query "Clusters[?starts_with(Name, '$PROJECT_PREFIX')].{Id:Id,Name:Name,State:Status.State,Created:Status.Timeline.CreationDateTime}" \
        --output table 2>/dev/null || true)
    
    if [[ -z "$clusters" || "$clusters" == *"None"* ]]; then
        print_info "No active EMR clusters found."
        return
    fi
    
    print_warning "Active EMR clusters found:"
    echo "$clusters"
    
    # Calculate running time and estimated costs
    aws emr list-clusters --region "$AWS_REGION" --active \
        --query "Clusters[?starts_with(Name, '$PROJECT_PREFIX')]" \
        --output json | python3 -c "
import json
import sys
from datetime import datetime, timezone

clusters = json.load(sys.stdin)
now = datetime.now(timezone.utc)

for cluster in clusters:
    creation_time = datetime.fromisoformat(cluster['Status']['Timeline']['CreationDateTime'].replace('Z', '+00:00'))
    running_hours = (now - creation_time).total_seconds() / 3600
    
    # Estimate cost (rough calculation based on typical EMR pricing)
    estimated_cost_per_hour = 1.50  # Approximate for m5.xlarge master + 2 core nodes
    estimated_cost = running_hours * estimated_cost_per_hour
    
    print(f'')
    print(f'Cluster: {cluster[\"Name\"]}')
    print(f'  ID: {cluster[\"Id\"]}')
    print(f'  Running time: {running_hours:.1f} hours')
    print(f'  Estimated cost: \${estimated_cost:.2f}')
    
    if running_hours > 4:
        print(f'  ⚠️  LONG RUNNING: Consider terminating if not in use')
    if running_hours > 8:
        print(f'  🚨 VERY EXPENSIVE: Immediate attention required')
"
    
    print_warning "Remember to terminate EMR clusters when not in use!"
    print_info "Use: aws emr terminate-clusters --cluster-ids <cluster-id>"
}

# Function to check S3 storage costs
check_s3_storage() {
    print_step "Checking S3 storage usage..."
    
    local buckets=(
        "${PROJECT_PREFIX}-raw-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-clean-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-analytics-${ENVIRONMENT}"
        "${PROJECT_PREFIX}-athena-results-${ENVIRONMENT}"
    )
    
    local total_size=0
    
    for bucket in "${buckets[@]}"; do
        if aws s3api head-bucket --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null; then
            print_info "Analyzing bucket: $bucket"
            
            # Get bucket size
            local bucket_size=$(aws s3 ls s3://"$bucket" --recursive --human-readable --summarize 2>/dev/null | \
                grep "Total Size:" | awk '{print $3, $4}' || echo "0 Bytes")
            
            echo "  Size: $bucket_size"
            
            # Check lifecycle policies
            local lifecycle=$(aws s3api get-bucket-lifecycle-configuration --bucket "$bucket" --region "$AWS_REGION" 2>/dev/null || echo "None")
            if [[ "$lifecycle" == "None" ]]; then
                print_warning "  No lifecycle policy configured"
            else
                print_info "  Lifecycle policy configured"
            fi
            
            # Get object count and storage classes
            aws s3api list-objects-v2 --bucket "$bucket" --region "$AWS_REGION" \
                --query 'Contents[].StorageClass' --output text 2>/dev/null | \
                sort | uniq -c | while read count class; do
                    echo "  $count objects in ${class:-STANDARD} class"
                done
        else
            print_warning "Bucket $bucket does not exist or not accessible"
        fi
    done
}

# Function to check Glue job costs
check_glue_jobs() {
    print_step "Checking Glue jobs and crawlers..."
    
    # Check Glue jobs
    local jobs=$(aws glue get-jobs --region "$AWS_REGION" \
        --query "Jobs[?starts_with(Name, '$PROJECT_PREFIX')]" --output json 2>/dev/null || echo '[]')
    
    if [[ "$jobs" != '[]' ]]; then
        echo "$jobs" | python3 -c "
import json
import sys

jobs = json.load(sys.stdin)
print(f'Found {len(jobs)} Glue jobs:')
for job in jobs:
    print(f'  {job[\"Name\"]}')
    if 'MaxCapacity' in job:
        print(f'    Max Capacity: {job[\"MaxCapacity\"]} DPUs')
    if 'NumberOfWorkers' in job:
        print(f'    Workers: {job[\"NumberOfWorkers\"]}')
    if 'WorkerType' in job:
        print(f'    Worker Type: {job[\"WorkerType\"]}')
"
    else
        print_info "No Glue jobs found"
    fi
    
    # Check Glue crawlers
    local crawlers=$(aws glue get-crawlers --region "$AWS_REGION" \
        --query "CrawlerList[?starts_with(Name, '$PROJECT_PREFIX')]" --output json 2>/dev/null || echo '[]')
    
    if [[ "$crawlers" != '[]' ]]; then
        echo "$crawlers" | python3 -c "
import json
import sys

crawlers = json.load(sys.stdin)
print(f'Found {len(crawlers)} Glue crawlers:')
for crawler in crawlers:
    print(f'  {crawler[\"Name\"]} - State: {crawler[\"State\"]}')
    if crawler['State'] == 'RUNNING':
        print(f'    ⚠️  Currently running - consuming DPUs')
"
    else
        print_info "No Glue crawlers found"
    fi
}

# Function to provide cost optimization recommendations
provide_recommendations() {
    print_step "Cost Optimization Recommendations"
    
    cat << 'EOF'
🔧 EMR OPTIMIZATION:
   • Use Spot instances for non-critical workloads (60-90% cost savings)
   • Terminate clusters immediately after job completion
   • Use EMR Notebooks for development instead of keeping clusters running
   • Consider EMR Serverless for sporadic workloads

💾 S3 OPTIMIZATION:
   • Implement lifecycle policies to transition old data to IA/Glacier
   • Use S3 Intelligent-Tiering for automatic cost optimization
   • Enable S3 request metrics to identify unused data
   • Compress data before storing (Parquet with Snappy compression)

⚙️  GLUE OPTIMIZATION:
   • Use Glue Data Catalog instead of maintaining separate metadata stores
   • Optimize Glue job DPU allocation based on data size
   • Use Glue job bookmarks to process only new data
   • Schedule crawlers to run only when needed

📊 MONITORING:
   • Set up AWS Budgets with alerts at 50%, 80%, and 100% thresholds
   • Use AWS Cost Explorer for detailed cost analysis
   • Enable AWS Cost and Usage Reports for granular insights
   • Monitor CloudWatch metrics for resource utilization

🚀 GENERAL BEST PRACTICES:
   • Tag all resources for cost allocation
   • Use Reserved Instances for predictable workloads
   • Regularly review and clean up unused resources
   • Implement automated resource scheduling (stop/start)
EOF
}

# Function to auto-optimize EMR settings
optimize_emr_settings() {
    print_step "EMR Cost Optimization Settings"
    
    print_info "Recommended EMR cluster configuration for cost optimization:"
    
    cat << EOF

# EMR Cluster Launch Command with Cost Optimizations
aws emr create-cluster \\
    --name "${PROJECT_PREFIX}-optimized-cluster" \\
    --release-label emr-6.15.0 \\
    --instance-groups '[
        {
            "Name":"Master",
            "Market":"ON_DEMAND",
            "InstanceRole":"MASTER",
            "InstanceType":"m5.xlarge",
            "InstanceCount":1
        },
        {
            "Name":"Core",
            "Market":"SPOT",
            "BidPrice":"0.10",
            "InstanceRole":"CORE",
            "InstanceType":"m5.xlarge",
            "InstanceCount":2
        }
    ]' \\
    --applications Name=Spark Name=Hadoop \\
    --ec2-attributes '{
        "KeyName":"your-key-pair",
        "InstanceProfile":"EMR_EC2_DefaultRole",
        "SubnetId":"your-subnet-id"
    }' \\
    --service-role EMR_DefaultRole \\
    --enable-debugging \\
    --log-uri s3://${PROJECT_PREFIX}-logs-${ENVIRONMENT}/emr-logs/ \\
    --auto-terminate \\
    --configurations '[
        {
            "Classification":"spark-defaults",
            "Properties":{
                "spark.sql.adaptive.enabled":"true",
                "spark.sql.adaptive.coalescePartitions.enabled":"true",
                "spark.sql.adaptive.skewJoin.enabled":"true"
            }
        }
    ]' \\
    --region ${AWS_REGION}

Key optimizations:
- Uses Spot instances for Core nodes (60-90% cost savings)
- Auto-terminates when idle
- Optimized Spark configuration for efficiency
- Centralized logging for debugging
EOF
}

# Function to create cost alert script
create_cost_alert_script() {
    print_step "Creating daily cost monitoring script..."
    
    cat > "scripts/daily-cost-check.sh" << 'EOF'
#!/bin/bash

# Daily Cost Check Script
# Run this daily to monitor your AWS costs

AWS_REGION=${AWS_REGION:-us-east-1}
THRESHOLD=${DAILY_THRESHOLD:-10}

# Get yesterday's costs
START_DATE=$(date -d "1 day ago" +%Y-%m-%d)
END_DATE=$(date +%Y-%m-%d)

echo "Checking costs for $START_DATE..."

COST=$(aws ce get-cost-and-usage \
    --time-period Start="$START_DATE",End="$END_DATE" \
    --granularity DAILY \
    --metrics BlendedCost \
    --region us-east-1 \
    --query 'ResultsByTime[0].Total.BlendedCost.Amount' \
    --output text 2>/dev/null || echo "0")

echo "Daily cost: \$${COST}"

if (( $(echo "$COST > $THRESHOLD" | bc -l) )); then
    echo "⚠️  Cost exceeds threshold of \$${THRESHOLD}!"
    echo "Consider reviewing your resources."
else
    echo "✅ Cost within acceptable range."
fi
EOF
    
    chmod +x scripts/daily-cost-check.sh
    print_info "Created scripts/daily-cost-check.sh"
    print_info "Run this script daily or set up a cron job: 0 9 * * * /path/to/daily-cost-check.sh"
}

# Function to show cost dashboard
show_cost_dashboard() {
    print_step "AWS Cost Management Resources"
    
    cat << EOF

📊 AWS Cost Management Links:
   
   Billing Dashboard:
   https://console.aws.amazon.com/billing/home#/

   Cost Explorer:
   https://console.aws.amazon.com/ce/home

   Budgets:
   https://console.aws.amazon.com/billing/home#/budgets

   Cost and Usage Reports:
   https://console.aws.amazon.com/billing/home#/reports

   CloudWatch Billing Alarms:
   https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#alarmsV2:

💡 Quick Actions:
   • Set up billing alerts in CloudWatch
   • Create a monthly budget with email notifications
   • Enable AWS Cost Anomaly Detection
   • Review AWS Trusted Advisor cost optimization recommendations

EOF
}

# Main function
main() {
    print_info "AWS Data Lake Cost Optimization Tool"
    print_info "======================================"
    
    load_config
    
    case "${1:-all}" in
        "costs")
            check_current_costs
            ;;
        "emr")
            check_emr_clusters
            ;;
        "s3")
            check_s3_storage
            ;;
        "glue")
            check_glue_jobs
            ;;
        "recommendations")
            provide_recommendations
            ;;
        "optimize-emr")
            optimize_emr_settings
            ;;
        "create-alerts")
            create_cost_alert_script
            ;;
        "dashboard")
            show_cost_dashboard
            ;;
        "all"|*)
            check_current_costs
            check_emr_clusters
            check_s3_storage
            check_glue_jobs
            provide_recommendations
            show_cost_dashboard
            ;;
    esac
    
    print_info "Cost optimization check completed!"
}

# Show help
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    cat << EOF
AWS Data Lake Cost Optimization Script

Usage: $0 [OPTION]

Options:
  all              Run all cost checks and show recommendations (default)
  costs            Check current AWS costs
  emr              Check EMR cluster status and costs
  s3               Check S3 storage usage
  glue             Check Glue jobs and crawlers
  recommendations  Show cost optimization recommendations
  optimize-emr     Show optimized EMR configuration
  create-alerts    Create daily cost monitoring script
  dashboard        Show cost management dashboard links
  --help, -h       Show this help message

Examples:
  $0                    # Run all checks
  $0 emr               # Check only EMR costs
  $0 recommendations   # Show optimization tips
EOF
    exit 0
fi

# Run main function
main "$@"