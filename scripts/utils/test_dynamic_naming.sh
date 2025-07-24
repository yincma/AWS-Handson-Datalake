#!/bin/bash
# Test script for dynamic naming mechanism
# Verifies that all stack names and dependencies are correctly resolved

set -e

# Load common functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

print_step "Testing Dynamic Naming Mechanism"

# Load configuration
load_config

print_info "Configuration:"
print_info "  PROJECT_PREFIX: ${PROJECT_PREFIX}"
print_info "  ENVIRONMENT: ${ENVIRONMENT}"
print_info "  AWS_REGION: ${AWS_REGION}"

echo ""
print_step "Testing Stack Name Generation"

# Test direct stack name generation
print_info "Testing get_stack_name function:"
s3_stack=$(get_stack_name "s3")
iam_stack=$(get_stack_name "iam")
glue_stack=$(get_stack_name "glue")
lakeformation_stack=$(get_stack_name "lakeformation")

print_info "  S3 Stack: $s3_stack"
print_info "  IAM Stack: $iam_stack"
print_info "  Glue Stack: $glue_stack"
print_info "  Lake Formation Stack: $lakeformation_stack"

echo ""
print_step "Testing Dependency Stack Name Resolution"

# Test dependency resolution
print_info "Testing get_dependency_stack_name function:"
s3_dep=$(get_dependency_stack_name "s3_storage")
iam_dep=$(get_dependency_stack_name "iam_roles")
glue_dep=$(get_dependency_stack_name "glue_catalog")

print_info "  S3 Dependency: $s3_dep"
print_info "  IAM Dependency: $iam_dep"
print_info "  Glue Dependency: $glue_dep"

echo ""
print_step "Testing CloudFormation Export Names"

# Test export name generation
print_info "Expected Export Names:"
print_info "  S3 Raw Bucket ARN: ${s3_dep}-RawDataBucketArn"
print_info "  S3 Clean Bucket ARN: ${s3_dep}-CleanDataBucketArn"
print_info "  S3 Analytics Bucket ARN: ${s3_dep}-AnalyticsDataBucketArn"
print_info "  IAM Glue Role ARN: ${iam_dep}-GlueCrawlerRoleArn"
print_info "  Glue Database Name: ${glue_dep}-GlueDatabaseName"

echo ""
print_step "Checking Actual Stack Status"

# Check if stacks exist
print_info "Checking stack existence:"
for stack_type in s3_storage iam_roles glue_catalog lake_formation; do
    stack_name=$(get_dependency_stack_name "$stack_type")
    if check_stack_exists "$stack_name"; then
        status=$(get_stack_status "$stack_name")
        print_success "  $stack_type: EXISTS (Status: $status)"
        
        # Check exports for S3 stack
        if [[ "$stack_type" == "s3_storage" && "$status" == "CREATE_COMPLETE" ]]; then
            print_info "  Checking exports for $stack_name:"
            exports=$(aws cloudformation list-exports --query "Exports[?starts_with(Name, \`${stack_name}\`)].Name" --output text 2>/dev/null || echo "")
            if [[ -n "$exports" ]]; then
                for export in $exports; do
                    print_info "    - $export"
                done
            else
                print_warning "    No exports found"
            fi
        fi
    else
        print_warning "  $stack_type: NOT FOUND"
    fi
done

echo ""
print_step "Testing Parameter Passing Simulation"

# Simulate parameter passing for each module
print_info "IAM Module Parameters:"
s3_stack_param=$(get_dependency_stack_name "s3_storage")
print_info "  S3StackName=$s3_stack_param"

print_info "Glue Module Parameters:"
print_info "  S3StackName=$s3_stack_param"
iam_stack_param=$(get_dependency_stack_name "iam_roles")
print_info "  IAMStackName=$iam_stack_param"

print_info "Lake Formation Module Parameters:"
print_info "  S3StackName=$s3_stack_param"
print_info "  IAMStackName=$iam_stack_param"
glue_stack_param=$(get_dependency_stack_name "glue_catalog")
print_info "  GlueStackName=$glue_stack_param"

echo ""
print_step "Summary"

# Check if naming is consistent
if [[ "$s3_stack" == "$s3_dep" || "$s3_dep" =~ -s3- || "$s3_dep" =~ -s3_storage- ]]; then
    print_success "Stack naming is consistent"
else
    print_warning "Stack naming might have inconsistencies"
    print_info "  Direct name: $s3_stack"
    print_info "  Dependency name: $s3_dep"
fi

# Final recommendation
echo ""
print_step "Recommendations"

if check_stack_exists "$s3_dep"; then
    s3_status=$(get_stack_status "$s3_dep")
    if [[ "$s3_status" == "CREATE_COMPLETE" || "$s3_status" == "UPDATE_COMPLETE" ]]; then
        print_success "S3 stack is healthy. You can proceed with deployment."
    else
        print_warning "S3 stack status is $s3_status. You may need to fix it first."
    fi
else
    print_info "S3 stack not found. Fresh deployment recommended:"
    print_info "  ./scripts/cli/datalake deploy --full"
fi

print_success "Dynamic naming test completed!"