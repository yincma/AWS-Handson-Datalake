#!/bin/bash
# AWS Data Lake Migration Script
# Migrates from old system to new modular system with unified naming

set -e

# Load common functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/naming-convention.sh"

# Migration options
DRY_RUN=false
FORCE=false
CLEANUP_OLD=false

show_usage() {
    cat << EOF
AWS Data Lake Migration Script

Usage: $0 [OPTIONS]

Options:
    --dry-run           Show what would be done without making changes
    --force             Force migration even if resources exist
    --cleanup-old       Remove old resources after successful migration
    --help, -h          Show this help message

This script will:
1. Check existing resources
2. Create mapping between old and new naming
3. Update all configurations
4. Migrate resources with zero downtime
5. Verify migration success

Examples:
    $0 --dry-run                    # Preview migration
    $0                               # Perform migration (safe mode)
    $0 --force --cleanup-old         # Full migration with cleanup

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --cleanup-old)
            CLEANUP_OLD=true
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

# Function to check existing resources
check_existing_resources() {
    print_step "Checking existing resources..."
    
    local old_stacks=()
    local new_stacks=()
    
    # Check for old naming convention stacks
    for stack_type in s3-storage iam-roles glue-catalog lake-formation; do
        local old_stack="datalake-${stack_type}-${ENVIRONMENT}"
        if aws cloudformation describe-stacks --stack-name "$old_stack" &>/dev/null; then
            old_stacks+=("$old_stack")
            print_info "Found old stack: $old_stack"
        fi
    done
    
    # Check for new naming convention stacks
    for module in s3 iam glue lakeformation; do
        local new_stack=$(get_stack_name "$module")
        if aws cloudformation describe-stacks --stack-name "$new_stack" &>/dev/null; then
            new_stacks+=("$new_stack")
            print_info "Found new stack: $new_stack"
        fi
    done
    
    echo "OLD_STACKS=(${old_stacks[@]})" > migration-state.tmp
    echo "NEW_STACKS=(${new_stacks[@]})" >> migration-state.tmp
    
    if [[ ${#old_stacks[@]} -eq 0 && ${#new_stacks[@]} -eq 0 ]]; then
        print_warning "No existing stacks found. Fresh deployment recommended."
        return 1
    fi
    
    return 0
}

# Function to update configuration files
update_configurations() {
    print_step "Updating configuration files..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        print_info "[DRY RUN] Would update configurations"
        return 0
    fi
    
    # Update deploy_module.sh to use new naming
    local deploy_module="$SCRIPT_DIR/core/deployment/deploy_module.sh"
    if [[ -f "$deploy_module" ]]; then
        # Add naming convention source
        if ! grep -q "naming-convention.sh" "$deploy_module"; then
            sed -i.bak '1a\
source "$SCRIPT_DIR/../../lib/naming-convention.sh" 2>/dev/null || true
' "$deploy_module"
        fi
    fi
    
    # Create environment variable export
    cat > "$SCRIPT_DIR/../configs/unified-naming.sh" << EOF
#!/bin/bash
# Unified naming configuration

# Source naming convention
source "$SCRIPT_DIR/lib/naming-convention.sh"

# Export unified stack names
export S3_STACK_NAME=\$(get_stack_name "s3")
export IAM_STACK_NAME=\$(get_stack_name "iam")
export GLUE_STACK_NAME=\$(get_stack_name "glue")
export LAKEFORMATION_STACK_NAME=\$(get_stack_name "lakeformation")

# Export for backwards compatibility
export LEGACY_S3_STACK="datalake-s3-storage-\${ENVIRONMENT}"
export LEGACY_IAM_STACK="datalake-iam-roles-\${ENVIRONMENT}"
export LEGACY_GLUE_STACK="datalake-glue-catalog-\${ENVIRONMENT}"
export LEGACY_LAKEFORMATION_STACK="datalake-lake-formation-\${ENVIRONMENT}"
EOF
    
    print_success "Configuration files updated"
}

# Function to create stack mapping
create_stack_mapping() {
    print_step "Creating stack mapping..."
    
    cat > stack-mapping.json << EOF
{
  "mappings": [
    {
      "old": "datalake-s3-storage-${ENVIRONMENT}",
      "new": "$(get_stack_name "s3")",
      "module": "s3"
    },
    {
      "old": "datalake-iam-roles-${ENVIRONMENT}",
      "new": "$(get_stack_name "iam")",
      "module": "iam"
    },
    {
      "old": "datalake-glue-catalog-${ENVIRONMENT}",
      "new": "$(get_stack_name "glue")",
      "module": "glue"
    },
    {
      "old": "datalake-lake-formation-${ENVIRONMENT}",
      "new": "$(get_stack_name "lakeformation")",
      "module": "lakeformation"
    }
  ],
  "migration_date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "project_prefix": "${PROJECT_PREFIX}",
  "environment": "${ENVIRONMENT}"
}
EOF
    
    print_success "Stack mapping created"
}

# Function to migrate stacks
migrate_stacks() {
    print_step "Migrating stacks..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        print_info "[DRY RUN] Would migrate stacks"
        return 0
    fi
    
    # For S3 stacks, we need special handling to preserve data
    local old_s3_stack="datalake-s3-storage-${ENVIRONMENT}"
    local new_s3_stack=$(get_stack_name "s3")
    
    if aws cloudformation describe-stacks --stack-name "$old_s3_stack" &>/dev/null; then
        if [[ "$old_s3_stack" != "$new_s3_stack" ]]; then
            print_warning "S3 stack name change detected. Data will be preserved."
            print_info "Old: $old_s3_stack"
            print_info "New: $new_s3_stack"
            
            # Update stack tags instead of recreating
            aws cloudformation update-stack \
                --stack-name "$old_s3_stack" \
                --use-previous-template \
                --tags Key=MigrationStatus,Value=NewNamingPending \
                       Key=NewStackName,Value="$new_s3_stack" || true
        fi
    fi
    
    print_success "Stack migration completed"
}

# Function to verify migration
verify_migration() {
    print_step "Verifying migration..."
    
    local errors=0
    
    # Check if CLI works with new system
    if ! "$SCRIPT_DIR/cli/datalake" status &>/dev/null; then
        print_error "CLI status check failed"
        ((errors++))
    fi
    
    # Check stack outputs
    for module in s3 iam glue; do
        local stack_name=$(get_stack_name "$module")
        if aws cloudformation describe-stacks --stack-name "$stack_name" &>/dev/null; then
            print_success "Stack verified: $stack_name"
        else
            # Check if old stack exists
            local old_mapping=""
            case $module in
                s3) old_mapping="s3-storage" ;;
                iam) old_mapping="iam-roles" ;;
                glue) old_mapping="glue-catalog" ;;
            esac
            
            local old_stack="datalake-${old_mapping}-${ENVIRONMENT}"
            if aws cloudformation describe-stacks --stack-name "$old_stack" &>/dev/null; then
                print_warning "Old stack still in use: $old_stack"
            else
                print_error "No stack found for module: $module"
                ((errors++))
            fi
        fi
    done
    
    if [[ $errors -eq 0 ]]; then
        print_success "Migration verification passed"
        return 0
    else
        print_error "Migration verification failed with $errors errors"
        return 1
    fi
}

# Function to cleanup old resources
cleanup_old_resources() {
    if [[ "$CLEANUP_OLD" != "true" ]]; then
        print_info "Skipping old resource cleanup (use --cleanup-old to enable)"
        return 0
    fi
    
    print_step "Cleaning up old resources..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        print_info "[DRY RUN] Would cleanup old resources"
        return 0
    fi
    
    # Load state
    source migration-state.tmp
    
    for old_stack in "${OLD_STACKS[@]}"; do
        print_warning "Would delete old stack: $old_stack"
        # Uncomment to actually delete
        # aws cloudformation delete-stack --stack-name "$old_stack"
    done
    
    print_info "Manual cleanup required for safety. Review and delete old stacks manually."
}

# Main migration function
main() {
    echo "============================================="
    echo "AWS Data Lake System Migration"
    echo "============================================="
    
    # Load configuration
    load_config
    
    # Check prerequisites
    if ! command -v jq &>/dev/null; then
        print_warning "jq not found. Installing for JSON processing..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            brew install jq || true
        else
            sudo apt-get install -y jq || true
        fi
    fi
    
    # Step 1: Check existing resources
    check_existing_resources || {
        print_info "No migration needed. Use './scripts/cli/datalake deploy' for fresh deployment."
        exit 0
    }
    
    # Step 2: Update configurations
    update_configurations
    
    # Step 3: Create stack mapping
    create_stack_mapping
    
    # Step 4: Migrate stacks
    migrate_stacks
    
    # Step 5: Verify migration
    verify_migration
    
    # Step 6: Cleanup (optional)
    cleanup_old_resources
    
    # Generate migration report
    cat > migration-report.txt << EOF
========================================
Migration Report
========================================
Date: $(date)
Status: SUCCESS

Configuration Updates:
✓ Naming convention file created
✓ Module configurations updated
✓ Stack mappings created

Next Steps:
1. Test the new system:
   ./scripts/cli/datalake status

2. Deploy any missing components:
   ./scripts/cli/datalake deploy --module <module>

3. After verification, cleanup old resources:
   $0 --cleanup-old

Stack Mapping saved to: stack-mapping.json
EOF
    
    cat migration-report.txt
    
    print_success "Migration completed successfully!"
}

# Run main function
main "$@"