#!/bin/bash

# =============================================================================
# Modular Interface Standard
# Version: 1.0.0
# Description: Define unified module interface specifications to ensure all modules follow the same API
# =============================================================================

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load common utility library
source "$SCRIPT_DIR/../common.sh"

readonly MODULE_INTERFACE_VERSION="1.0"

# =============================================================================
# Module Interface Specifications
# =============================================================================

# List of standard functions that each module must implement
readonly REQUIRED_MODULE_FUNCTIONS=(
    "validate"      # Validate module configuration and prerequisites
    "deploy"        # Deploy module resources
    "status"        # Check module status
    "cleanup"       # Clean up module resources
    "rollback"      # Rollback module changes
)

# Optional module functions
readonly OPTIONAL_MODULE_FUNCTIONS=(
    "configure"     # Configure module
    "test"          # Test module functionality
    "monitor"       # Monitor module
    "backup"        # Backup module data
    "restore"       # Restore module data
)

# =============================================================================
# Module Auto Loader
# =============================================================================

# Find and load module file based on module name
load_module_if_needed() {
    local module_name="$1"
    local function_name="${module_name}_validate"  # Use validate as test function
    
    # If function already exists, module is already loaded
    if declare -F "$function_name" >/dev/null; then
        print_debug "Module already loaded: $module_name"
        return 0
    fi
    
    print_debug "Attempting to load module: $module_name"
    
    # Define possible module file paths
    local module_file_candidates=(
        "$PROJECT_ROOT/scripts/core/infrastructure/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/catalog/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/compute/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/monitoring/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/data_processing/${module_name}.sh"
        "$PROJECT_ROOT/scripts/core/${module_name}.sh"
    )
    
    # Try to load module file
    for module_file in "${module_file_candidates[@]}"; do
        if [[ -f "$module_file" ]]; then
            print_debug "Found module file: $module_file"
            
            # Load module file
            if source "$module_file"; then
                print_debug "Successfully loaded module: $module_name"
                
                # Verify module is correctly loaded (check if validate function exists)
                if declare -F "$function_name" >/dev/null; then
                    print_debug "Module validation passed: $module_name"
                    return 0
                else
                    print_warning "Module file loaded but validation function does not exist: $function_name"
                fi
            else
                print_error "Failed to load module file: $module_file"
            fi
        fi
    done
    
    print_error "Module file not found: $module_name"
    print_debug "Search paths: ${module_file_candidates[*]}"
    return 1
}

# =============================================================================
# Module Interface Executor
# =============================================================================

module_interface() {
    local action="$1"
    local module_name="$2"
    shift 2
    
    if [[ -z "$action" || -z "$module_name" ]]; then
        print_error "Usage: module_interface <action> <module_name> [args...]"
        return 1
    fi
    
    local function_name="${module_name}_${action}"
    
    # Try to load module (if not already loaded)
    if ! load_module_if_needed "$module_name"; then
        print_error "Unable to load module: $module_name"
        return 1
    fi
    
    # Check if function exists
    if ! declare -F "$function_name" >/dev/null; then
        print_error "Module function does not exist: $function_name"
        print_info "Make sure module '$module_name' implements the '$action' function"
        return 1
    fi
    
    # Record operation start
    print_debug "Executing module operation: $module_name.$action"
    start_timer "${module_name}_${action}"
    
    # Execute module function
    local exit_code=0
    if "$function_name" "$@"; then
        print_success "Module operation successful: $module_name.$action"
    else
        exit_code=$?
        print_error "Module operation failed: $module_name.$action (exit code: $exit_code)"
    fi
    
    # Record operation completion
    end_timer "${module_name}_${action}"
    
    return $exit_code
}

# =============================================================================
# Module Validator
# =============================================================================

validate_module_implementation() {
    local module_name="$1"
    local module_file="$2"
    
    if [[ -z "$module_name" || -z "$module_file" ]]; then
        print_error "Usage: validate_module_implementation <module_name> <module_file>"
        return 1
    fi
    
    if [[ ! -f "$module_file" ]]; then
        print_error "Module file does not exist: $module_file"
        return 1
    fi
    
    print_step "Validating module implementation: $module_name"
    
    # Load module file
    source "$module_file"
    
    local missing_functions=()
    local validation_errors=0
    
    # Check required functions
    for func in "${REQUIRED_MODULE_FUNCTIONS[@]}"; do
        local function_name="${module_name}_${func}"
        
        if ! declare -F "$function_name" >/dev/null; then
            missing_functions+=("$function_name")
            validation_errors=$((validation_errors + 1))
        else
            print_debug "✓ Found required function: $function_name"
        fi
    done
    
    # Check optional functions
    local optional_functions=()
    for func in "${OPTIONAL_MODULE_FUNCTIONS[@]}"; do
        local function_name="${module_name}_${func}"
        
        if declare -F "$function_name" >/dev/null; then
            optional_functions+=("$function_name")
            print_debug "✓ Found optional function: $function_name"
        fi
    done
    
    # Output validation results
    if [[ $validation_errors -eq 0 ]]; then
        print_success "Module '$module_name' implementation validation passed"
        print_info "Implemented optional functions: ${#optional_functions[@]}"
        
        if [[ ${#optional_functions[@]} -gt 0 ]]; then
            for func in "${optional_functions[@]}"; do
                print_debug "  - $func"
            done
        fi
        
        return 0
    else
        print_error "Module '$module_name' implementation validation failed"
        print_error "Missing $validation_errors required functions:"
        
        for func in "${missing_functions[@]}"; do
            print_error "  - $func"
        done
        
        return 1
    fi
}

# =============================================================================
# Batch Module Operations
# =============================================================================

execute_batch_operation() {
    local operation="$1"
    shift
    local modules=("$@")
    
    if [[ -z "$operation" || ${#modules[@]} -eq 0 ]]; then
        print_error "Usage: execute_batch_operation <operation> <module1> [module2] ..."
        return 1
    fi
    
    print_step "Batch executing operation: $operation"
    print_info "Number of modules: ${#modules[@]}"
    
    local success_count=0
    local failed_modules=()
    
    for module in "${modules[@]}"; do
        print_info "Processing module: $module"
        
        if module_interface "$operation" "$module"; then
            success_count=$((success_count + 1))
            print_success "✓ $module.$operation successful"
        else
            failed_modules+=("$module")
            print_error "✗ $module.$operation failed"
        fi
        
        echo  # Separator
    done
    
    # Output batch operation results
    print_step "Batch operation results"
    print_info "Successful: $success_count/${#modules[@]}"
    
    if [[ ${#failed_modules[@]} -gt 0 ]]; then
        print_error "Failed modules:"
        for module in "${failed_modules[@]}"; do
            print_error "  - $module"
        done
        return 1
    else
        print_success "All module operations completed successfully"
        return 0
    fi
}

# =============================================================================
# Dependency Management
# =============================================================================

# Disable associative arrays for Bash 3.x compatibility
# declare -A MODULE_DEPENDENCIES
MODULE_DEPENDENCIES=""

register_module_dependency() {
    # Disabled for Bash 3.x compatibility
    print_debug "Module dependency registration is disabled (Bash 3.x compatibility)"
}

get_module_dependencies() {
    # Disabled for Bash 3.x compatibility
    print_debug "Module dependency retrieval is disabled (Bash 3.x compatibility)"
}

resolve_deployment_order() {
    local modules=("$@")
    local resolved_order=()
    local processed=()
    
    print_debug "Resolving deployment order, input modules: ${modules[*]}"
    
    # Simple dependency resolution (simplified version of topological sort)
    # Note: This is a basic implementation, real projects may need more complex algorithms
    
    local remaining_modules=("${modules[@]}")
    
    while [[ ${#remaining_modules[@]} -gt 0 ]]; do
        local progress_made=false
        local new_remaining=()
        
        for module in "${remaining_modules[@]}"; do
            local dependencies
            dependencies=$(get_module_dependencies "$module")
            
            if [[ -z "$dependencies" ]]; then
                # No dependencies, can be processed directly
                resolved_order+=("$module")
                processed+=("$module")
                progress_made=true
                print_debug "Adding module without dependencies: $module"
            else
                # Check if all dependencies have been processed
                local all_deps_resolved=true
                IFS=',' read -ra deps <<< "$dependencies"
                
                for dep in "${deps[@]}"; do
                    if [[ ! " ${processed[*]} " =~ " $dep " ]]; then
                        all_deps_resolved=false
                        break
                    fi
                done
                
                if [[ "$all_deps_resolved" == true ]]; then
                    resolved_order+=("$module")
                    processed+=("$module")
                    progress_made=true
                    print_debug "Adding module with satisfied dependencies: $module"
                else
                    new_remaining+=("$module")
                fi
            fi
        done
        
        remaining_modules=("${new_remaining[@]}")
        
        # Detect circular dependencies
        if [[ "$progress_made" == false && ${#remaining_modules[@]} -gt 0 ]]; then
            print_error "Detected circular dependencies or unsatisfied dependencies:"
            for module in "${remaining_modules[@]}"; do
                local deps
                deps=$(get_module_dependencies "$module")
                print_error "  $module -> $deps"
            done
            return 1
        fi
    done
    
    print_debug "Resolved deployment order: ${resolved_order[*]}"
    printf '%s\n' "${resolved_order[@]}"
}

# =============================================================================
# Module Lifecycle Management
# =============================================================================

deploy_modules_in_order() {
    local modules=("$@")
    
    print_step "Deploy modules in dependency order"
    
    # Resolve deployment order
    local ordered_modules
    if ! ordered_modules=($(resolve_deployment_order "${modules[@]}")); then
        print_error "Unable to resolve deployment order"
        return 1
    fi
    
    print_info "Deployment order: ${ordered_modules[*]}"
    
    # Deploy in order
    local deployed_modules=()
    
    for module in "${ordered_modules[@]}"; do
        print_info "Deploying module: $module"
        
        # Validate module
        if ! module_interface "validate" "$module"; then
            print_error "Module validation failed: $module"
            
            # Rollback already deployed modules
            if [[ ${#deployed_modules[@]} -gt 0 ]]; then
                print_warning "Rolling back already deployed modules..."
                rollback_modules "${deployed_modules[@]}"
            fi
            
            return 1
        fi
        
        # Deploy module
        if ! module_interface "deploy" "$module"; then
            print_error "Module deployment failed: $module"
            
            # Rollback already deployed modules
            if [[ ${#deployed_modules[@]} -gt 0 ]]; then
                print_warning "Rolling back already deployed modules..."
                rollback_modules "${deployed_modules[@]}"
            fi
            
            return 1
        fi
        
        deployed_modules+=("$module")
        print_success "Module deployment successful: $module"
    done
    
    print_success "All module deployments completed"
    return 0
}

rollback_modules() {
    local modules=("$@")
    
    print_step "Rolling back modules"
    
    # Rollback in reverse order
    local reversed_modules=()
    for ((i=${#modules[@]}-1; i>=0; i--)); do
        reversed_modules+=("${modules[i]}")
    done
    
    print_info "Rollback order: ${reversed_modules[*]}"
    
    for module in "${reversed_modules[@]}"; do
        print_info "Rolling back module: $module"
        
        if module_interface "rollback" "$module"; then
            print_success "Module rollback successful: $module"
        else
            print_error "Module rollback failed: $module"
        fi
    done
}

# =============================================================================
# Module Status Check
# =============================================================================

check_all_modules_status() {
    local modules=("$@")
    
    print_step "Checking all module status"
    
    local healthy_count=0
    local unhealthy_modules=()
    
    for module in "${modules[@]}"; do
        if module_interface "status" "$module"; then
            healthy_count=$((healthy_count + 1))
            print_success "✓ $module: Healthy"
        else
            unhealthy_modules+=("$module")
            print_error "✗ $module: Unhealthy"
        fi
    done
    
    # Output status summary
    echo
    print_info "Status summary:"
    print_info "Healthy modules: $healthy_count/${#modules[@]}"
    
    if [[ ${#unhealthy_modules[@]} -gt 0 ]]; then
        print_warning "Unhealthy modules:"
        for module in "${unhealthy_modules[@]}"; do
            print_warning "  - $module"
        done
        return 1
    else
        print_success "All modules are healthy"
        return 0
    fi
}

# =============================================================================
# Utility Functions
# =============================================================================

list_available_modules() {
    local modules_dir="${1:-$PROJECT_ROOT/scripts/core}"
    
    print_info "Available modules:"
    
    if [[ -d "$modules_dir" ]]; then
        find "$modules_dir" -name "*.sh" -type f | while read -r module_file; do
            local module_name
            module_name=$(basename "$module_file" .sh)
            echo "  - $module_name"
        done
    else
        print_warning "Module directory does not exist: $modules_dir"
    fi
}

generate_module_template() {
    local module_name="$1"
    local output_file="$2"
    
    if [[ -z "$module_name" ]]; then
        print_error "Usage: generate_module_template <module_name> [output_file]"
        return 1
    fi
    
    output_file="${output_file:-${module_name}_module.sh}"
    
    cat > "$output_file" << EOF
#!/bin/bash

# =============================================================================
# ${module_name^} Module
# Version: 1.0.0
# Description: Implementation of ${module_name} module
# =============================================================================

# Load common utility library
SCRIPT_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
source "\$SCRIPT_DIR/../lib/common.sh"

# =============================================================================
# Module Configuration
# =============================================================================

readonly ${module_name^^}_MODULE_VERSION="1.0.0"

# =============================================================================
# Required Function Implementation
# =============================================================================

${module_name}_validate() {
    print_info "Validating ${module_name} module configuration"
    
    # Add validation logic here
    
    return 0
}

${module_name}_deploy() {
    print_info "Deploying ${module_name} module"
    
    # Add deployment logic here
    
    return 0
}

${module_name}_status() {
    print_info "Checking ${module_name} module status"
    
    # Add status check logic here
    
    return 0
}

${module_name}_cleanup() {
    print_info "Cleaning up ${module_name} module resources"
    
    # Add cleanup logic here
    
    return 0
}

${module_name}_rollback() {
    print_info "Rolling back ${module_name} module changes"
    
    # Add rollback logic here
    
    return 0
}

# =============================================================================
# Optional Function Implementation
# =============================================================================

${module_name}_configure() {
    print_info "Configuring ${module_name} module"
    
    # Add configuration logic here
    
    return 0
}

${module_name}_test() {
    print_info "Testing ${module_name} module functionality"
    
    # Add testing logic here
    
    return 0
}

# =============================================================================
# Module-specific Helper Functions
# =============================================================================

# Add module-specific functions here

# =============================================================================
# If this script is executed directly
# =============================================================================

if [[ "\${BASH_SOURCE[0]}" == "\${0}" ]]; then
    # Load module interface
    source "\$SCRIPT_DIR/../lib/interfaces/module_interface.sh"
    
    # Execute passed operation
    if [[ \$# -gt 0 ]]; then
        module_interface "\$1" "${module_name}" "\${@:2}"
    else
        echo "Usage: \$0 <action> [args...]"
        echo "Available actions: validate, deploy, status, cleanup, rollback"
    fi
fi
EOF
    
    chmod +x "$output_file"
    print_success "Module template generated: $output_file"
}

# =============================================================================
# Main Function
# =============================================================================

show_interface_help() {
    cat << EOF
Modular Interface System v$MODULE_INTERFACE_VERSION

This system defines standardized module interfaces to ensure all modules follow a unified API.

Standard interface functions:
    validate        Validate module configuration and prerequisites
    deploy          Deploy module resources
    status          Check module status
    cleanup         Clean up module resources
    rollback        Rollback module changes

Optional interface functions:
    configure       Configure module
    test            Test module functionality
    monitor         Monitor module
    backup          Backup module data
    restore         Restore module data

Usage examples:
    # Execute single module operation
    module_interface deploy s3_storage

    # Batch operations
    execute_batch_operation validate s3_storage iam_roles glue_catalog

    # Check module status
    check_all_modules_status s3_storage iam_roles glue_catalog

    # Deploy in dependency order
    deploy_modules_in_order s3_storage iam_roles glue_catalog

EOF
}

# If this script is executed directly, show help
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
        show_interface_help
    else
        echo "This is a library file and should not be executed directly."
        echo "Use --help to view usage instructions."
        exit 1
    fi
fi