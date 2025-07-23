#!/bin/bash

# =============================================================================
# CloudFormation Template Validation Tool
# Version: 1.0.0
# Description: Validate CloudFormation template syntax, security and best practices
# =============================================================================

set -eu
# pipefailはBash 3.x互換性のため無効化

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load common utility library
source "$SCRIPT_DIR/lib/common.sh"

readonly TEMPLATE_VALIDATOR_VERSION="1.0.0"

# =============================================================================
# Global Variables
# =============================================================================

VALIDATION_ERRORS=0
VALIDATION_WARNINGS=0
TEMPLATE_DIR="${PROJECT_ROOT}/templates"
VALIDATION_REPORT=""
DETAILED_OUTPUT=false
STRICT_MODE=false

# =============================================================================
# Helper Functions
# =============================================================================

increment_error() {
    VALIDATION_ERRORS=$((VALIDATION_ERRORS + 1))
}

increment_warning() {
    VALIDATION_WARNINGS=$((VALIDATION_WARNINGS + 1))
}

print_validation_error() {
    local template="$1"
    local message="$2"
    print_error "$(basename "$template"): $message"
    increment_error
}

print_validation_warning() {
    local template="$1"
    local message="$2"
    print_warning "$(basename "$template"): $message"
    increment_warning
}

print_validation_info() {
    local template="$1"
    local message="$2"
    if [[ "$DETAILED_OUTPUT" == true ]]; then
        print_info "$(basename "$template"): $message"
    fi
}

# =============================================================================
# CloudFormation Syntax Validation
# =============================================================================

validate_cloudformation_syntax() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "Validating CloudFormation syntax: $template_name"
    
    # Check if file exists
    if [[ ! -f "$template" ]]; then
        print_validation_error "$template" "Template file does not exist"
        return 1
    fi
    
    # Check if file is empty
    if [[ ! -s "$template" ]]; then
        print_validation_error "$template" "Template file is empty"
        return 1
    fi
    
    # AWS CLI syntax validation
    local validation_output
    if validation_output=$(aws cloudformation validate-template --template-body "file://$template" 2>&1); then
        print_validation_info "$template" "AWS syntax validation passed"
        
        # Check template description
        if echo "$validation_output" | grep -q '"Description"'; then
            print_validation_info "$template" "Contains template description"
        else
            print_validation_warning "$template" "Recommend adding template description"
        fi
        
        return 0
    else
        print_validation_error "$template" "AWS syntax validation failed: $validation_output"
        return 1
    fi
}

# =============================================================================
# CFN-Lint Validation
# =============================================================================

check_cfn_lint_available() {
    if command -v cfn-lint &>/dev/null; then
        return 0
    elif python3 -m cfn_lint --version &>/dev/null; then
        return 0
    else
        print_warning "cfn-lint not installed, skipping advanced validation"
        print_info "Installation command: pip install cfn-lint"
        return 1
    fi
}

validate_with_cfn_lint() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    if ! check_cfn_lint_available; then
        return 0
    fi
    
    print_debug "Executing cfn-lint validation: $template_name"
    
    local cfn_lint_cmd
    if command -v cfn-lint &>/dev/null; then
        cfn_lint_cmd="cfn-lint"
    else
        cfn_lint_cmd="python3 -m cfn_lint"
    fi
    
    # Execute cfn-lint, ignoring certain warnings
    local lint_output
    if lint_output=$($cfn_lint_cmd "$template" \
        --ignore-checks W3002 W3005 \
        --format parseable 2>&1); then
        
        print_validation_info "$template" "CFN-Lint validation passed"
        return 0
    else
        # Analyze output, distinguish errors and warnings
        while IFS= read -r line; do
            if [[ -n "$line" ]]; then
                if [[ "$line" =~ .*:[E][0-9]+:.* ]]; then
                    print_validation_error "$template" "CFN-Lint error: $line"
                elif [[ "$line" =~ .*:[W][0-9]+:.* ]]; then
                    print_validation_warning "$template" "CFN-Lint warning: $line"
                elif [[ "$line" =~ .*:[I][0-9]+:.* ]]; then
                    print_validation_info "$template" "CFN-Lint info: $line"
                fi
            fi
        done <<< "$lint_output"
        
        # If there are errors and in strict mode, return failure
        if [[ "$STRICT_MODE" == true ]] && echo "$lint_output" | grep -q ":[E][0-9]:"; then
            return 1
        fi
        
        return 0
    fi
}

# =============================================================================
# Custom Validation Rules
# =============================================================================

validate_security_rules() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "Validating security rules: $template_name"
    
    # Check S3 bucket encryption
    if grep -q "AWS::S3::Bucket" "$template"; then
        if ! grep -q "BucketEncryption\|ServerSideEncryptionConfiguration" "$template"; then
            print_validation_error "$template" "S3 bucket not configured with encryption"
        else
            print_validation_info "$template" "S3 bucket encryption configuration check passed"
        fi
        
        # Check public access blocking
        if ! grep -q "PublicAccessBlockConfiguration" "$template"; then
            print_validation_warning "$template" "S3 bucket not configured with public access blocking"
        else
            print_validation_info "$template" "S3 bucket public access blocking configuration check passed"
        fi
    fi
    
    # Check IAM policies
    if grep -q "AWS::IAM::" "$template"; then
        # Check for overly broad permissions
        if grep -q '"Effect": "Allow".*"Action": "\*"' "$template" || \
           grep -q '"Effect": "Allow".*"Resource": "\*"' "$template"; then
            print_validation_warning "$template" "Found potentially overly broad IAM permissions"
        fi
        
        # Check for root permissions
        if grep -q '"Principal": "\*"' "$template"; then
            print_validation_error "$template" "Found dangerous wildcard principal"
        fi
    fi
    
    # Check for hardcoded sensitive information
    local sensitive_patterns=(
        "AKIA[0-9A-Z]{16}"          # AWS Access Key
        "[0-9]{12}"                 # AWS Account ID (需要更精确的检查)
        "password.*['\"][^'\"]{8,}" # Possible password
    )
    
    for pattern in "${sensitive_patterns[@]}"; do
        if grep -qE "$pattern" "$template"; then
            print_validation_error "$template" "May contain hardcoded sensitive information: $pattern"
        fi
    done
}

validate_naming_conventions() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "Validating naming conventions: $template_name"
    
    # Check if resource naming uses parameters
    local resource_count=0
    local parameterized_count=0
    
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*[A-Za-z][A-Za-z0-9]*:$ ]]; then
            resource_count=$((resource_count + 1))
            
            # Check if resource name uses Ref or Sub functions
            local next_lines
            next_lines=$(grep -A 10 "$line" "$template" | head -10)
            if echo "$next_lines" | grep -q "Ref:\|!Ref\|Sub:\|!Sub"; then
                parameterized_count=$((parameterized_count + 1))
            fi
        fi
    done < <(grep -E "^[[:space:]]*[A-Za-z][A-Za-z0-9]*:$" "$template")
    
    if [[ $resource_count -gt 0 ]]; then
        local param_percentage=$((parameterized_count * 100 / resource_count))
        if [[ $param_percentage -lt 50 ]]; then
            print_validation_warning "$template" "Recommend more parameterized resource naming ($param_percentage% parameterized)"
        else
            print_validation_info "$template" "Good resource naming parameterization ($param_percentage%)"
        fi
    fi
}

validate_best_practices() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "Validating best practices: $template_name"
    
    # Check for description
    if ! grep -q "^Description:" "$template"; then
        print_validation_warning "$template" "Recommend adding template description"
    fi
    
    # Check for tags
    if ! grep -q "Tags:" "$template"; then
        print_validation_warning "$template" "Recommend adding tags to resources"
    fi
    
    # Check if conditions are used
    if grep -q "Conditions:" "$template"; then
        print_validation_info "$template" "Using conditional logic"
    fi
    
    # Check for outputs
    if ! grep -q "Outputs:" "$template"; then
        print_validation_warning "$template" "Recommend adding useful outputs"
    fi
    
    # Check template size (CloudFormation limit is 51200 bytes)
    local template_size
    template_size=$(wc -c < "$template")
    if [[ $template_size -gt 51200 ]]; then
        print_validation_error "$template" "Template size exceeds CloudFormation limit (${template_size} > 51200 bytes)"
    elif [[ $template_size -gt 40000 ]]; then
        print_validation_warning "$template" "Template size approaching CloudFormation limit (${template_size} bytes)"
    fi
}

validate_dependencies() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_debug "Validating dependencies: $template_name"
    
    # Check circular dependencies (simple check)
    local dependencies=()
    
    # Extract Ref references
    while IFS= read -r line; do
        if [[ "$line" =~ Ref:[[:space:]]*([A-Za-z][A-Za-z0-9]*) ]] || \
           [[ "$line" =~ !Ref[[:space:]]+([A-Za-z][A-Za-z0-9]*) ]]; then
            dependencies+=("${BASH_REMATCH[1]}")
        fi
    done < "$template"
    
    # Check if referencing non-existent resources
    # Simple check here, more complex logic requires parsing entire template structure
    if [[ ${#dependencies[@]} -gt 0 ]]; then
        print_validation_info "$template" "Found ${#dependencies[@]} resource references"
    fi
}

# =============================================================================
# Comprehensive Validation Functions
# =============================================================================

validate_single_template() {
    local template="$1"
    local template_name
    template_name=$(basename "$template")
    
    print_info "Validating template: $template_name"
    
    # Initialize template-specific counters
    local template_errors=0
    local template_warnings=0
    local initial_errors=$VALIDATION_ERRORS
    local initial_warnings=$VALIDATION_WARNINGS
    
    # Execute various validations
    validate_cloudformation_syntax "$template"
    validate_with_cfn_lint "$template"
    validate_security_rules "$template"
    validate_naming_conventions "$template"
    validate_best_practices "$template"
    validate_dependencies "$template"
    
    # Calculate template-specific errors and warnings
    template_errors=$((VALIDATION_ERRORS - initial_errors))
    template_warnings=$((VALIDATION_WARNINGS - initial_warnings))
    
    # Output template validation results
    if [[ $template_errors -eq 0 ]]; then
        if [[ $template_warnings -eq 0 ]]; then
            print_success "✓ $template_name: Validation passed, no issues"
        else
            print_info "⚠ $template_name: Validation passed with $template_warnings warnings"
        fi
    else
        print_error "✗ $template_name: Validation failed with $template_errors errors, $template_warnings warnings"
    fi
    
    echo  # Empty line separator
}

validate_all_templates() {
    local template_pattern="${1:-*.yaml}"
    local template_count=0
    
    print_step "Starting batch CloudFormation template validation"
    print_info "Template validator version: $TEMPLATE_VALIDATOR_VERSION"
    print_info "Template directory: $TEMPLATE_DIR"
    print_info "Template pattern: $template_pattern"
    
    # Check if template directory exists
    if [[ ! -d "$TEMPLATE_DIR" ]]; then
        handle_error 1 "Template directory does not exist: $TEMPLATE_DIR"
    fi
    
    # Find template files
    local templates=()
    while IFS= read -r -d '' template; do
        templates+=("$template")
        template_count=$((template_count + 1))
    done < <(find "$TEMPLATE_DIR" -name "$template_pattern" -type f -print0)
    
    if [[ $template_count -eq 0 ]]; then
        print_warning "No matching template files found: $template_pattern"
        return 0
    fi
    
    print_info "Found $template_count template files"
    echo
    
    # Validate each template
    local current=0
    for template in "${templates[@]}"; do
        current=$((current + 1))
        show_progress $current $template_count "Validating..."
        validate_single_template "$template"
    done
    
    finish_progress
}

# =============================================================================
# Report Generation
# =============================================================================

generate_validation_report() {
    local output_file="${1:-validation_report.txt}"
    
    print_step "Generating validation report"
    
    local report_content
    report_content=$(cat << EOF
CloudFormation Template Validation Report
========================
Generation Time: $(date)
Validator Version: $TEMPLATE_VALIDATOR_VERSION
Template Directory: $TEMPLATE_DIR

Validation Summary:
========
Total Errors: $VALIDATION_ERRORS
Total Warnings: $VALIDATION_WARNINGS
Validation Status: $([[ $VALIDATION_ERRORS -eq 0 ]] && echo "PASSED" || echo "FAILED")

Recommendations:
====
EOF
    )
    
    if [[ $VALIDATION_ERRORS -gt 0 ]]; then
        report_content+=$'\n- Fix all errors before deployment'
    fi
    
    if [[ $VALIDATION_WARNINGS -gt 0 ]]; then
        report_content+=$'\n- Consider fixing warnings to improve template quality'
    fi
    
    if [[ $VALIDATION_ERRORS -eq 0 && $VALIDATION_WARNINGS -eq 0 ]]; then
        report_content+=$'\n- All templates validated successfully, safe to deploy'
    fi
    
    # Write report file
    echo "$report_content" > "$output_file"
    print_success "Validation report saved: $output_file"
}

# =============================================================================
# Utility Functions
# =============================================================================

install_cfn_lint() {
    print_step "Installing cfn-lint"
    
    if command -v pip3 &>/dev/null; then
        pip3 install cfn-lint
    elif command -v pip &>/dev/null; then
        pip install cfn-lint
    else
        print_error "pip not found, please install cfn-lint manually"
        return 1
    fi
    
    print_success "cfn-lint installation complete"
}

fix_common_issues() {
    local template="$1"
    local backup_file="${template}.backup"
    
    print_info "Attempting to fix common issues: $(basename "$template")"
    
    # Create backup
    cp "$template" "$backup_file"
    
    # Fix common format issues
    # Remove trailing spaces
    sed -i.tmp 's/[[:space:]]*$//' "$template" && rm "${template}.tmp"
    
    # Ensure file ends with newline
    [[ -n "$(tail -c1 "$template")" ]] && echo >> "$template"
    
    print_info "Basic fixes complete, backup saved as: $(basename "$backup_file")"
}

# =============================================================================
# Main Function and CLI
# =============================================================================

show_help() {
    cat << EOF
CloudFormation Template Validator v$TEMPLATE_VALIDATOR_VERSION

Usage: $0 [options] [template pattern]

Options:
    -h, --help              Show help information
    -v, --verbose           Verbose output
    -s, --strict            Strict mode (warnings treated as errors)
    -d, --template-dir DIR  Specify template directory (default: templates/)
    -r, --report FILE       Generate validation report to specified file
    -f, --fix               Attempt to fix common issues automatically
    --install-cfn-lint      Install cfn-lint tool
    --single FILE           Validate single template file

Parameters:
    template pattern        Template file pattern to validate (default: *.yaml)

Examples:
    $0                          # Validate all YAML templates
    $0 s3*.yaml                 # Validate YAML templates starting with s3
    $0 --single templates/iam-roles-policies.yaml  # Validate single template
    $0 --verbose --report validation.txt  # Verbose output and generate report
    $0 --strict                 # Strict mode validation
    $0 --install-cfn-lint       # Install cfn-lint tool

EOF
}

main() {
    local template_pattern="*.yaml"
    local single_template=""
    local report_file=""
    local auto_fix=false
    local install_lint=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--verbose)
                DETAILED_OUTPUT=true
                LOG_LEVEL="DEBUG"
                shift
                ;;
            -s|--strict)
                STRICT_MODE=true
                shift
                ;;
            -d|--template-dir)
                TEMPLATE_DIR="$2"
                shift 2
                ;;
            -r|--report)
                report_file="$2"
                shift 2
                ;;
            -f|--fix)
                auto_fix=true
                shift
                ;;
            --install-cfn-lint)
                install_lint=true
                shift
                ;;
            --single)
                single_template="$2"
                shift 2
                ;;
            -*)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
            *)
                template_pattern="$1"
                shift
                ;;
        esac
    done
    
    # Validate prerequisites
    validate_prerequisites
    
    # Install cfn-lint (if requested)
    if [[ "$install_lint" == true ]]; then
        install_cfn_lint
        exit 0
    fi
    
    # Start validation
    start_timer "validation"
    
    if [[ -n "$single_template" ]]; then
        # Validate single template
        if [[ ! -f "$single_template" ]]; then
            handle_error 1 "Template file does not exist: $single_template"
        fi
        
        validate_single_template "$single_template"
        
        # Auto fix (if requested)
        if [[ "$auto_fix" == true ]]; then
            fix_common_issues "$single_template"
        fi
    else
        # Batch validation
        validate_all_templates "$template_pattern"
    fi
    
    end_timer "validation"
    
    # Generate report
    if [[ -n "$report_file" ]]; then
        generate_validation_report "$report_file"
    fi
    
    # Output final results
    echo
    print_step "Validation Results Summary"
    
    if [[ $VALIDATION_ERRORS -eq 0 ]]; then
        if [[ $VALIDATION_WARNINGS -eq 0 ]]; then
            print_success "🎉 All templates validated successfully, no issues found!"
        else
            print_info "✅ All templates validated successfully, but with $VALIDATION_WARNINGS warnings"
            if [[ "$STRICT_MODE" == true ]]; then
                print_error "Warnings treated as errors in strict mode"
                exit 1
            fi
        fi
        exit 0
    else
        print_error "❌ Found $VALIDATION_ERRORS errors and $VALIDATION_WARNINGS warnings"
        echo
        print_info "Fix recommendations:"
        print_info "1. Review the error messages above and fix templates"
        print_info "2. Re-run validation to ensure issues are resolved"
        print_info "3. Consider using --fix option to automatically fix common issues"
        exit 1
    fi
}

# If running this script directly, execute main function
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi