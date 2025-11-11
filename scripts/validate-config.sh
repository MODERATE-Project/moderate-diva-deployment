#!/bin/bash

# DIVA Configuration Validation Script
# Validates mandatory configuration variables to prevent deployment failures

set -e # Exit on any error

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Symbols
ERROR_SYMBOL="❌"
SUCCESS_SYMBOL="✅"
WARNING_SYMBOL="⚠️"

# -----------------------------------------------------------------------------
# UI Functions
# -----------------------------------------------------------------------------

# Print header
print_header() {
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                 Validating Configuration                   ║"
    echo "╚════════════════════════════════════════════════════════════╝"
}

# Print error and exit
print_error() {
    echo -e "${RED}${ERROR_SYMBOL} ERROR: $1${NC}"
    shift
    while [[ $# -gt 0 ]]; do
        echo "   $1"
        shift
    done
    exit 1
}

# Print warning
print_warning() {
    echo -e "${YELLOW}${WARNING_SYMBOL} WARNING: $1${NC}"
    shift
    while [[ $# -gt 0 ]]; do
        echo "   $1"
        shift
    done
}

# Print success
print_success() {
    echo -e "${GREEN}${SUCCESS_SYMBOL} $1${NC}"
}

# -----------------------------------------------------------------------------
# Core Logic
# -----------------------------------------------------------------------------

# Load environment variables from .env file
load_env_variables() {
    if [ ! -f .env ]; then
        print_error ".env file not found!" \
            "Please copy .env.default to .env and configure the variables." \
            "Command: cp .env.default .env"
    fi

    # The following block sources the .env file.
    # 'set -o allexport' makes all defined variables exported to the environment.
    # It includes a fallback mechanism to parse the file line by line
    # if sourcing fails, which is more robust for files with syntax issues.
    set -o allexport
    # shellcheck source=/dev/null
    source .env 2>/dev/null || {
        while IFS= read -r line || [[ -n "$line" ]]; do
            # Skip comments and empty lines
            if [[ "$line" =~ ^[[:space:]]*# || -z "${line// /}" ]]; then
                continue
            fi
            # Export lines that look like variable assignments
            if [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
                export "$line"
            fi
        done <.env
    }
    set +o allexport
}

# -----------------------------------------------------------------------------
# Validation Functions
# -----------------------------------------------------------------------------

# Validate that a required variable is set
validate_required_variable() {
    local var_name="$1"
    local var_value="$2"
    local description="$3"

    if [ -z "$var_value" ]; then
        print_error "$var_name is not configured!" \
            "Please set $var_name to $description."
    fi
}

# Check for localhost usage and warn if found
check_localhost_warning() {
    if [ "$MACHINE_URL" = "localhost" ] || [ "$MACHINE_URL" = "127.0.0.1" ]; then
        print_warning "MACHINE_URL is set to '$MACHINE_URL'" \
            "This is suitable for development but may cause issues in production." \
            "For production deployments, use the actual hostname or IP address." \
            ""
    fi
}

# Run all configuration validation checks
run_validations() {
    validate_required_variable "MACHINE_URL" "$MACHINE_URL" "the hostname or IP where services will be accessible"
    validate_required_variable "PROJECT_NAME" "$PROJECT_NAME" "a unique identifier for this deployment"
    validate_required_variable "DIGITAL_TWIN_FOLDER" "$DIGITAL_TWIN_FOLDER" "the base directory for DIVA components"

    # Check for non-critical issues
    check_localhost_warning
}

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------

main() {
    print_header
    load_env_variables
    run_validations

    print_success "Configuration validation passed!"
    echo ""
}

# Run main function
main "$@"
