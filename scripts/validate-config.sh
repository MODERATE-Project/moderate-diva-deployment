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

# Validate AUTH_KEY is a path to a valid SSH private key file
validate_auth_key() {
    # Check if AUTH_KEY is set
    if [ -z "$AUTH_KEY" ]; then
        print_error "AUTH_KEY is not configured!" \
            "Please set AUTH_KEY in your .env file to the path of your SSH private key." \
            "Example: AUTH_KEY=~/.ssh/id_ed25519"
    fi

    # Check if the key path points to a public key
    if [[ "$AUTH_KEY" == *.pub ]]; then
        print_error "AUTH_KEY is pointing to a public key file (.pub)!" \
            "Please set AUTH_KEY to the path of your PRIVATE key." \
            "The private key for '$AUTH_KEY' would typically be '${AUTH_KEY%.pub}'." \
            "Current value: '$AUTH_KEY'"
    fi

    # Expand tilde to home directory
    local key_path="${AUTH_KEY/#\~/$HOME}"

    # Check if file exists
    if [ ! -f "$key_path" ]; then
        print_error "AUTH_KEY file not found!" \
            "The SSH private key file specified in AUTH_KEY does not exist at this path: '$key_path'." \
            "Current AUTH_KEY value: '$AUTH_KEY'" \
            "" \
            "Please ensure the path is correct and the file exists." \
            "If you don't have an SSH key, you can generate one using: ssh-keygen -t ed25519"
    fi
}

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
    validate_auth_key
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
