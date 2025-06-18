#!/bin/bash

# DIVA Certificate Verification Script
# Validates SSL certificates and displays certificate details

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

# Required environment variables
REQUIRED_VARS=(
    "SERVER_CRT"
    "KEYSTORE_JKS"
    "TRUSTSTORE_JKS"
    "KEYSTORE_PASSWORD"
)

# -----------------------------------------------------------------------------
# UI Functions
# -----------------------------------------------------------------------------

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

# Print success
print_success() {
    echo -e "${GREEN}${SUCCESS_SYMBOL} $1${NC}"
}

# Print section header
print_section_header() {
    local title="$1"
    local title_length=${#title}
    local box_width=60
    local padding=$(((box_width - title_length) / 2))

    echo "╔════════════════════════════════════════════════════════════╗"
    printf "║%*s%s%*s║\n" $padding "" "$title" $((box_width - title_length - padding)) ""
    echo "╚════════════════════════════════════════════════════════════╝"
}

# -----------------------------------------------------------------------------
# Validation Functions
# -----------------------------------------------------------------------------

# Check that required environment variables are set
check_required_environment_variables() {
    local missing_vars=()

    for var in "${REQUIRED_VARS[@]}"; do
        if [ -z "${!var}" ]; then
            missing_vars+=("$var")
        fi
    done

    if [ ${#missing_vars[@]} -gt 0 ]; then
        print_error "Required environment variables are not set!" \
            "The following variables must be defined:" \
            "$(printf " - %s\n" "${missing_vars[@]}")" \
            "" \
            "This script should be called from the Taskfile which sets these variables."
    fi
}

# Check if required tools are available
check_required_tools() {
    local missing_tools=()

    if ! command -v openssl >/dev/null 2>&1; then
        missing_tools+=("openssl")
    fi

    if ! command -v keytool >/dev/null 2>&1; then
        missing_tools+=("keytool")
    fi

    if [ ${#missing_tools[@]} -gt 0 ]; then
        print_error "Required tools are missing!" \
            "Please install the following tools:" \
            "$(printf " - %s\n" "${missing_tools[@]}")" \
            "" \
            "On Ubuntu/Debian: sudo apt-get install openssl openjdk-11-jdk" \
            "On RHEL/CentOS: sudo yum install openssl java-11-openjdk-devel"
    fi
}

# Validate that certificate files exist
validate_certificate_files() {
    local missing_files=()

    if [ ! -f "$SERVER_CRT" ]; then
        missing_files+=("Server Certificate: $SERVER_CRT")
    fi

    if [ ! -f "$KEYSTORE_JKS" ]; then
        missing_files+=("Java Keystore: $KEYSTORE_JKS")
    fi

    if [ ! -f "$TRUSTSTORE_JKS" ]; then
        missing_files+=("Java Truststore: $TRUSTSTORE_JKS")
    fi

    if [ ${#missing_files[@]} -gt 0 ]; then
        print_error "Required certificate files are missing!" \
            "The following files could not be found:" \
            "$(printf " - %s\n" "${missing_files[@]}")" \
            "" \
            "Please run 'task generate-certificates' to create the certificates first."
    fi
}

# -----------------------------------------------------------------------------
# Certificate Display Functions
# -----------------------------------------------------------------------------

# Display server certificate details
display_server_certificate() {
    print_section_header "Server Certificate Details"
    if ! openssl x509 -in "$SERVER_CRT" -text -noout; then
        print_error "Failed to read server certificate from $SERVER_CRT"
    fi
    echo ""
}

# Display Java keystore contents
display_keystore_contents() {
    print_section_header "Java Keystore Contents"
    if ! keytool -list -keystore "$KEYSTORE_JKS" -storepass "$KEYSTORE_PASSWORD" 2>/dev/null; then
        print_error "Failed to read Java keystore from $KEYSTORE_JKS" \
            "Please check that the keystore exists and the password is correct." \
            "Current keystore password: $KEYSTORE_PASSWORD"
    fi
    echo ""
}

# Display Java truststore contents
display_truststore_contents() {
    print_section_header "Java Truststore Contents"
    if ! keytool -list -keystore "$TRUSTSTORE_JKS" -storepass "$KEYSTORE_PASSWORD" 2>/dev/null; then
        print_error "Failed to read Java truststore from $TRUSTSTORE_JKS" \
            "Please check that the truststore exists and the password is correct." \
            "Current keystore password: $KEYSTORE_PASSWORD"
    fi
    echo ""
}

# Run all certificate verification checks
run_certificate_verification() {
    display_server_certificate
    display_keystore_contents
    display_truststore_contents
}

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------

main() {
    check_required_environment_variables
    check_required_tools
    validate_certificate_files
    run_certificate_verification

    print_success "Certificate verification completed successfully!"
    echo ""
}

# Run main function
main "$@"
