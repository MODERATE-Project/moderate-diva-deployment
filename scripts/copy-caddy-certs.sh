#!/bin/bash

# Copy SSL certificates from Caddy's data directory to expected locations
# This copies certificates from the bind-mounted caddy/data directory
# Usage: ./copy-caddy-certs.sh <machine_url> <cert_dir>

set -e

# Get the absolute path of the script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MACHINE_URL="$1"
CERT_DIR="$2"

if [ -z "$MACHINE_URL" ] || [ -z "$CERT_DIR" ]; then
    echo "Usage: $0 <machine_url> <cert_dir>"
    exit 1
fi

# Convert CERT_DIR to absolute path if it's relative
if [[ "$CERT_DIR" != /* ]]; then
    CERT_DIR="$(cd "$(dirname "$CERT_DIR")" 2>/dev/null && pwd)/$(basename "$CERT_DIR")" || CERT_DIR="$(pwd)/$CERT_DIR"
fi

SERVER_CRT="$CERT_DIR/server.crt"
SERVER_KEY="$CERT_DIR/server.key"
CADDY_DATA_DIR="$PROJECT_ROOT/caddy/data"

echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"
echo "Caddy data directory: $CADDY_DATA_DIR"
echo "Certificate directory: $CERT_DIR"
echo "Linking SSL certificates from Caddy data directory..."

# Wait for certificates to be available
echo "Waiting for Caddy to obtain certificates..."
TIMEOUT=300
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    # Look for certificate files in the Caddy data directory
    # First try to find exact domain match (top-level domain)
    CRT_FILE=$(find "$CADDY_DATA_DIR" -name "*.crt" -path "*/$MACHINE_URL/*" 2>/dev/null | head -1 || echo "")
    KEY_FILE=$(find "$CADDY_DATA_DIR" -name "*.key" -path "*/$MACHINE_URL/*" 2>/dev/null | head -1 || echo "")
    
    # If exact domain not found, try broader search but prioritize shorter paths (likely top-level)
    if [ -z "$CRT_FILE" ] || [ -z "$KEY_FILE" ]; then
        CRT_FILE=$(find "$CADDY_DATA_DIR" -name "*.crt" -path "*$MACHINE_URL*" 2>/dev/null | awk '{print length($0) " " $0}' | sort -n | head -1 | cut -d' ' -f2- || echo "")
        KEY_FILE=$(find "$CADDY_DATA_DIR" -name "*.key" -path "*$MACHINE_URL*" 2>/dev/null | awk '{print length($0) " " $0}' | sort -n | head -1 | cut -d' ' -f2- || echo "")
    fi

    if [ -n "$CRT_FILE" ] && [ -n "$KEY_FILE" ] && [ -s "$CRT_FILE" ] && [ -s "$KEY_FILE" ]; then
        echo "Certificates found in Caddy data directory"
        break
    fi

    echo "Waiting for certificates... ($ELAPSED/$TIMEOUT seconds)"
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

# Final check for certificate files with improved selection logic
# First try to find exact domain match (top-level domain)
CRT_FILE=$(find "$CADDY_DATA_DIR" -name "*.crt" -path "*/$MACHINE_URL/*" 2>/dev/null | head -1 || echo "")
KEY_FILE=$(find "$CADDY_DATA_DIR" -name "*.key" -path "*/$MACHINE_URL/*" 2>/dev/null | head -1 || echo "")

# If exact domain not found, try broader search but prioritize shorter paths (likely top-level)
if [ -z "$CRT_FILE" ] || [ -z "$KEY_FILE" ]; then
    echo "Exact domain match not found, searching for certificates with domain in path..."
    CRT_FILE=$(find "$CADDY_DATA_DIR" -name "*.crt" -path "*$MACHINE_URL*" 2>/dev/null | awk '{print length($0) " " $0}' | sort -n | head -1 | cut -d' ' -f2- || echo "")
    KEY_FILE=$(find "$CADDY_DATA_DIR" -name "*.key" -path "*$MACHINE_URL*" 2>/dev/null | awk '{print length($0) " " $0}' | sort -n | head -1 | cut -d' ' -f2- || echo "")
fi

# Final fallback: if still no certificates found, try generic search
if [ -z "$CRT_FILE" ] || [ -z "$KEY_FILE" ]; then
    echo "Domain-specific certificates not found, searching for any certificates as fallback..."
    CRT_FILE=$(find "$CADDY_DATA_DIR" -name "*.crt" 2>/dev/null | head -1 || echo "")
    KEY_FILE=$(find "$CADDY_DATA_DIR" -name "*.key" 2>/dev/null | head -1 || echo "")
fi

if [ -z "$CRT_FILE" ] || [ -z "$KEY_FILE" ]; then
    echo "Error: Could not find certificate files in Caddy data directory"
    echo "Available certificate files:"
    find "$CADDY_DATA_DIR" -name "*.crt" -o -name "*.key" -o -name "*.pem" 2>/dev/null || echo "No certificate files found"
    echo "Caddy data directory contents:"
    ls -la "$CADDY_DATA_DIR" 2>/dev/null || echo "Caddy data directory not accessible"
    exit 1
fi

echo "Selected certificate files:"
echo "Certificate: $CRT_FILE"
echo "Private key: $KEY_FILE"

# Copy certificates to the target directory
# If the target files exist, remove them first
[ -f "$SERVER_CRT" ] && rm "$SERVER_CRT"
[ -f "$SERVER_KEY" ] && rm "$SERVER_KEY"

# Ensure the certificate directory exists
mkdir -p "$CERT_DIR"

# Copy the certificate files
cp "$CRT_FILE" "$SERVER_CRT"
cp "$KEY_FILE" "$SERVER_KEY"

echo "SSL certificates copied successfully:"
echo "Certificate: $CRT_FILE -> $SERVER_CRT"
echo "Private key: $KEY_FILE -> $SERVER_KEY"
echo ""
echo "Note: Certificates are copied, not linked. Re-run this script after Caddy renews certificates."
