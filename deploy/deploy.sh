#!/bin/bash
# Deployment script for terminatooor
# Run this on the VPS to pull latest changes and rebuild

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=== Deploying terminatooor ==="
echo "Directory: $PROJECT_DIR"

# Pull latest changes
echo ""
echo "Pulling latest changes..."
git fetch origin
git reset --hard origin/master

# Show what we're deploying
COMMIT=$(git rev-parse --short HEAD)
COMMIT_MSG=$(git log -1 --pretty=%B | head -1)
echo "Deploying: $COMMIT - $COMMIT_MSG"

# Build release binary
echo ""
echo "Building release binary..."
cd terminator
cargo build --release

# Restart service if running
if systemctl is-active --quiet terminatooor; then
    echo ""
    echo "Restarting service..."
    sudo systemctl restart terminatooor
    sleep 2
    if systemctl is-active --quiet terminatooor; then
        echo "Service restarted successfully"
    else
        echo "WARNING: Service failed to start!"
        sudo journalctl -u terminatooor -n 20 --no-pager
        exit 1
    fi
else
    echo ""
    echo "Service not running. Start with: sudo systemctl start terminatooor"
fi

echo ""
echo "=== Deployment complete: $COMMIT ==="
