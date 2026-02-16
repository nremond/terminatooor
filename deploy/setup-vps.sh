#!/bin/bash
# Initial VPS setup script for terminatooor
# Run this once on a fresh Ubuntu/Debian VPS

set -e

echo "=== Setting up VPS for terminatooor ==="

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    echo "Please run as a regular user, not root"
    exit 1
fi

# Install system dependencies
echo ""
echo "Installing system dependencies..."
sudo apt update
sudo apt install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    git \
    curl

# Install Rust
if ! command -v rustc &> /dev/null; then
    echo ""
    echo "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
else
    echo ""
    echo "Rust already installed: $(rustc --version)"
fi

# Create liquidator user if not exists
if ! id "liquidator" &>/dev/null; then
    echo ""
    echo "Creating liquidator user..."
    sudo useradd -r -s /bin/false -d /opt/terminatooor liquidator
fi

# Create directories
echo ""
echo "Creating directories..."
sudo mkdir -p /opt/terminatooor/keys
sudo chown -R $USER:$USER /opt/terminatooor




# Clone repository if not exists
if [ ! -d /opt/terminatooor/.git ]; then
    echo ""
    echo "Cloning repository..."
    # Update this URL to your actual GitLab repo
    git clone git@github.com:nremond/terminatooor.git /opt/terminatooor
else
    echo ""
    echo "Repository already cloned"
fi

# Create .env file template if not exists
if [ ! -f /opt/terminatooor/.env ]; then
    echo ""
    echo "Creating .env template..."
    cp /opt/terminatooor/deploy/.env.example /opt/terminatooor/.env
    chmod 600 /opt/terminatooor/.env
    echo "IMPORTANT: Edit /opt/terminatooor/.env with your actual values"
fi

# Install systemd service
echo ""
echo "Installing systemd service..."
sudo cp /opt/terminatooor/deploy/terminatooor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable terminatooor

# Set permissions
echo ""
echo "Setting permissions..."
sudo chown -R liquidator:liquidator /opt/terminatooor/keys
sudo chmod 700 /opt/terminatooor/keys

echo ""
echo "=== VPS Setup Complete ==="
echo ""
echo "Next steps:"
echo "1. Copy your liquidator keypair to /opt/terminatooor/keys/liquidator-keypair.json"
echo "2. Edit /opt/terminatooor/.env with your configuration"
echo "3. Build: cd /opt/terminatooor && ./deploy/deploy.sh"
echo "4. Start: sudo systemctl start terminatooor"
echo "5. Logs: sudo journalctl -u terminatooor -f"
