#!/usr/bin/env bash
# One-shot setup script for a fresh Hetzner Ubuntu VPS.
# Run as root (or a sudo user) on first boot.
# Creates the dedicated fifth-grape system user, installs dependencies,
# and registers systemd units.
#
# Prerequisites: you have SSH access to the VPS as root or a sudo user.
#
# After this script finishes, run Step 2 (rsync) from your local machine to copy
# the project into /home/fifth-grape/backend/, then rerun this script
# to install the venv and start services.

set -euo pipefail

USER_NAME="fifth-grape"
USER_HOME="/home/$USER_NAME"
PROJECT_DIR="$USER_HOME/backend"

echo "▶ Creating user '$USER_NAME' if missing"
if ! id "$USER_NAME" >/dev/null 2>&1; then
  sudo useradd -m -s /bin/bash "$USER_NAME"
  echo "  user created"
else
  echo "  user already exists"
fi

echo "▶ Installing system packages"
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip sqlite3 tmux rsync curl ca-certificates

echo "▶ Installing cloudflared"
if ! command -v cloudflared >/dev/null; then
  curl -sL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
    -o /tmp/cloudflared
  sudo install -m 0755 /tmp/cloudflared /usr/local/bin/cloudflared
fi

echo "▶ Ensuring project dirs exist"
sudo -u "$USER_NAME" mkdir -p "$PROJECT_DIR" "$USER_HOME/.cloudflared"

if [ ! -f "$PROJECT_DIR/requirements.txt" ]; then
  cat <<EOF

──────────────────────────────────────────────────────────────
Backend files not found in $PROJECT_DIR yet.

From your LOCAL machine, run:

  VPS=root@<vps-ip>

  # 1. Copy the backend (run from inside the repo):
  rsync -avz --progress \\
    --exclude='.venv' --exclude='__pycache__' --exclude='*.pyc' \\
    --exclude='.git'  --exclude='dumps/'       --exclude='*.log' \\
    ./ \$VPS:/home/fifth-grape/backend/

  # 2. Copy the .env (sensitive):
  scp .env \$VPS:/home/fifth-grape/backend/.env

  # 3. Copy the tunnel credentials (sensitive):
  scp ~/.cloudflared/fifth-grape.yml \\
      ~/.cloudflared/d14e8dd1-8e02-4466-877d-2678dc27b541.json \\
      \$VPS:/home/fifth-grape/.cloudflared/

  # 4. Fix ownership (since rsync came in as root):
  ssh \$VPS 'chown -R fifth-grape:fifth-grape /home/fifth-grape'

Then re-run this script on the VPS to finish setup.
──────────────────────────────────────────────────────────────
EOF
  exit 0
fi

echo "▶ Creating Python venv and installing dependencies"
sudo -u "$USER_NAME" bash -c "cd '$PROJECT_DIR' && python3 -m venv .venv && .venv/bin/pip install --upgrade pip && .venv/bin/pip install -r requirements.txt"

echo "▶ Checking .env"
if [ ! -f "$PROJECT_DIR/.env" ]; then
  echo "   !! .env is missing. Copy it over before starting the API."
  exit 1
fi

echo "▶ Installing systemd units"
sudo cp "$PROJECT_DIR/deploy/fifth-grape-api.service" /etc/systemd/system/
sudo cp "$PROJECT_DIR/deploy/fifth-grape-tunnel.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable fifth-grape-api fifth-grape-tunnel

echo "▶ Starting services"
sudo systemctl restart fifth-grape-api
sudo systemctl restart fifth-grape-tunnel

echo ""
echo "✓ Setup complete."
echo "  Check status:    systemctl status fifth-grape-api fifth-grape-tunnel"
echo "  Tail logs:       journalctl -u fifth-grape-api -f"
echo "                   journalctl -u fifth-grape-tunnel -f"
echo "  Public URL:      https://fifth-api.grapesfarm.com"
