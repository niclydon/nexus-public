#!/bin/bash
# install.sh — Set up Nexus on Primary-Server (Ubuntu 24.04)
# Installs deps, builds, runs migrations, installs systemd services.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Nexus Install (Primary-Server) ==="
echo "Project: $PROJECT_DIR"

# --- Node.js deps ---
echo "[1/4] Installing dependencies..."
cd "$PROJECT_DIR"
npm ci --production=false
echo "  Dependencies installed"

# --- Build ---
echo "[2/4] Building..."
npm run build
echo "  Build complete"

# --- Database migrations ---
echo "[3/4] Running migrations..."
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
    npx tsx packages/core/src/migrate.ts
    echo "  Migrations applied"
else
    echo "  WARNING: .env not found, skipping migrations"
    echo "  Copy .env.example to .env and fill in values, then run:"
    echo "    npm run migrate"
fi

# --- systemd services ---
echo "[4/4] Installing systemd services..."

for SERVICE in nexus-api nexus-worker; do
    SERVICE_SRC="$PROJECT_DIR/systemd/${SERVICE}.service"
    SERVICE_DST="/etc/systemd/system/${SERVICE}.service"

    if [ -f "$SERVICE_SRC" ]; then
        sudo cp "$SERVICE_SRC" "$SERVICE_DST"
        sudo systemctl daemon-reload
        sudo systemctl enable "$SERVICE"
        sudo systemctl restart "$SERVICE"
        echo "  ${SERVICE}: installed and started"
    else
        echo "  WARNING: ${SERVICE}.service not found"
    fi
done

echo ""
echo "=== Install Complete ==="
echo ""
echo "Services:"
echo "  nexus-api    — port 7700"
echo "  nexus-worker — agent cycle runner"
echo ""
echo "Health: curl http://localhost:7700/health"
echo "Logs:   journalctl -u nexus-api -f"
echo "        journalctl -u nexus-worker -f"
