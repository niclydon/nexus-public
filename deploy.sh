#!/bin/bash
# Nexus deploy script — builds on Primary-Server, syncs to Services, restarts
# Usage: ssh primary-server "cd ~/Projects/nexus && ./deploy.sh"
set -euo pipefail

echo "=== Nexus Deploy ==="

echo "[1/5] Pulling latest code..."
git pull

echo "[2/5] Building all packages..."
rm -rf packages/*/dist packages/*/*.tsbuildinfo
npm run build

echo "[3/5] Syncing to Services..."
sudo rsync -a --delete --exclude=.git --exclude=.env ~/Projects/nexus/ ~/Services/nexus/
sudo chown -R root:owner ~/Services/nexus
sudo chmod -R 750 ~/Services/nexus

echo "[4/5] Verifying sync..."
# Check that a recently-built file exists in Services
PROJ_SIZE=$(stat -c%s ~/Projects/nexus/packages/worker/dist/handlers/index.js 2>/dev/null || echo 0)
SERV_SIZE=$(stat -c%s ~/Services/nexus/packages/worker/dist/handlers/index.js 2>/dev/null || echo 0)
if [ "$PROJ_SIZE" != "$SERV_SIZE" ]; then
  echo "ERROR: File size mismatch after rsync! Projects=$PROJ_SIZE Services=$SERV_SIZE"
  echo "Retrying rsync..."
  sudo rsync -a --delete --exclude=.git --exclude=.env ~/Projects/nexus/ ~/Services/nexus/
  sudo chown -R root:owner ~/Services/nexus
  sudo chmod -R 750 ~/Services/nexus
fi

echo "[5/5] Restarting services..."
sudo systemctl restart nexus-api nexus-worker

echo "=== Deploy complete ==="
systemctl is-active nexus-api nexus-worker
