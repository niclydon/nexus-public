#!/bin/bash
# deploy.sh — Deploy Nexus to Primary-Server from GitHub
# Must be run as root (sudo) since /Services is root-owned.
#
# Usage: ssh primary-server "sudo /opt/nexus/Services/nexus/scripts/deploy.sh"

set -euo pipefail

SERVICES_DIR="/opt/nexus/Services/nexus"
PROJECTS_DIR="/opt/nexus/Projects/nexus"

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: deploy.sh must be run as root (sudo)" >&2
    exit 1
fi

echo "=== Nexus Deploy ==="

# Pull latest from GitHub into Projects (as owner, since Projects is user-owned)
echo "[1/5] Pulling from GitHub..."
sudo -u owner bash -c "cd $PROJECTS_DIR && git pull"

# Sync source to Services (excludes .git, node_modules, dist, .env)
echo "[2/5] Syncing to Services..."
rsync -a --delete \
    --exclude=.git \
    --exclude=node_modules \
    --exclude=dist \
    --exclude=.tsbuildinfo \
    --exclude=.env \
    "$PROJECTS_DIR/" "$SERVICES_DIR/"

# Install deps and build
echo "[3/6] Building..."
cd "$SERVICES_DIR"
npm ci --production=false
rm -rf packages/*/dist packages/*/*.tsbuildinfo
npm run build

# Verify the build actually produced the worker entry point — past deploys
# silently left dist/index.js missing because tsc was running incrementally
# off a stale tsbuildinfo, leaving the worker crash-looping for 30+ minutes
# before the watchdog noticed.
echo "[4/6] Verifying build outputs..."
REQUIRED=(
  "$SERVICES_DIR/packages/worker/dist/index.js"
  "$SERVICES_DIR/packages/api/dist/index.js"
  "$SERVICES_DIR/packages/core/dist/index.js"
)
for f in "${REQUIRED[@]}"; do
  if [ ! -s "$f" ]; then
    echo "FATAL: $f missing or empty after build" >&2
    echo "       (tsc skipped emit — try removing all tsbuildinfo and rebuilding)" >&2
    exit 1
  fi
done
echo "  ok: all required dist outputs present"

# Lock down: root owns, owner group can read+execute
chown -R root:owner "$SERVICES_DIR"
chmod -R 750 "$SERVICES_DIR"

# Run migrations (needs DB access)
echo "[5/6] Running migrations..."
set -a
source "$SERVICES_DIR/.env" 2>/dev/null || true
set +a
npx tsx packages/core/src/migrate.ts
echo "  Migrations applied"

# Copy systemd service files if changed
echo "[6/6] Restarting services..."
cp "$SERVICES_DIR/systemd/"*.service /etc/systemd/system/ 2>/dev/null && systemctl daemon-reload
systemctl reset-failed nexus-api nexus-worker 2>/dev/null || true
systemctl restart nexus-api nexus-worker
sleep 5

# Verify the services actually came up — exit non-zero if any are failing.
fail=0
for svc in nexus-api nexus-worker; do
  state=$(systemctl is-active "$svc")
  echo "  $svc: $state"
  if [ "$state" != "active" ]; then
    echo "FATAL: $svc failed to start. Recent logs:" >&2
    journalctl -u "$svc" --since "30 sec ago" --no-pager | tail -25 >&2
    fail=1
  fi
done
[ $fail -eq 0 ] || exit 1

# ─────────────────────────────────────────────────────────────────────────────
# Secondary-Server deploy — runs the same source over Thunderbolt to the satellite host
# that runs the bulk worker. Skipped if --no-secondary-server is passed.
# Secondary-Server has its own /Services/nexus tree and rebuilds locally.
# ─────────────────────────────────────────────────────────────────────────────
if [ "${1:-}" != "--no-secondary-server" ]; then
  echo ""
  echo "=== Secondary-Server Deploy ==="

  echo "[1/4] Rsync Primary-Server Projects → Secondary-Server Services"
  # Rsync as owner (Secondary-Server Services is owned by owner — different from
  # Primary-Server where it's root-owned). Excludes match the Primary-Server flow.
  sudo -u owner rsync -a --delete \
    --exclude=.git --exclude=node_modules --exclude=dist \
    --exclude=.tsbuildinfo --exclude=.env --exclude=.venv-topic-model \
    "$PROJECTS_DIR/" "secondary-server:/opt/nexus/Services/nexus/"

  echo "[2/4] Build on Secondary-Server"
  sudo -u owner ssh -o BatchMode=yes secondary-server '
    set -e
    cd /opt/nexus/Services/nexus
    sudo find packages -type d -name dist -exec rm -rf {} + 2>/dev/null || true
    sudo find packages -name "*.tsbuildinfo" -delete 2>/dev/null || true
    sudo npm run build:core 2>&1 | tail -3
    sudo npm run build --workspace=@nexus/worker 2>&1 | tail -3
  '

  echo "[3/4] Verify dist on Secondary-Server"
  sudo -u owner ssh -o BatchMode=yes secondary-server '
    if [ ! -s /opt/nexus/Services/nexus/packages/worker/dist/index.js ]; then
      echo "FATAL: dist/index.js missing on secondary-server after build" >&2
      exit 1
    fi
    echo "  ok: dist/index.js present"
  '

  echo "[4/4] Restart secondary-server-bulk + verify"
  sudo -u owner ssh -o BatchMode=yes secondary-server '
    sudo systemctl reset-failed secondary-server-bulk 2>/dev/null || true
    sudo systemctl restart secondary-server-bulk
    sleep 4
    state=$(systemctl is-active secondary-server-bulk)
    echo "  secondary-server-bulk: $state"
    if [ "$state" != "active" ]; then
      echo "FATAL: secondary-server-bulk failed to start. Recent logs:" >&2
      sudo journalctl -u secondary-server-bulk --since "30 sec ago" --no-pager | tail -25 >&2
      exit 1
    fi
  '
fi

echo ""
echo "=== Deploy Complete ==="
