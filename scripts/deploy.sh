#!/bin/bash
# deploy.sh — generic Nexus deployment script for a source checkout plus a
# service checkout. All host-specific values come from environment variables so
# the public repo does not encode operator names, server names, or local paths.
#
# Required env (or edit defaults below):
#   NEXUS_SERVICES_DIR
#   NEXUS_PROJECTS_DIR
#   NEXUS_DEPLOY_USER
#
# Optional env:
#   NEXUS_ROOT_GROUP
#   NEXUS_SYSTEMD_SERVICES      (space-separated, default: "nexus-api nexus-worker nexus-mcp")
#   NEXUS_SECONDARY_HOST
#   NEXUS_SECONDARY_SERVICES_DIR
#   NEXUS_SECONDARY_BULK_SERVICE
#
# Usage:
#   sudo NEXUS_SERVICES_DIR=/srv/nexus/service \
#        NEXUS_PROJECTS_DIR=/srv/nexus/source \
#        NEXUS_DEPLOY_USER=nexus \
#        ./scripts/deploy.sh

set -euo pipefail

SERVICES_DIR="${NEXUS_SERVICES_DIR:-/srv/nexus/service}"
PROJECTS_DIR="${NEXUS_PROJECTS_DIR:-/srv/nexus/source}"
DEPLOY_USER="${NEXUS_DEPLOY_USER:-nexus}"
ROOT_GROUP="${NEXUS_ROOT_GROUP:-$DEPLOY_USER}"
SYSTEMD_SERVICES_RAW="${NEXUS_SYSTEMD_SERVICES:-nexus-api nexus-worker nexus-mcp}"
SECONDARY_HOST="${NEXUS_SECONDARY_HOST:-}"
SECONDARY_SERVICES_DIR="${NEXUS_SECONDARY_SERVICES_DIR:-$SERVICES_DIR}"
SECONDARY_BULK_SERVICE="${NEXUS_SECONDARY_BULK_SERVICE:-nexus-bulk}"

read -r -a SYSTEMD_SERVICES <<< "$SYSTEMD_SERVICES_RAW"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: deploy.sh must be run as root (sudo)" >&2
  exit 1
fi

echo "=== Nexus Deploy ==="
echo "Services dir: $SERVICES_DIR"
echo "Projects dir: $PROJECTS_DIR"
echo "Deploy user : $DEPLOY_USER"

echo "[1/6] Pulling latest source checkout..."
sudo -u "$DEPLOY_USER" bash -c "cd \"$PROJECTS_DIR\" && git pull --ff-only origin main"

echo "[2/6] Syncing source to service checkout..."
GIT_COMMIT="$(sudo -u "$DEPLOY_USER" git -C "$PROJECTS_DIR" rev-parse HEAD)"
GIT_BRANCH="$(sudo -u "$DEPLOY_USER" git -C "$PROJECTS_DIR" rev-parse --abbrev-ref HEAD)"
GIT_DESCRIBE="$(sudo -u "$DEPLOY_USER" git -C "$PROJECTS_DIR" describe --always --dirty 2>/dev/null || echo "$GIT_COMMIT")"
rsync -a --delete \
  --exclude=.git \
  --exclude=node_modules \
  --exclude=dist \
  --exclude=.tsbuildinfo \
  --exclude=.env \
  "$PROJECTS_DIR/" "$SERVICES_DIR/"

cat > "$SERVICES_DIR/build-info.json" <<EOF
{
  "service": "nexus",
  "deployed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_commit": "$GIT_COMMIT",
  "git_branch": "$GIT_BRANCH",
  "git_describe": "$GIT_DESCRIBE",
  "source_path": "$PROJECTS_DIR"
}
EOF

echo "[3/6] Installing dependencies and building..."
cd "$SERVICES_DIR"
npm ci --production=false
rm -rf packages/*/dist packages/*/*.tsbuildinfo
npm run build

echo "[4/6] Verifying build outputs..."
REQUIRED=(
  "$SERVICES_DIR/packages/worker/dist/index.js"
  "$SERVICES_DIR/packages/api/dist/index.js"
  "$SERVICES_DIR/packages/core/dist/index.js"
  "$SERVICES_DIR/packages/mcp/dist/index.js"
)
for f in "${REQUIRED[@]}"; do
  if [ ! -s "$f" ]; then
    echo "FATAL: $f missing or empty after build" >&2
    echo "       Try removing stale tsbuildinfo files and rebuilding." >&2
    exit 1
  fi
done
echo "  ok: required dist outputs present"

chown -R root:"$ROOT_GROUP" "$SERVICES_DIR"
chmod -R 750 "$SERVICES_DIR"

echo "[5/6] Running migrations..."
set -a
source "$SERVICES_DIR/.env" 2>/dev/null || true
set +a
npx tsx packages/core/src/migrate.ts
echo "  Migrations applied"

echo "[6/6] Restarting services..."
cp "$SERVICES_DIR/systemd/"*.service /etc/systemd/system/ 2>/dev/null && systemctl daemon-reload
systemctl reset-failed "${SYSTEMD_SERVICES[@]}" 2>/dev/null || true
systemctl restart "${SYSTEMD_SERVICES[@]}"
sleep 5

fail=0
for svc in "${SYSTEMD_SERVICES[@]}"; do
  state="$(systemctl is-active "$svc")"
  echo "  $svc: $state"
  if [ "$state" != "active" ]; then
    echo "FATAL: $svc failed to start. Recent logs:" >&2
    journalctl -u "$svc" --since "30 sec ago" --no-pager | tail -25 >&2
    fail=1
  fi
done
[ "$fail" -eq 0 ] || exit 1

if [ -n "$SECONDARY_HOST" ] && [ "${1:-}" != "--no-secondary" ]; then
  echo ""
  echo "=== Secondary Deploy ==="

  echo "[1/4] Rsync source checkout to $SECONDARY_HOST:$SECONDARY_SERVICES_DIR"
  sudo -u "$DEPLOY_USER" rsync -a --delete \
    --exclude=.git \
    --exclude=node_modules \
    --exclude=dist \
    --exclude=.tsbuildinfo \
    --exclude=.env \
    "$PROJECTS_DIR/" "$SECONDARY_HOST:$SECONDARY_SERVICES_DIR/"

  echo "[2/4] Build on secondary host"
  sudo -u "$DEPLOY_USER" ssh -o BatchMode=yes "$SECONDARY_HOST" "
    set -e
    cd \"$SECONDARY_SERVICES_DIR\"
    find packages -type d -name dist -exec rm -rf {} + 2>/dev/null || true
    find packages -name '*.tsbuildinfo' -delete 2>/dev/null || true
    npm ci --production=false >/tmp/nexus-secondary-npm-ci.log
    npm run build
  "

  echo "[3/4] Verify worker build on secondary host"
  sudo -u "$DEPLOY_USER" ssh -o BatchMode=yes "$SECONDARY_HOST" "
    if [ ! -s \"$SECONDARY_SERVICES_DIR/packages/worker/dist/index.js\" ]; then
      echo 'FATAL: worker dist/index.js missing on secondary host after build' >&2
      exit 1
    fi
    echo '  ok: worker dist/index.js present'
  "

  echo "[4/4] Restart secondary bulk service"
  sudo -u "$DEPLOY_USER" ssh -o BatchMode=yes "$SECONDARY_HOST" "
    sudo systemctl reset-failed \"$SECONDARY_BULK_SERVICE\" 2>/dev/null || true
    sudo systemctl restart \"$SECONDARY_BULK_SERVICE\"
    sleep 4
    state=\$(systemctl is-active \"$SECONDARY_BULK_SERVICE\")
    echo \"  $SECONDARY_BULK_SERVICE: \$state\"
    if [ \"\$state\" != 'active' ]; then
      echo 'FATAL: secondary bulk service failed to start. Recent logs:' >&2
      sudo journalctl -u \"$SECONDARY_BULK_SERVICE\" --since '30 sec ago' --no-pager | tail -25 >&2
      exit 1
    fi
  "
fi

echo ""
echo "=== Deploy Complete ==="
