#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/nebula/nebula-fastapi}"
SERVICE_NAME="${SERVICE_NAME:-nebula-fastapi.service}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8001/healthz}"

cd "$APP_DIR"

git pull --ff-only
./venv/bin/pip install -r requirements.txt
sudo systemctl restart "$SERVICE_NAME"
sleep 2
curl -fsS "$HEALTH_URL"
