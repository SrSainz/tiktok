#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/SrSainz/apps/tiktok"
cd "$APP_DIR"

if [ ! -d .git ]; then
  echo "No git repo found in $APP_DIR"
  exit 1
fi

git fetch origin
git reset --hard origin/master

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

.venv/bin/pip install -r requirements.txt
mkdir -p data output work

mkdir -p "$HOME/.config/systemd/user"
cp deploy/systemd/tiktok-backend.service "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
systemctl --user enable tiktok-backend.service >/dev/null 2>&1 || true
systemctl --user restart tiktok-backend.service

echo "Clip Studio ES deployed on NAS"
echo "Health:  http://127.0.0.1:8780/api/health"
echo "Studio:  http://127.0.0.1:8780/studio"
