#!/usr/bin/env bash
# ==========================================================================
# STFC daily pull — called by systemd timer
# ==========================================================================
set -euo pipefail

APP_DIR="/opt/stfc"
VENV="$APP_DIR/venv/bin"
LOG_TAG="stfc-scraper"

cd "$APP_DIR"

logger -t "$LOG_TAG" "Starting pull..."

# Run the API puller (xvfb provides virtual display for non-headless Chromium)
xvfb-run --auto-servernum "$VENV/python" pull_api.py
logger -t "$LOG_TAG" "Pull complete"

# Hourly alerts (joins/leaves/level-ups)
"$VENV/python" send_hourly_alerts.py || logger -t "$LOG_TAG" "WARNING: Hourly alerts failed"

# Daily Discord notification
"$VENV/python" send_discord_notification.py || logger -t "$LOG_TAG" "WARNING: Discord notification failed"

logger -t "$LOG_TAG" "All done"
