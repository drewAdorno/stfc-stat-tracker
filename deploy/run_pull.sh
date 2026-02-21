#!/usr/bin/env bash
# ==========================================================================
# STFC hourly pull — called by systemd timer
# ==========================================================================
set -uo pipefail

APP_DIR="/opt/stfc"
VENV="$APP_DIR/venv/bin"
LOG_TAG="stfc-scraper"

cd "$APP_DIR"

logger -t "$LOG_TAG" "Starting pull..."

# Run the API puller (xvfb provides virtual display for non-headless Chromium)
if xvfb-run --auto-servernum "$VENV/python" pull_api.py 2>&1; then
    logger -t "$LOG_TAG" "Pull complete"
else
    logger -t "$LOG_TAG" "ERROR: Pull failed!"
    "$VENV/python" send_failure_alert.py "Scraper failed — cookies may need refreshing" || true
    exit 1
fi

# Hourly alerts (joins/leaves/level-ups)
"$VENV/python" send_hourly_alerts.py || logger -t "$LOG_TAG" "WARNING: Hourly alerts failed"

# Daily Discord notification
"$VENV/python" send_discord_notification.py || logger -t "$LOG_TAG" "WARNING: Discord notification failed"

logger -t "$LOG_TAG" "All done"
