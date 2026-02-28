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

# Run the Scopely API scraper (no browser needed)
if "$VENV/python" -u pull_scopely.py 2>&1; then
    logger -t "$LOG_TAG" "Pull complete"
else
    EXIT_CODE=$?
    if [ "$EXIT_CODE" -eq 2 ]; then
        logger -t "$LOG_TAG" "ERROR: Auth expired — update auth.json on server"
        "$VENV/python" send_failure_alert.py "Auth expired — relaunch game and upload fresh auth.json" || true
    else
        logger -t "$LOG_TAG" "ERROR: Pull failed (exit $EXIT_CODE)"
        "$VENV/python" send_failure_alert.py "Scraper failed (exit $EXIT_CODE)" || true
    fi
    exit 1
fi

# Hourly alerts (joins/leaves/level-ups)
"$VENV/python" send_hourly_alerts.py || logger -t "$LOG_TAG" "WARNING: Hourly alerts failed"

# Daily Discord notification
"$VENV/python" send_discord_notification.py || logger -t "$LOG_TAG" "WARNING: Discord notification failed"

logger -t "$LOG_TAG" "All done"
