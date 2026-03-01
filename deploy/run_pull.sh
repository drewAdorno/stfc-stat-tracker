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

# Pre-check auth validity before full scrape
"$VENV/python" check_auth.py 2>&1 || AUTH_EXIT=$?
if [ "${AUTH_EXIT:-0}" -eq 2 ]; then
    # Auth expired — wait 90s for the watcher to push fresh tokens, then retry
    logger -t "$LOG_TAG" "Auth expired — waiting 90s for fresh tokens..."
    sleep 90
    "$VENV/python" check_auth.py 2>&1 || AUTH_RETRY=$?
    if [ "${AUTH_RETRY:-0}" -eq 2 ]; then
        logger -t "$LOG_TAG" "ERROR: Auth still expired after retry"
        "$VENV/python" send_failure_alert.py "Auth expired — relaunch game and upload fresh auth.json" || true
        exit 1
    fi
    logger -t "$LOG_TAG" "Auth refreshed after retry"
elif [ "${AUTH_EXIT:-0}" -ne 0 ]; then
    logger -t "$LOG_TAG" "WARNING: Auth check had non-auth error (exit $AUTH_EXIT), proceeding anyway"
fi

# Run the Scopely API scraper (no browser needed)
if "$VENV/python" -u pull_scopely.py 2>&1; then
    logger -t "$LOG_TAG" "Pull complete"
else
    EXIT_CODE=$?
    if [ "$EXIT_CODE" -eq 2 ]; then
        logger -t "$LOG_TAG" "ERROR: Auth expired mid-scrape"
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
