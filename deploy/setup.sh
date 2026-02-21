#!/usr/bin/env bash
# ==========================================================================
# STFC Stat Tracker — EC2 Bootstrap Script
# Run once on a fresh Ubuntu 24.04 t2.micro:
#   sudo bash setup.sh
# ==========================================================================
set -euo pipefail

APP_USER="stfc"
APP_DIR="/opt/stfc"
REPO_URL="https://github.com/drewAdorno/stfc-stat-tracker.git"
DOMAIN="ncctracker.top"

echo "=== 1. System packages ==="
apt-get update
apt-get install -y \
    python3 python3-pip python3-venv \
    nginx certbot python3-certbot-nginx \
    chromium-browser \
    git curl

echo "=== 2. Create app user ==="
id -u "$APP_USER" &>/dev/null || useradd -r -m -s /bin/bash "$APP_USER"

echo "=== 3. Clone repo ==="
if [ -d "$APP_DIR" ]; then
    echo "  $APP_DIR already exists, pulling latest..."
    cd "$APP_DIR"
    git pull
else
    git clone "$REPO_URL" "$APP_DIR"
fi
chown -R "$APP_USER":"$APP_USER" "$APP_DIR"

echo "=== 4. Python virtualenv ==="
sudo -u "$APP_USER" python3 -m venv "$APP_DIR/venv"
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install --upgrade pip
sudo -u "$APP_USER" "$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt"

echo "=== 5. Install Playwright Chromium ==="
sudo -u "$APP_USER" "$APP_DIR/venv/bin/playwright" install chromium
# Install system deps for Playwright's bundled Chromium
"$APP_DIR/venv/bin/playwright" install-deps chromium

echo "=== 6. Create data directory ==="
sudo -u "$APP_USER" mkdir -p "$APP_DIR/data"
sudo -u "$APP_USER" mkdir -p "$APP_DIR/browser_session"

echo "=== 7. Copy .env file (if exists locally) ==="
if [ -f "$APP_DIR/.env" ]; then
    echo "  .env already present"
else
    echo "  WARNING: No .env file found. Copy it manually:"
    echo "    scp .env ubuntu@<ip>:/opt/stfc/.env"
fi

echo "=== 8. Install systemd units ==="
cp "$APP_DIR/deploy/stfc-scraper.service" /etc/systemd/system/
cp "$APP_DIR/deploy/stfc-scraper.timer"   /etc/systemd/system/
systemctl daemon-reload
systemctl enable stfc-scraper.timer
systemctl start stfc-scraper.timer
echo "  Timer active:"
systemctl list-timers stfc-scraper.timer --no-pager

echo "=== 9. Configure nginx ==="
cp "$APP_DIR/deploy/stfc-nginx.conf" /etc/nginx/sites-available/stfc
ln -sf /etc/nginx/sites-available/stfc /etc/nginx/sites-enabled/stfc
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl reload nginx

echo "=== 10. SSL certificate ==="
echo "  Run this after DNS is pointed to this server:"
echo "    sudo certbot --nginx -d $DOMAIN -d www.$DOMAIN"

echo ""
echo "============================================"
echo "  Setup complete!"
echo "  Dashboard: http://$DOMAIN"
echo "  Scraper timer: systemctl status stfc-scraper.timer"
echo ""
echo "  NEXT STEPS:"
echo "  1. Copy .env file:  scp .env ubuntu@<ip>:/opt/stfc/.env"
echo "  2. Copy cookies:    scp data/session_cookies.json ubuntu@<ip>:/opt/stfc/data/"
echo "  3. Copy browser_session/ (or do first cookie refresh manually)"
echo "  4. Import history:  sudo -u stfc /opt/stfc/venv/bin/python -c \\"
echo "       \"from db import get_db, import_history_json; import_history_json(get_db())\""
echo "  5. Point DNS A record for $DOMAIN to this server's IP"
echo "  6. Run: sudo certbot --nginx -d $DOMAIN"
echo "  7. Test scraper: sudo systemctl start stfc-scraper.service"
echo "============================================"
