"""
Send a Discord alert when the scraper fails.
Usage: python send_failure_alert.py "Error message here"
"""

import sys
from pathlib import Path

from send_discord_notification import load_webhook_url, post_webhook, safe_print

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
COOLDOWN_FILE = DATA_DIR / ".last_failure_alert"


def main():
    message = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Scraper failed"

    # Don't spam — only alert once per 6 hours
    import time
    if COOLDOWN_FILE.exists():
        try:
            last_sent = float(COOLDOWN_FILE.read_text().strip())
            if time.time() - last_sent < 6 * 3600:
                safe_print("Failure alert suppressed (cooldown active)")
                return
        except (ValueError, OSError):
            pass

    webhook_url = load_webhook_url()
    if not webhook_url:
        safe_print("No webhook URL configured — cannot send failure alert")
        sys.exit(1)

    embed = {
        "title": "Scraper Failure",
        "description": (
            f"{message}\n\n"
            "**To fix:** Copy fresh cookies from Windows:\n"
            "```\n"
            "scp -i STFC_pem.pem data/session_cookies.json "
            "ubuntu@3.16.255.133:/opt/stfc/data/\n"
            "```"
        ),
        "color": 0xFF0000,  # red
        "footer": {"text": "ncctracker.top"},
    }

    if post_webhook(webhook_url, embed):
        COOLDOWN_FILE.write_text(str(time.time()))
        safe_print("Failure alert sent to Discord")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
