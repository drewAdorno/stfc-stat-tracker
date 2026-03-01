"""
Watch auth.json for changes and immediately SCP to EC2.

Replaces the 5-hour scheduled upload_auth.sh task.
Run as a background process (e.g., Windows Task Scheduler at logon):
    pythonw deploy/watch_auth.py

Polls every 30 seconds. On change, uploads immediately.
Also uploads on startup if auth.json is newer than last upload.
"""

import subprocess
import sys
import time
from pathlib import Path

AUTH_FILE = Path("C:/Users/drewa/Desktop/stfc-api/auth.json")
PEM = Path("C:/Users/drewa/Downloads/STFC_pem.pem")
REMOTE = "ubuntu@3.16.255.133:/opt/stfc/auth.json"
STATE_FILE = Path(__file__).parent.parent / "data" / ".last_auth_upload"

POLL_INTERVAL = 30  # seconds


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_mtime():
    """Get auth.json modification time, or 0 if missing."""
    try:
        return AUTH_FILE.stat().st_mtime
    except FileNotFoundError:
        return 0


def get_last_upload_mtime():
    """Get the mtime we last uploaded, from state file."""
    try:
        return float(STATE_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return 0


def upload():
    """SCP auth.json to EC2. Returns True on success."""
    if not AUTH_FILE.exists():
        log("auth.json not found — skipping")
        return False
    if not PEM.exists():
        log(f"PEM key not found at {PEM} — skipping")
        return False

    try:
        result = subprocess.run(
            ["scp", "-i", str(PEM), str(AUTH_FILE), REMOTE],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            mtime = get_mtime()
            STATE_FILE.write_text(str(mtime))
            log("auth.json uploaded to EC2")
            return True
        else:
            log(f"SCP failed: {result.stderr.strip()}")
            return False
    except subprocess.TimeoutExpired:
        log("SCP timed out")
        return False
    except Exception as e:
        log(f"Upload error: {e}")
        return False


def main():
    log("Auth watcher started")
    log(f"Watching: {AUTH_FILE}")

    # Upload on startup if file is newer than last upload
    current_mtime = get_mtime()
    last_uploaded = get_last_upload_mtime()
    if current_mtime > last_uploaded:
        log("auth.json newer than last upload — pushing now")
        upload()
    else:
        log("auth.json unchanged since last upload")

    last_mtime = get_mtime()

    while True:
        time.sleep(POLL_INTERVAL)
        current_mtime = get_mtime()
        if current_mtime != last_mtime and current_mtime > 0:
            log(f"auth.json changed (mtime {last_mtime:.0f} → {current_mtime:.0f})")
            upload()
            last_mtime = current_mtime


if __name__ == "__main__":
    main()
