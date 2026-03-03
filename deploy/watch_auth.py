"""
Watch auth.json and game_news.json for changes and immediately SCP to EC2.

Replaces the 5-hour scheduled upload_auth.sh task.
Run as a background process (e.g., Windows Task Scheduler at logon):
    pythonw deploy/watch_auth.py

Polls every 30 seconds. On change, uploads immediately.
Also uploads on startup if files are newer than last upload.
"""

import subprocess
import sys
import time
from pathlib import Path

PEM = Path("C:/Users/drewa/Downloads/STFC_pem.pem")
EC2_HOST = "ubuntu@3.16.255.133"
STATE_DIR = Path(__file__).parent.parent / "data"

POLL_INTERVAL = 30  # seconds

# Files to watch: (local_path, remote_path, state_file)
WATCH_FILES = [
    {
        "name": "auth.json",
        "local": Path("C:/Users/drewa/Desktop/stfc/stfc-api/auth.json"),
        "remote": f"{EC2_HOST}:/opt/stfc/auth.json",
        "state": STATE_DIR / ".last_auth_upload",
    },
    {
        "name": "game_news.json",
        "local": Path("C:/Users/drewa/Desktop/stfc/stfc-api/game_news.json"),
        "remote": f"{EC2_HOST}:/opt/stfc/data/game_news.json",
        "state": STATE_DIR / ".last_game_news_upload",
    },
]


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_mtime(path):
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0


def get_last_upload_mtime(state_file):
    try:
        return float(state_file.read_text().strip())
    except (FileNotFoundError, ValueError):
        return 0


def upload(entry):
    """SCP a file to EC2. Returns True on success."""
    if not entry["local"].exists():
        log(f"{entry['name']} not found — skipping")
        return False
    if not PEM.exists():
        log(f"PEM key not found at {PEM} — skipping")
        return False

    try:
        result = subprocess.run(
            ["scp", "-i", str(PEM), str(entry["local"]), entry["remote"]],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            mtime = get_mtime(entry["local"])
            entry["state"].write_text(str(mtime))
            log(f"{entry['name']} uploaded to EC2")
            return True
        else:
            log(f"SCP {entry['name']} failed: {result.stderr.strip()}")
            return False
    except subprocess.TimeoutExpired:
        log(f"SCP {entry['name']} timed out")
        return False
    except Exception as e:
        log(f"Upload {entry['name']} error: {e}")
        return False


def main():
    log("File watcher started")
    for entry in WATCH_FILES:
        log(f"Watching: {entry['local']}")

    # Upload on startup if files are newer than last upload
    last_mtimes = {}
    for entry in WATCH_FILES:
        current = get_mtime(entry["local"])
        last_uploaded = get_last_upload_mtime(entry["state"])
        if current > last_uploaded:
            log(f"{entry['name']} newer than last upload — pushing now")
            upload(entry)
        else:
            log(f"{entry['name']} unchanged since last upload")
        last_mtimes[entry["name"]] = get_mtime(entry["local"])

    while True:
        time.sleep(POLL_INTERVAL)
        for entry in WATCH_FILES:
            current = get_mtime(entry["local"])
            prev = last_mtimes[entry["name"]]
            if current != prev and current > 0:
                log(f"{entry['name']} changed (mtime {prev:.0f} → {current:.0f})")
                upload(entry)
                last_mtimes[entry["name"]] = current


if __name__ == "__main__":
    main()
