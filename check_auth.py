"""
Quick auth validation — makes a single lightweight API call to check if auth.json is still valid.

Exit codes:
  0 = auth is valid
  2 = auth expired or invalid
  1 = other error (network, missing file, etc.)
"""

import json
import sys
from pathlib import Path

import requests

_LOCAL_AUTH = Path(__file__).parent / "auth.json"
_DEV_AUTH = Path("C:/Users/drewa/Desktop/stfc/stfc-api/auth.json")
AUTH_FILE = _LOCAL_AUTH if _LOCAL_AUTH.exists() else _DEV_AUTH


def main():
    if not AUTH_FILE.exists():
        print("No auth.json found", flush=True)
        sys.exit(1)

    with open(AUTH_FILE, "r") as f:
        auth = json.load(f)

    headers = {
        "X-AUTH-SESSION-ID": auth["session_id"],
        "X-TRANSACTION-ID": auth["session_id"],
        "X-PRIME-VERSION": auth["prime_version"],
        "X-Instance-ID": str(auth["instance_id"]),
        "Content-Type": "application/json",
    }

    # Minimal API call — fetch our own profile (single ID batch)
    url = auth["game_server"] + "/user_profile/profiles"
    try:
        r = requests.post(url, json={"user_ids": []}, headers=headers, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}", flush=True)
        sys.exit(1)

    if r.status_code in (401, 403):
        print(f"Auth expired (HTTP {r.status_code})", flush=True)
        sys.exit(2)

    if r.status_code == 200:
        try:
            r.json()
            print("Auth valid", flush=True)
            sys.exit(0)
        except (json.JSONDecodeError, ValueError):
            print("Auth expired (non-JSON response)", flush=True)
            sys.exit(2)

    print(f"Unexpected status {r.status_code}", flush=True)
    sys.exit(1)


if __name__ == "__main__":
    main()
