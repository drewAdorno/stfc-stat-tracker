"""Fetch alliance bank/inventory directly via the game server HTTP API.

Replaces the mod-side dump (which required opening the Alliance Inventory
screen in-game). Cron-friendly: reads auth.json, POSTs to
`/alliance/get_alliance_bank_resources`, writes the standard
`alliance_inventory.json` schema that `db.ingest_alliance_inventory()` consumes.

Usage:
    python fetch_alliance_inventory.py
    python fetch_alliance_inventory.py --user-id q32bd4a5c6724c70a85c0299e6e2d4a2
    python fetch_alliance_inventory.py --dry-run

User ID resolution order:
    1. --user-id CLI arg
    2. auth.json "user_id" field
    3. tokens.json JWT accountId (chest collector token)
"""

import argparse
import base64
import json
import sys
import time
from pathlib import Path

import requests

# --- Paths ---
_LOCAL_AUTH = Path(__file__).parent / "auth.json"
_DEV_AUTH = Path("C:/Users/drewa/Desktop/stfc/stfc-api/auth.json")
AUTH_FILE = _LOCAL_AUTH if _LOCAL_AUTH.exists() else _DEV_AUTH

TOKENS_FILE = Path("C:/Users/drewa/Desktop/stfc/stfc_chest_collector/tokens.json")

OUTPUT_FILE = Path("C:/Users/drewa/Desktop/stfc/stfc-api/alliance_inventory.json")
RAW_DEBUG_FILE = Path("C:/Users/drewa/Desktop/stfc/stfc-api/alliance_bank_raw.json")

ENDPOINT_PATH = "/alliance/get_alliance_bank_resources"


def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="replace").decode("ascii"))


def load_auth():
    if not AUTH_FILE.exists():
        safe_print(f"FATAL: auth.json not found at {AUTH_FILE}")
        safe_print("Launch the game once with the mod loaded to generate it.")
        sys.exit(2)
    with open(AUTH_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_user_id(auth, cli_user_id):
    if cli_user_id:
        return cli_user_id, "cli"
    if auth.get("user_id"):
        return auth["user_id"], "auth.json"
    if TOKENS_FILE.exists():
        try:
            with open(TOKENS_FILE, "r", encoding="utf-8") as f:
                tokens = json.load(f)
            token = tokens.get("token", "").removeprefix("Bearer ").strip()
            payload_b64 = token.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            uid = payload.get("accountId")
            if uid:
                return uid, "tokens.json JWT"
        except Exception as e:
            safe_print(f"  (tokens.json JWT decode failed: {e})")
    safe_print("FATAL: could not resolve own user_id.")
    safe_print("  Try one of:")
    safe_print("    --user-id <hex>  (q-prefix + 31 hex chars)")
    safe_print("    add \"user_id\": \"...\" to auth.json")
    safe_print(f"    populate {TOKENS_FILE} with a chest-collector JWT")
    sys.exit(2)


def fetch_bank(auth, user_id):
    url = auth["game_server"] + ENDPOINT_PATH
    headers = {
        "X-AUTH-SESSION-ID": auth["session_id"],
        "X-TRANSACTION-ID": auth["session_id"],
        "X-PRIME-VERSION": auth["prime_version"],
        "X-Instance-ID": str(auth["instance_id"]),
        "Content-Type": "application/json",
    }
    body = {"user_id": user_id}

    r = requests.post(url, headers=headers, json=body, timeout=15)
    if r.status_code in (401, 403):
        safe_print(f"FATAL: auth expired (HTTP {r.status_code}). Relaunch the game.")
        sys.exit(2)
    if r.status_code != 200:
        safe_print(f"FATAL: HTTP {r.status_code}\n{r.text[:500]}")
        sys.exit(2)
    try:
        return r.json()
    except ValueError:
        safe_print("FATAL: response not JSON (likely auth expired). Relaunch game.")
        safe_print(r.text[:500])
        sys.exit(2)


def normalise_items(raw):
    """Map server response to {refid, type, count} schema used on disk.

    Server may use snake_case or camelCase; the inventory entries usually live
    under "resources", but defensively check a few keys.
    """
    candidates = (
        raw.get("alliance_bank")
        or raw.get("resources")
        or raw.get("inventory_items")
        or raw.get("inventoryItems")
        or raw.get("items")
        or []
    )
    items = []
    for entry in candidates:
        if not isinstance(entry, dict):
            continue
        params = entry.get("params") or entry.get("_params") or {}
        refid = (
            entry.get("refid")
            or entry.get("ref_id")
            or params.get("refid")
            or params.get("ref_id")
        )
        item_type = entry.get("type") or entry.get("inventory_item_type")
        count = entry.get("count")
        if refid is None or count is None:
            continue
        items.append({
            "refid": int(refid),
            "type": int(item_type) if item_type is not None else 0,
            "count": int(count),
        })
    return items


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--user-id", help="Override own in-game user id (hex)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print summary, don't write alliance_inventory.json")
    ap.add_argument("--debug", action="store_true",
                    help="Always write raw response to alliance_bank_raw.json")
    args = ap.parse_args()

    auth = load_auth()
    user_id, source = resolve_user_id(auth, args.user_id)
    safe_print(f"[auth]    server={auth['game_server']}  instance={auth['instance_id']}")
    safe_print(f"[user_id] {user_id}  (from {source})")

    raw = fetch_bank(auth, user_id)
    items = normalise_items(raw)

    if not items or args.debug:
        with open(RAW_DEBUG_FILE, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, ensure_ascii=False)
        safe_print(f"[debug]   wrote raw response to {RAW_DEBUG_FILE}")

    if not items:
        safe_print("FATAL: response had no parseable items. Inspect raw above.")
        safe_print(f"  top-level keys: {list(raw.keys())[:20]}")
        sys.exit(1)

    out = {"items": items, "timestamp": int(time.time())}
    safe_print(f"[ok]      parsed {len(items)} inventory items")

    total_count = sum(it["count"] for it in items)
    type_breakdown = {}
    for it in items:
        type_breakdown[it["type"]] = type_breakdown.get(it["type"], 0) + 1
    safe_print(f"          total quantity across all items: {total_count:,}")
    safe_print(f"          type breakdown: {type_breakdown}")

    if args.dry_run:
        safe_print("[dry-run] not writing output")
        for it in items[:10]:
            safe_print(f"          refid={it['refid']:>10}  type={it['type']}  count={it['count']:>12,}")
        if len(items) > 10:
            safe_print(f"          ...and {len(items) - 10} more")
        return

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    safe_print(f"[wrote]   {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
