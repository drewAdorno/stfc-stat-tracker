"""
Hourly Discord alerts for NWS Alliance: joins, leaves.
Compares the 2 most recent daily snapshots from the SQLite DB and posts
separate Discord embeds for each event type detected.
"""

import json
import sys
from pathlib import Path

from send_discord_notification import (
    load_webhook_url, post_webhook, safe_print, format_abbr, parse_abbr,
    truncate_field,
)
from db import get_db, get_latest_two_dates, get_members_for_date

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SENT_ALERTS_FILE = BASE_DIR / ".sent_hourly_alerts"

EMBED_COLOR = 0x4DABF7  # blue accent


def detect_changes(prev_members, curr_members):
    """Compare two member dicts and return joined, left lists."""
    prev_ids = set(prev_members.keys())
    curr_ids = set(curr_members.keys())

    joined = []
    for mid in sorted(curr_ids - prev_ids):
        m = curr_members[mid]
        joined.append({
            "name": m.get("name", mid),
            "level": m.get("level", "?"),
            "power": m.get("power", "0"),
        })

    left = []
    for mid in sorted(prev_ids - curr_ids):
        m = prev_members[mid]
        left.append({
            "name": m.get("name", mid),
            "level": m.get("level", "?"),
            "power": m.get("power", "0"),
        })

    return {"joined": joined, "left": left}


def build_alert_embeds(changes):
    """Build separate Discord embeds for each non-empty change type."""
    embeds = []

    if changes["joined"]:
        lines = []
        for m in changes["joined"]:
            power = format_abbr(parse_abbr(m["power"]))
            lines.append(f"**{m['name']}** \u2014 Lv{m['level']}, {power} power")
        embeds.append({
            "title": "✅ Member Joined",
            "description": truncate_field("\n".join(lines)),
            "color": 0x51CF66,  # green
            "footer": {"text": "nws.stfcdrew.lol"},
        })

    if changes["left"]:
        lines = []
        for m in changes["left"]:
            power = format_abbr(parse_abbr(m["power"]))
            lines.append(f"**{m['name']}** \u2014 was Lv{m['level']}, {power} power")
        embeds.append({
            "title": "🚪 Member Left",
            "description": truncate_field("\n".join(lines)),
            "color": 0xFF6B6B,  # red
            "footer": {"text": "nws.stfcdrew.lol"},
        })

    return embeds


def has_changes(changes):
    """Return True if any change type is non-empty."""
    return bool(changes["joined"] or changes["left"])


def _make_change_key(change_type, item):
    """Create a stable key for a single change event (for deduplication)."""
    return f"{change_type}:{item['name']}"


def _load_sent_alerts():
    """Load the set of already-sent alert keys for the current date pair."""
    if not SENT_ALERTS_FILE.exists():
        return None, set()
    try:
        with open(SENT_ALERTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("dates"), set(data.get("sent", []))
    except (json.JSONDecodeError, ValueError):
        return None, set()


def _save_sent_alerts(dates_key, sent_keys):
    """Save the set of sent alert keys for the current date pair."""
    with open(SENT_ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump({"dates": dates_key, "sent": sorted(sent_keys)}, f)


def _filter_unsent(changes, sent_keys):
    """Remove already-sent changes, returning only new ones."""
    filtered = {}
    for change_type in ("joined", "left"):
        filtered[change_type] = [
            item for item in changes[change_type]
            if _make_change_key(change_type, item) not in sent_keys
        ]
    return filtered


def main():
    conn = get_db()
    prev_date, curr_date = get_latest_two_dates(conn)
    if not prev_date or not curr_date:
        safe_print("Not enough snapshots for hourly alert (need at least 2 days).")
        conn.close()
        sys.exit(0)

    safe_print(f"Comparing {prev_date} vs {curr_date}")

    prev_members = get_members_for_date(conn, prev_date)
    curr_members = get_members_for_date(conn, curr_date)
    conn.close()

    # Guard against bad scrapes — if either snapshot has very few members,
    # the site was probably down and we'd send false join/leave alerts.
    if len(curr_members) < 10:
        safe_print(f"WARNING: Current snapshot has only {len(curr_members)} members — scrape may have failed. Skipping alerts.")
        sys.exit(0)
    if len(prev_members) < 10:
        safe_print(f"WARNING: Previous snapshot has only {len(prev_members)} members — skipping alerts.")
        sys.exit(0)

    changes = detect_changes(prev_members, curr_members)

    if not has_changes(changes):
        safe_print("No joins or leaves detected.")
        sys.exit(0)

    # Deduplication: skip changes we've already alerted on for this date pair
    dates_key = f"{prev_date}:{curr_date}"
    saved_dates, sent_keys = _load_sent_alerts()
    if saved_dates != dates_key:
        # Date pair changed (new day) — reset sent keys
        sent_keys = set()

    changes = _filter_unsent(changes, sent_keys)

    if not has_changes(changes):
        safe_print("All changes already alerted — skipping.")
        sys.exit(0)

    counts = []
    if changes["joined"]:
        counts.append(f"{len(changes['joined'])} joined")
    if changes["left"]:
        counts.append(f"{len(changes['left'])} left")
    safe_print(f"Changes detected: {', '.join(counts)}")

    webhook_url = load_webhook_url()
    if not webhook_url:
        sys.exit(0)

    embeds = build_alert_embeds(changes)
    failed = False
    for embed in embeds:
        if not post_webhook(webhook_url, embed):
            failed = True

    # Record what we sent (even partial success) so we don't re-send
    for change_type in ("joined", "left"):
        for item in changes[change_type]:
            sent_keys.add(_make_change_key(change_type, item))
    _save_sent_alerts(dates_key, sent_keys)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
