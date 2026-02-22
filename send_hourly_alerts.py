"""
Hourly Discord alerts for NCC Alliance: level ups, joins, leaves.
Compares the 2 most recent daily snapshots from the SQLite DB and posts
separate Discord embeds for each event type detected.
"""

import json
import random
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

LEVEL_UP_MESSAGES = [
    "🎉 Congrats! Keep climbing!",
    "🚀 To boldly go to the next level!",
    "💪 Nice grind, Commander!",
    "🔥 Unstoppable!",
    "⭐ The fleet grows stronger!",
    "🏆 Another one bites the dust!",
    "📈 Stonks! Level goes up!",
]


def detect_changes(prev_members, curr_members):
    """Compare two member dicts and return joined, left, level_ups lists."""
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

    level_ups = []
    for mid in sorted(prev_ids & curr_ids):
        prev_level = int(prev_members[mid].get("level", "0") or "0")
        curr_level = int(curr_members[mid].get("level", "0") or "0")
        if curr_level > prev_level:
            level_ups.append({
                "name": curr_members[mid].get("name", mid),
                "old_level": str(prev_level),
                "new_level": str(curr_level),
            })

    return {"joined": joined, "left": left, "level_ups": level_ups}


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
            "footer": {"text": "ncctracker.top"},
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
            "footer": {"text": "ncctracker.top"},
        })

    if changes["level_ups"]:
        lines = []
        for m in changes["level_ups"]:
            lines.append(f"**{m['name']}** \u2014 Lv{m['old_level']} \u2192 Lv{m['new_level']}")
        lines.append("")
        lines.append(random.choice(LEVEL_UP_MESSAGES))
        embeds.append({
            "title": "⬆️ Level Up",
            "description": truncate_field("\n".join(lines)),
            "color": 0x4DABF7,  # blue
            "footer": {"text": "ncctracker.top"},
        })

    return embeds


def has_changes(changes):
    """Return True if any change type is non-empty."""
    return bool(changes["joined"] or changes["left"] or changes["level_ups"])


def _make_change_key(change_type, item):
    """Create a stable key for a single change event (for deduplication)."""
    if change_type == "level_ups":
        return f"levelup:{item['name']}:{item['new_level']}"
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
    for change_type in ("joined", "left", "level_ups"):
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
        safe_print("No joins, leaves, or level-ups detected.")
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
    if changes["level_ups"]:
        counts.append(f"{len(changes['level_ups'])} leveled up")
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
    for change_type in ("joined", "left", "level_ups"):
        for item in changes[change_type]:
            sent_keys.add(_make_change_key(change_type, item))
    _save_sent_alerts(dates_key, sent_keys)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
