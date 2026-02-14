"""
Hourly Discord alerts for NCC Alliance: level ups, joins, leaves.
Compares the 2 most recent alliance_*.json snapshots and posts
separate Discord embeds for each event type detected.
Stdlib only — no pip installs needed.
"""

import glob
import json
import random
import sys
from pathlib import Path

from send_discord_notification import (
    load_webhook_url, post_webhook, safe_print, format_abbr, parse_abbr,
    truncate_field,
)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

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


def get_two_latest_snapshots(data_dir=None):
    """Return (prev, curr) paths for the 2 most recent alliance_*.json files."""
    d = data_dir or DATA_DIR
    files = sorted(glob.glob(str(d / "alliance_*.json")))
    if len(files) < 2:
        return None, None
    return Path(files[-2]), Path(files[-1])


def load_members(path):
    """Load a snapshot and return a dict keyed by member ID."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    members = {}
    for m in data.get("members", []):
        mid = m.get("id")
        if mid:
            members[mid] = m
    return members


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


def main():
    prev_path, curr_path = get_two_latest_snapshots()
    if not prev_path or not curr_path:
        safe_print("Not enough snapshots for hourly alert (need at least 2).")
        sys.exit(0)

    safe_print(f"Comparing {prev_path.name} vs {curr_path.name}")

    prev_members = load_members(prev_path)
    curr_members = load_members(curr_path)

    changes = detect_changes(prev_members, curr_members)

    if not has_changes(changes):
        safe_print("No joins, leaves, or level-ups detected.")
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
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
