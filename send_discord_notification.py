"""
Discord Webhook Notification for NCC Alliance Daily Report.
Reads data/latest.json and data/history.json, computes analytics,
and POSTs a rich embed to a Discord webhook URL from .env file.
Stdlib only — no pip installs needed.
"""

import json
import os
import re
import sys
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
ENV_FILE = BASE_DIR / ".env"

EMBED_COLOR = 0x4DABF7  # blue accent
LAST_SENT_FILE = DATA_DIR / ".last_notification_date"

NUMERIC_FIELDS = [
    "level", "power", "helps", "rss_contrib", "iso_contrib",
    "players_killed", "hostiles_killed", "resources_mined", "resources_raided",
]


def safe_print(*args, **kwargs):
    """Print that won't crash on Unicode in cp1252 console."""
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        text = " ".join(str(a) for a in args)
        print(text.encode("ascii", errors="replace").decode(), **kwargs)


# ---------------------------------------------------------------------------
# Number formatting (ported from index.html / admin.html)
# ---------------------------------------------------------------------------

def parse_abbr(s):
    """Parse abbreviated numbers like '190.58M', '198.96K', '42'."""
    if not s:
        return 0
    s = str(s).strip().replace(",", "")
    m = re.match(r"^([\d.]+)\s*([KMBQT]?)$", s, re.IGNORECASE)
    if not m:
        return 0
    num = float(m.group(1))
    suffix = m.group(2).upper()
    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12, "Q": 1e15}
    return num * multipliers.get(suffix, 1)


def format_abbr(n):
    """Format a number with abbreviation (no sign prefix)."""
    if n == 0:
        return "0"
    a = abs(n)
    sign = "-" if n < 0 else ""
    for threshold, suf in [(1e15, "Q"), (1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")]:
        if a >= threshold:
            val = a / threshold
            formatted = f"{val:.2f}".rstrip("0").rstrip(".")
            return f"{sign}{formatted}{suf}"
    formatted = f"{a:.2f}".rstrip("0").rstrip(".")
    return f"{sign}{formatted}"


def format_delta(n):
    """Format a number with +/- sign prefix."""
    if n == 0:
        return "0"
    sign = "+" if n > 0 else "-"
    a = abs(n)
    for threshold, suf in [(1e15, "Q"), (1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")]:
        if a >= threshold:
            val = a / threshold
            formatted = f"{val:.2f}".rstrip("0").rstrip(".")
            return f"{sign}{formatted}{suf}"
    formatted = f"{a:.2f}".rstrip("0").rstrip(".")
    return f"{sign}{formatted}"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_json(path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_snapshot_days_ago(history, days):
    """Find the history snapshot closest to N days ago."""
    if not history:
        return None
    target = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    best = None
    for entry in history:
        if entry["date"] <= target:
            best = entry
    if not best:
        best = history[0]
    return best


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def compute_description(latest, history):
    """Build the embed description with alliance summary + 1-day deltas."""
    s = latest.get("summary", {})
    power = s.get("total_power", "0")
    members_count = s.get("member_count", "?")
    helps = s.get("total_helps", "0")
    rss = s.get("total_rss", "0")
    iso = s.get("total_iso", "0")

    snap1 = get_snapshot_days_ago(history, 1)

    def delta_str(current_str, past_key, past_summary):
        if not past_summary or past_key not in past_summary:
            return ""
        diff = parse_abbr(current_str) - parse_abbr(past_summary[past_key])
        return f" ({format_delta(diff)})"

    past_s = snap1["summary"] if snap1 else {}

    lines = []
    lines.append(
        f"Total Power: {power}{delta_str(power, 'total_power', past_s)}"
        f" | Members: {members_count}"
    )
    lines.append(
        f"Helps: {helps}{delta_str(helps, 'total_helps', past_s)}"
        f" | RSS: {rss}{delta_str(rss, 'total_rss', past_s)}"
        f" | ISO: {iso}{delta_str(iso, 'total_iso', past_s)}"
    )
    return "\n".join(lines)


def find_new_members(members):
    """Members who joined in the last 7 days."""
    cutoff = datetime.now() - timedelta(days=7)
    result = []
    for m in members:
        jd = m.get("join_date", "")
        if not jd:
            continue
        try:
            joined = datetime.strptime(jd, "%b %d, %Y")
        except ValueError:
            continue
        if joined >= cutoff:
            result.append(m)
    return result


def find_left_members(members, history):
    """Members in yesterday's snapshot but not in current data."""
    if not history or len(history) < 2:
        return []
    current_ids = {m.get("id") for m in members if m.get("id")}
    # Use the second-to-last snapshot (yesterday) to compare
    yesterday = history[-2] if len(history) >= 2 else history[-1]
    left = []
    for pid, mdata in yesterday.get("members", {}).items():
        if pid not in current_ids:
            left.append({
                "name": mdata.get("name", pid),
                "power": mdata.get("power", "0"),
                "last_seen": yesterday["date"],
            })
    return left


def find_inactive(members, history):
    """Walk history backwards, count consecutive zero-change days per member.

    Skips the latest entry (current/partial day) since the notification runs
    right after the first scrape of the day, when data is nearly identical to
    the previous day's final scrape.  Also skips entries with empty member data
    (failed scrapes) so they don't break the consecutive-day walk.
    """
    # Filter to entries that actually have member data
    valid = [h for h in history if h.get("members")]
    if len(valid) < 3:
        return []
    # Drop the latest (partial day) entry; compare completed days only
    valid = valid[:-1]
    inactive = []
    for m in members:
        mid = m.get("id")
        if not mid:
            continue
        days_inactive = 0
        for i in range(len(valid) - 1, 0, -1):
            curr_snap = valid[i].get("members", {})
            prev_snap = valid[i - 1].get("members", {})
            curr_data = curr_snap.get(mid)
            prev_data = prev_snap.get(mid)
            if not curr_data or not prev_data:
                break
            changed = False
            for f in NUMERIC_FIELDS:
                if parse_abbr(curr_data.get(f, "0")) != parse_abbr(prev_data.get(f, "0")):
                    changed = True
                    break
            if changed:
                break
            days_inactive += 1
        if days_inactive > 0:
            inactive.append({"name": m["name"], "days": days_inactive})
    inactive.sort(key=lambda x: x["days"], reverse=True)
    return inactive[:5]


def find_power_movers(members, history):
    """Top 5 gainers and losers by 7-day power delta."""
    snap7 = get_snapshot_days_ago(history, 7)
    if not snap7 or not snap7.get("members"):
        return [], []
    deltas = []
    for m in members:
        mid = m.get("id")
        if not mid:
            continue
        past = snap7["members"].get(mid)
        if not past:
            continue
        curr = parse_abbr(m.get("power", "0"))
        prev = parse_abbr(past.get("power", "0"))
        deltas.append({"name": m["name"], "delta": curr - prev})
    deltas.sort(key=lambda x: x["delta"], reverse=True)
    gainers = [d for d in deltas[:5] if d["delta"] > 0]
    losers_all = [d for d in deltas if d["delta"] < 0]
    losers_all.sort(key=lambda x: x["delta"])
    losers = losers_all[:5]
    return gainers, losers


def find_lowest_helps(members, history):
    """Bottom 5 members by helps gained since first appearance."""
    if not history:
        return []
    results = []
    for m in members:
        mid = m.get("id")
        if not mid:
            continue
        # Find earliest snapshot containing this member
        first_helps = None
        for snap in history:
            past = snap.get("members", {}).get(mid)
            if past:
                first_helps = parse_abbr(past.get("helps", "0"))
                break
        if first_helps is None:
            continue
        gained = parse_abbr(m.get("helps", "0")) - first_helps
        results.append({"name": m["name"], "gained": gained})
    results.sort(key=lambda x: x["gained"])
    return results[:5]


# ---------------------------------------------------------------------------
# Embed builder
# ---------------------------------------------------------------------------

def truncate_field(text, limit=1024):
    """Truncate field value to Discord's limit."""
    if len(text) <= limit:
        return text
    return text[:limit - 4] + "\n..."


def build_embed(latest, history):
    """Build the Discord embed payload."""
    members = latest.get("members", [])
    today = datetime.now().strftime("%Y-%m-%d")

    embed = {
        "title": f"NCC Daily Report \u2014 {today}",
        "description": compute_description(latest, history),
        "color": EMBED_COLOR,
        "footer": {"text": "ncctracker.top"},
    }

    fields = []

    # 1. New Members (last 7 days)
    new_members = find_new_members(members)
    if new_members:
        lines = []
        for m in new_members:
            lines.append(f"**{m['name']}** \u2014 Lv{m.get('level', '?')}, {m.get('power', '?')} power, joined {m.get('join_date', '?')}")
        fields.append({
            "name": "New Members (7d)",
            "value": truncate_field("\n".join(lines)),
            "inline": False,
        })

    # 2. Members Who Left
    left = find_left_members(members, history)
    if left:
        lines = []
        for m in left:
            lines.append(f"**{m['name']}** \u2014 {m['power']} power, last seen {m['last_seen']}")
        fields.append({
            "name": "Members Who Left",
            "value": truncate_field("\n".join(lines)),
            "inline": False,
        })

    # 3. Inactive Alerts
    inactive = find_inactive(members, history)
    if inactive:
        lines = []
        for m in inactive:
            lines.append(f"**{m['name']}** \u2014 {m['days']}d inactive")
        fields.append({
            "name": "Inactive Alerts",
            "value": truncate_field("\n".join(lines)),
            "inline": False,
        })

    # 4 & 5. Power Movers
    gainers, losers = find_power_movers(members, history)
    if gainers:
        lines = [f"**{m['name']}** \u2014 {format_delta(m['delta'])}" for m in gainers]
        fields.append({
            "name": "Power Gainers (7d)",
            "value": truncate_field("\n".join(lines)),
            "inline": True,
        })
    if losers:
        lines = [f"**{m['name']}** \u2014 {format_delta(m['delta'])}" for m in losers]
        fields.append({
            "name": "Power Losers (7d)",
            "value": truncate_field("\n".join(lines)),
            "inline": True,
        })

    # 6. Lowest Helps Gained
    lowest_helps = find_lowest_helps(members, history)
    if lowest_helps:
        lines = [f"**{m['name']}** \u2014 {format_abbr(m['gained'])} gained" for m in lowest_helps]
        fields.append({
            "name": "Lowest Helps Gained",
            "value": truncate_field("\n".join(lines)),
            "inline": False,
        })

    if fields:
        embed["fields"] = fields

    return embed


# ---------------------------------------------------------------------------
# Webhook posting
# ---------------------------------------------------------------------------

def load_webhook_url():
    """Load DISCORD_WEBHOOK_URL from .env file."""
    if not ENV_FILE.exists():
        safe_print("WARNING: .env file not found. Create it with DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...")
        return None
    with open(ENV_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "DISCORD_WEBHOOK_URL":
                return value.strip().strip('"').strip("'")
    safe_print("WARNING: DISCORD_WEBHOOK_URL not found in .env file.")
    return None


def post_webhook(url, embed):
    """POST the embed to the Discord webhook."""
    payload = json.dumps({"embeds": [embed]}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "NCC-Alliance-Tracker/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            safe_print(f"Discord notification sent (HTTP {resp.status})")
            return True
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        safe_print(f"ERROR: Discord webhook returned HTTP {e.code}: {body}")
        return False
    except Exception as e:
        safe_print(f"ERROR: Failed to send Discord notification: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def already_sent_today():
    """Check if we already sent a notification today."""
    today = datetime.now().strftime("%Y-%m-%d")
    if LAST_SENT_FILE.exists():
        last = LAST_SENT_FILE.read_text().strip()
        if last == today:
            return True
    return False


def mark_sent_today():
    """Record that we sent a notification today."""
    LAST_SENT_FILE.write_text(datetime.now().strftime("%Y-%m-%d"))


def main():
    # Only send once per day (scraper runs hourly)
    if already_sent_today():
        safe_print("Discord notification already sent today, skipping.")
        sys.exit(0)

    webhook_url = load_webhook_url()
    if not webhook_url:
        sys.exit(0)  # missing config is not a fatal error

    latest = load_json(DATA_DIR / "latest.json")
    if not latest:
        safe_print("ERROR: data/latest.json not found")
        sys.exit(1)

    members = latest.get("members", [])
    if len(members) < 10:
        safe_print(f"WARNING: latest.json has only {len(members)} members — scrape may have failed. Skipping notification.")
        sys.exit(0)

    history = load_json(DATA_DIR / "history.json") or []
    history.sort(key=lambda e: e.get("date", ""))

    embed = build_embed(latest, history)

    # Check total embed size (Discord limit: 6000 chars)
    total = len(embed.get("title", "")) + len(embed.get("description", ""))
    total += len(embed.get("footer", {}).get("text", ""))
    for f in embed.get("fields", []):
        total += len(f.get("name", "")) + len(f.get("value", ""))
    if total > 5900:
        safe_print(f"WARNING: Embed size {total} chars, trimming fields...")
        while total > 5900 and embed.get("fields"):
            removed = embed["fields"].pop()
            total -= len(removed.get("name", "")) + len(removed.get("value", ""))

    safe_print(f"Embed: {embed['title']} ({total} chars, {len(embed.get('fields', []))} fields)")

    if not post_webhook(webhook_url, embed):
        sys.exit(1)

    mark_sent_today()


if __name__ == "__main__":
    main()
