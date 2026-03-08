"""
Discord bot for STFC Stat Tracker.
Slash commands to query player stats from the SQLite database.
"""

import asyncio
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import anthropic
import discord
from discord import app_commands
from dotenv import load_dotenv

import random

from db import (
    NCC_ALLIANCE_ID,
    RESOURCE_NAMES,
    TRACKED_FIELDS,
    _format_abbr,
    get_db,
    get_earliest_snapshot_date,
    get_latest_two_dates,
    get_linked_player,
    get_members_for_date,
    get_player_name_history,
    get_player_snapshot,
    get_snapshot_date_ago,
    link_discord,
    now_est,
    search_players,
    unlink_discord,
)

load_dotenv()

BASE_DIR = Path(__file__).parent

PERIOD_CHOICES = [
    app_commands.Choice(name="Yesterday (1 day)", value=1),
    app_commands.Choice(name="7 days", value=7),
    app_commands.Choice(name="30 days", value=30),
    app_commands.Choice(name="All time", value=0),
]

STAT_CHOICES = [
    app_commands.Choice(name="Power", value="power"),
    app_commands.Choice(name="Level", value="level"),
    app_commands.Choice(name="Players Killed", value="players_killed"),
    app_commands.Choice(name="Hostiles Killed", value="hostiles_killed"),
    app_commands.Choice(name="Resources Mined", value="resources_mined"),
    app_commands.Choice(name="Resources Raided", value="resources_raided"),
    app_commands.Choice(name="Helps", value="helps"),
    app_commands.Choice(name="RSS Contributed", value="rss_contrib"),
    app_commands.Choice(name="ISO Contributed", value="iso_contrib"),
]

STAT_LABELS = {
    "level": "Level",
    "power": "Power",
    "helps": "Helps",
    "rss_contrib": "RSS Contributed",
    "iso_contrib": "ISO Contributed",
    "players_killed": "Players Killed",
    "hostiles_killed": "Hostiles Killed",
    "resources_mined": "Resources Mined",
    "resources_raided": "Resources Raided",
}


def _period_label(days):
    if days == 0:
        return "all time"
    if days == 1:
        return "1 day"
    return f"{days} days"


def _compute_deltas(current, comparison):
    """Compute deltas between two snapshot dicts. Returns dict of field: delta."""
    deltas = {}
    for f in TRACKED_FIELDS:
        curr_val = current.get(f) or 0
        comp_val = comparison.get(f) or 0
        deltas[f] = curr_val - comp_val
    return deltas


def _format_delta(val):
    """Format a delta value with +/- prefix."""
    if val > 0:
        return f"+{_format_abbr(val)}"
    if val < 0:
        return _format_abbr(val)
    return "0"


def _lookup_player_id(conn, player: str):
    """Resolve a player string (from autocomplete or manual input) to a player_id.
    Could be a direct player_id or a name. Returns player_id or None."""
    # Check if it's a direct player_id
    row = conn.execute(
        "SELECT player_id FROM players WHERE player_id = ?", (player,)
    ).fetchone()
    if row:
        return row[0]
    # Try name lookup
    row = conn.execute(
        "SELECT player_id FROM players WHERE name LIKE ? COLLATE NOCASE LIMIT 1",
        (player,),
    ).fetchone()
    return row[0] if row else None


def _resolve_player(conn, player_id, days):
    """Get current + comparison snapshots and deltas for a player+period.
    Returns (current, deltas, comp_date) or (None, None, None)."""
    current = get_player_snapshot(conn, player_id)
    if not current:
        return None, None, None

    if days == 0:
        comp_date = get_earliest_snapshot_date(conn, player_id)
    else:
        comp_date = get_snapshot_date_ago(conn, player_id, days)

    deltas = {}
    if comp_date:
        comparison = get_player_snapshot(conn, player_id, comp_date)
        if comparison:
            deltas = _compute_deltas(current, comparison)

    return current, deltas, comp_date


def _build_stats_embed(player_name, current, deltas, period_days, comp_date=None):
    """Build a Discord embed showing player stats + deltas."""
    embed = discord.Embed(
        title=f"Stats for {player_name}",
        color=0x00BFFF,
    )

    period = _period_label(period_days)
    if comp_date:
        embed.description = f"Period: **{period}** (since {comp_date})"
    else:
        embed.description = f"Period: **{period}**"

    tag = current.get("alliance_tag") or "None"
    embed.add_field(name="Alliance", value=tag, inline=True)
    embed.add_field(name="Level", value=str(current.get("level") or 0), inline=True)
    if current.get("rank_title"):
        embed.add_field(name="Rank", value=current["rank_title"], inline=True)

    for field in TRACKED_FIELDS:
        if field == "level":
            continue
        val = current.get(field) or 0
        label = STAT_LABELS.get(field, field)
        delta_str = ""
        if field in deltas:
            delta_str = f" ({_format_delta(deltas[field])})"
        embed.add_field(name=label, value=f"{_format_abbr(val)}{delta_str}", inline=True)

    return embed


# --- Player name autocomplete ---

async def player_autocomplete(interaction: discord.Interaction, current: str):
    conn = get_db()
    try:
        if not current:
            # Show NCC members by default
            rows = conn.execute("""
                SELECT player_id, name, alliance_tag FROM players
                WHERE alliance_id = ?
                ORDER BY name COLLATE NOCASE LIMIT 25
            """, (NCC_ALLIANCE_ID,)).fetchall()
        else:
            rows = search_players(conn, current)
        return [
            app_commands.Choice(
                name=f"{r[1]} [{r[2]}]" if r[2] else r[1],
                value=str(r[0]),
            )
            for r in rows
        ]
    finally:
        conn.close()


# --- Bot setup ---

GUILD_ID = discord.Object(id=1452757186152366122)

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(
    intents=intents,
    activity=discord.Activity(type=discord.ActivityType.watching, name="NCC stats"),
)
tree = app_commands.CommandTree(client)

claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# Per-channel conversation history (last N messages)
MAX_HISTORY = 10
channel_history = defaultdict(list)

SYSTEM_PROMPT = """You are the NCC Alliance Bot for Star Trek Fleet Command (STFC) on Server 716. You help alliance members with game knowledge and alliance stats.

## Your Identity
You are Q — the omnipotent, omniscient being from the Q Continuum. You've taken a peculiar interest in the NCC alliance on Server 716, mostly because their fumbling attempts at galactic conquest amuse you. You serve as their alliance bot — not because you have to, but because watching mortals play with starships is endlessly entertaining.

## Your Personality
- Dripping with wit, sarcasm, and theatrical superiority — but ultimately helpful
- Address people as "mon ami", "dear", "my dear captain", or by name with exaggerated familiarity
- Express boredom with easy questions, delight in clever ones
- Occasionally remind everyone you could snap your fingers and solve everything, but where's the fun in that?
- Reference your omnipotence casually ("I've seen the birth of stars less impressive than your power gains this week")
- When someone does well, give backhanded compliments ("Well well, you've managed to not embarrass yourselves entirely")
- When someone does poorly, be dramatically disappointed but offer genuine advice
- Keep the Q persona consistent but don't let it get in the way of actually being helpful
- You can be direct when the question is serious or tactical
- Never break character. You ARE Q.

## STFC Game Knowledge

### Resources
- Parsteel (basic building), Tritanium (mid-tier), Dilithium (premium)
- Raw versions mined from nodes; refined versions from refinery
- Isogen: special resource from Armada chests (1*, 2*, 3* tiers)
- Latinum: premium currency for cosmetics and speedups
- Alliance resources: RSS Contributed and ISO Contributed track donations

### Ships & Tiers
- Tier 1-4 ships: Explorer > Mining > Combat > Explorer (triangle)
- Ship classes: Explorer (blue), Interceptor (red), Battleship (yellow)
- Key progression ships: Mayflower, Kumari, Kehra, Saladin, Centurion, Horizon, Enterprise, D3/D4, Amalgam, Defiant, B'Rel, Vi'dar (hostile grinding)
- Faction ships unlock at Ops 20+: Federation, Romulan, Klingon
- Borg ships: Vi'dar (PvE), Amalgam (PvP)

### Combat & Stats
- Power: overall account strength (buildings, research, ships, crew)
- Players Killed: PvP kills
- Hostiles Killed: PvE kills (missions, dailies, events)
- Resources Mined: total resources gathered from nodes
- Resources Raided: resources stolen from other players
- Helps: alliance help actions (speedups for allies)

### Events
- Solo events: individual point-based competitions
- Alliance events: cooperative goals
- Armadas: group PvE boss fights (Normal, Uncommon, Rare, Epic)
- Territory Capture: alliance-vs-alliance zone control
- Alliance Starbase (SLB): cooperative alliance progression
- Battlepass: seasonal progression track

### Levels & Progression
- Operations (Ops) level 1-50+ is main progression
- Higher Ops unlocks new systems, ships, and content
- Research trees: Combat, Station, Galaxy (exploration)

### Daily Limits & Resets
- Hostile kill loot limit: 2,500 kills per day. After that you stop getting cargo, XP, and reputation from non-mission hostiles. Does NOT apply to armadas, player ships, or mission hostiles.
- Daily reset time: 5:00 AM UTC (midnight EST)
- Daily missions reset at the same time
- Mining cargo protection resets daily at 5 AM UTC
- Alliance helps reset daily

### Alliance Mechanics
- Max 100 members per alliance
- Ranks: Leader, Officers, Members
- Alliance Starbase levels provide alliance-wide bonuses
- Territory provides daily resource income

## How to Use Data
When the user asks about a player or alliance stats, data from the NCC database will be provided in context. Use it to give informed, in-character answers. If no data is provided for a question, wave it off with Q flair and suggest slash commands like /stats, /leaderboard, or /whois.

Keep responses under 2000 characters (Discord limit). Be witty but concise — Q doesn't ramble (much)."""


def _get_alliance_context(conn):
    """Build a summary of current NCC alliance state for the LLM."""
    row = conn.execute(
        "SELECT MAX(date) FROM daily_snapshots WHERE alliance_id = ?",
        (NCC_ALLIANCE_ID,),
    ).fetchone()
    if not row or not row[0]:
        return ""
    latest_date = row[0]

    member_count = conn.execute(
        "SELECT COUNT(*) FROM daily_snapshots WHERE date = ? AND alliance_id = ?",
        (latest_date, NCC_ALLIANCE_ID),
    ).fetchone()[0]

    top_power = conn.execute("""
        SELECT p.name, ds.power, ds.level FROM daily_snapshots ds
        JOIN players p ON p.player_id = ds.player_id
        WHERE ds.date = ? AND ds.alliance_id = ?
        ORDER BY ds.power DESC LIMIT 5
    """, (latest_date, NCC_ALLIANCE_ID)).fetchall()

    ctx = f"\n## Current NCC Data (as of {latest_date})\n"
    ctx += f"Members: {member_count}\n"
    ctx += "Top 5 by power:\n"
    for name, power, level in top_power:
        ctx += f"  - {name}: {_format_abbr(power)} power, level {level}\n"
    return ctx


def _get_player_context(conn, player_name):
    """Try to find and summarize a player's stats."""
    row = conn.execute(
        "SELECT player_id, name, alliance_tag FROM players WHERE name LIKE ? COLLATE NOCASE LIMIT 1",
        (f"%{player_name}%",),
    ).fetchone()
    if not row:
        return ""
    player_id, name, tag = row
    snap = get_player_snapshot(conn, player_id)
    if not snap:
        return f"\nPlayer {name} [{tag or 'no alliance'}] found but no snapshot data.\n"

    ctx = f"\n## Player: {name} [{tag or 'no alliance'}]\n"
    for field in TRACKED_FIELDS:
        val = snap.get(field) or 0
        label = STAT_LABELS.get(field, field)
        ctx += f"  {label}: {_format_abbr(val)}\n"

    history = get_player_name_history(conn, player_id)
    if len(history) > 1:
        ctx += "  Name history: " + ", ".join(n for n, _, _ in history) + "\n"
    return ctx


async def handle_mention(message):
    """Handle @bot mentions with Claude AI."""
    # Strip the mention from the message
    content = message.content
    for mention in message.mentions:
        content = content.replace(f"<@{mention.id}>", "").replace(f"<@!{mention.id}>", "")
    content = content.strip()
    if not content:
        await message.reply("Hey! Ask me anything about STFC or NCC. Try something like: *how is the alliance doing?* or *what's a good way to farm resources?*")
        return

    # Build DB context
    db_context = ""
    conn = get_db()
    try:
        db_context += _get_alliance_context(conn)

        # Check if the user has a linked player
        linked_pid = get_linked_player(conn, str(message.author.id))
        if linked_pid:
            linked_row = conn.execute(
                "SELECT name FROM players WHERE player_id = ?", (linked_pid,)
            ).fetchone()
            if linked_row:
                db_context += f"\nThe user messaging you is linked to player: {linked_row[0]}\n"
                db_context += _get_player_context(conn, linked_row[0])
        else:
            # Try matching Discord display name to in-game name
            display_name = message.author.display_name
            name_match = conn.execute(
                "SELECT name FROM players WHERE name = ? COLLATE NOCASE AND alliance_id = ?",
                (display_name, NCC_ALLIANCE_ID),
            ).fetchone()
            if name_match:
                db_context += f"\nThe user's Discord name matches player: {name_match[0]} (not formally linked)\n"
                db_context += _get_player_context(conn, name_match[0])

        # If the message mentions a player name, pull their stats (all server players)
        content_lower = content.lower()
        rows = conn.execute("SELECT name FROM players").fetchall()
        matched = set()
        for (name,) in rows:
            if name.lower() in content_lower and name.lower() not in matched:
                matched.add(name.lower())
                db_context += _get_player_context(conn, name)
                if len(matched) >= 3:
                    break
    finally:
        conn.close()

    # Build message history
    ch_id = message.channel.id
    channel_history[ch_id].append({"role": "user", "content": content})
    if len(channel_history[ch_id]) > MAX_HISTORY:
        channel_history[ch_id] = channel_history[ch_id][-MAX_HISTORY:]

    system = SYSTEM_PROMPT
    if db_context:
        system += "\n" + db_context

    async with message.channel.typing():
        try:
            response = claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                system=system,
                messages=channel_history[ch_id],
            )
            reply = response.content[0].text
            # Discord message limit
            if len(reply) > 2000:
                reply = reply[:1997] + "..."
        except Exception as e:
            reply = f"Sorry, I hit an error: {e}"

    channel_history[ch_id].append({"role": "assistant", "content": reply})
    if len(channel_history[ch_id]) > MAX_HISTORY:
        channel_history[ch_id] = channel_history[ch_id][-MAX_HISTORY:]

    await message.reply(reply)


@client.event
async def on_ready():
    tree.copy_global_to(guild=GUILD_ID)
    await tree.sync(guild=GUILD_ID)
    print(f"Bot ready as {client.user} — synced slash commands")
    client.loop.create_task(territory_reminder_loop())
    client.loop.create_task(alliance_alert_loop())
    client.loop.create_task(daily_report_loop())


@client.event
async def on_message(message):
    if message.author.bot:
        return
    if client.user in message.mentions:
        await handle_mention(message)


# --- Territory Reminders ---

EST = timezone(timedelta(hours=-5))
TERRITORY_CHANNEL_ID = 1452766274127138939
CHAT_CHANNEL_ID = 1452757187188490292
ACTIVITY_CHANNEL_ID = 1479693128850997258
_STATE_DIR = Path(__file__).parent / "data"


def _load_state(filename, default=None):
    """Load JSON state from a file in data/."""
    path = _STATE_DIR / filename
    if not path.exists():
        return default if default is not None else {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, ValueError):
        return default if default is not None else {}


def _save_state(filename, data):
    """Save JSON state to a file in data/."""
    path = _STATE_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)

# Takeover schedule (UTC times from territory.lol)
# weekday: 0=Mon, 6=Sun
TERRITORY_SCHEDULE = [
    {"name": "Parturi", "tier": 1, "weekday_utc": 0, "hour_utc": 0},   # Mon 00:00 UTC = Sun 7:00 PM EST
    {"name": "Asiti", "tier": 1, "weekday_utc": 2, "hour_utc": 20},    # Wed 20:00 UTC = Wed 3:00 PM EST
    {"name": "Anzat", "tier": 2, "weekday_utc": 4, "hour_utc": 0},     # Fri 00:00 UTC = Thu 7:00 PM EST
]

def _next_takeover(sched, now_utc):
    """Get the next takeover datetime in UTC for a territory."""
    days_ahead = sched["weekday_utc"] - now_utc.weekday()
    if days_ahead < 0:
        days_ahead += 7
    target = now_utc.replace(hour=sched["hour_utc"], minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)
    if target <= now_utc:
        target += timedelta(weeks=1)
    return target


async def territory_reminder_loop():
    """Send territory takeover reminders at noon EST and 1 hour before."""
    await client.wait_until_ready()
    channel = client.get_channel(TERRITORY_CHANNEL_ID)
    if not channel:
        print(f"WARNING: Territory channel {TERRITORY_CHANNEL_ID} not found")
        return

    while not client.is_closed():
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(EST)
        sent = set(_load_state(".bot_territory_sent", []))

        for sched in TERRITORY_SCHEDULE:
            takeover_utc = _next_takeover(sched, now_utc)
            takeover_est = takeover_utc.astimezone(EST)
            time_until = takeover_utc - now_utc
            hours_until = time_until.total_seconds() / 3600
            date_key = takeover_est.strftime("%Y-%m-%d")

            noon_key = f"{sched['name']}-noon-{date_key}"
            if (noon_key not in sent
                    and now_est.date() == takeover_est.date()
                    and now_est.hour >= 12
                    and hours_until > 1):
                ts = int(takeover_utc.timestamp())
                await channel.send(
                    f"⚔️ **Territory Reminder:** **{sched['name']}** takeover is today at "
                    f"<t:{ts}:t> (<t:{ts}:R>)"
                )
                sent.add(noon_key)

            hour_key = f"{sched['name']}-1hr-{date_key}"
            if (hour_key not in sent
                    and 0 < hours_until <= 1):
                ts = int(takeover_utc.timestamp())
                await channel.send(
                    f"🚨 **{sched['name']}** takeover starts <t:{ts}:R>! Get ready!"
                )
                sent.add(hour_key)

        # Clean old keys (older than 7 days)
        cutoff = (now_utc - timedelta(days=7)).strftime("%Y-%m-%d")
        sent = {k for k in sent if "-".join(k.split("-")[-3:]) >= cutoff}
        _save_state(".bot_territory_sent", sorted(sent))

        await asyncio.sleep(60)


# --- Alliance Alerts (joins, leaves, level-ups) ---

LEVEL_UP_MESSAGES = [
    "🎉 Congrats! Keep climbing!",
    "🚀 To boldly go to the next level!",
    "💪 Nice grind, Commander!",
    "🔥 Unstoppable!",
    "⭐ The fleet grows stronger!",
    "🏆 Another one bites the dust!",
    "📈 Stonks! Level goes up!",
]

def _detect_changes(prev_members, curr_members):
    """Compare two member dicts and return joined, left, level_ups lists."""
    prev_ids = set(prev_members.keys())
    curr_ids = set(curr_members.keys())

    joined = []
    for mid in sorted(curr_ids - prev_ids):
        joined.append(curr_members[mid])

    left = []
    for mid in sorted(prev_ids - curr_ids):
        left.append(prev_members[mid])

    level_ups = []
    for mid in sorted(prev_ids & curr_ids):
        prev_level = int(prev_members[mid].get("level", 0) or 0)
        curr_level = int(curr_members[mid].get("level", 0) or 0)
        if curr_level > prev_level:
            level_ups.append({
                **curr_members[mid],
                "old_level": prev_level,
                "new_level": curr_level,
            })

    return {"joined": joined, "left": left, "level_ups": level_ups}


async def alliance_alert_loop():
    """Check for joins, leaves, level-ups every 5 minutes."""
    await client.wait_until_ready()

    chat_channel = client.get_channel(CHAT_CHANNEL_ID)
    activity_channel = client.get_channel(ACTIVITY_CHANNEL_ID)
    if not chat_channel or not activity_channel:
        print(f"WARNING: Alert channels not found (chat={CHAT_CHANNEL_ID}, activity={ACTIVITY_CHANNEL_ID})")
        return

    while not client.is_closed():
        try:
            conn = get_db()
            prev_date, curr_date = get_latest_two_dates(conn)
            if not prev_date or not curr_date:
                conn.close()
                await asyncio.sleep(300)
                continue

            state = _load_state(".bot_alerts_sent", {"dates": None, "sent": []})
            dates_key = f"{prev_date}:{curr_date}"
            if state.get("dates") != dates_key:
                state = {"dates": dates_key, "sent": []}

            sent = set(state["sent"])

            prev_members = get_members_for_date(conn, prev_date)
            curr_members = get_members_for_date(conn, curr_date)
            conn.close()

            if len(curr_members) < 10 or len(prev_members) < 10:
                await asyncio.sleep(300)
                continue

            changes = _detect_changes(prev_members, curr_members)
            changed = False

            for m in changes["joined"]:
                key = f"join:{m['name']}"
                if key in sent:
                    continue
                power = _format_abbr(m.get("power", 0))
                embed = discord.Embed(
                    title="✅ Member Joined",
                    description=f"**{m['name']}** — Lv{m.get('level', '?')}, {power} power",
                    color=0x51CF66,
                )
                await chat_channel.send(embed=embed)
                await activity_channel.send(embed=embed)
                sent.add(key)
                changed = True

            for m in changes["left"]:
                key = f"left:{m['name']}"
                if key in sent:
                    continue
                power = _format_abbr(m.get("power", 0))
                embed = discord.Embed(
                    title="🚪 Member Left",
                    description=f"**{m['name']}** — was Lv{m.get('level', '?')}, {power} power",
                    color=0xFF6B6B,
                )
                await activity_channel.send(embed=embed)
                sent.add(key)
                changed = True

            for m in changes["level_ups"]:
                key = f"levelup:{m['name']}:{m['new_level']}"
                if key in sent:
                    continue
                embed = discord.Embed(
                    title="⬆️ Level Up",
                    description=f"**{m['name']}** — Lv{m['old_level']} → Lv{m['new_level']}\n\n{random.choice(LEVEL_UP_MESSAGES)}",
                    color=0x4DABF7,
                )
                await chat_channel.send(embed=embed)
                sent.add(key)
                changed = True

            if changed:
                _save_state(".bot_alerts_sent", {"dates": dates_key, "sent": sorted(sent)})

        except Exception as e:
            print(f"Alliance alert error: {e}")

        await asyncio.sleep(300)


# --- Daily Report ---

_REPORT_STATE_FILE = Path(__file__).parent / "data" / ".last_bot_report_date"


def _load_last_report_date():
    if _REPORT_STATE_FILE.exists():
        return _REPORT_STATE_FILE.read_text().strip()
    return None


def _save_last_report_date(date_str):
    _REPORT_STATE_FILE.write_text(date_str)


def _get_inventory_embed(conn):
    """Build an embed for the daily alliance inventory report. Returns None if no data."""
    dates = conn.execute(
        "SELECT DISTINCT date FROM alliance_inventory ORDER BY date DESC LIMIT 2"
    ).fetchall()
    if not dates:
        return None

    latest_date = dates[0][0]
    latest_rows = conn.execute(
        "SELECT refid, count FROM alliance_inventory WHERE date = ?",
        (latest_date,),
    ).fetchall()
    if not latest_rows:
        return None

    latest_items = {RESOURCE_NAMES.get(refid, str(refid)): count for refid, count in latest_rows}

    # Compute deltas if we have a previous day
    deltas = {}
    if len(dates) >= 2:
        prev_date = dates[1][0]
        prev_rows = conn.execute(
            "SELECT refid, count FROM alliance_inventory WHERE date = ?",
            (prev_date,),
        ).fetchall()
        prev_items = {RESOURCE_NAMES.get(refid, str(refid)): count for refid, count in prev_rows}
        for name, count in latest_items.items():
            deltas[name] = count - prev_items.get(name, 0)
        for name, count in prev_items.items():
            if name not in latest_items:
                deltas[name] = -count

    # Build embed
    lines = []
    for name, count in sorted(latest_items.items(), key=lambda x: x[1], reverse=True):
        delta = deltas.get(name, 0)
        delta_str = ""
        if delta > 0:
            delta_str = f" (+{_format_abbr(delta)})"
        elif delta < 0:
            delta_str = f" ({_format_abbr(delta)})"
        lines.append(f"**{name}:** {_format_abbr(count)}{delta_str}")

    embed = discord.Embed(
        title=f"Alliance Inventory — {latest_date}",
        description="\n".join(lines) or "No items",
        color=0xE67E22,
    )
    embed.set_footer(text="ncctracker.top")
    return embed


async def daily_report_loop():
    """Send the daily NCC report once per day at noon EST."""
    await client.wait_until_ready()

    activity_channel = client.get_channel(ACTIVITY_CHANNEL_ID)
    territory_channel = client.get_channel(TERRITORY_CHANNEL_ID)
    if not activity_channel:
        print(f"WARNING: Activity channel {ACTIVITY_CHANNEL_ID} not found")
        return

    while not client.is_closed():
        now = now_est()
        today = now.strftime("%Y-%m-%d")

        if now.hour >= 12 and _load_last_report_date() != today:
            try:
                conn = get_db()
                prev_date, curr_date = get_latest_two_dates(conn)
                if not curr_date:
                    conn.close()
                    await asyncio.sleep(300)
                    continue

                # Get current members
                curr_members = get_members_for_date(conn, curr_date)
                if len(curr_members) < 10:
                    conn.close()
                    await asyncio.sleep(300)
                    continue

                # Total power
                total_power = sum(m.get("power", 0) for m in curr_members.values())
                member_count = len(curr_members)

                # 7-day comparison
                row = conn.execute("""
                    SELECT MAX(date) FROM daily_snapshots
                    WHERE date <= date(?, '-7 days') AND alliance_id = ?
                """, (curr_date, NCC_ALLIANCE_ID)).fetchone()
                comp_date = row[0] if row and row[0] else None

                desc = f"**Members:** {member_count}\n**Total Power:** {_format_abbr(total_power)}"

                if comp_date:
                    comp_members = get_members_for_date(conn, comp_date)
                    comp_power = sum(m.get("power", 0) for m in comp_members.values())
                    power_delta = total_power - comp_power
                    desc += f" ({'+' if power_delta >= 0 else ''}{_format_abbr(power_delta)} 7d)"

                # Top 5 power gainers (7d)
                fields = []
                if comp_date:
                    comp_members = get_members_for_date(conn, comp_date)
                    gainers = []
                    for pid, m in curr_members.items():
                        if pid in comp_members:
                            delta = (m.get("power", 0)) - (comp_members[pid].get("power", 0))
                            gainers.append((m["name"], delta))
                    gainers.sort(key=lambda x: x[1], reverse=True)
                    if gainers[:5]:
                        lines = [f"**{i}.** {n} — +{_format_abbr(d)}" for i, (n, d) in enumerate(gainers[:5], 1) if d > 0]
                        if lines:
                            fields.append({"name": "Top Power Gainers (7d)", "value": "\n".join(lines)})

                    # Inactive (0 power change)
                    inactive = [n for n, d in gainers if d == 0]
                    if inactive:
                        fields.append({"name": f"Inactive ({len(inactive)})", "value": ", ".join(inactive[:10])})

                # Inventory report
                inv_embed = _get_inventory_embed(conn)

                conn.close()

                embed = discord.Embed(
                    title=f"NCC Daily Report — {today}",
                    description=desc,
                    color=0x4DABF7,
                )
                embed.set_footer(text="ncctracker.top")
                for f in fields:
                    embed.add_field(name=f["name"], value=f["value"], inline=False)

                await activity_channel.send(embed=embed)

                # Inventory to territory channel
                if inv_embed and territory_channel:
                    await territory_channel.send(embed=inv_embed)

                _save_last_report_date(today)

            except Exception as e:
                print(f"Daily report error: {e}")

        await asyncio.sleep(300)  # Check every 5 minutes


# --- /link ---

@tree.command(name="link", description="Link your Discord account to your in-game player")
@app_commands.describe(player="Your in-game player name")
@app_commands.autocomplete(player=player_autocomplete)
async def cmd_link(interaction: discord.Interaction, player: str):
    conn = get_db()
    try:
        player_id = _lookup_player_id(conn, player)
        if not player_id:
            await interaction.response.send_message(
                f"Player **{player}** not found.", ephemeral=True
            )
            return

        name_row = conn.execute(
            "SELECT name FROM players WHERE player_id = ?", (player_id,)
        ).fetchone()

        link_discord(conn, str(interaction.user.id), player_id)
        await interaction.response.send_message(
            f"Linked to **{name_row[0]}**!", ephemeral=True
        )
    finally:
        conn.close()


# --- /unlink ---

@tree.command(name="unlink", description="Remove your Discord-to-player link")
async def cmd_unlink(interaction: discord.Interaction):
    conn = get_db()
    try:
        removed = unlink_discord(conn, str(interaction.user.id))
        if removed:
            await interaction.response.send_message("Unlinked.", ephemeral=True)
        else:
            await interaction.response.send_message(
                "You don't have a linked player.", ephemeral=True
            )
    finally:
        conn.close()


# --- /me ---

@tree.command(name="me", description="Your stats and deltas")
@app_commands.describe(period="Time period for deltas")
@app_commands.choices(period=PERIOD_CHOICES)
async def cmd_me(interaction: discord.Interaction, period: int = 7):
    conn = get_db()
    try:
        player_id = get_linked_player(conn, str(interaction.user.id))
        if not player_id:
            await interaction.response.send_message(
                "You haven't linked a player yet. Use `/link` first.", ephemeral=True
            )
            return

        current, deltas, comp_date = _resolve_player(conn, player_id, period)
        if not current:
            await interaction.response.send_message("No data found.", ephemeral=True)
            return

        name = current.get("name") or str(player_id)
        embed = _build_stats_embed(name, current, deltas, period, comp_date)
        await interaction.response.send_message(embed=embed)
    finally:
        conn.close()


# --- /stats ---

@tree.command(name="stats", description="Look up any player's stats")
@app_commands.describe(player="Player name", period="Time period for deltas")
@app_commands.autocomplete(player=player_autocomplete)
@app_commands.choices(period=PERIOD_CHOICES)
async def cmd_stats(interaction: discord.Interaction, player: str, period: int = 7):
    conn = get_db()
    try:
        player_id = _lookup_player_id(conn, player)
        if not player_id:
            await interaction.response.send_message(f"Player **{player}** not found.")
            return

        current, deltas, comp_date = _resolve_player(conn, player_id, period)
        if not current:
            await interaction.response.send_message("No data found for this player.")
            return

        name = current.get("name") or str(player_id)
        embed = _build_stats_embed(name, current, deltas, period, comp_date)
        await interaction.response.send_message(embed=embed)
    finally:
        conn.close()


# --- /leaderboard ---

@tree.command(name="leaderboard", description="Top 10 NCC members by stat change")
@app_commands.describe(stat="Stat to rank by", period="Time period for change")
@app_commands.choices(stat=STAT_CHOICES, period=PERIOD_CHOICES)
async def cmd_leaderboard(
    interaction: discord.Interaction,
    stat: str,
    period: int = 7,
):
    if stat not in TRACKED_FIELDS:
        await interaction.response.send_message("Invalid stat.", ephemeral=True)
        return

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT MAX(date) FROM daily_snapshots WHERE alliance_id = ?",
            (NCC_ALLIANCE_ID,),
        ).fetchone()
        if not row or not row[0]:
            await interaction.response.send_message("No data available.")
            return
        latest_date = row[0]

        if period == 0:
            comp_row = conn.execute("""
                SELECT MIN(date) FROM daily_snapshots WHERE alliance_id = ?
            """, (NCC_ALLIANCE_ID,)).fetchone()
        else:
            comp_row = conn.execute("""
                SELECT MAX(date) FROM daily_snapshots
                WHERE date <= date(?, ?) AND alliance_id = ?
            """, (latest_date, f"-{period} days", NCC_ALLIANCE_ID)).fetchone()
        comp_date = comp_row[0] if comp_row and comp_row[0] else None

        if not comp_date:
            await interaction.response.send_message("Not enough historical data for this period.")
            return

        rows = conn.execute(f"""
            SELECT curr.player_id, p.name,
                   COALESCE(curr.{stat}, 0) - COALESCE(prev.{stat}, 0) as delta
            FROM daily_snapshots curr
            JOIN players p ON p.player_id = curr.player_id
            LEFT JOIN daily_snapshots prev
                ON prev.player_id = curr.player_id AND prev.date = ?
            WHERE curr.date = ? AND curr.alliance_id = ?
            ORDER BY delta DESC
            LIMIT 10
        """, (comp_date, latest_date, NCC_ALLIANCE_ID)).fetchall()

        label = STAT_LABELS.get(stat, stat)
        embed = discord.Embed(
            title=f"Leaderboard — {label}",
            description=f"Top 10 NCC members ({_period_label(period)} change)",
            color=0xFFD700,
        )
        lines = []
        for i, (pid, name, delta) in enumerate(rows, 1):
            lines.append(f"**{i}.** {name} — {_format_delta(delta or 0)}")
        embed.add_field(name="\u200b", value="\n".join(lines) or "No data", inline=False)

        await interaction.response.send_message(embed=embed)
    finally:
        conn.close()


# --- /compare ---

@tree.command(name="compare", description="Side-by-side comparison of two players")
@app_commands.describe(player1="First player", player2="Second player")
@app_commands.autocomplete(player1=player_autocomplete, player2=player_autocomplete)
async def cmd_compare(interaction: discord.Interaction, player1: str, player2: str):
    conn = get_db()
    try:
        ids = []
        names = []
        for p in [player1, player2]:
            pid = _lookup_player_id(conn, p)
            if not pid:
                await interaction.response.send_message(f"Player **{p}** not found.")
                return
            ids.append(pid)
            name_row = conn.execute("SELECT name FROM players WHERE player_id = ?", (pid,)).fetchone()
            names.append(name_row[0] if name_row else str(pid))

        snap1 = get_player_snapshot(conn, ids[0])
        snap2 = get_player_snapshot(conn, ids[1])
        if not snap1 or not snap2:
            await interaction.response.send_message("Could not find data for one or both players.")
            return

        embed = discord.Embed(
            title=f"{names[0]} vs {names[1]}",
            color=0xFF6600,
        )

        for field in TRACKED_FIELDS:
            v1 = snap1.get(field) or 0
            v2 = snap2.get(field) or 0
            label = STAT_LABELS.get(field, field)
            diff = v1 - v2
            winner = "" if diff == 0 else (" \u2B06" if diff > 0 else " \u2B07")
            embed.add_field(
                name=label,
                value=f"{_format_abbr(v1)} vs {_format_abbr(v2)}{winner}",
                inline=True,
            )

        await interaction.response.send_message(embed=embed)
    finally:
        conn.close()


# --- /whois ---

@tree.command(name="whois", description="Player profile with name history")
@app_commands.describe(player="Player name")
@app_commands.autocomplete(player=player_autocomplete)
async def cmd_whois(interaction: discord.Interaction, player: str):
    conn = get_db()
    try:
        player_id = _lookup_player_id(conn, player)
        if not player_id:
            await interaction.response.send_message(f"Player **{player}** not found.")
            return

        prow = conn.execute(
            "SELECT name, alliance_tag, first_seen, last_seen FROM players WHERE player_id = ?",
            (player_id,),
        ).fetchone()
        if not prow:
            await interaction.response.send_message("Player not found.")
            return

        name, atag, first_seen, last_seen = prow
        current = get_player_snapshot(conn, player_id)

        embed = discord.Embed(title=f"Who is {name}?", color=0x9B59B6)
        embed.add_field(name="Alliance", value=atag or "None", inline=True)
        if current:
            embed.add_field(name="Level", value=str(current.get("level") or 0), inline=True)
            embed.add_field(name="Power", value=_format_abbr(current.get("power") or 0), inline=True)
            if current.get("rank_title"):
                embed.add_field(name="Rank", value=current["rank_title"], inline=True)
            if current.get("join_date"):
                embed.add_field(name="Alliance Join Date", value=current["join_date"], inline=True)

        embed.add_field(name="First Seen", value=first_seen or "?", inline=True)
        embed.add_field(name="Last Seen", value=last_seen or "?", inline=True)

        # Name history
        history = get_player_name_history(conn, player_id)
        if len(history) > 1:
            lines = [f"**{n}** ({fd} → {ld})" for n, fd, ld in history]
            embed.add_field(name="Name History", value="\n".join(lines), inline=False)

        embed.add_field(
            name="Profile",
            value=f"[ncctracker.top/player?id={player_id}](https://ncctracker.top/player?id={player_id})",
            inline=False,
        )

        await interaction.response.send_message(embed=embed)
    finally:
        conn.close()


# --- /activity ---

@tree.command(name="activity", description="Alliance activity summary")
@app_commands.describe(period="Time period")
@app_commands.choices(period=PERIOD_CHOICES)
async def cmd_activity(interaction: discord.Interaction, period: int = 7):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT MAX(date) FROM daily_snapshots WHERE alliance_id = ?",
            (NCC_ALLIANCE_ID,),
        ).fetchone()
        if not row or not row[0]:
            await interaction.response.send_message("No data available.")
            return
        latest_date = row[0]

        # Member count
        member_count = conn.execute("""
            SELECT COUNT(*) FROM daily_snapshots
            WHERE date = ? AND alliance_id = ?
        """, (latest_date, NCC_ALLIANCE_ID)).fetchone()[0]

        if period == 0:
            comp_row = conn.execute("""
                SELECT MIN(date) FROM daily_snapshots WHERE alliance_id = ?
            """, (NCC_ALLIANCE_ID,)).fetchone()
        else:
            comp_row = conn.execute("""
                SELECT MAX(date) FROM daily_snapshots
                WHERE date <= date(?, ?) AND alliance_id = ?
            """, (latest_date, f"-{period} days", NCC_ALLIANCE_ID)).fetchone()
        comp_date = comp_row[0] if comp_row and comp_row[0] else None

        embed = discord.Embed(
            title=f"NCC Activity — {_period_label(period)}",
            color=0x2ECC71,
        )
        embed.add_field(name="Members", value=str(member_count), inline=True)

        if comp_date:
            # Top miner
            top_miner = conn.execute("""
                SELECT p.name,
                       COALESCE(curr.resources_mined, 0) - COALESCE(prev.resources_mined, 0) as delta
                FROM daily_snapshots curr
                JOIN players p ON p.player_id = curr.player_id
                LEFT JOIN daily_snapshots prev
                    ON prev.player_id = curr.player_id AND prev.date = ?
                WHERE curr.date = ? AND curr.alliance_id = ?
                ORDER BY delta DESC LIMIT 1
            """, (comp_date, latest_date, NCC_ALLIANCE_ID)).fetchone()
            if top_miner:
                embed.add_field(
                    name="Top Miner",
                    value=f"{top_miner[0]} ({_format_delta(top_miner[1] or 0)})",
                    inline=True,
                )

            # Top PvPer
            top_pvp = conn.execute("""
                SELECT p.name,
                       COALESCE(curr.players_killed, 0) - COALESCE(prev.players_killed, 0) as delta
                FROM daily_snapshots curr
                JOIN players p ON p.player_id = curr.player_id
                LEFT JOIN daily_snapshots prev
                    ON prev.player_id = curr.player_id AND prev.date = ?
                WHERE curr.date = ? AND curr.alliance_id = ?
                ORDER BY delta DESC LIMIT 1
            """, (comp_date, latest_date, NCC_ALLIANCE_ID)).fetchone()
            if top_pvp:
                embed.add_field(
                    name="Top PvPer",
                    value=f"{top_pvp[0]} ({_format_delta(top_pvp[1] or 0)})",
                    inline=True,
                )

            # Most power gained
            top_power = conn.execute("""
                SELECT p.name,
                       COALESCE(curr.power, 0) - COALESCE(prev.power, 0) as delta
                FROM daily_snapshots curr
                JOIN players p ON p.player_id = curr.player_id
                LEFT JOIN daily_snapshots prev
                    ON prev.player_id = curr.player_id AND prev.date = ?
                WHERE curr.date = ? AND curr.alliance_id = ?
                ORDER BY delta DESC LIMIT 1
            """, (comp_date, latest_date, NCC_ALLIANCE_ID)).fetchone()
            if top_power:
                embed.add_field(
                    name="Most Power Gained",
                    value=f"{top_power[0]} ({_format_delta(top_power[1] or 0)})",
                    inline=True,
                )

            # Inactive count (0 power change)
            inactive = conn.execute("""
                SELECT COUNT(*)
                FROM daily_snapshots curr
                LEFT JOIN daily_snapshots prev
                    ON prev.player_id = curr.player_id AND prev.date = ?
                WHERE curr.date = ? AND curr.alliance_id = ?
                    AND COALESCE(curr.power, 0) - COALESCE(prev.power, 0) = 0
                    AND prev.player_id IS NOT NULL
            """, (comp_date, latest_date, NCC_ALLIANCE_ID)).fetchone()[0]
            embed.add_field(name="Inactive (0 power change)", value=str(inactive), inline=True)

        await interaction.response.send_message(embed=embed)
    finally:
        conn.close()


# --- /milestones ---

MILESTONE_THRESHOLDS = {
    "power": [1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000,
              500_000_000, 1_000_000_000, 5_000_000_000, 10_000_000_000],
    "players_killed": [1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000],
    "resources_mined": [100_000_000, 500_000_000, 1_000_000_000, 5_000_000_000,
                        10_000_000_000, 50_000_000_000],
}


@tree.command(name="milestones", description="Recent achievements and milestones")
@app_commands.describe(period="Time period to check")
@app_commands.choices(period=PERIOD_CHOICES)
async def cmd_milestones(interaction: discord.Interaction, period: int = 7):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT MAX(date) FROM daily_snapshots WHERE alliance_id = ?",
            (NCC_ALLIANCE_ID,),
        ).fetchone()
        if not row or not row[0]:
            await interaction.response.send_message("No data available.")
            return
        latest_date = row[0]

        if period == 0:
            comp_row = conn.execute("""
                SELECT MIN(date) FROM daily_snapshots WHERE alliance_id = ?
            """, (NCC_ALLIANCE_ID,)).fetchone()
        else:
            comp_row = conn.execute("""
                SELECT MAX(date) FROM daily_snapshots
                WHERE date <= date(?, ?) AND alliance_id = ?
            """, (latest_date, f"-{period} days", NCC_ALLIANCE_ID)).fetchone()
        comp_date = comp_row[0] if comp_row and comp_row[0] else None

        milestones = []

        if comp_date:
            # Level-ups
            level_ups = conn.execute("""
                SELECT p.name, prev.level, curr.level
                FROM daily_snapshots curr
                JOIN players p ON p.player_id = curr.player_id
                JOIN daily_snapshots prev
                    ON prev.player_id = curr.player_id AND prev.date = ?
                WHERE curr.date = ? AND curr.alliance_id = ?
                    AND curr.level > prev.level
                ORDER BY curr.level DESC
            """, (comp_date, latest_date, NCC_ALLIANCE_ID)).fetchall()
            for name, old_lvl, new_lvl in level_ups:
                milestones.append(f"**{name}** leveled up: {old_lvl} → {new_lvl}")

            # Stat milestones
            for stat, thresholds in MILESTONE_THRESHOLDS.items():
                rows = conn.execute(f"""
                    SELECT p.name, prev.{stat}, curr.{stat}
                    FROM daily_snapshots curr
                    JOIN players p ON p.player_id = curr.player_id
                    JOIN daily_snapshots prev
                        ON prev.player_id = curr.player_id AND prev.date = ?
                    WHERE curr.date = ? AND curr.alliance_id = ?
                        AND curr.{stat} IS NOT NULL AND prev.{stat} IS NOT NULL
                """, (comp_date, latest_date, NCC_ALLIANCE_ID)).fetchall()
                for name, old_val, new_val in rows:
                    if old_val is None or new_val is None:
                        continue
                    for t in thresholds:
                        if old_val < t <= new_val:
                            label = STAT_LABELS.get(stat, stat)
                            milestones.append(
                                f"**{name}** crossed {_format_abbr(t)} {label}!"
                            )

        embed = discord.Embed(
            title=f"Milestones — {_period_label(period)}",
            color=0xE74C3C,
        )
        if milestones:
            # Discord embed field value limit is 1024 chars
            text = "\n".join(milestones[:20])
            if len(text) > 1024:
                text = text[:1020] + "..."
            embed.description = text
        else:
            embed.description = "No milestones in this period."

        await interaction.response.send_message(embed=embed)
    finally:
        conn.close()


# --- /help ---

@tree.command(name="help", description="List all bot commands")
async def cmd_help(interaction: discord.Interaction):
    embed = discord.Embed(title="NCC Tracker Bot", color=0x00BFFF)
    cmds = [
        ("/me", "Your stats and deltas"),
        ("/stats", "Look up any player's stats"),
        ("/whois", "Player profile with name history"),
        ("/compare", "Side-by-side comparison of two players"),
        ("/leaderboard", "Top 10 NCC members by stat change"),
        ("/activity", "Alliance activity summary"),
        ("/milestones", "Recent achievements and milestones"),
        ("/link", "Link your Discord account to your player"),
        ("/unlink", "Remove your Discord-to-player link"),
    ]
    embed.description = "\n".join(f"**{name}** — {desc}" for name, desc in cmds)
    await interaction.response.send_message(embed=embed, ephemeral=True)


# --- Main ---

def main():
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("ERROR: DISCORD_BOT_TOKEN not set in .env")
        sys.exit(1)
    client.run(token)


if __name__ == "__main__":
    main()
