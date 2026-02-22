"""
SQLite database module for STFC Stat Tracker.
Stores all player data in data/stfc.db and exports JSON files for dashboards.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "stfc.db"

NCC_ALLIANCE_ID = 3974286889
NCC_ALLIANCE_NAME = "Discovery"
SERVER = 716

TRACKED_FIELDS = [
    "level", "power", "helps", "rss_contrib", "iso_contrib",
    "players_killed", "hostiles_killed", "resources_mined", "resources_raided",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS players (
    player_id   INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    server      INTEGER NOT NULL,
    alliance_id INTEGER,
    alliance_tag TEXT,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_snapshots (
    player_id       INTEGER NOT NULL,
    date            TEXT NOT NULL,
    level           INTEGER,
    power           INTEGER,
    helps           INTEGER,
    rss_contrib     INTEGER,
    iso_contrib     INTEGER,
    players_killed  INTEGER,
    hostiles_killed INTEGER,
    resources_mined INTEGER,
    resources_raided INTEGER,
    rank_title      TEXT,
    join_date       TEXT,
    alliance_id     INTEGER,
    alliance_tag    TEXT,
    PRIMARY KEY (player_id, date)
);

CREATE TABLE IF NOT EXISTS pull_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pulled_at   TEXT NOT NULL,
    server      INTEGER NOT NULL,
    total_players INTEGER,
    source      TEXT DEFAULT 'api'
);
"""


def get_db():
    """Return a sqlite3 connection, creating DB + tables if needed."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def upsert_players(conn, mapped_players, date):
    """Bulk insert/update players and daily_snapshots for a given date.

    mapped_players: list of dicts from pull_api.map_player() with integer values.
    date: YYYY-MM-DD string.
    """
    cur = conn.cursor()

    for m in mapped_players:
        player_id = int(m["id"])

        # Upsert into players table
        cur.execute("""
            INSERT INTO players (player_id, name, server, alliance_id, alliance_tag, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                name = excluded.name,
                server = excluded.server,
                alliance_id = excluded.alliance_id,
                alliance_tag = excluded.alliance_tag,
                last_seen = excluded.last_seen
        """, (
            player_id,
            m.get("name", ""),
            SERVER,
            m.get("alliance_id", 0),
            m.get("alliance_tag", ""),
            date,
            date,
        ))

        # Upsert into daily_snapshots
        cur.execute("""
            INSERT INTO daily_snapshots
                (player_id, date, level, power, helps, rss_contrib, iso_contrib,
                 players_killed, hostiles_killed, resources_mined, resources_raided,
                 rank_title, join_date, alliance_id, alliance_tag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id, date) DO UPDATE SET
                level = excluded.level,
                power = excluded.power,
                helps = excluded.helps,
                rss_contrib = excluded.rss_contrib,
                iso_contrib = excluded.iso_contrib,
                players_killed = excluded.players_killed,
                hostiles_killed = excluded.hostiles_killed,
                resources_mined = excluded.resources_mined,
                resources_raided = excluded.resources_raided,
                rank_title = excluded.rank_title,
                join_date = excluded.join_date,
                alliance_id = excluded.alliance_id,
                alliance_tag = excluded.alliance_tag
        """, (
            player_id, date,
            m.get("level", 0),
            m.get("power", 0),
            m.get("helps", 0),
            m.get("rss_contrib", 0),
            m.get("iso_contrib", 0),
            m.get("players_killed", 0),
            m.get("hostiles_killed", 0),
            m.get("resources_mined", 0),
            m.get("resources_raided", 0),
            m.get("rank", ""),
            m.get("join_date", ""),
            m.get("alliance_id", 0),
            m.get("alliance_tag", ""),
        ))

    conn.commit()


def log_pull(conn, server, total_players, source="api"):
    """Record a pull run in the pull_log table."""
    conn.execute(
        "INSERT INTO pull_log (pulled_at, server, total_players, source) VALUES (?, ?, ?, ?)",
        (datetime.now().isoformat(), server, total_players, source),
    )
    conn.commit()


def _format_date(iso_str):
    """Convert ISO date/datetime string to MM-DD-YYYY format."""
    if not iso_str:
        return ""
    date_part = iso_str.split("T")[0]
    try:
        y, m, d = date_part.split("-")
        return f"{m}-{d}-{y}"
    except ValueError:
        return date_part


def _format_abbr(n):
    """Format an integer as an abbreviated string (e.g., 77100000 -> '77.10M').
    Matches the format the dashboards expect."""
    if n is None:
        n = 0
    n = int(n)
    if n == 0:
        return "0"
    abs_n = abs(n)
    if abs_n >= 1e15:
        return f"{n / 1e15:.2f}Q"
    if abs_n >= 1e12:
        return f"{n / 1e12:.2f}T"
    if abs_n >= 1e9:
        return f"{n / 1e9:.2f}B"
    if abs_n >= 1e6:
        return f"{n / 1e6:.2f}M"
    if abs_n >= 1e3:
        return f"{n / 1e3:.2f}K"
    return str(n)


def export_latest_json(conn, alliance_id=NCC_ALLIANCE_ID, league=""):
    """Query the most recent snapshot for an alliance and write data/latest.json.

    Output format matches what dashboards expect:
    - members: list of dicts with abbreviated string values
    - summary: dict with abbreviated totals
    """
    # Find the most recent date with data for this alliance
    row = conn.execute(
        "SELECT MAX(date) FROM daily_snapshots WHERE alliance_id = ?",
        (alliance_id,),
    ).fetchone()
    if not row or not row[0]:
        return
    latest_date = row[0]

    # Get the most recent pull timestamp
    pull_row = conn.execute(
        "SELECT pulled_at FROM pull_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    pulled_at = pull_row[0] if pull_row else datetime.now().isoformat()

    # Fetch all members for that date + alliance
    rows = conn.execute("""
        SELECT player_id, level, power, helps, rss_contrib, iso_contrib,
               players_killed, hostiles_killed, resources_mined, resources_raided,
               rank_title, join_date, alliance_id, alliance_tag
        FROM daily_snapshots
        WHERE date = ? AND alliance_id = ?
        ORDER BY power DESC
    """, (latest_date, alliance_id)).fetchall()

    members = []
    total_power = 0
    total_helps = 0
    total_rss = 0
    total_iso = 0
    level_sum = 0

    for r in rows:
        (pid, level, power, helps, rss_c, iso_c,
         pk, hk, rm, rr, rank_title, join_date, aid, atag) = r

        # Look up current player name
        name_row = conn.execute(
            "SELECT name FROM players WHERE player_id = ?", (pid,)
        ).fetchone()
        name = name_row[0] if name_row else str(pid)

        members.append({
            "name": name,
            "rank": rank_title or "",
            "level": _format_abbr(level),
            "power": _format_abbr(power),
            "helps": _format_abbr(helps),
            "rss_contrib": _format_abbr(rss_c),
            "iso_contrib": _format_abbr(iso_c),
            "join_date": _format_date(join_date),
            "id": str(pid),
            "players_killed": _format_abbr(pk),
            "hostiles_killed": _format_abbr(hk),
            "resources_mined": _format_abbr(rm),
            "resources_raided": _format_abbr(rr),
            "alliance_tag": atag or "",
            "alliance_name": NCC_ALLIANCE_NAME if aid == NCC_ALLIANCE_ID else "",
            "alliance_id": aid or 0,
        })

        total_power += power or 0
        total_helps += helps or 0
        total_rss += rss_c or 0
        total_iso += iso_c or 0
        level_sum += level or 0

    avg_level = round(level_sum / len(members)) if members else 0

    record = {
        "pulled_at": pulled_at,
        "alliance_url": f"https://v3.stfc.pro/alliances/{alliance_id}",
        "alliance_name": NCC_ALLIANCE_NAME if alliance_id == NCC_ALLIANCE_ID else "",
        "alliance_tag": "NCC" if alliance_id == NCC_ALLIANCE_ID else "",
        "summary": {
            "total_power": _format_abbr(total_power),
            "member_count": str(len(members)),
            "total_helps": _format_abbr(total_helps),
            "total_rss": _format_abbr(total_rss),
            "total_iso": _format_abbr(total_iso),
            "avg_level": str(avg_level),
            "league": league,
        },
        "members": members,
    }

    latest_file = DATA_DIR / "latest.json"
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)


def export_history_json(conn, alliance_id=NCC_ALLIANCE_ID):
    """Query all daily snapshots and write data/history.json.

    Output format: list of {date, summary, members} where members is
    a dict keyed by player_id with abbreviated string values.
    """
    dates = conn.execute(
        "SELECT DISTINCT date FROM daily_snapshots WHERE alliance_id = ? ORDER BY date",
        (alliance_id,),
    ).fetchall()

    history = []
    for (date,) in dates:
        rows = conn.execute("""
            SELECT ds.player_id, ds.level, ds.power, ds.helps, ds.rss_contrib,
                   ds.iso_contrib, ds.players_killed, ds.hostiles_killed,
                   ds.resources_mined, ds.resources_raided, p.name
            FROM daily_snapshots ds
            JOIN players p ON p.player_id = ds.player_id
            WHERE ds.date = ? AND ds.alliance_id = ?
        """, (date, alliance_id)).fetchall()

        members_snapshot = {}
        total_power = 0
        total_helps = 0
        total_rss = 0
        total_iso = 0
        level_sum = 0

        for r in rows:
            (pid, level, power, helps, rss_c, iso_c,
             pk, hk, rm, rr, name) = r

            members_snapshot[str(pid)] = {
                "level": _format_abbr(level),
                "power": _format_abbr(power),
                "helps": _format_abbr(helps),
                "rss_contrib": _format_abbr(rss_c),
                "iso_contrib": _format_abbr(iso_c),
                "players_killed": _format_abbr(pk),
                "hostiles_killed": _format_abbr(hk),
                "resources_mined": _format_abbr(rm),
                "resources_raided": _format_abbr(rr),
                "name": name,
            }

            total_power += power or 0
            total_helps += helps or 0
            total_rss += rss_c or 0
            total_iso += iso_c or 0
            level_sum += level or 0

        count = len(members_snapshot)
        avg_level = round(level_sum / count) if count else 0

        summary = {
            "total_power": _format_abbr(total_power),
            "member_count": str(count),
            "total_helps": _format_abbr(total_helps),
            "total_rss": _format_abbr(total_rss),
            "total_iso": _format_abbr(total_iso),
            "avg_level": str(avg_level),
        }

        history.append({
            "date": date,
            "summary": summary,
            "members": members_snapshot,
        })

    history_file = DATA_DIR / "history.json"
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def _parse_abbr(s):
    """Parse an abbreviated string like '77.10M' to an integer.
    Used for migrating existing history.json data."""
    if not s:
        return 0
    s = str(s).strip().replace(",", "")
    multipliers = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12, "Q": 1e15}
    if s[-1].upper() in multipliers:
        try:
            return int(float(s[:-1]) * multipliers[s[-1].upper()])
        except (ValueError, IndexError):
            return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def import_history_json(conn):
    """One-time migration: read existing history.json and backfill the DB."""
    history_file = DATA_DIR / "history.json"
    if not history_file.exists():
        print("No history.json found, skipping import.")
        return

    with open(history_file, "r", encoding="utf-8") as f:
        history = json.load(f)

    print(f"Importing {len(history)} days from history.json...")
    cur = conn.cursor()

    for entry in history:
        date = entry["date"]
        members = entry.get("members", {})

        for pid_str, m in members.items():
            player_id = int(pid_str)
            name = m.get("name", "")
            level = _parse_abbr(m.get("level", 0))
            power = _parse_abbr(m.get("power", 0))
            helps = _parse_abbr(m.get("helps", 0))
            rss_c = _parse_abbr(m.get("rss_contrib", 0))
            iso_c = _parse_abbr(m.get("iso_contrib", 0))
            pk = _parse_abbr(m.get("players_killed", 0))
            hk = _parse_abbr(m.get("hostiles_killed", 0))
            rm = _parse_abbr(m.get("resources_mined", 0))
            rr = _parse_abbr(m.get("resources_raided", 0))

            # Upsert player
            cur.execute("""
                INSERT INTO players (player_id, name, server, alliance_id, alliance_tag, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(player_id) DO UPDATE SET
                    name = CASE WHEN excluded.last_seen > players.last_seen THEN excluded.name ELSE players.name END,
                    first_seen = MIN(players.first_seen, excluded.first_seen),
                    last_seen = MAX(players.last_seen, excluded.last_seen)
            """, (player_id, name, SERVER, NCC_ALLIANCE_ID, "NCC", date, date))

            # Upsert snapshot
            cur.execute("""
                INSERT INTO daily_snapshots
                    (player_id, date, level, power, helps, rss_contrib, iso_contrib,
                     players_killed, hostiles_killed, resources_mined, resources_raided,
                     alliance_id, alliance_tag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(player_id, date) DO UPDATE SET
                    level = excluded.level,
                    power = excluded.power,
                    helps = excluded.helps,
                    rss_contrib = excluded.rss_contrib,
                    iso_contrib = excluded.iso_contrib,
                    players_killed = excluded.players_killed,
                    hostiles_killed = excluded.hostiles_killed,
                    resources_mined = excluded.resources_mined,
                    resources_raided = excluded.resources_raided
            """, (player_id, date, level, power, helps, rss_c, iso_c, pk, hk, rm, rr,
                  NCC_ALLIANCE_ID, "NCC"))

    conn.commit()
    total = cur.execute("SELECT COUNT(*) FROM daily_snapshots").fetchone()[0]
    players = cur.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    print(f"Import complete: {players} players, {total} snapshot rows.")


def export_server_alliances_json(conn):
    """Query all alliances on the server and write data/server_alliances.json.

    Aggregates player data by alliance for the most recent date, with 7-day deltas.
    """
    # Find the most recent date with data
    row = conn.execute("SELECT MAX(date) FROM daily_snapshots").fetchone()
    if not row or not row[0]:
        return
    latest_date = row[0]

    # Find a date ~7 days ago for deltas
    delta_row = conn.execute("""
        SELECT MAX(date) FROM daily_snapshots
        WHERE date <= date(?, '-7 days')
    """, (latest_date,)).fetchone()
    delta_date = delta_row[0] if delta_row and delta_row[0] else None

    # Get pull timestamp
    pull_row = conn.execute(
        "SELECT pulled_at FROM pull_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    pulled_at = pull_row[0] if pull_row else datetime.now().isoformat()

    # Aggregate current data by alliance
    rows = conn.execute("""
        SELECT alliance_id, alliance_tag,
               COUNT(*) as member_count,
               SUM(power) as total_power,
               AVG(level) as avg_level,
               MAX(level) as max_level,
               SUM(players_killed) as total_pvp,
               SUM(hostiles_killed) as total_hk,
               SUM(resources_mined) as total_mined,
               SUM(resources_raided) as total_raided
        FROM daily_snapshots
        WHERE date = ? AND alliance_id IS NOT NULL AND alliance_id != 0
        GROUP BY alliance_id
        ORDER BY total_power DESC
    """, (latest_date,)).fetchall()

    # Get 7-day-ago power by alliance for deltas
    past_power = {}
    if delta_date:
        past_rows = conn.execute("""
            SELECT alliance_id, SUM(power) as total_power
            FROM daily_snapshots
            WHERE date = ? AND alliance_id IS NOT NULL AND alliance_id != 0
            GROUP BY alliance_id
        """, (delta_date,)).fetchall()
        past_power = {r[0]: r[1] or 0 for r in past_rows}

    alliances = []
    for rank, r in enumerate(rows, 1):
        aid, atag, count, power, avg_lvl, max_lvl, pvp, hk, mined, raided = r
        past_p = past_power.get(aid, 0)
        alliances.append({
            "alliance_id": aid,
            "alliance_tag": atag or "",
            "rank": rank,
            "member_count": count or 0,
            "total_power": power or 0,
            "avg_level": round(avg_lvl) if avg_lvl else 0,
            "max_level": max_lvl or 0,
            "total_pvp": pvp or 0,
            "total_hk": hk or 0,
            "total_mined": mined or 0,
            "total_raided": raided or 0,
            "power_delta_7d": (power or 0) - past_p,
        })

    record = {
        "pulled_at": pulled_at,
        "as_of_date": latest_date,
        "delta_base_date": delta_date or latest_date,
        "alliance_count": len(alliances),
        "alliances": alliances,
    }

    out_file = DATA_DIR / "server_alliances.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)


def export_server_players_json(conn):
    """Query all server players and write data/server_players.json.

    Includes current snapshot values and 7-day power delta + alliance movement.
    """
    # Find the most recent date with data
    row = conn.execute("SELECT MAX(date) FROM daily_snapshots").fetchone()
    if not row or not row[0]:
        return
    latest_date = row[0]

    # Find a date ~7 days ago for deltas/movement
    delta_row = conn.execute("""
        SELECT MAX(date) FROM daily_snapshots
        WHERE date <= date(?, '-7 days')
    """, (latest_date,)).fetchone()
    delta_date = delta_row[0] if delta_row and delta_row[0] else None

    # Get pull timestamp
    pull_row = conn.execute(
        "SELECT pulled_at FROM pull_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    pulled_at = pull_row[0] if pull_row else datetime.now().isoformat()

    # Get all current players
    rows = conn.execute("""
        SELECT ds.player_id, p.name, ds.alliance_id, ds.alliance_tag,
               ds.level, ds.power, ds.helps, ds.rss_contrib, ds.iso_contrib,
               ds.players_killed, ds.hostiles_killed, ds.resources_mined,
               ds.resources_raided
        FROM daily_snapshots ds
        JOIN players p ON p.player_id = ds.player_id
        WHERE ds.date = ?
        ORDER BY ds.power DESC
    """, (latest_date,)).fetchall()

    # Get 7-day-ago data for deltas and movement detection
    past_data = {}
    if delta_date:
        past_rows = conn.execute("""
            SELECT player_id, power, alliance_id, alliance_tag
            FROM daily_snapshots
            WHERE date = ?
        """, (delta_date,)).fetchall()
        past_data = {r[0]: {"power": r[1] or 0, "alliance_id": r[2], "alliance_tag": r[3] or ""} for r in past_rows}

    players = []
    for r in rows:
        (pid, name, aid, atag, level, power, helps, rss_c, iso_c,
         pk, hk, rm, rr) = r

        past = past_data.get(pid)
        power_delta = (power or 0) - past["power"] if past else 0
        moved = False
        prev_tag = None
        if past and past["alliance_id"] != aid:
            moved = True
            prev_tag = past["alliance_tag"]

        players.append({
            "id": str(pid),
            "name": name or "",
            "alliance_id": aid or 0,
            "alliance_tag": atag or "",
            "level": level or 0,
            "power": power or 0,
            "helps": helps or 0,
            "rss_contrib": rss_c or 0,
            "iso_contrib": iso_c or 0,
            "players_killed": pk or 0,
            "hostiles_killed": hk or 0,
            "resources_mined": rm or 0,
            "resources_raided": rr or 0,
            "power_delta_7d": power_delta,
            "moved": moved,
            "prev_alliance_tag": prev_tag,
        })

    record = {
        "pulled_at": pulled_at,
        "as_of_date": latest_date,
        "player_count": len(players),
        "players": players,
    }

    out_file = DATA_DIR / "server_players.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)


def get_latest_two_dates(conn, alliance_id=NCC_ALLIANCE_ID):
    """Return (prev_date, curr_date) for the two most recent snapshot dates,
    or (None, None) if fewer than 2 exist. Used by send_hourly_alerts."""
    rows = conn.execute("""
        SELECT DISTINCT date FROM daily_snapshots
        WHERE alliance_id = ?
        ORDER BY date DESC LIMIT 2
    """, (alliance_id,)).fetchall()
    if len(rows) < 2:
        return None, None
    return rows[1][0], rows[0][0]


def get_members_for_date(conn, date, alliance_id=NCC_ALLIANCE_ID):
    """Return a dict of {player_id_str: {name, level, power, ...}} for a date.
    Used by send_hourly_alerts for join/leave detection."""
    rows = conn.execute("""
        SELECT ds.player_id, p.name, ds.level, ds.power
        FROM daily_snapshots ds
        JOIN players p ON p.player_id = ds.player_id
        WHERE ds.date = ? AND ds.alliance_id = ?
    """, (date, alliance_id)).fetchall()

    members = {}
    for pid, name, level, power in rows:
        members[str(pid)] = {
            "id": str(pid),
            "name": name,
            "level": level or 0,
            "power": power or 0,
        }
    return members
