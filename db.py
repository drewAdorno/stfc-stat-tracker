"""
SQLite database module for STFC Stat Tracker.
Stores all player data in data/stfc.db and exports JSON files for dashboards.
"""

import json
import math
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

# US Eastern Time (EST = UTC-5, EDT = UTC-4)
# Using fixed UTC-5 so the day always resets at midnight EST.
EST = timezone(timedelta(hours=-5))


def now_est():
    """Return the current datetime in US Eastern (EST, UTC-5)."""
    return datetime.now(EST)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "stfc.db"

NCC_ALLIANCE_ID = "2616095065411838478"
NCC_ALLIANCE_NAME = "Discovery"
SERVER = 716
ROE_VIOLATION_TYPES = {
    "opc hit": "OPC hit",
    "upc hit": "OPC hit",
    "token space hit": "Token space hit",
    "armada interference": "Armada interference",
    "friendly alliance hit": "Friendly alliance hit",
}

TRACKED_FIELDS = [
    "level", "power", "helps", "rss_contrib", "iso_contrib",
    "players_killed", "hostiles_killed", "resources_mined", "resources_raided",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS players (
    player_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    server      INTEGER NOT NULL,
    alliance_id TEXT,
    alliance_tag TEXT,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_snapshots (
    player_id       TEXT NOT NULL,
    date            TEXT NOT NULL,
    name            TEXT,
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
    alliance_id     TEXT,
    alliance_tag    TEXT,
    alliance_name   TEXT,
    PRIMARY KEY (player_id, date)
);

CREATE TABLE IF NOT EXISTS pull_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pulled_at   TEXT NOT NULL,
    server      INTEGER NOT NULL,
    total_players INTEGER,
    source      TEXT DEFAULT 'api'
);

CREATE TABLE IF NOT EXISTS discord_links (
    discord_user_id TEXT PRIMARY KEY,
    player_id       TEXT NOT NULL,
    linked_at       TEXT NOT NULL,
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

CREATE TABLE IF NOT EXISTS daily_stat_changes (
    date        TEXT NOT NULL,
    field       TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    sample_player_id TEXT,
    old_value   INTEGER,
    new_value   INTEGER,
    PRIMARY KEY (date, field)
);

CREATE TABLE IF NOT EXISTS alliance_inventory (
    date        TEXT NOT NULL,
    refid       INTEGER NOT NULL,
    item_type   INTEGER NOT NULL,
    count       INTEGER NOT NULL,
    PRIMARY KEY (date, refid)
);

CREATE TABLE IF NOT EXISTS roe_violations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    reported_at         TEXT NOT NULL,
    offense_date        TEXT NOT NULL,
    reported_by         TEXT NOT NULL DEFAULT '',
    offender_player_id  TEXT,
    offender_name       TEXT NOT NULL,
    offender_alliance_id TEXT,
    offender_alliance_tag TEXT,
    offender_alliance_name TEXT,
    victim_player_id    TEXT,
    victim_name         TEXT,
    violation_type      TEXT NOT NULL,
    system_name         TEXT,
    screenshots         TEXT,
    notes               TEXT,
    source              TEXT NOT NULL DEFAULT 'manual',
    source_ref          TEXT
);

CREATE INDEX IF NOT EXISTS idx_roe_violations_offender
    ON roe_violations(offender_player_id, offense_date);

CREATE INDEX IF NOT EXISTS idx_roe_violations_alliance
    ON roe_violations(offender_alliance_id, offender_alliance_tag, offense_date);
"""


def _migrate_player_id_to_text(conn):
    """One-time migration: convert player_id columns from INTEGER to TEXT.

    SQLite doesn't support ALTER COLUMN, so we recreate tables with TEXT types
    and copy data with CAST(player_id AS TEXT).
    """
    # Check if migration is needed by inspecting the players table schema
    cols = conn.execute("PRAGMA table_info(players)").fetchall()
    pid_col = next((c for c in cols if c[1] == "player_id"), None)
    if not pid_col or pid_col[2].upper() == "TEXT":
        return  # Already TEXT or table doesn't exist yet

    print("[migration] Converting player_id from INTEGER to TEXT...")
    conn.execute("PRAGMA foreign_keys=OFF")

    # --- players ---
    conn.execute("""CREATE TABLE players_new (
        player_id TEXT PRIMARY KEY, name TEXT NOT NULL, server INTEGER NOT NULL,
        alliance_id TEXT, alliance_tag TEXT, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL
    )""")
    conn.execute("""INSERT INTO players_new
        SELECT CAST(player_id AS TEXT), name, server, CAST(alliance_id AS TEXT),
               alliance_tag, first_seen, last_seen FROM players""")
    conn.execute("DROP TABLE players")
    conn.execute("ALTER TABLE players_new RENAME TO players")

    # --- daily_snapshots ---
    # Get existing columns to handle optional ones
    ds_cols = {r[1] for r in conn.execute("PRAGMA table_info(daily_snapshots)")}
    name_col = ", name" if "name" in ds_cols else ""
    aname_col = ", alliance_name" if "alliance_name" in ds_cols else ""
    name_col_def = ", name TEXT" if "name" in ds_cols else ""
    aname_col_def = ", alliance_name TEXT" if "alliance_name" in ds_cols else ""

    conn.execute(f"""CREATE TABLE daily_snapshots_new (
        player_id TEXT NOT NULL, date TEXT NOT NULL{name_col_def},
        level INTEGER, power INTEGER, helps INTEGER, rss_contrib INTEGER,
        iso_contrib INTEGER, players_killed INTEGER, hostiles_killed INTEGER,
        resources_mined INTEGER, resources_raided INTEGER, rank_title TEXT,
        join_date TEXT, alliance_id TEXT, alliance_tag TEXT{aname_col_def},
        PRIMARY KEY (player_id, date)
    )""")
    conn.execute(f"""INSERT INTO daily_snapshots_new
        SELECT CAST(player_id AS TEXT), date{name_col},
               level, power, helps, rss_contrib, iso_contrib,
               players_killed, hostiles_killed, resources_mined, resources_raided,
               rank_title, join_date, CAST(alliance_id AS TEXT), alliance_tag{aname_col}
        FROM daily_snapshots""")
    conn.execute("DROP TABLE daily_snapshots")
    conn.execute("ALTER TABLE daily_snapshots_new RENAME TO daily_snapshots")

    # --- discord_links ---
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='discord_links'").fetchone():
        conn.execute("""CREATE TABLE discord_links_new (
            discord_user_id TEXT PRIMARY KEY, player_id TEXT NOT NULL,
            linked_at TEXT NOT NULL, FOREIGN KEY (player_id) REFERENCES players(player_id)
        )""")
        conn.execute("""INSERT INTO discord_links_new
            SELECT discord_user_id, CAST(player_id AS TEXT), linked_at FROM discord_links""")
        conn.execute("DROP TABLE discord_links")
        conn.execute("ALTER TABLE discord_links_new RENAME TO discord_links")

    # --- daily_stat_changes ---
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_stat_changes'").fetchone():
        conn.execute("""CREATE TABLE daily_stat_changes_new (
            date TEXT NOT NULL, field TEXT NOT NULL, detected_at TEXT NOT NULL,
            sample_player_id TEXT, old_value INTEGER, new_value INTEGER,
            PRIMARY KEY (date, field)
        )""")
        conn.execute("""INSERT INTO daily_stat_changes_new
            SELECT date, field, detected_at, CAST(sample_player_id AS TEXT),
                   old_value, new_value FROM daily_stat_changes""")
        conn.execute("DROP TABLE daily_stat_changes")
        conn.execute("ALTER TABLE daily_stat_changes_new RENAME TO daily_stat_changes")

    conn.commit()
    conn.execute("PRAGMA foreign_keys=ON")
    print("[migration] Done — all player_id columns are now TEXT.")


def _migrate_alliance_ids(conn, new_alliance_map):
    """Remap old stfc.pro alliance_ids to Scopely IDs using tag as the bridge.

    new_alliance_map: dict of {alliance_tag: new_alliance_id} from Scopely data.
    Only runs once (checks for short numeric-looking alliance_ids in old data).
    """
    # Check if there are old-style alliance IDs (stfc.pro used ~10-digit integers,
    # Scopely uses ~19-digit integers)
    old_count = conn.execute("""
        SELECT COUNT(DISTINCT alliance_id) FROM daily_snapshots
        WHERE length(alliance_id) < 15 AND alliance_id != '' AND alliance_tag != ''
    """).fetchone()[0]
    if old_count == 0:
        return

    # Build old_tag -> old_id mapping from short-id rows
    old_rows = conn.execute("""
        SELECT DISTINCT alliance_id, alliance_tag FROM daily_snapshots
        WHERE length(alliance_id) < 15 AND alliance_id != '' AND alliance_tag != ''
    """).fetchall()

    remapped = 0
    for old_id, tag in old_rows:
        new_id = new_alliance_map.get(tag)
        if not new_id or new_id == old_id:
            continue
        conn.execute("UPDATE daily_snapshots SET alliance_id = ? WHERE alliance_id = ?",
                     (new_id, old_id))
        conn.execute("UPDATE players SET alliance_id = ? WHERE alliance_id = ?",
                     (new_id, old_id))
        remapped += 1

    if remapped:
        conn.commit()
        print(f"[migration] Remapped {remapped} old alliance IDs to Scopely IDs")


def get_db():
    """Return a sqlite3 connection, creating DB + tables if needed."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # Migrate INTEGER player_id to TEXT if DB already exists
    if DB_PATH.exists():
        _migrate_player_id_to_text(conn)
    conn.executescript(SCHEMA)
    # Migrations: add columns to daily_snapshots if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(daily_snapshots)")}
    if "name" not in cols:
        conn.execute("ALTER TABLE daily_snapshots ADD COLUMN name TEXT")
    if "alliance_name" not in cols:
        conn.execute("ALTER TABLE daily_snapshots ADD COLUMN alliance_name TEXT")
    roe_cols = {r[1] for r in conn.execute("PRAGMA table_info(roe_violations)")}
    if roe_cols and "screenshots" not in roe_cols:
        conn.execute("ALTER TABLE roe_violations ADD COLUMN screenshots TEXT")
    conn.commit()
    return conn


DAILY_STAT_FIELDS = ["hostiles_killed", "resources_mined", "helps", "players_killed"]


def _detect_daily_stat_changes(conn, mapped_players, date):
    """Check if daily-reset stats changed since last pull and log the first detection.

    Compares incoming values against stored snapshots for a sample of players.
    Uses INSERT OR IGNORE so only the first detection per field per date is recorded.
    """
    # Build a lookup of incoming data by player_id
    incoming = {}
    for m in mapped_players:
        pid = str(m["id"])
        incoming[pid] = m

    # Get a sample of players that already have a snapshot for today
    sample_rows = conn.execute("""
        SELECT player_id, hostiles_killed, resources_mined, helps, players_killed
        FROM daily_snapshots
        WHERE date = ?
        LIMIT 5
    """, (date,)).fetchall()

    if not sample_rows:
        return  # First pull of the day, nothing to compare

    detected_at = now_est().isoformat()

    for row in sample_rows:
        pid = str(row[0])
        if pid not in incoming:
            continue
        stored = {
            "hostiles_killed": row[1] or 0,
            "resources_mined": row[2] or 0,
            "helps": row[3] or 0,
            "players_killed": row[4] or 0,
        }
        inc = incoming[pid]
        for field in DAILY_STAT_FIELDS:
            old_val = stored[field]
            new_val = int(inc.get(field, 0))
            if new_val != old_val:
                conn.execute("""
                    INSERT OR IGNORE INTO daily_stat_changes
                        (date, field, detected_at, sample_player_id, old_value, new_value)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (date, field, detected_at, pid, old_val, new_val))
                print(f"[daily_stat_change] {field} changed for player {pid}: "
                      f"{old_val} -> {new_val} (detected {detected_at})")

    conn.commit()


def upsert_players(conn, mapped_players, date):
    """Bulk insert/update players and daily_snapshots for a given date.

    mapped_players: list of dicts from pull_api.map_player() with integer values.
    date: YYYY-MM-DD string.
    """
    # Detect daily stat refreshes before overwriting snapshots
    _detect_daily_stat_changes(conn, mapped_players, date)

    cur = conn.cursor()

    for m in mapped_players:
        player_id = str(m["id"])

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
            str(m.get("alliance_id", "") or ""),
            m.get("alliance_tag", ""),
            date,
            date,
        ))

        # Upsert into daily_snapshots
        cur.execute("""
            INSERT INTO daily_snapshots
                (player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
                 players_killed, hostiles_killed, resources_mined, resources_raided,
                 rank_title, join_date, alliance_id, alliance_tag, alliance_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id, date) DO UPDATE SET
                name = excluded.name,
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
                alliance_tag = excluded.alliance_tag,
                alliance_name = excluded.alliance_name
        """, (
            player_id, date,
            m.get("name", ""),
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
            str(m.get("alliance_id", "") or ""),
            m.get("alliance_tag", ""),
            m.get("alliance_name", ""),
        ))

    conn.commit()


def clear_bad_rss_contrib_snapshots(conn):
    """Zero out snapshots polluted by the raided-as-RSS scraper bug.

    The early Scopely pulls on these dates stored `resources_raided` into
    `rss_contrib`, which makes later deltas look like massive negative drops.
    Keep the cleanup narrowly scoped to the known-corrupt snapshot dates.
    """
    cur = conn.execute("""
        UPDATE daily_snapshots
        SET rss_contrib = 0
        WHERE date IN ('2026-02-28', '2026-03-06')
          AND resources_raided > 0
          AND rss_contrib = resources_raided
    """)
    conn.commit()
    return cur.rowcount


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


def compute_activity_scores(players):
    """Compute activity scores (0-100) for a list of player dicts with raw integer stats.

    Uses a blend of log-absolute engagement, level-relative performance, and
    recency of activity (7d deltas).  If 7d delta fields are present on the
    player dicts (e.g. hostiles_killed_delta_7d), recency acts as a steep
    multiplier: zero recent gains → score drops to ~15% of base.

    Returns a dict {player_id_str: score_int}.
    """
    if not players:
        return {}

    WEIGHTS = [
        ("hostiles_killed", 0.35),
        ("resources_mined", 0.30),
        ("helps", 0.25),
        ("players_killed", 0.10),
    ]

    DELTA_FIELDS = [(f + "_delta_7d", w) for f, w in WEIGHTS]
    has_deltas = any(players[0].get(f) is not None for f, _ in DELTA_FIELDS)

    # --- Component 1: Log-absolute score ---
    raw_abs = []
    for p in players:
        s = sum(math.log1p(p.get(f, 0) or 0) * w for f, w in WEIGHTS)
        raw_abs.append(s)

    abs_min = min(raw_abs) if raw_abs else 0
    abs_max = max(raw_abs) if raw_abs else 1
    abs_range = abs_max - abs_min if abs_max != abs_min else 1

    # --- Component 2: Level-relative score ---
    # Group players into 5-level bands
    bands = {}
    for i, p in enumerate(players):
        level = p.get("level", 0) or 0
        band = (level // 5) * 5
        bands.setdefault(band, []).append(i)

    # Compute median for each stat per band
    band_medians = {}
    for band, indices in bands.items():
        medians = {}
        for field, _ in WEIGHTS:
            vals = sorted((players[i].get(field, 0) or 0) for i in indices)
            n = len(vals)
            if n % 2 == 1:
                medians[field] = vals[n // 2]
            else:
                medians[field] = (vals[n // 2 - 1] + vals[n // 2]) / 2
        band_medians[band] = medians

    raw_rel = []
    for p in players:
        level = p.get("level", 0) or 0
        band = (level // 5) * 5
        medians = band_medians[band]
        s = 0
        for field, w in WEIGHTS:
            median = medians[field]
            if median > 0:
                ratio = min((p.get(field, 0) or 0) / median, 5.0)
            else:
                ratio = 1.0 if (p.get(field, 0) or 0) > 0 else 0.0
            s += ratio * w
        raw_rel.append(s)

    rel_min = min(raw_rel) if raw_rel else 0
    rel_max = max(raw_rel) if raw_rel else 1
    rel_range = rel_max - rel_min if rel_max != rel_min else 1

    # --- Component 3: Recency (from 7d deltas) ---
    # Blended as a third component rather than a multiplier, so lifetime
    # engagement still counts even during a quiet week.
    raw_recency = []
    if has_deltas:
        for p in players:
            s = sum(math.log1p(max(p.get(f, 0) or 0, 0)) * w for f, w in DELTA_FIELDS)
            raw_recency.append(s)
        rec_min = min(raw_recency)
        rec_max = max(raw_recency)
        rec_range = rec_max - rec_min if rec_max != rec_min else 1

    # --- Blend and produce final scores ---
    # With deltas: 35% lifetime, 25% level-relative, 40% recency
    # Without deltas: 50/50 lifetime + level-relative
    scores = {}
    for i, p in enumerate(players):
        norm_abs = (raw_abs[i] - abs_min) / abs_range
        norm_rel = (raw_rel[i] - rel_min) / rel_range

        if has_deltas:
            norm_rec = (raw_recency[i] - rec_min) / rec_range
            final = int((norm_abs * 0.35 + norm_rel * 0.25 + norm_rec * 0.40) * 100)
        else:
            final = int((norm_abs * 0.5 + norm_rel * 0.5) * 100)

        final = max(0, min(100, final))
        pid = str(p.get("id", p.get("player_id", "")))
        scores[pid] = final

    return scores


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
            "alliance_id": aid or "",
        })

        total_power += power or 0
        total_helps += helps or 0
        total_rss += rss_c or 0
        total_iso += iso_c or 0
        level_sum += level or 0

    avg_level = round(level_sum / len(members)) if members else 0

    # Compute activity scores using all server players for normalization
    all_rows = conn.execute("""
        SELECT player_id, level, power, helps, players_killed,
               hostiles_killed, resources_mined
        FROM daily_snapshots WHERE date = ?
    """, (latest_date,)).fetchall()
    # Find 7d-ago snapshot date for recency calculation
    delta_row = conn.execute("""
        SELECT MAX(date) FROM daily_snapshots WHERE date <= date(?, '-7 days')
    """, (latest_date,)).fetchone()
    delta_date = delta_row[0] if delta_row and delta_row[0] else None
    past_7d = {}
    if delta_date:
        past_rows = conn.execute("""
            SELECT player_id, power, helps, players_killed,
                   hostiles_killed, resources_mined
            FROM daily_snapshots WHERE date = ?
        """, (delta_date,)).fetchall()
        past_7d = {r[0]: {"power": r[1] or 0, "helps": r[2] or 0,
                          "players_killed": r[3] or 0, "hostiles_killed": r[4] or 0,
                          "resources_mined": r[5] or 0} for r in past_rows}
    all_players_raw = []
    for r in all_rows:
        pid = r[0]
        d = {"id": str(pid), "level": r[1] or 0, "power": r[2] or 0,
             "helps": r[3] or 0, "players_killed": r[4] or 0,
             "hostiles_killed": r[5] or 0, "resources_mined": r[6] or 0}
        past = past_7d.get(pid, {})
        for f in ["power", "helps", "players_killed", "hostiles_killed", "resources_mined"]:
            d[f + "_delta_7d"] = max((d[f] or 0) - (past.get(f, 0) or 0), 0)
        all_players_raw.append(d)
    scores = compute_activity_scores(all_players_raw)
    for m in members:
        m["activity_score"] = scores.get(m["id"], 0)

    record = {
        "pulled_at": pulled_at,
        "alliance_url": "",
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
                   ds.resources_mined, ds.resources_raided,
                   COALESCE(ds.name, p.name) as name
            FROM daily_snapshots ds
            JOIN players p ON p.player_id = ds.player_id
            WHERE ds.date = ? AND ds.alliance_id = ?
        """, (date, alliance_id)).fetchall()

        members_snapshot = {}
        total_power = 0
        total_helps = 0
        total_rss = 0
        total_iso = 0
        total_pk = 0
        total_hk = 0
        total_rm = 0
        total_rr = 0
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
            total_pk += pk or 0
            total_hk += hk or 0
            total_rm += rm or 0
            total_rr += rr or 0
            level_sum += level or 0

        count = len(members_snapshot)
        avg_level = round(level_sum / count) if count else 0

        summary = {
            "total_power": _format_abbr(total_power),
            "member_count": str(count),
            "total_helps": _format_abbr(total_helps),
            "total_rss": _format_abbr(total_rss),
            "total_iso": _format_abbr(total_iso),
            "total_players_killed": _format_abbr(total_pk),
            "total_hostiles_killed": _format_abbr(total_hk),
            "total_resources_mined": _format_abbr(total_rm),
            "total_resources_raided": _format_abbr(total_rr),
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
            player_id = str(pid_str)
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
                    (player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
                     players_killed, hostiles_killed, resources_mined, resources_raided,
                     alliance_id, alliance_tag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(player_id, date) DO UPDATE SET
                    name = excluded.name,
                    level = excluded.level,
                    power = excluded.power,
                    helps = excluded.helps,
                    rss_contrib = excluded.rss_contrib,
                    iso_contrib = excluded.iso_contrib,
                    players_killed = excluded.players_killed,
                    hostiles_killed = excluded.hostiles_killed,
                    resources_mined = excluded.resources_mined,
                    resources_raided = excluded.resources_raided
            """, (player_id, date, name, level, power, helps, rss_c, iso_c, pk, hk, rm, rr,
                  NCC_ALLIANCE_ID, "NCC"))

    conn.commit()
    total = cur.execute("SELECT COUNT(*) FROM daily_snapshots").fetchone()[0]
    players = cur.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    print(f"Import complete: {players} players, {total} snapshot rows.")


def export_server_history_json(conn):
    """Query all daily snapshots for ALL server players and write data/server_history.json.

    Same per-player format as history.json but without alliance filtering and without
    the per-date summary object (not needed for individual player lookups, saves space).
    """
    dates = conn.execute(
        "SELECT DISTINCT date FROM daily_snapshots ORDER BY date",
    ).fetchall()

    history = []
    for (date,) in dates:
        rows = conn.execute("""
            SELECT ds.player_id, ds.level, ds.power, ds.helps, ds.rss_contrib,
                   ds.iso_contrib, ds.players_killed, ds.hostiles_killed,
                   ds.resources_mined, ds.resources_raided,
                   COALESCE(ds.name, p.name) as name
            FROM daily_snapshots ds
            JOIN players p ON p.player_id = ds.player_id
            WHERE ds.date = ?
        """, (date,)).fetchall()

        members_snapshot = {}
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

        history.append({
            "date": date,
            "members": members_snapshot,
        })

    history_file = DATA_DIR / "server_history.json"
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False)


def export_server_alliances_json(conn):
    """Query all alliances on the server and write data/server_alliances.json.

    Aggregates player data by alliance for the most recent date, with 1d/7d/30d deltas.
    """
    delta_periods = [1, 7, 30]

    # Find the most recent date with data
    row = conn.execute("SELECT MAX(date) FROM daily_snapshots").fetchone()
    if not row or not row[0]:
        return
    latest_date = row[0]

    # Find delta dates for each period
    # If no snapshot exists that far back, use the earliest available date
    earliest_date = conn.execute(
        "SELECT MIN(date) FROM daily_snapshots"
    ).fetchone()[0]
    delta_dates = {}
    for days in delta_periods:
        delta_row = conn.execute("""
            SELECT MAX(date) FROM daily_snapshots
            WHERE date <= date(?, ?)
        """, (latest_date, f'-{days} days')).fetchone()
        dd = delta_row[0] if delta_row and delta_row[0] else None
        if not dd and earliest_date and earliest_date < latest_date:
            dd = earliest_date
        delta_dates[days] = dd

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
        WHERE date = ? AND alliance_id IS NOT NULL AND alliance_id != '' AND alliance_id != '0'
        GROUP BY alliance_id
        ORDER BY total_power DESC
    """, (latest_date,)).fetchall()

    # Stats fields to compute deltas for
    delta_fields = [
        ("total_power", "SUM(power)"),
        ("member_count", "COUNT(*)"),
        ("avg_level", "AVG(level)"),
        ("total_pvp", "SUM(players_killed)"),
        ("total_hk", "SUM(hostiles_killed)"),
        ("total_mined", "SUM(resources_mined)"),
        ("total_raided", "SUM(resources_raided)"),
    ]
    agg_sql = ", ".join(f"{sql} as {name}" for name, sql in delta_fields)

    # Get past stats by alliance for each delta period
    past_stats = {}  # {days: {alliance_id: {field: value}}}
    for days in delta_periods:
        dd = delta_dates[days]
        if not dd:
            past_stats[days] = {}
            continue
        past_rows = conn.execute(f"""
            SELECT alliance_id, {agg_sql}
            FROM daily_snapshots
            WHERE date = ? AND alliance_id IS NOT NULL AND alliance_id != '' AND alliance_id != '0'
            GROUP BY alliance_id
        """, (dd,)).fetchall()
        past_stats[days] = {
            r[0]: {name: r[i + 1] or 0 for i, (name, _) in enumerate(delta_fields)}
            for r in past_rows
        }

    # Earliest stats per alliance (fallback when alliance has no data for a period)
    earliest_rows = conn.execute(f"""
        SELECT ds.alliance_id, {agg_sql}
        FROM daily_snapshots ds
        INNER JOIN (
            SELECT alliance_id, MIN(date) as min_date
            FROM daily_snapshots
            WHERE alliance_id IS NOT NULL AND alliance_id != '' AND alliance_id != '0'
            GROUP BY alliance_id
        ) e ON ds.alliance_id = e.alliance_id AND ds.date = e.min_date
        GROUP BY ds.alliance_id
    """).fetchall()
    earliest_stats = {
        r[0]: {name: r[i + 1] or 0 for i, (name, _) in enumerate(delta_fields)}
        for r in earliest_rows
    }

    alliances = []
    for rank, r in enumerate(rows, 1):
        aid, atag, count, power, avg_lvl, max_lvl, pvp, hk, mined, raided = r
        alliance = {
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
        }
        current_vals = {
            "total_power": power or 0,
            "member_count": count or 0,
            "avg_level": round(avg_lvl) if avg_lvl else 0,
            "total_pvp": pvp or 0,
            "total_hk": hk or 0,
            "total_mined": mined or 0,
            "total_raided": raided or 0,
        }
        for days in delta_periods:
            past_alliance = past_stats[days].get(aid)
            if past_alliance is None:
                past_alliance = earliest_stats.get(aid, {})
            for field_name, _ in delta_fields:
                past_val = past_alliance.get(field_name, 0)
                if field_name == "avg_level":
                    past_val = round(past_val) if past_val else 0
                alliance[f"{field_name}_delta_{days}d"] = current_vals[field_name] - (past_val or 0)
        alliances.append(alliance)

    # Compute avg activity score per alliance from server_players.json
    # (must be exported first via export_server_players_json)
    sp_file = DATA_DIR / "server_players.json"
    if sp_file.exists():
        with open(sp_file, "r", encoding="utf-8") as f:
            sp_data = json.load(f)
        # Group scores by alliance_id
        alliance_scores = {}
        for p in sp_data.get("players", []):
            aid = p.get("alliance_id", 0)
            if aid:
                alliance_scores.setdefault(aid, []).append(p.get("activity_score", 0))
        for a in alliances:
            scores = alliance_scores.get(a["alliance_id"], [])
            a["avg_activity_score"] = round(sum(scores) / len(scores)) if scores else 0

    record = {
        "pulled_at": pulled_at,
        "as_of_date": latest_date,
        "delta_dates": {
            f"{days}d": delta_dates[days] or latest_date
            for days in delta_periods
        },
        "alliance_count": len(alliances),
        "alliances": alliances,
    }

    out_file = DATA_DIR / "server_alliances.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)


def export_server_players_json(conn):
    """Query all server players and write data/server_players.json.

    Includes current snapshot values, 1d/7d/30d deltas for all 9 tracked
    fields, and alliance movement detection (based on 7-day window).
    """
    # Find the most recent date with data
    row = conn.execute("SELECT MAX(date) FROM daily_snapshots").fetchone()
    if not row or not row[0]:
        return
    latest_date = row[0]

    # Find dates for each delta period
    # If no snapshot exists that far back, use the earliest available date
    delta_periods = [1, 7, 30]
    earliest_date = conn.execute(
        "SELECT MIN(date) FROM daily_snapshots"
    ).fetchone()[0]
    delta_dates = {}
    for days in delta_periods:
        dr = conn.execute("""
            SELECT MAX(date) FROM daily_snapshots
            WHERE date <= date(?, ?)
        """, (latest_date, f"-{days} days")).fetchone()
        dd = dr[0] if dr and dr[0] else None
        # Fall back to earliest date if no data that far back
        if not dd and earliest_date and earliest_date < latest_date:
            dd = earliest_date
        delta_dates[days] = dd

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
               ds.resources_raided, ds.alliance_name
        FROM daily_snapshots ds
        JOIN players p ON p.player_id = ds.player_id
        WHERE ds.date = ?
        ORDER BY ds.power DESC
    """, (latest_date,)).fetchall()

    # Load past snapshots for each delta period
    past_snapshots = {}  # {days: {player_id: {field: value, ...}}}
    for days in delta_periods:
        dd = delta_dates[days]
        if not dd:
            past_snapshots[days] = {}
            continue
        past_rows = conn.execute("""
            SELECT player_id, level, power, helps, rss_contrib, iso_contrib,
                   players_killed, hostiles_killed, resources_mined, resources_raided,
                   alliance_id, alliance_tag
            FROM daily_snapshots
            WHERE date = ?
        """, (dd,)).fetchall()
        past_snapshots[days] = {
            r[0]: {
                "level": r[1] or 0, "power": r[2] or 0, "helps": r[3] or 0,
                "rss_contrib": r[4] or 0, "iso_contrib": r[5] or 0,
                "players_killed": r[6] or 0, "hostiles_killed": r[7] or 0,
                "resources_mined": r[8] or 0, "resources_raided": r[9] or 0,
                "alliance_id": r[10], "alliance_tag": r[11] or "",
            }
            for r in past_rows
        }

    # Load earliest snapshot per player (fallback when player has no data for a period)
    earliest_rows = conn.execute("""
        SELECT ds.player_id, ds.level, ds.power, ds.helps, ds.rss_contrib,
               ds.iso_contrib, ds.players_killed, ds.hostiles_killed,
               ds.resources_mined, ds.resources_raided, ds.alliance_id, ds.alliance_tag
        FROM daily_snapshots ds
        INNER JOIN (
            SELECT player_id, MIN(date) as min_date
            FROM daily_snapshots GROUP BY player_id
        ) e ON ds.player_id = e.player_id AND ds.date = e.min_date
    """).fetchall()
    earliest_snapshot = {
        r[0]: {
            "level": r[1] or 0, "power": r[2] or 0, "helps": r[3] or 0,
            "rss_contrib": r[4] or 0, "iso_contrib": r[5] or 0,
            "players_killed": r[6] or 0, "hostiles_killed": r[7] or 0,
            "resources_mined": r[8] or 0, "resources_raided": r[9] or 0,
            "alliance_id": r[10], "alliance_tag": r[11] or "",
        }
        for r in earliest_rows
    }

    players = []
    for r in rows:
        (pid, name, aid, atag, level, power, helps, rss_c, iso_c,
         pk, hk, rm, rr, aname) = r

        current_vals = {
            "level": level or 0, "power": power or 0, "helps": helps or 0,
            "rss_contrib": rss_c or 0, "iso_contrib": iso_c or 0,
            "players_killed": pk or 0, "hostiles_killed": hk or 0,
            "resources_mined": rm or 0, "resources_raided": rr or 0,
        }

        player = {
            "id": str(pid),
            "name": name or "",
            "alliance_id": aid or "",
            "alliance_tag": atag or "",
            "alliance_name": aname or "",
            **current_vals,
        }

        # Compute deltas for all fields × all periods
        # If a player has no snapshot for a period, fall back to earliest available
        for days in delta_periods:
            past = past_snapshots[days].get(pid)
            if not past:
                past = earliest_snapshot.get(pid)
            for field in TRACKED_FIELDS:
                delta = current_vals[field] - past[field] if past else 0
                player[f"{field}_delta_{days}d"] = delta

        # Alliance movement detection (check 1d, then 7d, then 30d)
        # Falls back to earliest snapshot so moves are caught even when a
        # player has no data for a specific delta date.
        # Filter out false positives from ID migration: ignore moves where
        # the tag didn't actually change (same tag or both empty).
        moved = False
        prev_tag = None
        moved_date = None
        current_tag = player.get("alliance_tag") or ""
        for days in delta_periods:
            past = past_snapshots[days].get(pid)
            if not past:
                past = earliest_snapshot.get(pid)
            if past and past["alliance_id"] != aid:
                past_tag = past["alliance_tag"] or ""
                # Only count as a real move if the tag actually changed
                if past_tag != current_tag:
                    moved = True
                    prev_tag = past_tag or None
                    moved_date = delta_dates[days]
                    break  # use the shortest window that detects a change

        player["moved"] = moved
        player["prev_alliance_tag"] = prev_tag
        player["moved_date"] = moved_date

        players.append(player)

    # Refine move dates: find the first snapshot date with the current tag
    moved_pids = [p["id"] for p in players if p["moved"]]
    if moved_pids:
        placeholders = ",".join("?" * len(moved_pids))
        move_rows = conn.execute(f"""
            SELECT player_id, MIN(date) as first_date
            FROM daily_snapshots
            WHERE player_id IN ({placeholders})
              AND alliance_tag = (
                  SELECT ds2.alliance_tag FROM daily_snapshots ds2
                  WHERE ds2.player_id = daily_snapshots.player_id
                  AND ds2.date = ?
              )
            GROUP BY player_id
        """, (*moved_pids, latest_date)).fetchall()
        first_new_tag = {r[0]: r[1] for r in move_rows}
        for p in players:
            if p["moved"] and p["id"] in first_new_tag:
                p["moved_date"] = first_new_tag[p["id"]]

    # Build alliance history per player from daily_snapshots
    alliance_rows = conn.execute("""
        SELECT player_id, alliance_id, alliance_tag,
               COALESCE(alliance_name, '') as alliance_name,
               MIN(date) as first_seen, MAX(date) as last_seen
        FROM daily_snapshots
        WHERE alliance_id IS NOT NULL AND alliance_id != '' AND alliance_id != '0'
        GROUP BY player_id, alliance_id
        ORDER BY player_id, first_seen
    """).fetchall()
    alliance_history = {}  # {player_id: [{...}]}
    for ar in alliance_rows:
        pid = str(ar[0])
        alliance_history.setdefault(pid, []).append({
            "alliance_id": ar[1],
            "alliance_tag": ar[2] or "",
            "alliance_name": ar[3] or "",
            "first_seen": ar[4],
            "last_seen": ar[5],
        })

    for p in players:
        stints = alliance_history.get(p["id"], [])
        # Exclude the current alliance from past alliances
        p["past_alliances"] = [
            s for s in stints if s["alliance_id"] != p["alliance_id"]
        ]

    # Compute activity scores across all server players
    scores = compute_activity_scores(players)
    for p in players:
        p["activity_score"] = scores.get(p["id"], 0)

    record = {
        "pulled_at": pulled_at,
        "as_of_date": latest_date,
        "delta_dates": {
            f"{days}d": delta_dates[days] or latest_date
            for days in delta_periods
        },
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


# --- ROE violation helpers ---

def _clean_text(value):
    """Normalize optional string values for DB storage."""
    return str(value or "").strip()


def record_roe_violation(
    conn,
    *,
    offender_name,
    violation_type,
    offense_date=None,
    reported_by="",
    offender_player_id="",
    offender_alliance_id="",
    offender_alliance_tag="",
    offender_alliance_name="",
    victim_player_id="",
    victim_name="",
    system_name="",
    screenshots="",
    notes="",
    source="manual",
    source_ref="",
):
    """Insert a single ROE violation record and return its row id."""
    offender_name = _clean_text(offender_name)
    violation_type = _clean_text(violation_type)
    normalized_type = ROE_VIOLATION_TYPES.get(violation_type.lower())
    if not offender_name:
        raise ValueError("offender_name is required")
    if not violation_type:
        raise ValueError("violation_type is required")
    if not normalized_type:
        allowed = ", ".join(sorted(set(ROE_VIOLATION_TYPES.values())))
        raise ValueError(f"violation_type must be one of: {allowed}")

    offense_date = _clean_text(offense_date) or now_est().strftime("%Y-%m-%d")
    reported_at = now_est().isoformat()

    cur = conn.execute("""
        INSERT INTO roe_violations (
            reported_at, offense_date, reported_by,
            offender_player_id, offender_name,
            offender_alliance_id, offender_alliance_tag, offender_alliance_name,
            victim_player_id, victim_name, violation_type,
            system_name, screenshots, notes, source, source_ref
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        reported_at,
        offense_date,
        _clean_text(reported_by),
        _clean_text(offender_player_id) or None,
        offender_name,
        _clean_text(offender_alliance_id) or None,
        _clean_text(offender_alliance_tag) or None,
        _clean_text(offender_alliance_name) or None,
        _clean_text(victim_player_id) or None,
        _clean_text(victim_name) or None,
        normalized_type,
        _clean_text(system_name) or None,
        _clean_text(screenshots) or None,
        _clean_text(notes) or None,
        _clean_text(source) or "manual",
        _clean_text(source_ref) or None,
    ))
    conn.commit()
    return cur.lastrowid


def _build_roe_violations_export(rows):
    """Build the ROE violations export payload from raw DB rows."""
    recent = []
    player_tallies = {}
    alliance_tallies = {}

    for row in rows:
        (
            violation_id,
            reported_at,
            offense_date,
            reported_by,
            offender_player_id,
            offender_name,
            offender_alliance_id,
            offender_alliance_tag,
            offender_alliance_name,
            victim_player_id,
            victim_name,
            violation_type,
            system_name,
            screenshots,
            notes,
            source,
            source_ref,
        ) = row

        recent.append({
            "id": violation_id,
            "reported_at": reported_at,
            "offense_date": offense_date,
            "reported_by": reported_by or "",
            "offender_player_id": offender_player_id or "",
            "offender_name": offender_name or "",
            "offender_alliance_id": offender_alliance_id or "",
            "offender_alliance_tag": offender_alliance_tag or "",
            "offender_alliance_name": offender_alliance_name or "",
            "victim_player_id": victim_player_id or "",
            "victim_name": victim_name or "",
            "violation_type": violation_type or "",
            "system_name": system_name or "",
            "screenshots": screenshots or "",
            "notes": notes or "",
            "source": source or "",
            "source_ref": source_ref or "",
        })

        player_key = offender_player_id or f"name:{(offender_name or '').strip().lower()}"
        player_entry = player_tallies.get(player_key)
        if player_entry is None:
            player_entry = {
                "offender_player_id": offender_player_id or "",
                "offender_name": offender_name or "",
                "offender_alliance_id": offender_alliance_id or "",
                "offender_alliance_tag": offender_alliance_tag or "",
                "offender_alliance_name": offender_alliance_name or "",
                "offense_count": 0,
                "last_offense_date": offense_date or "",
                "last_reported_at": reported_at or "",
                "latest_violation_type": violation_type or "",
            }
            player_tallies[player_key] = player_entry
        player_entry["offense_count"] += 1

        alliance_key = ""
        if offender_alliance_id:
            alliance_key = offender_alliance_id
        elif offender_alliance_tag and offender_alliance_tag.strip():
            alliance_key = f"tag:{offender_alliance_tag.strip().lower()}"
        elif offender_alliance_name and offender_alliance_name.strip():
            alliance_key = f"name:{offender_alliance_name.strip().lower()}"
        if alliance_key:
            alliance_entry = alliance_tallies.get(alliance_key)
            if alliance_entry is None:
                alliance_entry = {
                    "offender_alliance_id": offender_alliance_id or "",
                    "offender_alliance_tag": offender_alliance_tag or "",
                    "offender_alliance_name": offender_alliance_name or "",
                    "offense_count": 0,
                    "unique_offenders": set(),
                    "last_offense_date": offense_date or "",
                    "last_reported_at": reported_at or "",
                }
                alliance_tallies[alliance_key] = alliance_entry
            alliance_entry["offense_count"] += 1
            alliance_entry["unique_offenders"].add(player_key)

    player_summary = sorted(
        player_tallies.values(),
        key=lambda entry: (
            -entry["offense_count"],
            entry["offender_name"].lower(),
            entry["last_reported_at"],
        ),
    )

    alliance_summary = []
    for entry in alliance_tallies.values():
        alliance_summary.append({
            "offender_alliance_id": entry["offender_alliance_id"],
            "offender_alliance_tag": entry["offender_alliance_tag"],
            "offender_alliance_name": entry["offender_alliance_name"],
            "offense_count": entry["offense_count"],
            "unique_offender_count": len(entry["unique_offenders"]),
            "last_offense_date": entry["last_offense_date"],
            "last_reported_at": entry["last_reported_at"],
        })
    alliance_summary.sort(
        key=lambda entry: (
            -entry["offense_count"],
            -(entry["unique_offender_count"]),
            entry["offender_alliance_tag"].lower(),
            entry["offender_alliance_name"].lower(),
        )
    )

    return {
        "updated_at": now_est().isoformat(),
        "violation_count": len(recent),
        "unique_offender_count": len(player_summary),
        "alliance_count": len(alliance_summary),
        "player_tallies": player_summary,
        "alliance_tallies": alliance_summary,
        "recent_violations": recent,
    }


def export_roe_violations_json(conn):
    """Export ROE violations and tallies to data/roe_violations.json."""
    rows = conn.execute("""
        SELECT id, reported_at, offense_date, reported_by,
               offender_player_id, offender_name,
               offender_alliance_id, offender_alliance_tag, offender_alliance_name,
               victim_player_id, victim_name, violation_type,
               system_name, screenshots, notes, source, source_ref
        FROM roe_violations
        ORDER BY offense_date DESC, reported_at DESC, id DESC
    """).fetchall()

    payload = _build_roe_violations_export(rows)
    out_path = DATA_DIR / "roe_violations.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return payload


# --- Discord link helpers ---

def link_discord(conn, discord_user_id, player_id):
    """Link a Discord user to a player. Returns True on success."""
    conn.execute("""
        INSERT INTO discord_links (discord_user_id, player_id, linked_at)
        VALUES (?, ?, ?)
        ON CONFLICT(discord_user_id) DO UPDATE SET
            player_id = excluded.player_id,
            linked_at = excluded.linked_at
    """, (str(discord_user_id), str(player_id), datetime.now().isoformat()))
    conn.commit()
    return True


def unlink_discord(conn, discord_user_id):
    """Remove a Discord-to-player link. Returns True if a row was deleted."""
    cur = conn.execute(
        "DELETE FROM discord_links WHERE discord_user_id = ?",
        (str(discord_user_id),),
    )
    conn.commit()
    return cur.rowcount > 0


def get_linked_player(conn, discord_user_id):
    """Return player_id for a Discord user, or None if not linked."""
    row = conn.execute(
        "SELECT player_id FROM discord_links WHERE discord_user_id = ?",
        (str(discord_user_id),),
    ).fetchone()
    return row[0] if row else None


def search_players(conn, query, limit=25):
    """Search players by name prefix. Returns list of (player_id, name, alliance_tag).
    NCC members are listed first."""
    rows = conn.execute("""
        SELECT player_id, name, alliance_tag
        FROM players
        WHERE name LIKE ? COLLATE NOCASE
        ORDER BY
            CASE WHEN alliance_id = ? THEN 0 ELSE 1 END,
            name COLLATE NOCASE
        LIMIT ?
    """, (query + "%", NCC_ALLIANCE_ID, limit)).fetchall()
    return rows


def get_player_snapshot(conn, player_id, date=None):
    """Get a player's snapshot for a specific date (or latest).
    Returns dict with all tracked fields, or None."""
    if date:
        row = conn.execute("""
            SELECT date, level, power, helps, rss_contrib, iso_contrib,
                   players_killed, hostiles_killed, resources_mined, resources_raided,
                   rank_title, join_date, alliance_id, alliance_tag, name
            FROM daily_snapshots
            WHERE player_id = ? AND date = ?
        """, (player_id, date)).fetchone()
    else:
        row = conn.execute("""
            SELECT date, level, power, helps, rss_contrib, iso_contrib,
                   players_killed, hostiles_killed, resources_mined, resources_raided,
                   rank_title, join_date, alliance_id, alliance_tag, name
            FROM daily_snapshots
            WHERE player_id = ?
            ORDER BY date DESC LIMIT 1
        """, (player_id,)).fetchone()
    if not row:
        return None
    return {
        "date": row[0], "level": row[1], "power": row[2], "helps": row[3],
        "rss_contrib": row[4], "iso_contrib": row[5], "players_killed": row[6],
        "hostiles_killed": row[7], "resources_mined": row[8], "resources_raided": row[9],
        "rank_title": row[10], "join_date": row[11], "alliance_id": row[12],
        "alliance_tag": row[13], "name": row[14],
    }


def get_snapshot_date_ago(conn, player_id, days_ago):
    """Get the snapshot closest to N days ago for a player.
    Returns the date string, or None."""
    row = conn.execute("""
        SELECT date FROM daily_snapshots
        WHERE player_id = ? AND date <= date((SELECT MAX(date) FROM daily_snapshots WHERE player_id = ?), ?)
        ORDER BY date DESC LIMIT 1
    """, (player_id, player_id, f"-{days_ago} days")).fetchone()
    return row[0] if row else None


def get_earliest_snapshot_date(conn, player_id):
    """Return the earliest snapshot date for a player."""
    row = conn.execute(
        "SELECT MIN(date) FROM daily_snapshots WHERE player_id = ?",
        (player_id,),
    ).fetchone()
    return row[0] if row else None


def get_player_name_history(conn, player_id):
    """Return list of (name, first_date, last_date) for a player's name changes."""
    rows = conn.execute("""
        SELECT name, MIN(date) as first_date, MAX(date) as last_date
        FROM daily_snapshots
        WHERE player_id = ? AND name IS NOT NULL
        GROUP BY name
        ORDER BY first_date
    """, (player_id,)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


# ---------------------------------------------------------------------------
# Alliance Inventory
# ---------------------------------------------------------------------------

# Resource ID → display name mapping.
# IDs come from the game's protobuf InventoryItem.commonParams.refId.
RESOURCE_NAMES = {
    2910180549: "Refined Isogen 1*",
    3632155109: "Refined Isogen 2*",
    405275536:  "Refined Isogen 3*",
    2539513921: "Speed Up 5 Min",
    3051340822: "Speed Up 15 Min",
    1000621245: "Speed Up 1 Hour",
    921609496:  "Alliance Reserves",
    1774957625: "Collisional Plasma",
    3407752796: "Subspace Superconductor",
    183293177:  "Progenitor Core",
    3446322746: "Progenitor Diode",
    2234127593: "Progenitor Emitter",
    1384959993: "Progenitor Reactor",
    2026143028: "Alliance Tournament Points",
    3099044371: "Emerald Chain XP",
}


def ingest_alliance_inventory(conn, inventory_json_path):
    """Read alliance_inventory.json and insert a daily snapshot.

    The JSON is written by stfc-mod whenever the Alliance Inventory screen
    is opened in-game.  Format:
        {"items": [{"refid": int, "type": int, "count": int}, ...],
         "timestamp": epoch_seconds}

    Only inserts if today's date doesn't already have data (idempotent).
    Returns the number of items inserted, or 0 if skipped/missing.
    """
    inv_path = Path(inventory_json_path)
    if not inv_path.exists():
        return 0

    with open(inv_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data.get("items", [])
    if not items:
        return 0

    # Use the file's timestamp to determine the date (EST)
    ts = data.get("timestamp", 0)
    if ts:
        dt = datetime.fromtimestamp(ts, tz=EST)
    else:
        dt = now_est()
    date_str = dt.strftime("%Y-%m-%d")

    # Check if we already have data for today
    existing = conn.execute(
        "SELECT COUNT(*) FROM alliance_inventory WHERE date = ?",
        (date_str,),
    ).fetchone()[0]
    if existing > 0:
        return 0

    for item in items:
        conn.execute(
            "INSERT OR REPLACE INTO alliance_inventory (date, refid, item_type, count) VALUES (?, ?, ?, ?)",
            (date_str, item["refid"], item["type"], item["count"]),
        )
    conn.commit()
    return len(items)


def export_alliance_inventory_json(conn):
    """Export alliance inventory history to data/alliance_inventory.json.

    Output format:
    {
      "snapshots": [
        {"date": "2026-03-05", "items": {"Refined Isogen 1*": 739000, ...}},
        ...
      ],
      "latest": {"Refined Isogen 1*": 739000, ...},
      "deltas": {"Refined Isogen 1*": 12000, ...}  // vs previous day
    }
    """
    dates = conn.execute(
        "SELECT DISTINCT date FROM alliance_inventory ORDER BY date"
    ).fetchall()

    if not dates:
        return

    snapshots = []
    prev_items = {}
    for (date,) in dates:
        rows = conn.execute(
            "SELECT refid, count FROM alliance_inventory WHERE date = ?",
            (date,),
        ).fetchall()
        items = {}
        for refid, count in rows:
            name = RESOURCE_NAMES.get(refid, str(refid))
            items[name] = count
        snapshots.append({"date": date, "items": items})
        prev_items = items

    latest = snapshots[-1]["items"] if snapshots else {}
    deltas = {}
    if len(snapshots) >= 2:
        prev = snapshots[-2]["items"]
        for key, val in latest.items():
            deltas[key] = val - prev.get(key, 0)
        for key, val in prev.items():
            if key not in latest:
                deltas[key] = -val

    out = {
        "snapshots": snapshots,
        "latest": latest,
        "deltas": deltas,
    }

    out_path = DATA_DIR / "alliance_inventory.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
