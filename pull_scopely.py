"""
Scopely API Scraper for STFC Stat Tracker.

Replaces stfc.pro with direct Scopely platform + game server API calls.
- Stage 1: Rankings (no auth) — paginate military might leaderboard
- Stage 2: Profiles (auth) — batch lookup player names/levels/alliances
- Stage 3: Alliances (auth) — resolve alliance names/tags
- Stage 4: Player Stats (no auth, daily) — per-player protobuf stats

Usage:
    python pull_scopely.py                  # normal pull (stats once per day)
    python pull_scopely.py --with-stats     # force stats pull
    python pull_scopely.py --skip-stats     # skip stats entirely
    python pull_scopely.py --dry-run        # fetch + print, don't save to DB
"""

import argparse
import json
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

from db import (get_db, upsert_players, log_pull, now_est, export_latest_json,
                export_history_json, export_server_alliances_json,
                export_server_players_json, export_server_history_json,
                ingest_alliance_inventory, export_alliance_inventory_json,
                NCC_ALLIANCE_ID, _migrate_alliance_ids)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PLATFORM_BASE = "https://cdn-nv3-live.startrek.digitgaming.com"
# Auth file: check local project dir first, then Windows dev path
_LOCAL_AUTH = Path(__file__).parent / "auth.json"
_DEV_AUTH = Path("C:/Users/drewa/Desktop/stfc/stfc-api/auth.json")
AUTH_FILE = _LOCAL_AUTH if _LOCAL_AUTH.exists() else _DEV_AUTH
MILITARY_MIGHT_CONFIG = "3fcdb730de6656735924fa085dffb74b1954bf19"
RESOURCES_RAIDED_CONFIG = "e5422e292629984b1b3126b9aca593a2f7909a58"
RSS_CONTRIB_CONFIG = "32e959047182a77eb2ac98d8c547ebfcd7f11ded"
ISO_CONTRIB_CONFIG = "4d21cabdec534dbf5896b0441d774dd7c0f1252d"
SERVER = 716
PROFILE_BATCH_SIZE = 200

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
MIGRATED_FLAG = DATA_DIR / ".scopely_migrated"

# Stat hash prefix (8 chars) → name mappings from decoded player-stats protobuf.
# Full hashes vary per player/server, but the first 8 chars are stable identifiers.
STAT_PREFIX_MAP = {
    "10c9019d": "missions_completed",
    "585969ba": "helps",
    "b7ea2c94": "resources_mined",
    "296fbc7f": "hostiles_killed",
    "624304c1": "damage_to_players",
    "62e2fb32": "damage_to_hostiles",
    "6e22b779": "pvp_kd_ratio",
    "95cfcf9f": "players_killed",
    "7e32ee56": "power",
    "de842c4e": "assessment_rank",
    "e3b3dd89": "power_destroyed",
    "f92690d0": "arena_rating",
}

# Platform request delay (CDN, no auth) — CDN handles rapid requests fine
PLATFORM_DELAY = 0.05
# Game server request delay (authenticated)
GAME_DELAY = 0.3
# Stats parallel workers — CDN sustains ~37 req/s with 20 workers
STATS_WORKERS = 20
STATS_DELAY = 0.0


def safe_print(msg):
    """Print with fallback for Unicode chars on cp1252 consoles."""
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="replace").decode("ascii"))


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def load_auth():
    """Load auth credentials from the mod's auth.json dump."""
    if not AUTH_FILE.exists():
        safe_print(f"FATAL: Auth file not found at {AUTH_FILE}")
        safe_print("Launch the game once to generate it.")
        sys.exit(2)
    with open(AUTH_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def game_headers(auth):
    """Build headers for authenticated game server requests."""
    return {
        "X-AUTH-SESSION-ID": auth["session_id"],
        "X-TRANSACTION-ID": auth["session_id"],
        "X-PRIME-VERSION": auth["prime_version"],
        "X-Instance-ID": str(auth["instance_id"]),
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# HTTP helpers with retry
# ---------------------------------------------------------------------------

def _platform_get(path, params=None, retries=3):
    """GET from platform server with retry on 5xx/timeout."""
    url = PLATFORM_BASE + path
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r
            if r.status_code >= 500:
                safe_print(f"  [retry {attempt+1}/{retries}] {url} returned {r.status_code}")
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
        except requests.exceptions.Timeout:
            safe_print(f"  [retry {attempt+1}/{retries}] {url} timed out")
            time.sleep(2 * (attempt + 1))
            continue
    safe_print(f"FATAL: {url} failed after {retries} attempts")
    return None


def _game_post(auth, path, body, retries=2):
    """POST to game server with auth headers. Exits on 401/403."""
    url = auth["game_server"] + path
    headers = game_headers(auth)
    for attempt in range(retries):
        try:
            r = requests.post(url, json=body, headers=headers, timeout=15)
            if r.status_code == 200:
                # Verify response is valid JSON (expired auth can return non-JSON)
                try:
                    r.json()
                except (requests.exceptions.JSONDecodeError, ValueError):
                    safe_print(f"FATAL: Auth expired (got non-JSON response from {path}).")
                    safe_print("Relaunch the game to refresh auth.json, then retry.")
                    sys.exit(2)
                return r
            if r.status_code in (401, 403):
                safe_print(f"FATAL: Auth expired (HTTP {r.status_code}).")
                safe_print("Relaunch the game to refresh auth.json, then retry.")
                sys.exit(2)
            if r.status_code >= 500:
                safe_print(f"  [retry {attempt+1}/{retries}] {path} returned {r.status_code}")
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
        except requests.exceptions.Timeout:
            safe_print(f"  [retry {attempt+1}/{retries}] {path} timed out")
            time.sleep(2 * (attempt + 1))
            continue
    safe_print(f"WARNING: {path} failed after {retries} attempts")
    return None


# ---------------------------------------------------------------------------
# Stage 1: Rankings (NO AUTH)
# ---------------------------------------------------------------------------

def fetch_all_rankings():
    """Paginate the military might leaderboard to get all player IDs + scores."""
    all_results = []
    start = 0
    page_size = 500

    while True:
        r = _platform_get(
            f"/content/v1/products/prime/event/rankings/{SERVER}/{MILITARY_MIGHT_CONFIG}",
            params={"count": page_size, "start": start},
        )
        if r is None:
            break

        data = r.json()
        results = data.get("results", [])
        if not results:
            break

        all_results.extend(results)
        safe_print(f"  Rankings page {start // page_size + 1}: "
                   f"got {len(results)} (total {len(all_results)})")

        if len(results) < page_size:
            break

        start += page_size
        time.sleep(PLATFORM_DELAY)

    safe_print(f"Stage 1 complete: {len(all_results)} players from rankings")
    return all_results


def fetch_resources_raided():
    """Paginate the resources raided monthly leaderboard. Returns dict[hex_id → score]."""
    raided = {}
    start = 0
    page_size = 500

    while True:
        r = _platform_get(
            f"/content/v1/products/prime/event/rankings/{SERVER}/{RESOURCES_RAIDED_CONFIG}",
            params={"count": page_size, "start": start},
        )
        if r is None:
            break

        data = r.json()
        results = data.get("results", [])
        if not results:
            break

        for entry in results:
            raided[entry["id"]] = int(entry.get("score", 0))

        if len(results) < page_size:
            break

        start += page_size
        time.sleep(PLATFORM_DELAY)

    safe_print(f"  Resources raided: {len(raided)} players with scores")
    return raided


def fetch_alliance_contrib(alliance_ids):
    """Fetch RSS and ISO contribution scores for all alliances' members.

    Uses the by_group rankings endpoint (no auth) with alliance-scoped configs.
    Players who changed alliances may have scores under multiple alliance IDs,
    so we sum across all alliances to get their total contribution.
    Returns (rss_dict, iso_dict) where each is {hex_id: score}.
    """
    rss = {}
    iso = {}

    for config, label, target in [
        (RSS_CONTRIB_CONFIG, "RSS contrib", rss),
        (ISO_CONTRIB_CONFIG, "ISO contrib", iso),
    ]:
        for alliance_id in alliance_ids:
            start = 0
            page_size = 500
            while True:
                r = _platform_get(
                    f"/content/v1/products/prime/event/rankings/by_group/{SERVER}/{config}/{alliance_id}",
                    params={"count": page_size, "start": start},
                )
                if r is None:
                    break
                data = r.json()
                results = data.get("results", [])
                if not results:
                    break
                for entry in results:
                    pid = entry["id"]
                    score = int(entry.get("score", 0))
                    target[pid] = target.get(pid, 0) + score
                if len(results) < page_size:
                    break
                start += page_size
                time.sleep(PLATFORM_DELAY)
            time.sleep(PLATFORM_DELAY)
        safe_print(f"  {label}: {len(target)} players with scores")

    return rss, iso


# ---------------------------------------------------------------------------
# Stage 2: Batch Profile Lookup (AUTH)
# ---------------------------------------------------------------------------

def fetch_profiles(hex_ids, auth):
    """Batch lookup player profiles. Returns dict[hex_id → profile_dict]."""
    profiles = {}
    batches = [hex_ids[i:i + PROFILE_BATCH_SIZE]
               for i in range(0, len(hex_ids), PROFILE_BATCH_SIZE)]

    for batch_num, batch in enumerate(batches, 1):
        r = _game_post(auth, "/user_profile/profiles", {"user_ids": batch})
        if r is None:
            safe_print(f"  WARNING: Profile batch {batch_num}/{len(batches)} failed")
            continue

        data = r.json()
        # Response format: {"user_profiles": {hex_id: {user_id, name, level, ...}}}
        user_profiles = data.get("user_profiles", {})
        if isinstance(user_profiles, dict):
            for pid, p in user_profiles.items():
                if isinstance(p, dict):
                    profiles[pid] = p
        elif isinstance(data, list):
            for p in data:
                pid = p.get("user_id") or p.get("id", "")
                if pid:
                    profiles[pid] = p

        if batch_num % 10 == 0 or batch_num == len(batches):
            safe_print(f"  Profile batch {batch_num}/{len(batches)}: "
                       f"{len(profiles)} profiles so far")
        time.sleep(GAME_DELAY)

    safe_print(f"Stage 2 complete: {len(profiles)} profiles fetched")
    return profiles


# ---------------------------------------------------------------------------
# Stage 3: Alliance Resolution (AUTH)
# ---------------------------------------------------------------------------

def fetch_alliances(alliance_ids, auth):
    """Resolve alliance IDs to names/tags. Returns dict[str(alliance_id) → info]."""
    if not alliance_ids:
        return {}

    alliances = {}
    id_list = list(alliance_ids)
    # Batch into groups of 100 to avoid oversized requests
    batch_size = 100
    batches = [id_list[i:i + batch_size] for i in range(0, len(id_list), batch_size)]

    for batch_num, batch in enumerate(batches, 1):
        r = _game_post(auth, "/alliance/get_alliances_public_info",
                       {"alliance_ids": batch})
        if r is None:
            safe_print(f"  WARNING: Alliance batch {batch_num}/{len(batches)} failed")
            continue

        data = r.json()
        # Response: {"alliances_info": {id_str: {id, name, tag, level, ...}}}
        alliances_info = data.get("alliances_info", {})
        if isinstance(alliances_info, dict):
            for aid_str, a in alliances_info.items():
                if isinstance(a, dict):
                    alliances[aid_str] = a

        if len(batches) > 1:
            safe_print(f"  Alliance batch {batch_num}/{len(batches)}: "
                       f"{len(alliances)} resolved so far")
        time.sleep(GAME_DELAY)

    safe_print(f"Stage 3 complete: {len(alliances)} alliances resolved")
    return alliances


# ---------------------------------------------------------------------------
# Stage 4: Player Stats (NO AUTH, protobuf)
# ---------------------------------------------------------------------------

def decode_stats_proto(data):
    """Decode player-stats protobuf into {stat_name: float_value}.

    Wire format structure (observed):
    field 1 (outer wrapper)
      → field 2 (player container, has player ID at field 1)
        → field 2 (category group, has category name at field 1)
          → field 2 (stat entry)
            → field 1 (hash string)
            → field 3 (fixed32 = IEEE 754 float)
    """
    stats = {}
    try:
        # Top level: field 1 = outer wrapper
        outers = _proto_extract_submessages(data, field_num=1)
        for outer in outers:
            # Inside outer: field 2 = player container
            containers = _proto_extract_submessages(outer, field_num=2)
            for container in containers:
                # Inside container: field 2 = category groups
                categories = _proto_extract_submessages(container, field_num=2)
                for cat_data in categories:
                    # Inside category: field 2 = stat entries
                    entries = _proto_extract_submessages(cat_data, field_num=2)
                    for entry_data in entries:
                        hash_str = _proto_extract_string(entry_data, field_num=1)
                        float_val = _proto_extract_fixed32_float(entry_data, field_num=3)
                        if hash_str and float_val is not None:
                            prefix = hash_str[:8]
                            name = STAT_PREFIX_MAP.get(prefix, hash_str)
                            stats[name] = float_val
    except Exception:
        pass  # Malformed proto — return whatever we got
    return stats


def _read_varint(data, pos):
    """Read a protobuf varint, return (value, new_pos)."""
    val = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return val, pos


def _proto_extract_submessages(data, field_num):
    """Extract all length-delimited (wire type 2) values for a given field number."""
    results = []
    pos = 0
    while pos < len(data):
        try:
            tag, pos = _read_varint(data, pos)
            wire_type = tag & 0x07
            fn = tag >> 3
            if wire_type == 0:  # varint
                _, pos = _read_varint(data, pos)
            elif wire_type == 2:  # length-delimited
                length, pos = _read_varint(data, pos)
                chunk = data[pos:pos + length]
                pos += length
                if fn == field_num:
                    results.append(chunk)
            elif wire_type == 5:  # fixed32
                pos += 4
            elif wire_type == 1:  # fixed64
                pos += 8
            else:
                break
        except (IndexError, struct.error):
            break
    return results


def _proto_extract_string(data, field_num):
    """Extract the first string (wire type 2) for a field number."""
    chunks = _proto_extract_submessages(data, field_num)
    for chunk in chunks:
        try:
            s = chunk.decode("utf-8")
            if all(32 <= ord(c) < 127 for c in s):
                return s
        except (UnicodeDecodeError, ValueError):
            pass
    return None


def _proto_extract_fixed32_float(data, field_num):
    """Extract the first fixed32 (wire type 5) as a float for a field number."""
    pos = 0
    while pos < len(data):
        try:
            tag, pos = _read_varint(data, pos)
            wire_type = tag & 0x07
            fn = tag >> 3
            if wire_type == 0:
                _, pos = _read_varint(data, pos)
            elif wire_type == 2:
                length, pos = _read_varint(data, pos)
                pos += length
            elif wire_type == 5:
                if fn == field_num:
                    val = struct.unpack_from("<f", data, pos)[0]
                    return val
                pos += 4
            elif wire_type == 1:
                pos += 8
            else:
                break
        except (IndexError, struct.error):
            break
    return None


def fetch_player_stats(hex_id):
    """Fetch stats for a single player. Returns dict or None on failure."""
    r = _platform_get(f"/content/v1/products/prime/player-stats/{SERVER}/{hex_id}")
    if r is None or r.status_code != 200:
        return None
    if len(r.content) == 0:
        return None
    stats = decode_stats_proto(r.content)
    return stats if stats else None


def fetch_all_player_stats(hex_ids):
    """Fetch stats for all players in parallel. Returns dict[hex_id → stats]."""
    all_stats = {}
    failed = 0

    with ThreadPoolExecutor(max_workers=STATS_WORKERS) as executor:
        futures = {}
        for hex_id in hex_ids:
            f = executor.submit(_fetch_stats_with_delay, hex_id)
            futures[f] = hex_id

        done = 0
        for future in as_completed(futures):
            hex_id = futures[future]
            done += 1
            try:
                stats = future.result()
                if stats is not None:
                    all_stats[hex_id] = stats
                else:
                    failed += 1
            except Exception:
                failed += 1

            if done % 200 == 0 or done == len(hex_ids):
                safe_print(f"  Stats progress: {done}/{len(hex_ids)} "
                           f"({len(all_stats)} ok, {failed} failed)")

    safe_print(f"Stage 4 complete: {len(all_stats)} player stats fetched "
               f"({failed} failed)")
    return all_stats


def _fetch_stats_with_delay(hex_id):
    """Wrapper with optional delay for rate limiting."""
    if STATS_DELAY > 0:
        time.sleep(STATS_DELAY)
    return fetch_player_stats(hex_id)


# ---------------------------------------------------------------------------
# Mapping: Scopely data → upsert-compatible dict
# ---------------------------------------------------------------------------

def map_player(hex_id, ranking, profile, alliance_info, stats, resources_raided=0,
               rss_contrib=0, iso_contrib=0):
    """Map Scopely API data to the format expected by db.upsert_players().

    Returns a dict compatible with pull_api.map_player() output.
    """
    # Extract from profile — field names from /user_profile/profiles response
    name = profile.get("name", "")
    level = int(profile.get("level", 0) or 0)
    # Use ranking score (military might from leaderboard) as power
    power = int(ranking.get("score", 0) or 0)
    # alliance_id is a large integer in the response, convert to string
    raw_aid = profile.get("alliance_id")
    alliance_id = str(raw_aid) if raw_aid else ""

    # Alliance info
    a = alliance_info or {}
    alliance_tag = a.get("tag") or a.get("alliance_tag") or ""
    alliance_name = a.get("name") or a.get("alliance_name") or ""

    # Stats (from protobuf, all floats)
    helps = int(stats.get("helps", 0))
    hostiles_killed = int(stats.get("hostiles_killed", 0))
    players_killed = int(stats.get("players_killed", 0))
    resources_mined = int(stats.get("resources_mined", 0))

    return {
        "id": hex_id,
        "name": name,
        "level": level,
        "power": power,
        "helps": helps,
        "rss_contrib": rss_contrib,
        "iso_contrib": iso_contrib,
        "players_killed": players_killed,
        "hostiles_killed": hostiles_killed,
        "resources_mined": resources_mined,
        "resources_raided": resources_raided,
        "alliance_tag": alliance_tag,
        "alliance_name": alliance_name,
        "alliance_id": alliance_id,
        "rank": "",
        "join_date": "",
    }


# ---------------------------------------------------------------------------
# Player ID Bridge (one-time migration)
# ---------------------------------------------------------------------------

def bridge_player_ids(conn, profiles):
    """Match existing numeric player IDs to Scopely hex IDs by name.

    Updates player_id in players and daily_snapshots tables.
    Returns count of matched players.
    """
    # Build name → hex_id lookup from Scopely profiles
    name_to_hex = {}
    for hex_id, p in profiles.items():
        name = (p.get("name") or p.get("player_name")
                or p.get("username") or "").strip().lower()
        if name:
            name_to_hex[name] = hex_id

    # Get all existing players with numeric-looking IDs
    rows = conn.execute(
        "SELECT player_id, name FROM players WHERE player_id GLOB '[0-9]*'"
    ).fetchall()

    matched = 0
    for old_id, name in rows:
        if not name:
            continue
        hex_id = name_to_hex.get(name.strip().lower())
        if not hex_id:
            continue

        # Check if the hex_id already exists (avoid PK conflict)
        existing = conn.execute(
            "SELECT 1 FROM players WHERE player_id = ?", (hex_id,)
        ).fetchone()
        if existing:
            # Merge: update snapshots to point to hex_id, delete old player
            conn.execute(
                "UPDATE daily_snapshots SET player_id = ? WHERE player_id = ?",
                (hex_id, old_id))
            conn.execute("DELETE FROM players WHERE player_id = ?", (old_id,))
        else:
            # Rename player_id
            conn.execute(
                "UPDATE daily_snapshots SET player_id = ? WHERE player_id = ?",
                (hex_id, old_id))
            conn.execute(
                "UPDATE players SET player_id = ? WHERE player_id = ?",
                (hex_id, old_id))

        # Update discord_links too
        conn.execute(
            "UPDATE discord_links SET player_id = ? WHERE player_id = ?",
            (hex_id, old_id))

        matched += 1

    conn.commit()
    safe_print(f"[bridge] Matched {matched}/{len(rows)} players (numeric → hex)")
    unmatched = len(rows) - matched
    if unmatched > 0:
        safe_print(f"[bridge] {unmatched} players could not be matched by name")

    return matched



# ---------------------------------------------------------------------------
# Save data (same pattern as pull_api.py)
# ---------------------------------------------------------------------------

def save_data(all_mapped, total_count):
    """Store mapped players in SQLite and export JSON files for dashboards."""
    ncc_members = [m for m in all_mapped if m["alliance_id"] == NCC_ALLIANCE_ID]
    safe_print(f"NCC members found: {len(ncc_members)}")

    today = now_est().strftime("%Y-%m-%d")
    conn = get_db()

    # One-time bridge: migrate numeric IDs to hex IDs and alliance IDs
    if not MIGRATED_FLAG.exists():
        # Bridge player IDs
        profiles_for_bridge = {m["id"]: {"name": m["name"]} for m in all_mapped}
        bridged = bridge_player_ids(conn, profiles_for_bridge)

        # Bridge alliance IDs (old stfc.pro numeric → Scopely)
        alliance_tag_map = {}
        for m in all_mapped:
            tag = m.get("alliance_tag")
            aid = m.get("alliance_id")
            if tag and aid:
                alliance_tag_map[tag] = aid
        _migrate_alliance_ids(conn, alliance_tag_map)

        if bridged > 0 or len(all_mapped) > 0:
            MIGRATED_FLAG.write_text(now_est().isoformat())
            safe_print(f"[bridge] Migration flag set")

    upsert_players(conn, all_mapped, today)
    safe_print(f"Database updated ({len(all_mapped)} players upserted)")

    log_pull(conn, SERVER, total_count, source="scopely")

    export_latest_json(conn, NCC_ALLIANCE_ID)
    safe_print(f"Exported {DATA_DIR / 'latest.json'}")

    export_history_json(conn, NCC_ALLIANCE_ID)
    safe_print(f"Exported {DATA_DIR / 'history.json'}")

    export_server_alliances_json(conn)
    safe_print(f"Exported {DATA_DIR / 'server_alliances.json'}")

    _local_inv = BASE_DIR / "alliance_inventory.json"
    _dev_inv = Path("C:/Users/drewa/Desktop/stfc/stfc-api/alliance_inventory.json")
    inv_path = _local_inv if _local_inv.exists() else _dev_inv
    inv_count = ingest_alliance_inventory(conn, inv_path)
    if inv_count:
        safe_print(f"Ingested {inv_count} alliance inventory items")
    export_alliance_inventory_json(conn)
    safe_print(f"Exported {DATA_DIR / 'alliance_inventory.json'}")

    export_server_players_json(conn)
    safe_print(f"Exported {DATA_DIR / 'server_players.json'}")

    export_server_history_json(conn)
    safe_print(f"Exported {DATA_DIR / 'server_history.json'}")

    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="STFC Scopely API Scraper")
    parser.add_argument("--skip-stats", action="store_true",
                        help="Skip player stats entirely")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and print data without saving to DB")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)

    # --- Stage 1: Rankings ---
    safe_print("=== Stage 1: Fetching rankings ===")
    rankings = fetch_all_rankings()
    if len(rankings) < 10:
        safe_print(f"ERROR: Only got {len(rankings)} players from rankings — aborting.")
        sys.exit(1)

    hex_ids = [r["id"] for r in rankings]
    rankings_by_id = {r["id"]: r for r in rankings}

    # --- Fetch resources raided from monthly leaderboard ---
    safe_print("  Fetching resources raided leaderboard...")
    raided_scores = fetch_resources_raided()

    # Determine if we need stats
    do_stats = not args.skip_stats

    # --- Start stats in background (CDN, no auth) while we do profiles+alliances ---
    stats_future = None
    all_stats = {}
    if do_stats:
        safe_print(f"=== Stage 4: Starting stats fetch for {len(hex_ids)} players (background) ===")
        from concurrent.futures import ThreadPoolExecutor as _TPE
        bg_executor = _TPE(max_workers=1)
        stats_future = bg_executor.submit(fetch_all_player_stats, hex_ids)

    # --- Stage 2: Profiles (game server, auth) ---
    safe_print("=== Stage 2: Fetching profiles ===")
    auth = load_auth()
    profiles = fetch_profiles(hex_ids, auth)

    # --- Stage 3: Alliances (game server, auth) ---
    safe_print("=== Stage 3: Resolving alliances ===")
    alliance_ids = set()
    for p in profiles.values():
        aid = p.get("alliance_id")
        if aid:
            alliance_ids.add(aid)  # keep as original type (int) for the API
    alliances = fetch_alliances(alliance_ids, auth) if alliance_ids else {}

    # --- Fetch RSS/ISO contrib for all alliances ---
    safe_print(f"=== Fetching alliance contributions (RSS + ISO) for {len(alliance_ids)} alliances ===")
    rss_scores, iso_scores = fetch_alliance_contrib(alliance_ids)

    # --- Collect stats results ---
    if stats_future is not None:
        safe_print("=== Waiting for stats to finish ===")
        all_stats = stats_future.result()
        bg_executor.shutdown(wait=False)

    # --- Map all players (filter out cross-server) ---
    safe_print("=== Mapping players ===")
    all_mapped = []
    skipped_cross_server = 0
    for hex_id in hex_ids:
        profile = profiles.get(hex_id, {})

        # Skip players from other servers (Scopely rankings bug)
        gw = profile.get("gameworld_id")
        if gw is not None and gw != SERVER:
            skipped_cross_server += 1
            continue

        ranking = rankings_by_id.get(hex_id, {})
        aid = str(profile.get("alliance_id", "") or "")
        alliance_info = alliances.get(aid, {})
        stats = all_stats.get(hex_id, {})

        raided = raided_scores.get(hex_id, 0)
        rss_c = rss_scores.get(hex_id, 0)
        iso_c = iso_scores.get(hex_id, 0)
        mapped = map_player(hex_id, ranking, profile, alliance_info, stats, raided,
                            rss_c, iso_c)
        all_mapped.append(mapped)

    if skipped_cross_server:
        safe_print(f"  Skipped {skipped_cross_server} cross-server players")
    safe_print(f"Mapped {len(all_mapped)} players")

    if args.dry_run:
        safe_print("\n=== DRY RUN — first 20 players ===")
        for m in all_mapped[:20]:
            tag = f"[{m['alliance_tag']}] " if m['alliance_tag'] else ""
            safe_print(f"  {tag}{m['name'] or '(no name)'} — "
                       f"L{m['level']} — {m['power']:,} power — "
                       f"ID: {m['id']}")
        safe_print(f"\n  ... and {len(all_mapped) - 20} more")
        safe_print(f"  Players with names: {sum(1 for m in all_mapped if m['name'])}")
        safe_print(f"  Players with alliance: {sum(1 for m in all_mapped if m['alliance_tag'])}")
        safe_print(f"  Players with stats: {sum(1 for m in all_mapped if all_stats.get(m['id']))}")
        return

    # --- Save ---
    save_data(all_mapped, len(rankings))
    safe_print("Done!")


if __name__ == "__main__":
    main()
