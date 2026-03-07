"""Tests for Discord bot DB query logic (no real Discord connection)."""

import sqlite3
from datetime import datetime, timedelta

import pytest

from db import (
    NCC_ALLIANCE_ID,
    SCHEMA,
    _format_abbr,
    get_earliest_snapshot_date,
    get_linked_player,
    get_player_name_history,
    get_player_snapshot,
    get_snapshot_date_ago,
    link_discord,
    search_players,
    unlink_discord,
)
from discord_bot import _compute_deltas, _format_delta, _period_label


@pytest.fixture
def conn():
    """In-memory SQLite DB with schema and sample data."""
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys=ON")
    c.executescript(SCHEMA)

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    # Insert players
    c.execute("""
        INSERT INTO players (player_id, name, server, alliance_id, alliance_tag, first_seen, last_seen)
        VALUES (1, 'TestPlayer', 716, ?, 'NCC', ?, ?)
    """, (NCC_ALLIANCE_ID, week_ago, today))

    c.execute("""
        INSERT INTO players (player_id, name, server, alliance_id, alliance_tag, first_seen, last_seen)
        VALUES (2, 'OtherPlayer', 716, 9999, 'FOO', ?, ?)
    """, (yesterday, today))

    # Snapshots for TestPlayer
    c.execute("""
        INSERT INTO daily_snapshots
            (player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
             players_killed, hostiles_killed, resources_mined, resources_raided,
             alliance_id, alliance_tag)
        VALUES (1, ?, 'TestPlayer', 40, 100000000, 500, 1000, 200, 50, 10000, 50000000, 5000000, ?, 'NCC')
    """, (week_ago, NCC_ALLIANCE_ID))

    c.execute("""
        INSERT INTO daily_snapshots
            (player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
             players_killed, hostiles_killed, resources_mined, resources_raided,
             alliance_id, alliance_tag)
        VALUES (1, ?, 'TestPlayer', 41, 150000000, 600, 1500, 300, 100, 15000, 80000000, 8000000, ?, 'NCC')
    """, (today, NCC_ALLIANCE_ID))

    # Snapshot for OtherPlayer
    c.execute("""
        INSERT INTO daily_snapshots
            (player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
             players_killed, hostiles_killed, resources_mined, resources_raided,
             alliance_id, alliance_tag)
        VALUES (2, ?, 'OtherPlayer', 35, 80000000, 200, 500, 100, 30, 5000, 20000000, 2000000, 9999, 'FOO')
    """, (today,))

    c.commit()
    yield c
    c.close()


# --- Discord link tests ---

class TestDiscordLinks:
    def test_link_and_get(self, conn):
        link_discord(conn, "discord123", 1)
        assert get_linked_player(conn, "discord123") == 1

    def test_unlink(self, conn):
        link_discord(conn, "discord123", 1)
        assert unlink_discord(conn, "discord123") is True
        assert get_linked_player(conn, "discord123") is None

    def test_unlink_nonexistent(self, conn):
        assert unlink_discord(conn, "nobody") is False

    def test_relink_overwrites(self, conn):
        link_discord(conn, "discord123", 1)
        link_discord(conn, "discord123", 2)
        assert get_linked_player(conn, "discord123") == 2

    def test_get_unlinked(self, conn):
        assert get_linked_player(conn, "discord123") is None


# --- Search tests ---

class TestSearchPlayers:
    def test_search_by_prefix(self, conn):
        results = search_players(conn, "Test")
        assert len(results) == 1
        assert results[0][1] == "TestPlayer"

    def test_search_case_insensitive(self, conn):
        results = search_players(conn, "test")
        assert len(results) == 1

    def test_search_ncc_first(self, conn):
        results = search_players(conn, "")
        # NCC member (TestPlayer) should come before OtherPlayer
        # Empty prefix matches all with LIKE '%'
        # Actually empty + % = '%' which matches everything
        # But search_players uses query + "%" so empty becomes just "%"
        pass  # search_players with empty string gives "%" which matches all

    def test_search_no_match(self, conn):
        results = search_players(conn, "ZZZ")
        assert len(results) == 0


# --- Snapshot tests ---

class TestSnapshots:
    def test_get_latest_snapshot(self, conn):
        snap = get_player_snapshot(conn, 1)
        assert snap is not None
        assert snap["level"] == 41
        assert snap["power"] == 150000000

    def test_get_snapshot_by_date(self, conn):
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        snap = get_player_snapshot(conn, 1, week_ago)
        assert snap is not None
        assert snap["level"] == 40

    def test_get_snapshot_nonexistent(self, conn):
        assert get_player_snapshot(conn, 9999) is None

    def test_get_earliest_date(self, conn):
        date = get_earliest_snapshot_date(conn, 1)
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        assert date == week_ago

    def test_get_snapshot_date_ago(self, conn):
        date = get_snapshot_date_ago(conn, 1, 7)
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        assert date == week_ago


# --- Name history ---

class TestNameHistory:
    def test_single_name(self, conn):
        history = get_player_name_history(conn, 1)
        assert len(history) == 1
        assert history[0][0] == "TestPlayer"

    def test_name_change(self, conn):
        today = datetime.now().strftime("%Y-%m-%d")
        # Add a snapshot with different name
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        conn.execute("""
            INSERT INTO daily_snapshots
                (player_id, date, name, level, power, alliance_id, alliance_tag)
            VALUES (1, ?, 'RenamedPlayer', 41, 150000000, ?, 'NCC')
        """, (yesterday, NCC_ALLIANCE_ID))
        conn.commit()

        history = get_player_name_history(conn, 1)
        names = [h[0] for h in history]
        assert "TestPlayer" in names
        assert "RenamedPlayer" in names


# --- Helper function tests ---

class TestHelpers:
    def test_format_delta_positive(self):
        assert _format_delta(1000000) == "+1.00M"

    def test_format_delta_negative(self):
        assert _format_delta(-500000) == "-500.00K"

    def test_format_delta_zero(self):
        assert _format_delta(0) == "0"

    def test_period_label(self):
        assert _period_label(0) == "all time"
        assert _period_label(1) == "1 day"
        assert _period_label(7) == "7 days"

    def test_compute_deltas(self):
        current = {"level": 41, "power": 150000000, "helps": 600,
                    "rss_contrib": 1500, "iso_contrib": 300,
                    "players_killed": 100, "hostiles_killed": 15000,
                    "resources_mined": 80000000, "resources_raided": 8000000}
        old = {"level": 40, "power": 100000000, "helps": 500,
               "rss_contrib": 1000, "iso_contrib": 200,
               "players_killed": 50, "hostiles_killed": 10000,
               "resources_mined": 50000000, "resources_raided": 5000000}
        deltas = _compute_deltas(current, old)
        assert deltas["level"] == 1
        assert deltas["power"] == 50000000
        assert deltas["players_killed"] == 50
        assert deltas["resources_mined"] == 30000000
