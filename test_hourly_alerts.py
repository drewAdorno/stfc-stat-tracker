"""Tests for send_hourly_alerts.py"""

import json
import sqlite3
import pytest

import send_hourly_alerts as sha
import db as db_mod


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def prev_members():
    """Previous snapshot member dict."""
    return {
        "100": {"id": "100", "name": "Alice", "level": "39", "power": "80M"},
        "200": {"id": "200", "name": "Bob", "level": "35", "power": "50M"},
        "300": {"id": "300", "name": "Carol", "level": "28", "power": "10M"},
    }


@pytest.fixture
def curr_members():
    """Current snapshot member dict — Alice leveled up, Carol left, Dave joined."""
    return {
        "100": {"id": "100", "name": "Alice", "level": "40", "power": "85M"},
        "200": {"id": "200", "name": "Bob", "level": "35", "power": "51M"},
        "400": {"id": "400", "name": "Dave", "level": "30", "power": "20M"},
    }


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    """Create a temporary SQLite DB for testing."""
    monkeypatch.setattr(db_mod, "DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
    conn = db_mod.get_db()
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# detect_changes
# ---------------------------------------------------------------------------

class TestDetectChanges:
    def test_joined(self, prev_members, curr_members):
        changes = sha.detect_changes(prev_members, curr_members)
        names = [m["name"] for m in changes["joined"]]
        assert "Dave" in names

    def test_left(self, prev_members, curr_members):
        changes = sha.detect_changes(prev_members, curr_members)
        names = [m["name"] for m in changes["left"]]
        assert "Carol" in names

    def test_level_up(self, prev_members, curr_members):
        changes = sha.detect_changes(prev_members, curr_members)
        ups = {m["name"]: m for m in changes["level_ups"]}
        assert "Alice" in ups
        assert ups["Alice"]["old_level"] == "39"
        assert ups["Alice"]["new_level"] == "40"

    def test_no_false_level_up(self, prev_members, curr_members):
        """Bob stayed at level 35 — should not appear in level_ups."""
        changes = sha.detect_changes(prev_members, curr_members)
        names = [m["name"] for m in changes["level_ups"]]
        assert "Bob" not in names

    def test_no_changes(self):
        members = {"1": {"id": "1", "name": "X", "level": "10", "power": "1M"}}
        changes = sha.detect_changes(members, members)
        assert changes["joined"] == []
        assert changes["left"] == []
        assert changes["level_ups"] == []

    def test_empty_to_empty(self):
        changes = sha.detect_changes({}, {})
        assert not sha.has_changes(changes)

    def test_all_new(self):
        curr = {"1": {"id": "1", "name": "A", "level": "5", "power": "1K"}}
        changes = sha.detect_changes({}, curr)
        assert len(changes["joined"]) == 1
        assert changes["left"] == []

    def test_all_left(self):
        prev = {"1": {"id": "1", "name": "A", "level": "5", "power": "1K"}}
        changes = sha.detect_changes(prev, {})
        assert len(changes["left"]) == 1
        assert changes["joined"] == []


# ---------------------------------------------------------------------------
# build_alert_embeds
# ---------------------------------------------------------------------------

class TestBuildAlertEmbeds:
    def test_three_embeds_when_all_changes(self, prev_members, curr_members):
        changes = sha.detect_changes(prev_members, curr_members)
        embeds = sha.build_alert_embeds(changes)
        titles = [e["title"] for e in embeds]
        assert "✅ Member Joined" in titles
        assert "🚪 Member Left" in titles
        assert "⬆️ Level Up" in titles

    def test_each_embed_has_footer(self, prev_members, curr_members):
        changes = sha.detect_changes(prev_members, curr_members)
        embeds = sha.build_alert_embeds(changes)
        for embed in embeds:
            assert embed["footer"]["text"] == "ncctracker.top"

    def test_joined_embed_is_green(self):
        changes = {
            "joined": [{"name": "NewGuy", "level": "10", "power": "1M"}],
            "left": [],
            "level_ups": [],
        }
        embeds = sha.build_alert_embeds(changes)
        assert len(embeds) == 1
        assert embeds[0]["color"] == 0x51CF66
        assert embeds[0]["title"] == "✅ Member Joined"
        assert "NewGuy" in embeds[0]["description"]

    def test_left_embed_is_red(self):
        changes = {
            "joined": [],
            "left": [{"name": "GoneGuy", "level": "20", "power": "5M"}],
            "level_ups": [],
        }
        embeds = sha.build_alert_embeds(changes)
        assert len(embeds) == 1
        assert embeds[0]["color"] == 0xFF6B6B
        assert embeds[0]["title"] == "🚪 Member Left"
        assert "GoneGuy" in embeds[0]["description"]

    def test_level_up_embed_is_blue(self):
        changes = {
            "joined": [],
            "left": [],
            "level_ups": [{"name": "Leveler", "old_level": "29", "new_level": "30"}],
        }
        embeds = sha.build_alert_embeds(changes)
        assert len(embeds) == 1
        assert embeds[0]["color"] == 0x4DABF7
        assert embeds[0]["title"] == "⬆️ Level Up"
        assert "Lv29" in embeds[0]["description"]
        assert "Lv30" in embeds[0]["description"]

    def test_level_up_has_congrats_message(self):
        changes = {
            "joined": [],
            "left": [],
            "level_ups": [{"name": "Leveler", "old_level": "29", "new_level": "30"}],
        }
        embeds = sha.build_alert_embeds(changes)
        desc = embeds[0]["description"]
        assert any(msg in desc for msg in sha.LEVEL_UP_MESSAGES)

    def test_no_embeds_when_no_changes(self):
        changes = {"joined": [], "left": [], "level_ups": []}
        embeds = sha.build_alert_embeds(changes)
        assert embeds == []

    def test_joined_format_contains_power(self):
        changes = {
            "joined": [{"name": "Rich", "level": "40", "power": "100M"}],
            "left": [],
            "level_ups": [],
        }
        embeds = sha.build_alert_embeds(changes)
        desc = embeds[0]["description"]
        assert "100M" in desc
        assert "Lv40" in desc

    def test_left_format_says_was(self):
        changes = {
            "joined": [],
            "left": [{"name": "Gone", "level": "25", "power": "5M"}],
            "level_ups": [],
        }
        embeds = sha.build_alert_embeds(changes)
        assert "was Lv25" in embeds[0]["description"]

    def test_multiple_members_in_one_embed(self):
        changes = {
            "joined": [
                {"name": "A", "level": "10", "power": "1M"},
                {"name": "B", "level": "20", "power": "2M"},
            ],
            "left": [],
            "level_ups": [],
        }
        embeds = sha.build_alert_embeds(changes)
        assert len(embeds) == 1
        assert "A" in embeds[0]["description"]
        assert "B" in embeds[0]["description"]


# ---------------------------------------------------------------------------
# has_changes
# ---------------------------------------------------------------------------

class TestHasChanges:
    def test_true_with_joined(self):
        assert sha.has_changes({"joined": [{}], "left": [], "level_ups": []})

    def test_true_with_left(self):
        assert sha.has_changes({"joined": [], "left": [{}], "level_ups": []})

    def test_true_with_level_ups(self):
        assert sha.has_changes({"joined": [], "left": [], "level_ups": [{}]})

    def test_false_when_empty(self):
        assert not sha.has_changes({"joined": [], "left": [], "level_ups": []})


# ---------------------------------------------------------------------------
# DB-backed snapshot queries
# ---------------------------------------------------------------------------

def _insert_snapshot(conn, date, members):
    """Helper to insert test snapshot data into the DB."""
    alliance_id = db_mod.NCC_ALLIANCE_ID
    for mid, m in members.items():
        pid = int(mid)
        conn.execute("""
            INSERT OR REPLACE INTO players (player_id, name, server, alliance_id, alliance_tag, first_seen, last_seen)
            VALUES (?, ?, 716, ?, 'NCC', ?, ?)
        """, (pid, m["name"], alliance_id, date, date))
        conn.execute("""
            INSERT OR REPLACE INTO daily_snapshots
                (player_id, date, level, power, alliance_id, alliance_tag)
            VALUES (?, ?, ?, ?, ?, 'NCC')
        """, (pid, date, m.get("level", 0), m.get("power", 0), alliance_id))
    conn.commit()


class TestGetLatestTwoDates:
    def test_returns_two_dates(self, test_db):
        _insert_snapshot(test_db, "2026-02-14", {"1": {"name": "A", "level": 10, "power": 1000}})
        _insert_snapshot(test_db, "2026-02-15", {"1": {"name": "A", "level": 10, "power": 1000}})
        prev, curr = db_mod.get_latest_two_dates(test_db)
        assert prev == "2026-02-14"
        assert curr == "2026-02-15"

    def test_returns_none_with_one_date(self, test_db):
        _insert_snapshot(test_db, "2026-02-14", {"1": {"name": "A", "level": 10, "power": 1000}})
        prev, curr = db_mod.get_latest_two_dates(test_db)
        assert prev is None
        assert curr is None

    def test_returns_none_with_no_data(self, test_db):
        prev, curr = db_mod.get_latest_two_dates(test_db)
        assert prev is None
        assert curr is None

    def test_picks_last_two_of_many(self, test_db):
        for d in ["2026-02-12", "2026-02-13", "2026-02-14", "2026-02-15"]:
            _insert_snapshot(test_db, d, {"1": {"name": "A", "level": 10, "power": 1000}})
        prev, curr = db_mod.get_latest_two_dates(test_db)
        assert prev == "2026-02-14"
        assert curr == "2026-02-15"


class TestGetMembersForDate:
    def test_load(self, test_db):
        members = {
            "1": {"name": "A", "level": 10, "power": 5000},
            "2": {"name": "B", "level": 20, "power": 10000},
        }
        _insert_snapshot(test_db, "2026-02-14", members)
        result = db_mod.get_members_for_date(test_db, "2026-02-14")
        assert "1" in result
        assert "2" in result
        assert result["1"]["name"] == "A"

    def test_empty_date(self, test_db):
        result = db_mod.get_members_for_date(test_db, "2026-02-14")
        assert len(result) == 0
