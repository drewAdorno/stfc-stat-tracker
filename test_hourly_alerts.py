"""Tests for send_hourly_alerts.py"""

import json
import pytest

import send_hourly_alerts as sha


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


def _write_snapshot(path, members_list):
    """Write a minimal alliance snapshot file."""
    data = {"pulled_at": "2026-01-01T00:00:00", "members": members_list}
    path.write_text(json.dumps(data), encoding="utf-8")


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
# get_two_latest_snapshots
# ---------------------------------------------------------------------------

class TestGetTwoLatestSnapshots:
    def test_returns_two_paths(self, tmp_path):
        _write_snapshot(tmp_path / "alliance_2026-02-14_070000.json", [])
        _write_snapshot(tmp_path / "alliance_2026-02-14_080000.json", [])
        prev, curr = sha.get_two_latest_snapshots(tmp_path)
        assert prev.name == "alliance_2026-02-14_070000.json"
        assert curr.name == "alliance_2026-02-14_080000.json"

    def test_returns_none_with_one_file(self, tmp_path):
        _write_snapshot(tmp_path / "alliance_2026-02-14_070000.json", [])
        prev, curr = sha.get_two_latest_snapshots(tmp_path)
        assert prev is None
        assert curr is None

    def test_returns_none_with_no_files(self, tmp_path):
        prev, curr = sha.get_two_latest_snapshots(tmp_path)
        assert prev is None
        assert curr is None

    def test_picks_last_two_of_many(self, tmp_path):
        for h in ["06", "07", "08", "09"]:
            _write_snapshot(tmp_path / f"alliance_2026-02-14_{h}0000.json", [])
        prev, curr = sha.get_two_latest_snapshots(tmp_path)
        assert prev.name == "alliance_2026-02-14_080000.json"
        assert curr.name == "alliance_2026-02-14_090000.json"

    def test_ignores_non_matching_files(self, tmp_path):
        (tmp_path / "latest.json").write_text("{}", encoding="utf-8")
        (tmp_path / "history.json").write_text("[]", encoding="utf-8")
        _write_snapshot(tmp_path / "alliance_2026-02-14_070000.json", [])
        _write_snapshot(tmp_path / "alliance_2026-02-14_080000.json", [])
        prev, curr = sha.get_two_latest_snapshots(tmp_path)
        assert "alliance_" in prev.name
        assert "alliance_" in curr.name


# ---------------------------------------------------------------------------
# load_members
# ---------------------------------------------------------------------------

class TestLoadMembers:
    def test_load(self, tmp_path):
        members_list = [
            {"id": "1", "name": "A", "level": "10"},
            {"id": "2", "name": "B", "level": "20"},
        ]
        path = tmp_path / "test.json"
        _write_snapshot(path, members_list)
        result = sha.load_members(path)
        assert "1" in result
        assert "2" in result
        assert result["1"]["name"] == "A"

    def test_skips_members_without_id(self, tmp_path):
        members_list = [{"name": "NoID", "level": "5"}]
        path = tmp_path / "test.json"
        _write_snapshot(path, members_list)
        result = sha.load_members(path)
        assert len(result) == 0
