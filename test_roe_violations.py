"""Tests for ROE violation tracking."""

import json

import pytest

import db as db_mod


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    """Create a temporary tracker DB and data directory."""
    monkeypatch.setattr(db_mod, "DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
    conn = db_mod.get_db()
    yield conn, tmp_path
    conn.close()


def _insert_player(conn, player_id, name, alliance_id, alliance_tag, alliance_name):
    conn.execute("""
        INSERT INTO players (player_id, name, server, alliance_id, alliance_tag, first_seen, last_seen)
        VALUES (?, ?, 716, ?, ?, '2026-03-01', '2026-03-14')
    """, (player_id, name, alliance_id, alliance_tag))
    conn.execute("""
        INSERT INTO daily_snapshots (
            player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
            players_killed, hostiles_killed, resources_mined, resources_raided,
            alliance_id, alliance_tag, alliance_name
        )
        VALUES (?, '2026-03-14', ?, 40, 1000000, 0, 0, 0, 0, 0, 0, 0, ?, ?, ?)
    """, (player_id, name, alliance_id, alliance_tag, alliance_name))
    conn.commit()


class TestRoeViolations:
    def test_record_and_export_tallies(self, test_db):
        conn, tmp_path = test_db
        _insert_player(conn, "p1", "BadGuy", "a1", "FOE", "Foe Alliance")
        _insert_player(conn, "p2", "WorseGuy", "a1", "FOE", "Foe Alliance")
        _insert_player(conn, "n1", "Victim", db_mod.NCC_ALLIANCE_ID, "NCC", "Discovery")

        db_mod.record_roe_violation(
            conn,
            offender_name="BadGuy",
            offender_player_id="p1",
            offender_alliance_id="a1",
            offender_alliance_tag="FOE",
            offender_alliance_name="Foe Alliance",
            victim_player_id="n1",
            victim_name="Victim",
            violation_type="UPC hit",
            reported_by="OfficerOne",
            offense_date="2026-03-13",
            screenshots="https://example.com/shot-1.png",
            notes="First strike",
        )
        db_mod.record_roe_violation(
            conn,
            offender_name="BadGuy",
            offender_player_id="p1",
            offender_alliance_id="a1",
            offender_alliance_tag="FOE",
            offender_alliance_name="Foe Alliance",
            victim_name="Victim",
            violation_type="Token space hit",
            reported_by="OfficerTwo",
            offense_date="2026-03-14",
        )
        db_mod.record_roe_violation(
            conn,
            offender_name="WorseGuy",
            offender_player_id="p2",
            offender_alliance_id="a1",
            offender_alliance_tag="FOE",
            offender_alliance_name="Foe Alliance",
            violation_type="Armada interference",
            offense_date="2026-03-14",
        )

        payload = db_mod.export_roe_violations_json(conn)

        assert payload["violation_count"] == 3
        assert payload["unique_offender_count"] == 2
        assert payload["alliance_count"] == 1
        assert payload["player_tallies"][0]["offender_name"] == "BadGuy"
        assert payload["player_tallies"][0]["offense_count"] == 2
        assert payload["alliance_tallies"][0]["offender_alliance_tag"] == "FOE"
        assert payload["alliance_tallies"][0]["offense_count"] == 3
        assert payload["alliance_tallies"][0]["unique_offender_count"] == 2
        assert payload["recent_violations"][0]["violation_type"] in {"Token space hit", "Armada interference"}
        assert payload["recent_violations"][-1]["screenshots"] == "https://example.com/shot-1.png"

        exported = json.loads((tmp_path / "roe_violations.json").read_text(encoding="utf-8"))
        assert exported["violation_count"] == 3
        assert exported["player_tallies"][0]["offense_count"] == 2

    def test_requires_offender_and_type(self, test_db):
        conn, _ = test_db

        with pytest.raises(ValueError):
            db_mod.record_roe_violation(conn, offender_name="", violation_type="UPC hit")

        with pytest.raises(ValueError):
            db_mod.record_roe_violation(conn, offender_name="BadGuy", violation_type="")

        with pytest.raises(ValueError):
            db_mod.record_roe_violation(conn, offender_name="BadGuy", violation_type="Zero node hit")

    def test_alliance_tally_skips_blank_alliance(self, test_db):
        conn, _ = test_db

        db_mod.record_roe_violation(
            conn,
            offender_name="MysteryGuy",
            violation_type="UPC hit",
            offense_date="2026-03-14",
        )

        payload = db_mod.export_roe_violations_json(conn)
        assert payload["violation_count"] == 1
        assert payload["unique_offender_count"] == 1
        assert payload["alliance_count"] == 0
