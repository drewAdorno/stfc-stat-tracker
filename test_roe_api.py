"""Tests for the ROE FastAPI service."""

from fastapi.testclient import TestClient
import pytest

import db as db_mod


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Create an isolated ROE API client backed by a temp DB."""
    monkeypatch.setattr(db_mod, "DB_PATH", tmp_path / "test.db")
    monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
    monkeypatch.setenv("NCC_ADMIN_PASSWORD", "testpw")

    from roe_api import app

    conn = db_mod.get_db()
    conn.execute("""
        INSERT INTO players (player_id, name, server, alliance_id, alliance_tag, first_seen, last_seen)
        VALUES ('p1', 'BadGuy', 716, 'a1', 'FOE', '2026-03-01', '2026-03-14')
    """)
    conn.execute("""
        INSERT INTO daily_snapshots (
            player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
            players_killed, hostiles_killed, resources_mined, resources_raided,
            alliance_id, alliance_tag, alliance_name
        )
        VALUES ('p1', '2026-03-14', 'BadGuy', 40, 1000000, 0, 0, 0, 0, 0, 0, 0, 'a1', 'FOE', 'Foe Alliance')
    """)
    conn.commit()
    conn.close()

    return TestClient(app)


def _headers():
    return {"X-Admin-Password": "testpw"}


class TestRoeApi:
    def test_requires_auth(self, client):
        response = client.get("/api/roe/summary")
        assert response.status_code == 401

    def test_player_search(self, client):
        response = client.get("/api/players/search", params={"q": "Bad"}, headers=_headers())
        assert response.status_code == 200
        data = response.json()
        assert data["players"][0]["name"] == "BadGuy"
        assert data["players"][0]["alliance_tag"] == "FOE"

    def test_create_violation(self, client):
        response = client.post(
            "/api/roe/violations",
            json={
                "offender_query": "BadGuy",
                "violation_type": "OPC hit",
                "reported_by": "Officer",
                "victim_name": "Victim",
                "system_name": "Ty'Gokor",
                "notes": "Caught on survey",
            },
            headers=_headers(),
        )
        assert response.status_code == 200
        data = response.json()
        assert data["identity"]["name"] == "BadGuy"
        assert data["payload"]["violation_count"] == 1

    def test_summary_and_list(self, client):
        create_response = client.post(
            "/api/roe/violations",
            json={
                "offender_query": "BadGuy",
                "violation_type": "Zero node hit",
            },
            headers=_headers(),
        )
        assert create_response.status_code == 200

        summary_response = client.get("/api/roe/summary", headers=_headers())
        assert summary_response.status_code == 200
        summary = summary_response.json()
        assert summary["violation_count"] == 1
        assert summary["player_tallies"][0]["offense_count"] == 1

        list_response = client.get("/api/roe/violations", headers=_headers())
        assert list_response.status_code == 200
        listing = list_response.json()
        assert listing["violations"][0]["violation_type"] == "Zero node hit"
