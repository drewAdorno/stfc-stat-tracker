"""Regression tests for Scopely scraper config wiring."""

import sqlite3

from db import SCHEMA, clear_bad_rss_contrib_snapshots
import pull_scopely


def test_contribution_sources_skip_known_raided_rss_config():
    sources = pull_scopely.contribution_sources()

    assert ("4d21cabdec534dbf5896b0441d774dd7c0f1252d", "ISO contrib", "iso_contrib") in sources
    assert all(field_name != "rss_contrib" for _, _, field_name in sources)


def test_clear_bad_rss_contrib_snapshots_only_touches_known_bad_dates():
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA)
    conn.execute(
        """
        INSERT INTO daily_snapshots (
            player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
            players_killed, hostiles_killed, resources_mined, resources_raided,
            alliance_id, alliance_tag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("p1", "2026-03-06", "BadRow", 1, 1, 1, 500, 0, 0, 0, 0, 500, "a1", "TAG"),
    )
    conn.execute(
        """
        INSERT INTO daily_snapshots (
            player_id, date, name, level, power, helps, rss_contrib, iso_contrib,
            players_killed, hostiles_killed, resources_mined, resources_raided,
            alliance_id, alliance_tag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("p2", "2026-03-07", "GoodRow", 1, 1, 1, 500, 0, 0, 0, 0, 500, "a1", "TAG"),
    )

    cleared = clear_bad_rss_contrib_snapshots(conn)

    assert cleared == 1
    assert conn.execute(
        "SELECT rss_contrib FROM daily_snapshots WHERE player_id = 'p1'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT rss_contrib FROM daily_snapshots WHERE player_id = 'p2'"
    ).fetchone()[0] == 500
