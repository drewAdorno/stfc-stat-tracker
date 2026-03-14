"""Shared ROE workflows for CLI, API, and future Discord ingestion."""

from __future__ import annotations

from typing import Dict, List

from db import export_roe_violations_json, record_roe_violation


def fetch_player_candidates(conn, query: str, limit: int = 8) -> List[Dict[str, str]]:
    """Return likely player matches with latest alliance info."""
    normalized = str(query or "").strip()
    if not normalized:
        return []

    base_sql = """
        SELECT p.player_id,
               p.name,
               COALESCE(ds.alliance_id, p.alliance_id, '') AS alliance_id,
               COALESCE(ds.alliance_tag, p.alliance_tag, '') AS alliance_tag,
               COALESCE(ds.alliance_name, '') AS alliance_name
        FROM players p
        LEFT JOIN daily_snapshots ds
            ON ds.player_id = p.player_id
           AND ds.date = (
               SELECT MAX(date) FROM daily_snapshots WHERE player_id = p.player_id
           )
        WHERE {where_clause}
        ORDER BY p.name COLLATE NOCASE
        LIMIT ?
    """

    checks = [
        ("p.player_id = ?", normalized),
        ("p.name = ? COLLATE NOCASE", normalized),
        ("p.name LIKE ? COLLATE NOCASE", normalized + "%"),
        ("p.name LIKE ? COLLATE NOCASE", "%" + normalized + "%"),
    ]

    for where_clause, value in checks:
        rows = conn.execute(base_sql.format(where_clause=where_clause), (value, limit)).fetchall()
        if rows:
            return [
                {
                    "player_id": str(row[0] or ""),
                    "name": row[1] or "",
                    "alliance_id": row[2] or "",
                    "alliance_tag": row[3] or "",
                    "alliance_name": row[4] or "",
                }
                for row in rows
            ]
    return []


def resolve_player(conn, query: str) -> Dict[str, str]:
    """Resolve a player query to a single player identity."""
    matches = fetch_player_candidates(conn, query)
    if not matches:
        return {}
    if len(matches) == 1:
        return matches[0]

    exact_name = [m for m in matches if m["name"].lower() == query.strip().lower()]
    if len(exact_name) == 1:
        return exact_name[0]

    labels = []
    for match in matches[:5]:
        tag = f" [{match['alliance_tag']}]" if match["alliance_tag"] else ""
        labels.append(f"{match['name']}{tag}")
    raise ValueError(
        f"Ambiguous player '{query}'. Matches: {', '.join(labels)}. "
        "Use the exact name or player id."
    )


def merge_identity(identity: Dict[str, str], *, fallback_name: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
    """Apply manual overrides to a resolved player identity."""
    overrides = overrides or {}
    merged = dict(identity or {})
    merged["name"] = overrides.get("name") or merged.get("name") or fallback_name
    merged["alliance_id"] = overrides.get("alliance_id") or merged.get("alliance_id", "")
    merged["alliance_tag"] = overrides.get("alliance_tag") or merged.get("alliance_tag", "")
    merged["alliance_name"] = overrides.get("alliance_name") or merged.get("alliance_name", "")
    return merged


def create_violation(
    conn,
    *,
    offender_query: str,
    violation_type: str,
    reported_by: str = "",
    victim_name: str = "",
    victim_player_id: str = "",
    system_name: str = "",
    notes: str = "",
    offense_date: str = "",
    source: str = "manual",
    source_ref: str = "",
    offender_overrides: Dict[str, str] | None = None,
):
    """Create a violation, refresh the export, and return the result payload."""
    identity = resolve_player(conn, offender_query)
    identity = merge_identity(identity, fallback_name=offender_query, overrides=offender_overrides)

    violation_id = record_roe_violation(
        conn,
        offender_name=identity["name"],
        offender_player_id=identity.get("player_id", ""),
        offender_alliance_id=identity.get("alliance_id", ""),
        offender_alliance_tag=identity.get("alliance_tag", ""),
        offender_alliance_name=identity.get("alliance_name", ""),
        victim_player_id=victim_player_id,
        victim_name=victim_name,
        system_name=system_name,
        reported_by=reported_by,
        violation_type=violation_type,
        notes=notes,
        offense_date=offense_date,
        source=source,
        source_ref=source_ref,
    )
    payload = export_roe_violations_json(conn)

    return {
        "violation_id": violation_id,
        "identity": identity,
        "payload": payload,
    }


def list_violations(conn, limit: int = 50):
    """Return recent ROE violations in a compact shape."""
    rows = conn.execute("""
        SELECT id, reported_at, offense_date, offender_player_id, offender_name,
               offender_alliance_id, offender_alliance_tag, offender_alliance_name,
               victim_player_id, victim_name, violation_type, system_name, notes, reported_by
        FROM roe_violations
        ORDER BY offense_date DESC, reported_at DESC, id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [
        {
            "id": row[0],
            "reported_at": row[1] or "",
            "offense_date": row[2] or "",
            "offender_player_id": row[3] or "",
            "offender_name": row[4] or "",
            "offender_alliance_id": row[5] or "",
            "offender_alliance_tag": row[6] or "",
            "offender_alliance_name": row[7] or "",
            "victim_player_id": row[8] or "",
            "victim_name": row[9] or "",
            "violation_type": row[10] or "",
            "system_name": row[11] or "",
            "notes": row[12] or "",
            "reported_by": row[13] or "",
        }
        for row in rows
    ]


def get_summary(conn):
    """Return the current ROE export payload."""
    return export_roe_violations_json(conn)
