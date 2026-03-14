"""
Manual ROE violation management for STFC Stat Tracker.

Examples:
    python manage_roe.py add "BadGuy" --type "OPC hit" --reported-by "Drew"
    python manage_roe.py list --limit 10
    python manage_roe.py summary
"""

import argparse

from db import export_roe_violations_json, get_db
from roe_service import create_violation, get_summary, list_violations


def _add_violation(args) -> int:
    conn = get_db()
    try:
        result = create_violation(
            conn,
            offender_query=args.offender,
            violation_type=args.violation_type,
            reported_by=args.reported_by,
            victim_name=args.victim,
            system_name=args.system,
            notes=args.notes,
            offense_date=args.offense_date,
            source="manual",
            offender_overrides={
                "name": args.offender_name_override,
                "alliance_id": args.alliance_id,
                "alliance_tag": args.alliance_tag,
                "alliance_name": args.alliance_name,
            },
        )
    finally:
        conn.close()

    identity = result["identity"]
    violation_id = result["violation_id"]
    payload = result["payload"]
    alliance_bits = []
    if identity.get("alliance_tag"):
        alliance_bits.append(identity["alliance_tag"])
    if identity.get("alliance_name"):
        alliance_bits.append(identity["alliance_name"])
    alliance_label = " / ".join(alliance_bits) if alliance_bits else "unknown alliance"

    print(f"Recorded ROE violation #{violation_id} for {identity['name']} ({alliance_label}).")
    print(f"Tracker export refreshed with {payload['violation_count']} total logged violations.")
    return 0


def _list_violations(args) -> int:
    conn = get_db()
    try:
        rows = list_violations(conn, args.limit)
    finally:
        conn.close()

    if not rows:
        print("No ROE violations logged yet.")
        return 0

    for row in rows:
        tag = f" [{row['offender_alliance_tag']}]" if row["offender_alliance_tag"] else ""
        victim = f" -> {row['victim_name']}" if row["victim_name"] else ""
        system = f" @ {row['system_name']}" if row["system_name"] else ""
        reporter = f" | reported by {row['reported_by']}" if row["reported_by"] else ""
        print(
            f"#{row['id']} | {row['offense_date']} | {row['offender_name']}{tag} | "
            f"{row['violation_type']}{victim}{system}{reporter}"
        )
    return 0


def _summary(args) -> int:
    conn = get_db()
    try:
        payload = get_summary(conn)
    finally:
        conn.close()

    print(
        f"ROE violations: {payload['violation_count']} total | "
        f"{payload['unique_offender_count']} offenders | "
        f"{payload['alliance_count']} alliances"
    )

    top_players = payload["player_tallies"][: args.limit]
    if top_players:
        print("\nTop offenders:")
        for entry in top_players:
            tag = f" [{entry['offender_alliance_tag']}]" if entry["offender_alliance_tag"] else ""
            print(f"- {entry['offender_name']}{tag}: {entry['offense_count']}")

    top_alliances = payload["alliance_tallies"][: args.limit]
    if top_alliances:
        print("\nTop offending alliances:")
        for entry in top_alliances:
            label = entry["offender_alliance_tag"] or entry["offender_alliance_name"] or "Unknown"
            print(
                f"- {label}: {entry['offense_count']} offenses | "
                f"{entry['unique_offender_count']} offenders"
            )

    return 0


def _export_only(_args) -> int:
    conn = get_db()
    try:
        payload = export_roe_violations_json(conn)
    finally:
        conn.close()

    print(f"Exported data/roe_violations.json with {payload['violation_count']} logged violations.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage ROE violations for the tracker.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Log a new ROE violation")
    add_parser.add_argument("offender", help="Player id or player name")
    add_parser.add_argument("--type", dest="violation_type", required=True, help="Violation type")
    add_parser.add_argument("--reported-by", default="", help="Alliance member reporting it")
    add_parser.add_argument("--victim", default="", help="Alliance member who was hit")
    add_parser.add_argument("--system", default="", help="System where it happened")
    add_parser.add_argument("--notes", default="", help="Extra details")
    add_parser.add_argument("--offense-date", default="", help="Date of offense (YYYY-MM-DD)")
    add_parser.add_argument("--alliance-id", default="", help="Alliance id override")
    add_parser.add_argument("--alliance-tag", default="", help="Alliance tag override")
    add_parser.add_argument("--alliance-name", default="", help="Alliance name override")
    add_parser.add_argument("--offender-name", dest="offender_name_override", default="", help="Name override")
    add_parser.set_defaults(func=_add_violation)

    list_parser = subparsers.add_parser("list", help="List recent ROE violations")
    list_parser.add_argument("--limit", type=int, default=20, help="Max rows to show")
    list_parser.set_defaults(func=_list_violations)

    summary_parser = subparsers.add_parser("summary", help="Show ROE tallies")
    summary_parser.add_argument("--limit", type=int, default=10, help="Max rows per section")
    summary_parser.set_defaults(func=_summary)

    export_parser = subparsers.add_parser("export", help="Refresh roe_violations.json")
    export_parser.set_defaults(func=_export_only)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
