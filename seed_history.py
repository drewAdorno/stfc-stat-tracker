"""One-time script to seed history.json from existing snapshot files."""
import json
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data"

TRACKED_FIELDS = [
    "level", "power", "helps", "rss_contrib", "iso_contrib",
    "players_killed", "hostiles_killed", "resources_mined", "resources_raided",
]

history = []

# Find all alliance_*.json snapshot files
snapshots = sorted(DATA_DIR.glob("alliance_*.json"))
seen_dates = set()

for snap_file in snapshots:
    with open(snap_file, "r", encoding="utf-8") as f:
        record = json.load(f)

    pulled_at = record.get("pulled_at", "")
    date = pulled_at[:10]  # YYYY-MM-DD

    # Use the latest snapshot per day (overwrite earlier ones)

    members_snapshot = {}
    for m in record.get("members", []):
        members_snapshot[m["name"]] = {
            field: m.get(field, "0") for field in TRACKED_FIELDS
        }

    entry = {
        "date": date,
        "summary": record.get("summary", {}),
        "members": members_snapshot,
    }

    # Replace if we already have this date
    replaced = False
    for i, e in enumerate(history):
        if e.get("date") == date:
            history[i] = entry
            replaced = True
            break
    if not replaced:
        history.append(entry)

history.sort(key=lambda e: e["date"])

history_file = DATA_DIR / "history.json"
with open(history_file, "w", encoding="utf-8") as f:
    json.dump(history, f, indent=2, ensure_ascii=False)

print(f"Seeded history.json with {len(history)} day(s):")
for e in history:
    print(f"  {e['date']}: {len(e['members'])} members")
