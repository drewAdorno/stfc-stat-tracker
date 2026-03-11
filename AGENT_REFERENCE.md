# STFC Stat Tracker Agent Reference

This is a compact repo-specific reference for future coding sessions in Cursor.
It complements `CLAUDE.md` and focuses on the files and behaviors most likely to matter while editing.

## What This Repo Is

Star Trek Fleet Command tracker for NCC on Server 716.

Primary flow:

`Scopely APIs -> pull_scopely.py -> SQLite in data/stfc.db -> JSON exports in data/ -> static HTML dashboards`

## Source Of Truth

- Backend state lives in `data/stfc.db`.
- Frontend pages consume generated JSON in `data/`.
- Generated data files are gitignored and should not be treated as hand-edited source.
- Current production scraper path is `pull_scopely.py`, not `pull_api.py`.

## Main Runtime Files

- `pull_scopely.py`: production 4-stage scraper.
- `db.py`: schema, migrations, query helpers, JSON exports.
- `check_auth.py`: auth preflight for `auth.json`; exit code `2` means expired auth.
- `send_hourly_alerts.py`: join/leave/level-up Discord alerts with dedupe state in `.sent_hourly_alerts`.
- `send_discord_notification.py`: daily summary webhook sender.
- `send_failure_alert.py`: failure-path Discord notifications.
- `discord_bot.py`: Discord slash-command bot backed by SQLite.

## Scraper Flow

`pull_scopely.py` does:

1. Rankings from Scopely CDN to collect server player IDs and power.
2. Authenticated profile lookups for names, levels, and alliances.
3. Authenticated alliance lookups for alliance names and tags.
4. Per-player stats fetches from the CDN, then DB upsert plus JSON export.

Important details:

- Auth file is resolved from local `auth.json` first, then `C:/Users/drewa/Desktop/stfc/stfc-api/auth.json`.
- Auth failures intentionally exit with code `2`.
- Player IDs are Scopely hex strings and are stored as `TEXT`.
- Alliance IDs are also treated as string identifiers; `db.py` contains migration helpers for old IDs.

## Database Notes

Core tables in `db.py`:

- `players`: latest known player state.
- `daily_snapshots`: per-player daily snapshot history.
- `pull_log`: pull metadata.
- `discord_links`: Discord user to player mapping.
- `daily_stat_changes`: first detection of daily stat resets/changes.
- `alliance_inventory`: alliance inventory snapshots.

Exports produced from the DB:

- `data/latest.json`
- `data/history.json`
- `data/server_alliances.json`
- `data/server_players.json`
- `data/server_history.json`
- `data/alliance_inventory.json`

## Frontend Pages

These are standalone static HTML files with large inline CSS and JavaScript, not a componentized frontend app.

- `index.html`: public NCC tracker.
- `calendar.html`: game events/news calendar.
- `server.html`: password-gated server analytics.
- `leaderboard.html`: password-gated server leaderboard.
- `player.html`: player detail page with fallback to server-wide history.
- `admin.html`: password-gated admin page.

Editing implication:

- Shared UI logic is often duplicated across pages.
- Small UX changes may require touching multiple HTML files.

## Deploy And Ops

- Production hourly job: `deploy/run_pull.sh`
- EC2 path: `/opt/stfc`
- CI workflow: `.github/workflows/deploy.yml`

`deploy/run_pull.sh` behavior:

- Runs `check_auth.py`
- Waits and retries if auth is expired
- Runs `pull_scopely.py`
- Sends hourly alerts and daily notifications afterward

## Legacy / Easy-To-Misread Files

- `pull_api.py` is legacy `stfc.pro` scraping code and is no longer the main production path.
- `run_daily.bat` still points at legacy flow, so local and production entrypoints are not identical.
- `deploy/watch_auth.py` depends on files produced outside this repo, especially from `stfc-api`.

## Tests

Main automated tests are top-level `pytest` files:

- `test_discord_notification.py`
- `test_hourly_alerts.py`
- `test_discord_bot.py`

`test_curl_cffi.py` is closer to a manual/integration script than a normal fast unit test.

Run tests with:

```bash
pytest --tb=short -q
```

## Gotchas

- Do not commit generated JSON or SQLite files.
- Console Unicode can be unreliable on Windows cp1252; some scripts use `safe_print()`.
- Frontend pages are static and inline-heavy, so there is very little shared abstraction.
- Scraper and dashboard changes are more regression-prone than the current test coverage suggests.
- Password-gated pages use client-side auth with `sessionStorage` key `ncc_admin_auth`.

## Good Starting Points By Task

- Scraper or pipeline issue: `pull_scopely.py`, then `db.py`
- JSON shape / site data issue: `db.py` export functions plus consuming HTML page
- Discord alerts: `send_hourly_alerts.py` or `send_discord_notification.py`
- Bot command or player lookup issue: `discord_bot.py` plus DB helpers in `db.py`
- Production behavior mismatch: `deploy/run_pull.sh` and `.github/workflows/deploy.yml`
