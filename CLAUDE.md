# STFC Stat Tracker

## Project Overview
Star Trek Fleet Command alliance tracker for NWS / No Win Scenario (alliance ID `2656439294172226322`) on Server 724. Hourly data pulls from Scopely game APIs (public CDN + authenticated game server), stored in SQLite, served as static HTML dashboards on nws.stfcdrew.lol.

## Tech Stack
- **Python 3.11** (Windows) / **3.12** (CI/EC2)
- **SQLite** database at `data/stfc.db`
- **requests** for Scopely API calls
- **GitHub Actions** CI + deploy pipeline
- **Nginx + Let's Encrypt** on EC2

## Architecture

### Data Flow
`Scopely APIs` → `pull_scopely.py` → `data/stfc.db` → JSON exports → static HTML dashboards

### Scopely API Pipeline (4 stages)
1. **Rankings (NO AUTH)** — Paginate `GET /content/v1/products/prime/event/rankings/724/{config}` on CDN. Returns all ~3600 player hex IDs + power scores. Page size 500.
2. **Profiles (AUTH)** — `POST /user_profile/profiles` with batches of 200 hex IDs. Returns name, level, alliance_id, military_might.
3. **Alliances (AUTH)** — `POST /alliance/get_alliances_public_info` with unique alliance_ids. Returns alliance name, tag, level.
4. **Player Stats (NO AUTH)** — `GET /content/v1/products/prime/player-stats/724/{hex_id}` per player. Returns protobuf with 12 stats. 20 parallel workers.

### Auth
- Game mod dumps auth tokens to `auth.json` on each game launch
- EC2 uses `auth.json` in the project dir (`/opt/stfc/auth.json`)
- Required headers: `X-AUTH-SESSION-ID`, `X-TRANSACTION-ID`, `X-PRIME-VERSION`, `X-Instance-ID`
- On auth expiry (401/403), scraper exits with code 2 and sends Discord alert

### Key Files
| File | Purpose |
|---|---|
| `pull_scopely.py` | Main scraper — 4-stage Scopely API pipeline for all server 724 players |
| `db.py` | SQLite module: schema, upsert, JSON exports. Tables: `players`, `daily_snapshots`, `pull_log`. Player IDs are TEXT (hex strings). |
| `index.html` | Main NWS alliance dashboard (public) |
| `server.html` | Server 724 analytics (password-gated, 5 tabs) |
| `player.html` | Individual player detail page with name history (works for all server players, falls back to server data for non-NWS) |
| `admin.html` | Admin page (password-gated) |
| `calendar.html` | Calendar view |
| `send_discord_notification.py` | Daily Discord webhook summary |
| `send_hourly_alerts.py` | Hourly Discord alerts: joins, leaves, level-ups (with dedup via `.sent_hourly_alerts`) |
| `send_failure_alert.py` | Discord alert when scraper fails |
| `run_daily.bat` | Windows Task Scheduler script |
| `deploy/run_pull.sh` | EC2 systemd timer script (hourly at :01), runs `pull_scopely.py` |
| `deploy/stfc-nginx.conf` | Nginx config (clean URLs via `try_files $uri $uri.html`) |

### Generated Data Files (gitignored, regenerated from DB each pull)
- `data/latest.json` — current NWS member snapshot
- `data/history.json` — daily time-series with per-snapshot player names
- `data/server_alliances.json` — all server alliances with 7d deltas
- `data/server_players.json` — all ~1963 players with alliance movement tracking
- `data/server_history.json` — daily time-series for ALL server players (used by player.html for non-NWS players)

## Deployment

### EC2 (Production)
- **Instance**: Ubuntu 24.04, t2.micro, us-east-2
- **IP**: 3.16.255.133
- **SSH**: `ssh -i ~/Downloads/STFC_pem.pem ubuntu@3.16.255.133`
- **App dir**: `/opt/stfc` (venv at `/opt/stfc/venv`)
- **Domain**: nws.stfcdrew.lol (SSL via certbot)
- **Scraping**: systemd timer runs `deploy/run_pull.sh` hourly

### CI/CD Pipeline
Push to `master` → GitHub Actions runs `pytest` → deploys via SSH (`git pull`, pip install, nginx reload). Data JSON files are gitignored so deploys never conflict with scraper output.

### Critical Deploy Lessons
- **Never track generated data files in git.** This caused repeated deploy failures (merge conflicts, deleted files, broken JSON from conflict markers).
- After deploy, if data files are missing, regenerate from DB:
  ```bash
  cd /opt/stfc && venv/bin/python -c "
  from db import get_db, export_latest_json, export_history_json, export_server_alliances_json, export_server_players_json, export_server_history_json
  conn = get_db(); export_latest_json(conn); export_history_json(conn); export_server_alliances_json(conn); export_server_players_json(conn); export_server_history_json(conn); conn.close()"
  ```

## Testing
```bash
pytest --tb=short -q
```
Tests are in `test_discord_notification.py`, `test_hourly_alerts.py`. CI runs tests before every deploy.

## Password Gate
`server.html` and `admin.html` use a client-side password gate. Password: `salsa`. Stored in `sessionStorage` key `ncc_admin_auth`.

## Database Schema
- `players` — current state per player (name, alliance, first/last seen). `player_id` is TEXT (hex string from Scopely, e.g. `j3aff21af9b04f28b54fc78cc7a8f5db`)
- `daily_snapshots` — one row per player per date with stats + `name` column (preserves name at snapshot time for name history tracking)
- `pull_log` — scrape timestamps and player counts
- `discord_links` — maps player_id to Discord user ID

### ID Migration (completed Feb 2025)
- Player IDs migrated from stfc.pro numeric integers to Scopely hex strings (matched by name, 88% hit rate)
- Alliance IDs migrated from stfc.pro integers to Scopely 19-digit integers (matched by tag, 162/184 remapped)
- Schema columns changed from INTEGER to TEXT. Migration functions in `db.py` auto-detect and convert on first run.

## Discord Integration
- Webhook URL stored in `.env` as `DISCORD_WEBHOOK_URL`
- Daily summary: `send_discord_notification.py` (fires once per day, tracked via `data/.last_notification_date`)
- Hourly alerts: `send_hourly_alerts.py` (joins/leaves/level-ups, deduped via `.sent_hourly_alerts` JSON keyed by date pair)
- Failure alert: `send_failure_alert.py` (rate-limited via `.last_failure_alert`)

## Windows/MSYS Gotchas
- `taskkill /F /IM` needs double-slash flags in MSYS: `//F //IM`
- Python `print()` with Unicode fails on cp1252 console — use `safe_print()` wrapper from `send_discord_notification.py`
- SSH stdout works fine in MSYS now — capture output directly, do NOT redirect to temp files

