"""
STFC API-Based Data Puller
Pulls all server 716 player data via the v3.stfc.pro REST API.
Windows: Uses curl_cffi with Chrome TLS impersonation to bypass Cloudflare.
Linux/EC2: Uses Playwright browser to bypass Cloudflare (datacenter IPs get challenged).
Auto-refreshes cookies via Chrome CDP when they expire.
"""

import gzip
import json
import platform
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from curl_cffi import requests as cffi_requests
from playwright.sync_api import sync_playwright

from db import get_db, upsert_players, log_pull, export_latest_json, export_history_json

IS_WINDOWS = platform.system() == "Windows"

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SESSION_DIR = BASE_DIR / "browser_session"
COOKIE_FILE = DATA_DIR / "session_cookies.json"
API_BASE = "https://v3.stfc.pro/api/players"
NCC_ALLIANCE_ID = 3974286889
SERVER = 716
PAGE_SIZE = 100
IMPERSONATE = "chrome131"
DEBUG_PORT = 9222
ALLIANCE_URL = "https://v3.stfc.pro/alliances/3974286889"

if IS_WINDOWS:
    CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
else:
    # Prefer system chromium, fall back to google-chrome
    CHROME_PATH = (
        shutil.which("chromium-browser")
        or shutil.which("chromium")
        or shutil.which("google-chrome")
        or "/usr/bin/chromium-browser"
    )


def safe_print(msg):
    """Print with fallback for Unicode chars on cp1252 consoles."""
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", errors="replace").decode("ascii"))


# ---------------------------------------------------------------------------
# Cookie management
# ---------------------------------------------------------------------------

def cookies_expired():
    """Check if saved cookies exist and are still valid (not expired)."""
    if not COOKIE_FILE.exists():
        return True

    try:
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            cookies = json.load(f)
    except (json.JSONDecodeError, ValueError):
        return True

    now = time.time()
    for c in cookies:
        if c["name"] == "__Secure-better-auth.session_token":
            expires = c.get("expires", 0)
            if expires and expires < now:
                safe_print(f"Session token expired at {datetime.fromtimestamp(expires)}")
                return True
            return False

    # Missing session token entirely
    return True


def load_cookies():
    """Load session cookies from file as a dict."""
    with open(COOKIE_FILE, "r", encoding="utf-8") as f:
        cookies = json.load(f)
    return {c["name"]: c["value"] for c in cookies}


def _kill_chrome():
    """Kill lingering Chrome/Chromium processes (cross-platform)."""
    if IS_WINDOWS:
        subprocess.run(
            ["taskkill", "/F", "/IM", "chrome.exe"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        subprocess.run(
            ["pkill", "-f", "chromium|chrome"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )


def _refresh_cookies_linux():
    """Linux: use Playwright's bundled Chromium with persistent context directly."""
    safe_print("Refreshing cookies via Playwright persistent context...")
    SESSION_DIR.mkdir(exist_ok=True)

    cookies = []
    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=False,
            args=["--no-sandbox", "--disable-gpu"],
        )

        page = context.pages[0] if context.pages else context.new_page()
        page.goto(ALLIANCE_URL, wait_until="domcontentloaded", timeout=60_000)
        safe_print(f"Current URL: {page.url}")

        # Handle login redirect if needed
        if any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
            safe_print("Login page detected - waiting for auto-redirect...")
            for _ in range(60):
                time.sleep(2)
                if not any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                    safe_print(f"Redirected to: {page.url}")
                    break
            else:
                safe_print("ERROR: Stuck on login page. Manual login may be required.")
                context.close()
                return []

            time.sleep(5)

        cookies = context.cookies("https://v3.stfc.pro")
        context.close()

    return cookies


def _refresh_cookies_windows():
    """Windows: launch system Chrome via CDP, connect with Playwright."""
    _kill_chrome()
    time.sleep(2)

    stderr_log = BASE_DIR / "chrome_debug.log"
    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={DEBUG_PORT}",
        f"--user-data-dir={SESSION_DIR}",
        "--no-first-run",
        "--no-default-browser-check",
        ALLIANCE_URL,
    ]

    log_handle = open(stderr_log, "w")
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 7  # SW_SHOWMINNOACTIVE

    chrome_proc = subprocess.Popen(cmd, stderr=log_handle, stdout=log_handle, startupinfo=startupinfo)

    # Wait for Chrome to expose the debug websocket
    ws_url = None
    for i in range(30):
        time.sleep(1)
        try:
            content = stderr_log.read_text()
            match = re.search(r"(ws://\S+)", content)
            if match:
                ws_url = match.group(1)
                safe_print(f"Chrome ready (after {i+1}s)")
                break
        except Exception:
            pass

    if not ws_url:
        safe_print("ERROR: Could not get Chrome debug websocket URL")
        log_handle.close()
        chrome_proc.terminate()
        return []

    time.sleep(3)

    cookies = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(ws_url)
            context = browser.contexts[0]

            page = None
            for p in context.pages:
                if "stfc.pro" in p.url or "discord" in p.url:
                    page = p
                    break
            if not page and context.pages:
                page = context.pages[0]

            safe_print(f"Current URL: {page.url}")

            # Handle login if somehow needed (Discord session should persist)
            if any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                safe_print("Login page detected - waiting for auto-redirect...")
                for _ in range(60):
                    time.sleep(2)
                    try:
                        current_url = page.url
                    except Exception:
                        for p in context.pages:
                            if "stfc.pro" in p.url and "login" not in p.url.lower():
                                page = p
                                break
                        current_url = page.url
                    if not any(kw in current_url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                        safe_print(f"Redirected to: {current_url}")
                        break
                else:
                    safe_print("ERROR: Stuck on login page. Manual login may be required.")
                    browser.close()
                    return []

                time.sleep(5)

            # Make sure we're on stfc.pro (not a blank page)
            if "stfc.pro" not in page.url:
                page.goto(ALLIANCE_URL, wait_until="domcontentloaded", timeout=60_000)
                time.sleep(5)

            cookies = context.cookies("https://v3.stfc.pro")
            browser.close()

    finally:
        chrome_proc.terminate()
        _kill_chrome()

    return cookies


def refresh_cookies():
    """Refresh stfc.pro session cookies via browser automation."""
    safe_print("Refreshing cookies via Chrome CDP...")
    SESSION_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)

    if IS_WINDOWS:
        cookies = _refresh_cookies_windows()
    else:
        cookies = _refresh_cookies_linux()

    if not cookies:
        safe_print("ERROR: No cookies extracted from Chrome")
        return False

    # Check we got the required cookies
    cookie_names = {c["name"] for c in cookies}
    if "__Secure-better-auth.session_token" not in cookie_names:
        safe_print("ERROR: Session token not found in extracted cookies")
        return False

    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies, f, indent=2)

    safe_print(f"Cookies refreshed ({len(cookies)} cookies saved)")
    return True


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def api_get(cookie_dict, page):
    """Fetch a single page of players. Returns (data_dict, None) on success
    or (None, status_code) on auth/CF failure."""
    url = (
        f"{API_BASE}?server={SERVER}"
        f"&sortBy=power&sortOrder=desc"
        f"&page={page}&pageCount={PAGE_SIZE}"
    )
    resp = cffi_requests.get(url, cookies=cookie_dict, impersonate=IMPERSONATE)

    if resp.status_code in (401, 403):
        return None, resp.status_code
    if resp.status_code != 200:
        safe_print(f"ERROR: API returned status {resp.status_code}")
        safe_print(f"Response: {resp.text[:500]}")
        sys.exit(1)

    body = resp.content
    if body[:2] == b'\x1f\x8b':
        body = gzip.decompress(body)

    return json.loads(body.decode("utf-8")), None


def fetch_all_players(cookie_dict):
    """Paginate through all server 716 players."""
    page = 1
    all_players = []
    total_count = None

    while True:
        safe_print(f"Fetching page {page}...")
        data, err = api_get(cookie_dict, page)

        if err is not None:
            return None, err  # signal caller to refresh cookies

        if total_count is None:
            total_count = data.get("count", 0)
            total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
            safe_print(f"Total players on server {SERVER}: {total_count} ({total_pages} pages)")

        players = data.get("players", [])
        if not players:
            break

        all_players.extend(players)
        safe_print(f"  Page {page}: got {len(players)} players (total so far: {len(all_players)})")

        if len(all_players) >= total_count:
            break

        page += 1
        time.sleep(2)  # rate limit: 60 req / 120s

    safe_print(f"Fetched {len(all_players)} players total")
    return all_players, total_count


# ---------------------------------------------------------------------------
# Playwright-based API fetch (for Linux/EC2 where Cloudflare blocks curl_cffi)
# ---------------------------------------------------------------------------

def _load_cookie_list():
    """Load session cookies as a list of Playwright-format dicts."""
    with open(COOKIE_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    pw_cookies = []
    for c in raw:
        entry = {
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ".stfc.pro"),
            "path": c.get("path", "/"),
        }
        if c.get("expires") and c["expires"] > 0:
            entry["expires"] = c["expires"]
        if c.get("secure"):
            entry["secure"] = True
        if c.get("sameSite"):
            val = c["sameSite"].capitalize()
            if val in ("Strict", "Lax", "None"):
                entry["sameSite"] = val
        pw_cookies.append(entry)
    return pw_cookies


def fetch_all_players_browser():
    """Use Playwright browser to fetch API pages (bypasses Cloudflare)."""
    safe_print("Using Playwright browser for API calls (Cloudflare bypass)...")
    SESSION_DIR.mkdir(exist_ok=True)

    all_players = []
    total_count = None

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=False,
            args=["--no-sandbox", "--disable-gpu"],
        )

        # Inject session cookies
        try:
            pw_cookies = _load_cookie_list()
            context.add_cookies(pw_cookies)
            safe_print(f"Injected {len(pw_cookies)} cookies into browser")
        except Exception as e:
            safe_print(f"WARNING: Could not inject cookies: {e}")

        page = context.pages[0] if context.pages else context.new_page()

        # Visit the site first to establish Cloudflare clearance
        safe_print("Establishing Cloudflare clearance...")
        page.goto(ALLIANCE_URL, wait_until="domcontentloaded", timeout=60_000)
        time.sleep(5)
        safe_print(f"Landed on: {page.url}")
        safe_print(f"Page title: {page.title()}")

        # Check if we hit login page
        if "login" in page.url.lower():
            safe_print("ERROR: Redirected to login page. Session cookies may be expired.")
            context.close()
            return None, 403

        page_num = 1
        while True:
            url = (
                f"{API_BASE}?server={SERVER}"
                f"&sortBy=power&sortOrder=desc"
                f"&page={page_num}&pageCount={PAGE_SIZE}"
            )
            safe_print(f"Fetching page {page_num} via browser...")

            # Use fetch() inside the browser to avoid download triggers
            result = page.evaluate("""async (url) => {
                const resp = await fetch(url);
                return { status: resp.status, body: await resp.text() };
            }""", url)

            status = result["status"]
            body = result["body"]
            safe_print(f"  Status: {status}, Body length: {len(body)}")

            if status in (401, 403):
                safe_print(f"Browser API request returned {status}")
                safe_print(f"Body preview: {body[:300]}")
                context.close()
                return None, status

            if status != 200:
                safe_print(f"ERROR: Browser API returned status {status}")
                safe_print(f"Body preview: {body[:300]}")
                context.close()
                sys.exit(1)

            if not body or body[0] != '{':
                safe_print(f"ERROR: Response is not JSON. Preview: {body[:300]}")
                context.close()
                return None, 403

            data = json.loads(body)

            if total_count is None:
                total_count = data.get("count", 0)
                total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
                safe_print(f"Total players on server {SERVER}: {total_count} ({total_pages} pages)")

            players = data.get("players", [])
            if not players:
                break

            all_players.extend(players)
            safe_print(f"  Page {page_num}: got {len(players)} players (total so far: {len(all_players)})")

            if len(all_players) >= total_count:
                break

            page_num += 1
            time.sleep(2)

        # Save any new cookies (including cf_clearance) back to file
        fresh_cookies = context.cookies("https://v3.stfc.pro")
        if fresh_cookies:
            with open(COOKIE_FILE, "w", encoding="utf-8") as f:
                json.dump(fresh_cookies, f, indent=2)
            safe_print(f"Updated cookie file ({len(fresh_cookies)} cookies)")

        context.close()

    safe_print(f"Fetched {len(all_players)} players total")
    return all_players, total_count


# ---------------------------------------------------------------------------
# Data mapping & output
# ---------------------------------------------------------------------------

def map_player(raw):
    """Map API player data to our existing field format."""
    d = raw["data"]
    return {
        "name": d.get("owner", ""),
        "rank": d.get("rankdesc", ""),
        "level": d.get("level", 0),
        "power": d.get("power", 0),
        "helps": d.get("ahelps", 0),
        "rss_contrib": d.get("acontrib", 0),
        "iso_contrib": d.get("aisocontrib", 0),
        "join_date": d.get("ajoined", ""),
        "id": str(d.get("playerid", "")),
        "players_killed": d.get("pdestroyed", 0),
        "hostiles_killed": d.get("hdestroyed", 0),
        "resources_mined": d.get("rssmined", 0),
        "resources_raided": d.get("rss", 0),
        "alliance_tag": d.get("tag", ""),
        "alliance_name": d.get("name", ""),
        "alliance_id": d.get("allianceid", 0),
    }


def save_data(all_players, total_count):
    """Map all players, store in SQLite, and export JSON files for dashboards."""
    all_mapped = [map_player(p) for p in all_players]

    ncc_members = [m for m in all_mapped if m["alliance_id"] == NCC_ALLIANCE_ID]
    safe_print(f"NCC members found: {len(ncc_members)}")

    if len(ncc_members) == 0:
        safe_print("WARNING: No NCC members found in data!")

    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_db()

    # Store all server players in the DB (NCC members get tagged with alliance_id)
    upsert_players(conn, all_mapped, today)
    safe_print(f"Database updated ({len(all_mapped)} players upserted)")

    # Log this pull
    log_pull(conn, SERVER, total_count)

    # Export JSON files for dashboards
    export_latest_json(conn, NCC_ALLIANCE_ID)
    safe_print(f"Exported {DATA_DIR / 'latest.json'}")

    export_history_json(conn, NCC_ALLIANCE_ID)
    safe_print(f"Exported {DATA_DIR / 'history.json'}")

    conn.close()


def main():
    DATA_DIR.mkdir(exist_ok=True)
    SESSION_DIR.mkdir(exist_ok=True)

    if not IS_WINDOWS:
        # Linux/EC2: use Playwright browser to bypass Cloudflare
        if cookies_expired():
            safe_print("FATAL: Cookies expired. Copy fresh session_cookies.json from Windows.")
            sys.exit(1)

        all_players, result = fetch_all_players_browser()

        if all_players is None:
            safe_print(f"FATAL: Browser API returned {result}.")
            safe_print("Session cookies may be expired - copy fresh ones from Windows.")
            sys.exit(1)
    else:
        # Windows: use curl_cffi (residential IP passes Cloudflare)
        if cookies_expired():
            safe_print("Cookies expired or missing - refreshing automatically...")
            if not refresh_cookies():
                safe_print("FATAL: Could not refresh cookies.")
                sys.exit(1)

        cookie_dict = load_cookies()
        all_players, result = fetch_all_players(cookie_dict)

        if all_players is None:
            safe_print(f"API returned {result} - refreshing cookies and retrying...")
            if not refresh_cookies():
                safe_print("FATAL: Could not refresh cookies.")
                sys.exit(1)

            cookie_dict = load_cookies()
            all_players, result = fetch_all_players(cookie_dict)

            if all_players is None:
                safe_print(f"FATAL: API still returning {result} after cookie refresh.")
                safe_print("Manual intervention required - run extract_cookies.py.")
                sys.exit(1)

    if len(all_players) < 10:
        safe_print(f"ERROR: Only got {len(all_players)} players - something went wrong. Skipping save.")
        sys.exit(1)

    save_data(all_players, result)
    safe_print("Done!")


if __name__ == "__main__":
    main()
