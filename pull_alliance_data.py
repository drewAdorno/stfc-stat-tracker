"""
STFC Alliance Data Puller
Uses Playwright to scrape alliance data from v3.stfc.pro.
Launches Chrome normally (no automation flags) with remote debugging,
then connects Playwright via CDP. This avoids Cloudflare detection.
NOTE: Chrome must be fully closed before running this script.
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR / "browser_session"
DATA_DIR = BASE_DIR / "data"
ALLIANCE_URL = "https://v3.stfc.pro/alliances/3974286889"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DEBUG_PORT = 9222

LEADERBOARD_PAGES = [
    ("players_killed",    "https://v3.stfc.pro/players-killed",    "Players Killed"),
    ("resources_mined",   "https://v3.stfc.pro/resources-mined",   "Resources Mined"),
    ("hostiles_killed",   "https://v3.stfc.pro/hostiles-killed",   "Hostiles Killed"),
    ("resources_raided",  "https://v3.stfc.pro/resources-raided",  "Resources Raided"),
]


def ensure_dirs():
    SESSION_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)


def launch_chrome():
    """Launch Chrome normally with remote debugging enabled.
    Returns (process, ws_url) - the websocket URL for CDP connection."""
    stderr_log = BASE_DIR / "chrome_debug.log"
    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={DEBUG_PORT}",
        f"--user-data-dir={SESSION_DIR}",
        "--no-first-run",
        "--no-default-browser-check",
        ALLIANCE_URL,
    ]
    print("Launching Chrome (minimized, no automation flags)...")
    log_handle = open(stderr_log, "w")

    # Launch Chrome minimized so it doesn't steal focus
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 7  # SW_SHOWMINNOACTIVE

    proc = subprocess.Popen(cmd, stderr=log_handle, stdout=log_handle, startupinfo=startupinfo)

    # Wait for Chrome to write the DevTools URL to the log
    ws_url = None
    for i in range(30):
        time.sleep(1)
        try:
            content = stderr_log.read_text()
            match = re.search(r"(ws://\S+)", content)
            if match:
                ws_url = match.group(1)
                print(f"Got websocket URL: {ws_url} (after {i+1}s)")
                break
        except Exception:
            pass

    if not ws_url:
        print("ERROR: Could not get Chrome debug websocket URL")
        log_handle.close()
        proc.terminate()
        sys.exit(1)

    # Give Chrome a moment to fully initialize after the port is open
    time.sleep(3)
    return proc, ws_url


def select_combobox(page, current_text, option_text):
    """Click a combobox button and select an option from its dropdown."""
    btn = page.locator(f"button[role='combobox']:has-text('{current_text}')").first
    btn.click()
    time.sleep(1)
    page.locator(f"[role='option']:has-text('{option_text}')").first.click()
    time.sleep(2)


def scrape_leaderboard(page, url, stat_label):
    """Navigate to a leaderboard page, apply Server 716 + NCC filters,
    and return a dict mapping player name -> stat value."""
    print(f"\nScraping {stat_label} from {url}...")
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    time.sleep(5)

    # Select Server 716
    select_combobox(page, "All Servers", "716")

    # Alliance filter appears after server selection
    time.sleep(2)
    select_combobox(page, "All Alliances", "[NCC]")
    time.sleep(3)

    # Extract all rows (table: Rank, Player, Alliance, Server, Level, StatValue, empty)
    rows = page.evaluate("""() => {
        const rows = [];
        document.querySelectorAll('table tbody tr').forEach(tr => {
            const cells = [];
            let playerId = null;
            tr.querySelectorAll('td').forEach((td, idx) => {
                cells.push((td.innerText || '').trim());
                // Player name is in column 1
                if (idx === 1) {
                    const link = td.querySelector('a[href*="/players/"]');
                    if (link) {
                        const m = link.href.match(/\\/players\\/(\\d+)/);
                        if (m) playerId = m[1];
                    }
                }
            });
            if (cells.length >= 6) rows.push({cells: cells, playerId: playerId});
        });
        return rows;
    }""")

    result = {}
    for row in rows:
        # row: {cells: [rank, name, alliance, server, level, stat_value, ...], playerId}
        cells = row["cells"]
        key = row.get("playerId") or cells[1]  # prefer ID, fall back to name
        value = cells[5]
        result[key] = value

    print(f"  Got {len(result)} members for {stat_label}")
    return result


def pull_data(playwright, ws_endpoint):
    """Connect to Chrome via CDP and pull alliance data."""
    print(f"Connecting to Chrome via CDP...")
    browser = playwright.chromium.connect_over_cdp(ws_endpoint)

    context = browser.contexts[0]
    # Find the page with our URL, or use the first page
    page = None
    for p in context.pages:
        if "stfc.pro" in p.url or "discord" in p.url:
            page = p
            break
    if not page and context.pages:
        page = context.pages[0]
    if not page:
        page = context.new_page()
        page.goto(ALLIANCE_URL, wait_until="domcontentloaded", timeout=60_000)

    print(f"Current URL: {page.url}")

    # If we hit a login page, wait for user to log in
    if any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
        print()
        print("Login required! Please log in via Discord in the browser window.")
        print("Waiting for you to complete login...")
        for _ in range(120):  # wait up to ~4 minutes
            time.sleep(2)
            try:
                current_url = page.url
            except Exception:
                # Page might have navigated, re-grab it
                for p in context.pages:
                    if "stfc.pro" in p.url and "login" not in p.url.lower():
                        page = p
                        break
                current_url = page.url
            if not any(kw in current_url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                print(f"Login detected! Now at: {current_url}")
                break
        else:
            print("Timed out waiting for login.")
            browser.close()
            sys.exit(1)

        # Wait for page to fully load after login
        time.sleep(5)

    # Make sure we're on the alliance page
    if "alliances" not in page.url:
        print(f"Navigating to alliance page...")
        page.goto(ALLIANCE_URL, wait_until="domcontentloaded", timeout=60_000)
        time.sleep(5)

    title = page.title()
    print(f"Page title: {title}")

    # Extract alliance summary info
    summary = page.evaluate("""() => {
        const text = document.body.innerText || '';
        const info = {};

        // Pull key stats from the page text
        const powerMatch = text.match(/Power\\n([\\d.]+[KMB]?)/);
        const membersMatch = text.match(/Members\\n(\\d+)/);
        if (powerMatch) info.total_power = powerMatch[1];
        if (membersMatch) info.member_count = membersMatch[1];

        // Get alliance description and league info
        const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);
        for (let i = 0; i < lines.length; i++) {
            if (lines[i].includes('League')) info.league = lines[i];
            if (lines[i] === 'Total Helps') info.total_helps = lines[i+1] || '';
            if (lines[i] === 'RSS Contributions') info.total_rss = lines[i+1] || '';
            if (lines[i] === 'ISO Contributions') info.total_iso = lines[i+1] || '';
            if (lines[i] === 'Average Level') info.avg_level = lines[i+1] || '';
        }
        return info;
    }""")
    print(f"Alliance summary: {json.dumps(summary)}")

    # Extract member table from current page
    def extract_table_rows():
        return page.evaluate("""() => {
            const rows = [];
            document.querySelectorAll('table tr').forEach(tr => {
                const cells = [];
                let playerId = null;
                tr.querySelectorAll('th, td').forEach((cell, idx) => {
                    cells.push((cell.innerText || '').trim());
                    // First cell (Name) may contain a player profile link
                    if (idx === 0) {
                        const link = cell.querySelector('a[href*="/players/"]');
                        if (link) {
                            const m = link.href.match(/\\/players\\/(\\d+)/);
                            if (m) playerId = m[1];
                        }
                    }
                });
                const cleaned = cells.filter(c => c !== '');
                if (cleaned.length > 0) rows.push({cells: cleaned, playerId: playerId});
            });
            return rows;
        }""")

    # Collect all pages of members
    all_rows = extract_table_rows()
    header = all_rows[0] if all_rows else {}
    members = all_rows[1:] if len(all_rows) > 1 else []
    print(f"Page 1: got {len(members)} members")

    # Click through pagination
    page_num = 1
    while True:
        next_btn = page.query_selector('button:has-text("Next"), a:has-text("Next")')
        if not next_btn or not next_btn.is_enabled():
            break
        page_num += 1
        next_btn.click()
        time.sleep(3)  # wait for page to load

        rows = extract_table_rows()
        new_members = rows[1:] if len(rows) > 1 else []  # skip header
        print(f"Page {page_num}: got {len(new_members)} members")
        members.extend(new_members)

    print(f"Total members scraped: {len(members)}")

    # Build structured member list
    field_names = ["name", "rank", "level", "power", "helps", "rss_contrib", "iso_contrib", "join_date"]
    member_list = []
    for row in members:
        cells = row.get("cells", row) if isinstance(row, dict) else row
        member = {}
        for i, field in enumerate(field_names):
            member[field] = cells[i] if i < len(cells) else ""
        # Add player ID if available
        if isinstance(row, dict) and row.get("playerId"):
            member["id"] = row["playerId"]
        member_list.append(member)

    # Scrape leaderboard pages for additional stats
    leaderboard_data = {}
    for key, url, label in LEADERBOARD_PAGES:
        try:
            leaderboard_data[key] = scrape_leaderboard(page, url, label)
        except Exception as e:
            print(f"  WARNING: Failed to scrape {label}: {e}")
            leaderboard_data[key] = {}

    # Merge leaderboard stats into member records (match by ID, fall back to name)
    for member in member_list:
        lookup_key = member.get("id", member["name"])
        for key, _, _ in LEADERBOARD_PAGES:
            member[key] = leaderboard_data.get(key, {}).get(lookup_key, "0")

    # Save with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = DATA_DIR / f"alliance_{timestamp}.json"

    record = {
        "pulled_at": datetime.now().isoformat(),
        "alliance_url": ALLIANCE_URL,
        "summary": summary,
        "members": member_list,
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

    print(f"Data saved to {filename}")

    latest = DATA_DIR / "latest.json"
    with open(latest, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

    # Update history.json with today's snapshot
    update_history(record)

    print("Done!")
    browser.close()


TRACKED_FIELDS = [
    "level", "power", "helps", "rss_contrib", "iso_contrib",
    "players_killed", "hostiles_killed", "resources_mined", "resources_raided",
]


def update_history(record):
    """Append or update today's entry in history.json."""
    history_file = DATA_DIR / "history.json"

    # Load existing history
    history = []
    if history_file.exists():
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                history = json.load(f)
        except (json.JSONDecodeError, ValueError):
            history = []

    today = datetime.now().strftime("%Y-%m-%d")

    # Build compact member snapshot (player ID -> tracked numeric fields + name)
    members_snapshot = {}
    for m in record.get("members", []):
        key = m.get("id", m["name"])
        entry_data = {field: m.get(field, "0") for field in TRACKED_FIELDS}
        entry_data["name"] = m["name"]
        members_snapshot[key] = entry_data

    entry = {
        "date": today,
        "summary": record.get("summary", {}),
        "members": members_snapshot,
    }

    # Replace today's entry if it already exists, otherwise append
    replaced = False
    for i, e in enumerate(history):
        if e.get("date") == today:
            history[i] = entry
            replaced = True
            break
    if not replaced:
        history.append(entry)

    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)

    print(f"History updated ({len(history)} days tracked)")


def main():
    ensure_dirs()

    chrome_proc, ws_url = launch_chrome()

    try:
        with sync_playwright() as pw:
            pull_data(pw, ws_url)
    finally:
        chrome_proc.terminate()
        # Kill all remaining Chrome processes (child processes can linger)
        subprocess.run(
            ["taskkill", "/F", "/IM", "chrome.exe"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )


if __name__ == "__main__":
    main()
