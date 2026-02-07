"""
STFC Alliance Data Puller
Uses Playwright to scrape alliance data from v3.stfc.pro.
Launches Chrome normally (no automation flags) with remote debugging,
then connects Playwright via CDP. This avoids Cloudflare detection.
NOTE: Chrome must be fully closed before running this script.
"""

import json
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


def ensure_dirs():
    SESSION_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)


def launch_chrome():
    """Launch Chrome normally with remote debugging enabled.
    Returns (process, ws_url) - the websocket URL for CDP connection."""
    stderr_log = BASE_DIR / "chrome_debug.log"
    cmd = (
        f'"{CHROME_PATH}"'
        f" --remote-debugging-port={DEBUG_PORT}"
        f' --user-data-dir="{SESSION_DIR}"'
        f" --no-first-run"
        f" --no-default-browser-check"
        f" {ALLIANCE_URL}"
    )
    print("Launching Chrome (no automation flags)...")
    log_handle = open(stderr_log, "w")
    proc = subprocess.Popen(cmd, shell=True, stderr=log_handle, stdout=log_handle)

    # Wait for Chrome to write the DevTools URL to the log
    import re
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
                tr.querySelectorAll('th, td').forEach(cell => {
                    cells.push((cell.innerText || '').trim());
                });
                // Filter out empty trailing cells and header-only rows
                const cleaned = cells.filter(c => c !== '');
                if (cleaned.length > 0) rows.push(cleaned);
            });
            return rows;
        }""")

    # Collect all pages of members
    all_rows = extract_table_rows()
    header = all_rows[0] if all_rows else []
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
        member = {}
        for i, field in enumerate(field_names):
            member[field] = row[i] if i < len(row) else ""
        member_list.append(member)

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

    print("Done!")
    browser.close()


def main():
    ensure_dirs()

    chrome_proc, ws_url = launch_chrome()

    try:
        with sync_playwright() as pw:
            pull_data(pw, ws_url)
    finally:
        chrome_proc.terminate()


if __name__ == "__main__":
    main()
