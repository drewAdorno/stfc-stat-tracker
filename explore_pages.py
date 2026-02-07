"""
Quick script to explore the data tables on stfc.pro pages.
Launches Chrome via CDP, selects Server 716 + Alliance NCC filters,
then dumps table headers + sample rows from each page.
NOTE: Chrome must be fully closed before running this script.
"""

import json
import re
import subprocess
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR / "browser_session"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DEBUG_PORT = 9222

PAGES = [
    ("Players Killed", "https://v3.stfc.pro/players-killed"),
    ("Resources Mined", "https://v3.stfc.pro/resources-mined"),
    ("Hostiles Killed", "https://v3.stfc.pro/hostiles-killed"),
    ("Resources Raided", "https://v3.stfc.pro/resources-raided"),
]


def launch_chrome():
    stderr_log = BASE_DIR / "chrome_debug.log"
    cmd = (
        f'"{CHROME_PATH}"'
        f" --remote-debugging-port={DEBUG_PORT}"
        f' --user-data-dir="{SESSION_DIR}"'
        f" --no-first-run"
        f" --no-default-browser-check"
        f" about:blank"
    )
    print("Launching Chrome...")
    log_handle = open(stderr_log, "w")
    proc = subprocess.Popen(cmd, shell=True, stderr=log_handle, stdout=log_handle)

    ws_url = None
    for i in range(30):
        time.sleep(1)
        try:
            content = stderr_log.read_text()
            match = re.search(r"(ws://\S+)", content)
            if match:
                ws_url = match.group(1)
                print(f"Connected after {i+1}s")
                break
        except Exception:
            pass

    if not ws_url:
        print("ERROR: Could not get Chrome debug websocket URL")
        log_handle.close()
        proc.terminate()
        sys.exit(1)

    time.sleep(3)
    return proc, ws_url


def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def select_combobox_option(page, button_text, option_text):
    """Click a combobox button, then select an option from the dropdown."""
    safe_print(f"\n  Clicking combobox: '{button_text}'...")

    # Click the combobox button
    btn = page.locator(f"button[role='combobox']:has-text('{button_text}')").first
    btn.click()
    time.sleep(1)

    # Dump what options appeared
    options = page.evaluate("""() => {
        const items = [];
        // Look for listbox/popover options
        document.querySelectorAll('[role="option"], [role="listbox"] [role="option"], [data-radix-collection-item]').forEach((el, i) => {
            if (i < 20) items.push(el.textContent.trim());
        });
        return items;
    }""")
    safe_print(f"  Dropdown options (first 20): {options}")

    # Try to find and click the option
    try:
        opt = page.locator(f"[role='option']:has-text('{option_text}')").first
        opt.click()
        safe_print(f"  Selected: '{option_text}'")
        time.sleep(2)
    except Exception as e:
        safe_print(f"  Could not click option '{option_text}': {e}")
        # Press Escape to close dropdown
        page.keyboard.press("Escape")
        time.sleep(1)


def dump_comboboxes(page):
    """Show current state of all combobox buttons."""
    combos = page.evaluate("""() => {
        const items = [];
        document.querySelectorAll('button[role="combobox"]').forEach(btn => {
            items.push(btn.textContent.trim());
        });
        return items;
    }""")
    safe_print(f"  Current comboboxes: {combos}")
    return combos


def explore_page(page, name, url):
    safe_print(f"\n{'='*60}")
    safe_print(f"  {name}")
    safe_print(f"  {url}")
    safe_print(f"{'='*60}")

    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    time.sleep(5)

    # Show initial filter state
    safe_print("\n  --- Initial filters ---")
    dump_comboboxes(page)

    # Step 1: Select server 716
    select_combobox_option(page, "All Servers", "716")

    # Show filters after server selection (alliance dropdown might appear)
    safe_print("\n  --- Filters after server selection ---")
    dump_comboboxes(page)

    # Step 2: Look for alliance filter and select NCC
    combos = dump_comboboxes(page)
    for combo_text in combos:
        if 'alliance' in combo_text.lower() or 'all alliance' in combo_text.lower():
            select_combobox_option(page, combo_text, "NCC")
            break
    else:
        safe_print("  No alliance combobox found, trying 'All Alliances'...")
        try:
            select_combobox_option(page, "All Alliances", "NCC")
        except Exception:
            safe_print("  Could not find alliance filter!")

    # Show final filter state
    safe_print("\n  --- Final filters ---")
    dump_comboboxes(page)

    # Dump the filtered table
    time.sleep(3)
    table_data = page.evaluate("""() => {
        const results = [];
        document.querySelectorAll('table').forEach((table, ti) => {
            const headers = [];
            table.querySelectorAll('thead th, tr:first-child th').forEach(th => {
                headers.push(th.innerText.trim());
            });
            const rows = [];
            const bodyRows = table.querySelectorAll('tbody tr');
            const limit = Math.min(bodyRows.length, 5);
            for (let i = 0; i < limit; i++) {
                const cells = [];
                bodyRows[i].querySelectorAll('td').forEach(td => {
                    cells.push(td.innerText.trim());
                });
                rows.push(cells);
            }
            results.push({
                index: ti,
                total_rows: bodyRows.length,
                headers: headers,
                sample_rows: rows
            });
        });
        return results;
    }""")

    if table_data:
        for t in table_data:
            safe_print(f"\n  Table ({t['total_rows']} rows): {t['headers']}")
            for i, row in enumerate(t['sample_rows']):
                safe_print(f"    Row {i+1}: {row}")
    else:
        safe_print("  No tables found!")


def main():
    proc, ws_url = launch_chrome()

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(ws_url)
            context = browser.contexts[0]
            page = context.pages[0] if context.pages else context.new_page()

            # Explore first page to nail down the filter workflow
            name, url = PAGES[0]
            explore_page(page, name, url)

            browser.close()
    finally:
        proc.terminate()

    print("\n\nDone!")


if __name__ == "__main__":
    main()
