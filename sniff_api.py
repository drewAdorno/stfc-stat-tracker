"""
Network Sniffer for v3.stfc.pro
Launches Chrome via CDP (same as pull_alliance_data.py) and logs all
network requests/responses to discover API endpoints.

Usage: Close Chrome first, then run: python sniff_api.py
Output: prints all requests and saves interesting ones to data/api_sniff.json
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
DATA_DIR = BASE_DIR / "data"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DEBUG_PORT = 9222

ALLIANCE_URL = "https://v3.stfc.pro/alliances/3974286889"

PAGES_TO_VISIT = [
    ("alliance",         ALLIANCE_URL),
    ("players_killed",   "https://v3.stfc.pro/players-killed"),
    ("resources_mined",  "https://v3.stfc.pro/resources-mined"),
    ("hostiles_killed",  "https://v3.stfc.pro/hostiles-killed"),
    ("resources_raided", "https://v3.stfc.pro/resources-raided"),
]

# Skip noisy stuff we don't care about
IGNORE_PATTERNS = [
    ".js", ".css", ".woff", ".woff2", ".ttf", ".png", ".jpg", ".jpeg",
    ".gif", ".svg", ".ico", "favicon", "fonts.googleapis", "fonts.gstatic",
    "google-analytics", "googletagmanager", "gtag", "analytics",
    "cloudflare", "challenges.cloudflare", "cdn-cgi",
    "discord.com/api",  # OAuth noise
]


def should_log(url):
    lower = url.lower()
    return not any(pat in lower for pat in IGNORE_PATTERNS)


def launch_chrome():
    stderr_log = BASE_DIR / "chrome_debug.log"
    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={DEBUG_PORT}",
        f"--user-data-dir={SESSION_DIR}",
        "--no-first-run",
        "--no-default-browser-check",
        ALLIANCE_URL,
    ]
    print("Launching Chrome...")
    log_handle = open(stderr_log, "w")

    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 7  # SW_SHOWMINNOACTIVE

    proc = subprocess.Popen(cmd, stderr=log_handle, stdout=log_handle, startupinfo=startupinfo)

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

    time.sleep(3)
    return proc, ws_url


def select_combobox(page, current_text, option_text):
    btn = page.locator(f"button[role='combobox']:has-text('{current_text}')").first
    btn.click()
    time.sleep(1)
    page.locator(f"[role='option']:has-text('{option_text}')").first.click()
    time.sleep(2)


def main():
    SESSION_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)

    chrome_proc, ws_url = launch_chrome()
    captured = []  # all interesting requests

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
            if not page:
                page = context.new_page()

            print(f"Current URL: {page.url}")

            # Handle login if needed
            if any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                print("\nLogin required! Please log in via Discord in the browser window.")
                print("Waiting for login...")
                for _ in range(120):
                    time.sleep(2)
                    if not any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                        print(f"Login complete! Now at: {page.url}")
                        break
                else:
                    print("Timed out waiting for login.")
                    browser.close()
                    sys.exit(1)
                time.sleep(5)

            # Set up response listener - capture headers and try to get body
            def on_response(response):
                url = response.url
                if not should_log(url):
                    return

                entry = {
                    "url": url,
                    "status": response.status,
                    "method": response.request.method,
                    "content_type": response.headers.get("content-type", ""),
                    "request_headers": dict(response.request.headers),
                }

                # Try to capture response body for JSON responses
                body_preview = None
                if "json" in entry["content_type"] or "text" in entry["content_type"]:
                    try:
                        body = response.text()
                        if len(body) > 5000:
                            body_preview = body[:5000] + f"... (truncated, total {len(body)} chars)"
                        else:
                            body_preview = body
                    except Exception:
                        body_preview = "(could not read body)"

                entry["body_preview"] = body_preview
                captured.append(entry)

                # Print to console
                marker = "***" if "api" in url.lower() or "json" in entry["content_type"] else "   "
                print(f"  {marker} {response.request.method} {response.status} {url}")
                if "json" in entry["content_type"]:
                    print(f"       Content-Type: {entry['content_type']}")
                    if body_preview:
                        preview = body_preview[:200]
                        print(f"       Body: {preview}...")

            page.on("response", on_response)

            # Visit each page
            for label, url in PAGES_TO_VISIT:
                print(f"\n{'='*60}")
                print(f"Navigating to: {label} ({url})")
                print(f"{'='*60}")

                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                time.sleep(5)  # let XHR requests fire

                # For leaderboard pages, also apply filters to trigger filtered API calls
                if label != "alliance":
                    print(f"\n  Applying filters: Server 716, Alliance [NCC]...")
                    try:
                        select_combobox(page, "All Servers", "716")
                        time.sleep(2)
                        select_combobox(page, "All Alliances", "[NCC]")
                        time.sleep(3)
                    except Exception as e:
                        print(f"  WARNING: Could not apply filters: {e}")

                # Also try clicking pagination if present
                try:
                    next_btn = page.query_selector('button:has-text("Next"), a:has-text("Next")')
                    if next_btn and next_btn.is_enabled():
                        print(f"\n  Clicking Next page to capture pagination request...")
                        next_btn.click()
                        time.sleep(3)
                except Exception:
                    pass

            browser.close()

    finally:
        chrome_proc.terminate()
        subprocess.run(
            ["taskkill", "/F", "/IM", "chrome.exe"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    # Save results
    output_file = DATA_DIR / "api_sniff.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(captured, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"RESULTS: Captured {len(captured)} interesting requests")
    print(f"Saved to: {output_file}")
    print(f"{'='*60}")

    # Summary: highlight the most interesting ones
    api_calls = [c for c in captured if "json" in c.get("content_type", "") or "api" in c["url"].lower()]
    if api_calls:
        print(f"\nAPI/JSON endpoints found ({len(api_calls)}):")
        for c in api_calls:
            print(f"  {c['method']} {c['url']}")
            print(f"    Status: {c['status']}  Content-Type: {c['content_type']}")
            # Show auth-related headers
            for h in ["authorization", "cookie", "x-api-key", "x-auth-token"]:
                if h in c.get("request_headers", {}):
                    val = c["request_headers"][h]
                    if len(val) > 80:
                        val = val[:80] + "..."
                    print(f"    {h}: {val}")
    else:
        print("\nNo obvious API/JSON endpoints found.")
        print("The site might embed data in the HTML (SSR) - check the full log in api_sniff.json")


if __name__ == "__main__":
    main()
