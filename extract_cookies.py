"""
Extract session cookies from the Chrome browser session for v3.stfc.pro.
Launches Chrome via CDP, grabs cookies, and tests the API directly.
"""

import gzip
import json
import re
import subprocess
import sys
import time
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR / "browser_session"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DEBUG_PORT = 9222
ALLIANCE_URL = "https://v3.stfc.pro/alliances/3974286889"


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
    startupinfo.wShowWindow = 7
    proc = subprocess.Popen(cmd, stderr=log_handle, stdout=log_handle, startupinfo=startupinfo)

    ws_url = None
    for i in range(30):
        time.sleep(1)
        try:
            content = stderr_log.read_text()
            match = re.search(r"(ws://\S+)", content)
            if match:
                ws_url = match.group(1)
                print(f"Got websocket URL (after {i+1}s)")
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


def main():
    SESSION_DIR.mkdir(exist_ok=True)
    chrome_proc, ws_url = launch_chrome()

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

            print(f"Current URL: {page.url}")

            # Handle login if needed
            if any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                print("\nLogin required! Log in via Discord in the browser.")
                for _ in range(120):
                    time.sleep(2)
                    if not any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                        break
                time.sleep(5)

            # Get all cookies for stfc.pro
            cookies = context.cookies("https://v3.stfc.pro")
            print(f"\nFound {len(cookies)} cookies for v3.stfc.pro:")
            for c in cookies:
                val_preview = c['value'][:50] + "..." if len(c['value']) > 50 else c['value']
                print(f"  {c['name']} = {val_preview}")
                print(f"    domain={c['domain']}  path={c['path']}  httpOnly={c.get('httpOnly')}  secure={c.get('secure')}")
                if c.get('expires'):
                    print(f"    expires={c['expires']}")

            # Build cookie header for requests
            cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
            print(f"\nCookie header: {cookie_header[:100]}...")

            browser.close()

    finally:
        chrome_proc.terminate()
        subprocess.run(
            ["taskkill", "/F", "/IM", "chrome.exe"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    # Now test the API with the extracted cookies
    print("\n" + "=" * 60)
    print("Testing API with extracted cookies...")
    print("=" * 60)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Cookie": cookie_header,
        "Accept-Encoding": "gzip",
    }

    test_url = "https://v3.stfc.pro/api/players?server=716&alliance=3974286889&sortBy=pdestroyed&sortOrder=desc&page=1&pageCount=100&reRank=false"
    print(f"\nGET {test_url}")

    resp = requests.get(test_url, headers=headers)
    print(f"Status: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type')}")
    print(f"Content-Length: {len(resp.content)} bytes")

    if resp.status_code == 200:
        # Try to decompress if gzipped
        body = resp.content
        if body[:2] == b'\x1f\x8b':
            body = gzip.decompress(body)
            print(f"Decompressed: {len(body)} bytes")

        text = body.decode('utf-8', errors='replace')
        try:
            data = json.loads(text)
            print(f"\nSUCCESS! Got JSON with {data.get('count', '?')} total players")
            players = data.get('players', [])
            print(f"Players in this page: {len(players)}")
            if players:
                print(f"\nFirst player sample:")
                print(json.dumps(players[0], indent=2)[:1000])

            # Save the cookie info for reference
            cookie_file = BASE_DIR / "data" / "session_cookies.json"
            with open(cookie_file, "w") as f:
                json.dump(cookies, f, indent=2)
            print(f"\nCookies saved to: {cookie_file}")
        except json.JSONDecodeError:
            print(f"Response text (first 500): {text[:500]}")
    else:
        print(f"FAILED! Response: {resp.text[:500]}")


if __name__ == "__main__":
    main()
