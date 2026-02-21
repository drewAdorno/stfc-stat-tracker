"""
Test if curl_cffi can bypass Cloudflare with the session cookies.
Extracts cookies from Chrome session, then tests direct API call.
"""

import gzip
import json
import re
import subprocess
import sys
import time
from pathlib import Path

from curl_cffi import requests as cffi_requests
from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR / "browser_session"
DATA_DIR = BASE_DIR / "data"
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
    DATA_DIR.mkdir(exist_ok=True)

    # Step 1: Extract cookies from Chrome
    chrome_proc, ws_url = launch_chrome()
    cookies = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(ws_url)
            context = browser.contexts[0]
            page = context.pages[0] if context.pages else None

            if page:
                print(f"Current URL: {page.url}")
                if any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                    print("\nLogin required! Log in via Discord.")
                    for _ in range(120):
                        time.sleep(2)
                        if not any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                            break
                    time.sleep(5)

            cookies = context.cookies("https://v3.stfc.pro")
            print(f"\nExtracted {len(cookies)} cookies:")
            for c in cookies:
                print(f"  {c['name']} = {c['value'][:40]}...")

            browser.close()
    finally:
        chrome_proc.terminate()
        subprocess.run(
            ["taskkill", "/F", "/IM", "chrome.exe"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    if not cookies:
        print("No cookies found!")
        sys.exit(1)

    # Save cookies for future use
    cookie_file = DATA_DIR / "session_cookies.json"
    with open(cookie_file, "w") as f:
        json.dump(cookies, f, indent=2)
    print(f"\nCookies saved to {cookie_file}")

    # Step 2: Test with curl_cffi
    print("\n" + "=" * 60)
    print("Testing with curl_cffi (browser TLS impersonation)...")
    print("=" * 60)

    cookie_dict = {c["name"]: c["value"] for c in cookies}
    test_url = "https://v3.stfc.pro/api/players?server=716&alliance=3974286889&sortBy=pdestroyed&sortOrder=desc&page=1&pageCount=100&reRank=false"

    # Try different browser impersonation profiles
    for impersonate in ["chrome131", "chrome124", "chrome120", "chrome110"]:
        print(f"\nTrying impersonate={impersonate}...")
        try:
            resp = cffi_requests.get(
                test_url,
                cookies=cookie_dict,
                impersonate=impersonate,
            )
            print(f"  Status: {resp.status_code}")
            print(f"  Content-Type: {resp.headers.get('Content-Type', '?')}")

            if resp.status_code == 200:
                body = resp.content
                if body[:2] == b'\x1f\x8b':
                    body = gzip.decompress(body)
                    print(f"  Decompressed: {len(body)} bytes")
                text = body.decode('utf-8', errors='replace')
                try:
                    data = json.loads(text)
                    print(f"\n  SUCCESS! Got {data.get('count', '?')} total players")
                    players = data.get("players", [])
                    print(f"  Players in page: {len(players)}")
                    if players:
                        p = players[0]["data"]
                        print(f"  First player: {p.get('owner')} (Level {p.get('level')}, Server {p.get('server')})")

                    # Save sample response
                    sample_file = DATA_DIR / "api_sample_response.json"
                    with open(sample_file, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    print(f"\n  Full response saved to {sample_file}")
                except json.JSONDecodeError:
                    print(f"  Not JSON: {text[:200]}")
                break  # success, stop trying
            elif resp.status_code == 403:
                print(f"  Cloudflare blocked (403)")
            else:
                print(f"  Response: {resp.text[:200]}")
        except Exception as e:
            print(f"  Error: {e}")

    print("\nDone!")


if __name__ == "__main__":
    main()
