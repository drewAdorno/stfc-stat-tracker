"""
Capture the binary /api/players response body and try to decode it.
"""

import gzip
import json
import re
import subprocess
import sys
import time
import zlib
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).parent
SESSION_DIR = BASE_DIR / "browser_session"
DATA_DIR = BASE_DIR / "data"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DEBUG_PORT = 9222

ALLIANCE_URL = "https://v3.stfc.pro/alliances/3974286889"
# Just test one API call
TEST_URL = "https://v3.stfc.pro/players-killed"


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


def try_decode(raw_bytes):
    """Try various decodings of the binary data."""
    print(f"\nRaw bytes: {len(raw_bytes)} bytes")
    print(f"First 100 bytes (hex): {raw_bytes[:100].hex()}")
    print(f"First 100 bytes (repr): {repr(raw_bytes[:100])}")

    # Check magic bytes
    if raw_bytes[:2] == b'\x1f\x8b':
        print("\n-> Detected GZIP magic bytes!")
        try:
            decoded = gzip.decompress(raw_bytes)
            print(f"   Decompressed: {len(decoded)} bytes")
            text = decoded.decode('utf-8', errors='replace')
            print(f"   As text (first 500): {text[:500]}")
            return text
        except Exception as e:
            print(f"   GZIP decompress failed: {e}")

    # Try raw deflate
    try:
        decoded = zlib.decompress(raw_bytes)
        print("\n-> zlib decompress succeeded!")
        text = decoded.decode('utf-8', errors='replace')
        print(f"   As text (first 500): {text[:500]}")
        return text
    except Exception:
        pass

    # Try raw deflate without header
    try:
        decoded = zlib.decompress(raw_bytes, -zlib.MAX_WBITS)
        print("\n-> Raw deflate decompress succeeded!")
        text = decoded.decode('utf-8', errors='replace')
        print(f"   As text (first 500): {text[:500]}")
        return text
    except Exception:
        pass

    # Try brotli if available
    try:
        import brotli
        decoded = brotli.decompress(raw_bytes)
        print("\n-> Brotli decompress succeeded!")
        text = decoded.decode('utf-8', errors='replace')
        print(f"   As text (first 500): {text[:500]}")
        return text
    except ImportError:
        print("\n   (brotli module not installed, skipping)")
    except Exception:
        pass

    # Try msgpack if available
    try:
        import msgpack
        decoded = msgpack.unpackb(raw_bytes, raw=False)
        print("\n-> MessagePack decode succeeded!")
        print(f"   Data (first 500 chars): {str(decoded)[:500]}")
        return decoded
    except ImportError:
        print("   (msgpack module not installed, skipping)")
    except Exception:
        pass

    # Just try UTF-8 directly
    try:
        text = raw_bytes.decode('utf-8')
        print("\n-> Direct UTF-8 decode succeeded!")
        print(f"   Text (first 500): {text[:500]}")
        return text
    except Exception:
        pass

    # Try as JSON directly
    try:
        data = json.loads(raw_bytes)
        print("\n-> Direct JSON parse succeeded!")
        print(f"   Data: {json.dumps(data, indent=2)[:500]}")
        return data
    except Exception:
        pass

    print("\n-> Could not decode with any method.")
    return None


def main():
    SESSION_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)

    chrome_proc, ws_url = launch_chrome()
    captured_bodies = {}

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

            # Handle login
            if any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                print("\nLogin required! Log in via Discord in the browser.")
                for _ in range(120):
                    time.sleep(2)
                    if not any(kw in page.url.lower() for kw in ["login", "discord", "authorize", "oauth"]):
                        break
                time.sleep(5)

            # Capture response bodies - use response.body() for binary
            def on_response(response):
                url = response.url
                if "/api/players" not in url:
                    return
                print(f"\n>>> Captured: {url}")
                print(f"    Status: {response.status}")
                print(f"    Content-Type: {response.headers.get('content-type', '?')}")
                print(f"    Content-Encoding: {response.headers.get('content-encoding', 'none')}")
                print(f"    All response headers:")
                for k, v in response.headers.items():
                    print(f"      {k}: {v[:100]}")
                try:
                    raw = response.body()
                    captured_bodies[url] = raw
                    try_decode(raw)
                except Exception as e:
                    print(f"    Could not get body: {e}")
                    # Try text() as fallback
                    try:
                        text = response.text()
                        print(f"    text() returned: {text[:500]}")
                    except Exception as e2:
                        print(f"    text() also failed: {e2}")

            page.on("response", on_response)

            # Navigate to players-killed and apply server 716 filter
            print(f"\nNavigating to {TEST_URL}...")
            page.goto(TEST_URL, wait_until="domcontentloaded", timeout=60_000)
            time.sleep(5)

            # Apply server filter to get a more specific call
            print("\nApplying Server 716 filter...")
            btn = page.locator("button[role='combobox']:has-text('All Servers')").first
            btn.click()
            time.sleep(1)
            page.locator("[role='option']:has-text('716')").first.click()
            time.sleep(3)

            # Apply alliance filter
            print("\nApplying Alliance [NCC] filter...")
            btn = page.locator("button[role='combobox']:has-text('All Alliances')").first
            btn.click()
            time.sleep(1)
            page.locator("[role='option']:has-text('[NCC]')").first.click()
            time.sleep(3)

            browser.close()

    finally:
        chrome_proc.terminate()
        subprocess.run(
            ["taskkill", "/F", "/IM", "chrome.exe"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    # Save raw binary files for inspection
    for url, raw in captured_bodies.items():
        safe_name = url.split("/api/")[-1].replace("?", "_").replace("&", "_")[:80]
        out = DATA_DIR / f"api_raw_{safe_name}.bin"
        with open(out, "wb") as f:
            f.write(raw)
        print(f"\nSaved raw response to: {out}")

    print("\nDone!")


if __name__ == "__main__":
    main()
