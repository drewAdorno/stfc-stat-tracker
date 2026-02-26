"""Open a browser window to v3.stfc.pro for manual login, then save cookies."""
import json
import time
from playwright.sync_api import sync_playwright

TARGET = "https://v3.stfc.pro/alliances/3974286889"
COOKIES_PATH = "data/session_cookies.json"


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()

        # Load existing cookies to skip CF challenge if still valid
        try:
            with open(COOKIES_PATH, "r") as f:
                old_cookies = json.load(f)
            context.add_cookies(old_cookies)
            print("Loaded existing cookies into browser.")
        except Exception:
            print("No existing cookies loaded.")

        page = context.new_page()
        page.goto(TARGET, wait_until="domcontentloaded")

        print()
        print("=" * 60)
        print("  Log in via Discord in the browser window.")
        print("  Once you see alliance data, close the browser.")
        print("=" * 60)
        print()

        # Wait for the browser to be closed by the user
        try:
            while True:
                try:
                    page.title()
                except Exception:
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        # Save cookies
        cookies = context.cookies()
        stfc_cookies = [c for c in cookies if "stfc.pro" in c.get("domain", "")]

        with open(COOKIES_PATH, "w") as f:
            json.dump(stfc_cookies, f, indent=2)

        print(f"\nSaved {len(stfc_cookies)} cookies to {COOKIES_PATH}")

        try:
            browser.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
