#!/usr/bin/env python3
"""
Verify headless browser can reach log / credit related URLs.

Usage:
  python3 checkaccess.py --verify-headless
  python3 checkaccess.py --verify-headless --headed   # same checks, visible window

Requires: pip install playwright && playwright install chromium
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

VERIFY_URLS = (
    "https://lognavigator.cliveslot.com/lognavigator/logs/DHS3050/list",
    "https://backend-nc.osmplay.com/log/logThirdHttpReq",
    "https://oss-osm-log.osmplay.com/",
    "https://grafana.client8.me/d/281e8816-ccb0-4335-922b-6b248491fd28/core-metrics-arms-aliyun?orgId=1&from=now-1m&to=now&timezone=browser&refresh=1m"
)


def _check_one(page, url: str, *, timeout_ms: int) -> dict[str, Any]:
    out: dict[str, Any] = {"url": url, "ok": False, "error": None}
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        out["http_status"] = resp.status if resp else None
        out["final_url"] = page.url
        out["title"] = (page.title() or "").strip()[:200]
        # Treat 2xx/3xx as reachable; body may still be login wall.
        st = resp.status if resp else 0
        out["ok"] = 200 <= st < 400 if st else bool(page.url)
    except Exception as e:
        out["error"] = repr(e)
    return out


def run_verify(*, headless: bool, timeout_ms: int) -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(
            "❌ Playwright not installed. Run:\n"
            "  pip install playwright\n"
            "  playwright install chromium",
            file=sys.stderr,
        )
        return 1

    mode = "headless" if headless else "headed"
    print(f"→ Verifying {len(VERIFY_URLS)} URL(s) in Chromium ({mode})…\n")

    all_ok = True
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                ignore_https_errors=True,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            for url in VERIFY_URLS:
                r = _check_one(page, url, timeout_ms=timeout_ms)
                if r.get("error"):
                    all_ok = False
                    print(f"❌ {url}")
                    print(f"   error: {r['error']}")
                else:
                    ok = r.get("ok")
                    if not ok:
                        all_ok = False
                    mark = "✅" if ok else "⚠️"
                    print(f"{mark} {url}")
                    print(f"   http_status: {r.get('http_status')}")
                    print(f"   final_url:   {r.get('final_url')}")
                    print(f"   title:       {r.get('title')!r}")
                print()
        finally:
            browser.close()

    if all_ok:
        print("Summary: all navigations completed without Playwright errors (check http_status / title for auth walls).")
        return 0
    print("Summary: at least one URL failed or returned unexpected status.", file=sys.stderr)
    return 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Verify headless access to log URLs.")
    ap.add_argument(
        "--verify-headless",
        action="store_true",
        help="Open VERIFY_URLs in Chromium and report load result",
    )
    ap.add_argument(
        "--headed",
        action="store_true",
        help="Run with visible browser (same checks as headless)",
    )
    ap.add_argument(
        "--timeout-ms",
        type=int,
        default=60_000,
        help="Navigation timeout per URL (default 60000)",
    )
    args = ap.parse_args(argv)

    if not args.verify_headless:
        ap.print_help()
        print(
            "\nExample:\n  python3 checkaccess.py --verify-headless\n"
            "  python3 checkaccess.py --verify-headless --headed",
            file=sys.stderr,
        )
        return 2

    return run_verify(headless=not args.headed, timeout_ms=max(5_000, args.timeout_ms))


if __name__ == "__main__":
    raise SystemExit(main())
