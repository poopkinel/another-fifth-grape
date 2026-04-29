"""Fetch opening hours for verified stores via Google Places Details.

Default (no flags)  : DRY-RUN. Queries Places, prints what would be written,
                      touches nothing.
With --commit       : Writes opening_hours_json, opening_hours_tz, and the
                      forensic timestamp opening_hours_fetched_at inside a
                      single transaction.

Target set: stores where verified_by_places='verified' AND place_id IS NOT NULL.
By default, skips rows with a non-NULL opening_hours_fetched_at (so re-runs are
free). Use --force to re-query already-fetched rows (e.g. after a stale refresh
cycle or a schema change that invalidated earlier data).

What gets stored:
  - opening_hours_json       : JSON of the Places `opening_hours.periods` array,
                               verbatim (day/open/close/time local-HHMM). Empty
                               or missing → NULL.
  - opening_hours_tz         : 'Asia/Jerusalem' (hardcoded; all current stores
                               are Israeli). We still write it per-row so future
                               cross-border stores don't silently break.
  - opening_hours_fetched_at : ISO-8601 UTC timestamp. Set on EVERY successful
                               Places response, even when hours are empty, so
                               we don't re-query 24/7 gas-station-adjacent
                               stores that Places doesn't track.

Network errors (timeouts, 5xx) leave the row alone so the next run retries.
API errors (OVER_QUERY_LIMIT, REQUEST_DENIED) exit the script immediately to
avoid burning quota on a dead key.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Allow running from scripts/ subdir (siblings use raw sqlite3; we want init_db).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from dotenv import load_dotenv
from tqdm import tqdm

from app.db import get_conn, init_db

load_dotenv()

logger = logging.getLogger("fetch_hours")

PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
QPS_DELAY = 0.1
DEFAULT_TZ = "Asia/Jerusalem"
# Basic-data fields only (opening_hours is in the Basic tier; keeping the mask
# narrow avoids accidental billing drift into Contact/Atmosphere tiers).
DETAILS_FIELDS = "opening_hours"


def _parse_details(data: dict) -> tuple[str | None, list[dict] | None]:
    """Return (api_status, periods). periods is None when unavailable."""
    status = data.get("status")
    if status == "OK":
        result = data.get("result") or {}
        oh = result.get("opening_hours") or {}
        periods = oh.get("periods")
        return status, periods if isinstance(periods, list) else None
    if status in ("NOT_FOUND", "ZERO_RESULTS"):
        return status, None
    if status in ("OVER_QUERY_LIMIT", "REQUEST_DENIED", "INVALID_REQUEST"):
        raise RuntimeError(f"Places Details error: {status} — {data.get('error_message', '')}")
    logger.warning("Places Details returned unexpected status %s", status)
    return status, None


def fetch_details(session: requests.Session, api_key: str, place_id: str) -> dict:
    resp = session.get(
        PLACES_DETAILS_URL,
        params={
            "place_id": place_id,
            "fields": DETAILS_FIELDS,
            "key": api_key,
            "language": "he",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Fetch opening hours via Places Details")
    parser.add_argument("--limit", type=int, default=None, help="Max stores to query")
    parser.add_argument(
        "--commit", action="store_true",
        help="Write results to DB. Without this flag, runs in dry-run mode "
             "(queries Places, prints what would be written, touches nothing).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-query stores with a non-NULL opening_hours_fetched_at. "
             "Default skips them (cheap re-runs).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        sys.exit("ERROR: GOOGLE_MAPS_API_KEY not set in .env")

    init_db()

    # Build target set. Exclude already-fetched rows unless --force.
    where = [
        "verified_by_places = 'verified'",
        "place_id IS NOT NULL",
        "deleted_at IS NULL",
    ]
    if not args.force:
        where.append("opening_hours_fetched_at IS NULL")
    target_sql = (
        f"SELECT store_id, chain_id, place_id FROM stores "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY chain_id, store_id"
    )
    if args.limit:
        target_sql += f" LIMIT {int(args.limit)}"

    with get_conn() as conn:
        targets = conn.execute(target_sql).fetchall()

    if not targets:
        print("No target stores (all verified+place_id rows already fetched or set is empty).")
        return

    mode = "COMMIT" if args.commit else "DRY-RUN"
    print(f"[{mode}] Fetching Places Details for {len(targets)} stores ...")
    session = requests.Session()

    n_with_hours = n_no_hours = n_errors = 0

    for t in tqdm(targets, unit="store"):
        try:
            data = fetch_details(session, api_key, t["place_id"])
        except requests.RequestException as e:
            logger.warning("Network error for %s/%s: %s", t["chain_id"], t["store_id"], e)
            n_errors += 1
            time.sleep(QPS_DELAY)
            continue
        except RuntimeError as e:
            # API-level kill signal (quota, denied). Do not continue.
            sys.exit(f"FATAL: {e}")

        try:
            status, periods = _parse_details(data)
        except RuntimeError as e:
            sys.exit(f"FATAL: {e}")

        periods_json: str | None = None
        if periods:
            periods_json = json.dumps(periods, ensure_ascii=False, separators=(",", ":"))
            n_with_hours += 1
            if args.verbose:
                logger.info("  [hours] %s/%s → %d periods", t["chain_id"], t["store_id"], len(periods))
        else:
            n_no_hours += 1
            if args.verbose:
                logger.info("  [no-hours] %s/%s → status=%s", t["chain_id"], t["store_id"], status)

        if args.commit:
            now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
            with get_conn() as conn:
                conn.execute(
                    "UPDATE stores SET opening_hours_json = ?, "
                    "opening_hours_tz = ?, opening_hours_fetched_at = ? "
                    "WHERE chain_id = ? AND store_id = ?",
                    (periods_json, DEFAULT_TZ, now_iso, t["chain_id"], t["store_id"]),
                )

        time.sleep(QPS_DELAY)

    print(f"\nDone [{mode}].")
    print(f"  with hours: {n_with_hours}")
    print(f"  no hours:   {n_no_hours}")
    print(f"  errors:     {n_errors}")


if __name__ == "__main__":
    main()
