#!/usr/bin/env python3
"""Backfill lat/lng for stores using Google Geocoding API.

Reads stores with no coordinates, queries Google, writes results back.
Idempotent: re-running only processes rows that still need it.

Usage:
    python geocode.py                  # process all unresolved rows
    python geocode.py --limit 50       # cap for testing
    python geocode.py --retry-failed   # also re-try rows previously marked 'no_results'
    python geocode.py --dry-run        # query but don't write
"""

import argparse
import logging
import os
import sys
import time

import requests
from dotenv import load_dotenv
from tqdm import tqdm

from app.db import get_conn, init_db

load_dotenv()

logger = logging.getLogger("geocode")

GOOGLE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
QPS_DELAY = 0.05  # ~20 req/sec, well under Google's limits


def build_query(address: str, city: str) -> str:
    parts = [p for p in (address.strip(), city.strip()) if p]
    parts.append("Israel")
    return ", ".join(parts)


def geocode_one(session: requests.Session, api_key: str, query: str) -> tuple[float, float] | None:
    """Returns (lat, lng) on success, None if zero results. Raises on hard errors."""
    params = {
        "address": query,
        "key": api_key,
        "region": "il",
        "language": "he",
        "components": "country:IL",
    }
    resp = session.get(GOOGLE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    status = data.get("status")

    if status == "OK":
        loc = data["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"])
    if status == "ZERO_RESULTS":
        return None
    raise RuntimeError(f"Google API error: {status} — {data.get('error_message', '')}")


def main():
    parser = argparse.ArgumentParser(description="Geocode stores via Google Geocoding API")
    parser.add_argument("--limit", type=int, default=None, help="Max stores to process this run")
    parser.add_argument("--retry-failed", action="store_true", help="Re-try rows previously marked 'no_results'")
    parser.add_argument("--dry-run", action="store_true", help="Query but don't write to DB")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        sys.exit("ERROR: GOOGLE_MAPS_API_KEY not set (put it in .env at the repo root)")

    init_db()

    where = "lat IS NULL AND address != ''"
    if not args.retry_failed:
        where += " AND (geocode_status IS NULL OR geocode_status != 'no_results')"
    sql = f"""
        SELECT store_id, chain_id, address, city
        FROM stores
        WHERE {where}
        ORDER BY chain_id, store_id
    """
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"

    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()

    if not rows:
        print("Nothing to geocode.")
        return

    print(f"Geocoding {len(rows)} stores"
          + (" (dry run)" if args.dry_run else "")
          + " ...")

    session = requests.Session()
    n_ok = n_zero = n_err = 0

    for row in tqdm(rows, unit="store"):
        query = build_query(row["address"], row["city"])
        try:
            result = geocode_one(session, api_key, query)
        except requests.RequestException as e:
            logger.warning("HTTP error for %s: %s — leaving for retry", query, e)
            n_err += 1
            time.sleep(1.0)
            continue
        except RuntimeError as e:
            sys.exit(f"FATAL: {e}")

        if result is None:
            n_zero += 1
            if not args.dry_run:
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE stores SET geocode_status='no_results' WHERE store_id=? AND chain_id=?",
                        (row["store_id"], row["chain_id"]),
                    )
        else:
            n_ok += 1
            lat, lng = result
            if not args.dry_run:
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE stores SET lat=?, lng=?, geocode_status='ok' WHERE store_id=? AND chain_id=?",
                        (lat, lng, row["store_id"], row["chain_id"]),
                    )

        time.sleep(QPS_DELAY)

    print(f"\nDone. ok={n_ok}  no_results={n_zero}  http_errors={n_err}")


if __name__ == "__main__":
    main()
