#!/usr/bin/env python3
"""Backfill lat/lng for stores using Google Geocoding API.

Reads stores with no coordinates, queries Google, writes results back.
Idempotent: re-running only processes rows that still need it.

Usage:
    python geocode.py                         # process all unresolved rows
    python geocode.py --limit 50              # cap for testing
    python geocode.py --retry-failed          # also re-try rows marked 'no_results'
    python geocode.py --dry-run               # query but don't write
    python geocode.py --refetch-city-inferred # re-geocode rows whose city was
                                              # just backfilled, using the city
                                              # as an administrative_area bias
                                              # (fixes same-street-in-wrong-city
                                              #  cases like 'פ"ת' → Tel Aviv)
"""

import argparse
import logging
import math
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


def _is_numeric_city(s: str | None) -> bool:
    """Some chains (Hazi Hinam) ship <City> as a 4-digit municipal code (8300,
    6600, ...) rather than a place name. The codes don't match any Google
    administrative_area and don't help in the textual query either, so we
    treat them as missing for geocoding while keeping the raw value in the DB
    — a future code→name lookup pass can still upgrade them in place."""
    return bool(s) and s.strip().isdigit()


def build_query(address: str, city: str) -> str:
    parts = [address.strip()] if address else []
    if city and not _is_numeric_city(city):
        parts.append(city.strip())
    parts.append("Israel")
    return ", ".join(p for p in parts if p)


def geocode_one(
    session: requests.Session,
    api_key: str,
    query: str,
    city: str | None = None,
) -> tuple[float, float] | None:
    """Returns (lat, lng) on success, None if zero results. Raises on hard errors.

    If `city` is provided AND is a real place name (not a numeric municipal
    code), it's appended to `components` as an `administrative_area` bias —
    disambiguates same-named streets across cities (e.g. Begin 96 in Tel Aviv
    vs. Begin 96 in Petah Tikva).
    """
    components = "country:IL"
    if city and not _is_numeric_city(city):
        components += f"|administrative_area:{city}"
    params = {
        "address": query,
        "key": api_key,
        "region": "il",
        "language": "he",
        "components": components,
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


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in km."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def refetch_city_inferred(api_key: str, limit: int | None, dry_run: bool) -> None:
    """Re-geocode rows whose city was just backfilled by infer_city.py.

    Uses the city as an `administrative_area` bias so same-named streets resolve
    to the correct town. Compares the new coords with the existing (potentially
    wrong) coords and reports the km delta distribution.

    Writes only under --commit (i.e. not --dry-run), setting:
      lat, lng, geocode_status='city_verified', coords_refetched_at=<now>
    """
    init_db()

    where = (
        "city_inferred_at IS NOT NULL "
        "AND coords_refetched_at IS NULL "
        "AND address IS NOT NULL AND address != '' "
        "AND city IS NOT NULL AND city != '' "
        "AND deleted_at IS NULL"
    )
    sql = (
        f"SELECT store_id, chain_id, address, COALESCE(city_resolved, city) AS city, lat, lng "
        f"FROM stores WHERE {where} ORDER BY chain_id, store_id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"

    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()

    if not rows:
        print("Nothing to refetch.")
        return

    print(f"Refetching {len(rows)} stores with city region-bias"
          f"{' (dry run)' if dry_run else ''} ...")

    session = requests.Session()
    # Buckets for the delta histogram the user asked about.
    # "low" = ≤1 km (probably the backfill didn't change anything meaningful)
    # "med" = 1-10 km (town-scale correction)
    # "high" = >10 km (wrong-city fix — the whole point of this pass)
    # "failed" = Google returned no results under the bias
    # "nocoord" = row has city but lat IS NULL (can't compute delta; still valuable)
    results = []
    buckets = {"low": 0, "med": 0, "high": 0, "failed": 0, "nocoord": 0, "http_err": 0}

    for row in tqdm(rows, unit="store"):
        query = build_query(row["address"], row["city"])
        try:
            new_coords = geocode_one(session, api_key, query, city=row["city"])
        except requests.RequestException as e:
            logger.warning("HTTP error for %s: %s — skipping", query, e)
            buckets["http_err"] += 1
            time.sleep(1.0)
            continue
        except RuntimeError as e:
            sys.exit(f"FATAL: {e}")

        if new_coords is None:
            buckets["failed"] += 1
            results.append({"row": row, "new": None, "delta": None})
            time.sleep(QPS_DELAY)
            continue

        new_lat, new_lng = new_coords
        if row["lat"] is None or row["lng"] is None:
            delta = None
            buckets["nocoord"] += 1
        else:
            delta = haversine_km(row["lat"], row["lng"], new_lat, new_lng)
            if delta <= 1.0:
                buckets["low"] += 1
            elif delta <= 10.0:
                buckets["med"] += 1
            else:
                buckets["high"] += 1
        results.append({"row": row, "new": (new_lat, new_lng), "delta": delta})

        if not dry_run:
            with get_conn() as conn:
                conn.execute(
                    "UPDATE stores SET lat = ?, lng = ?, "
                    "geocode_status = 'city_verified', "
                    "coords_refetched_at = ? "
                    "WHERE store_id = ? AND chain_id = ?",
                    (new_lat, new_lng,
                     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     row["store_id"], row["chain_id"]),
                )

        time.sleep(QPS_DELAY)

    _report_refetch(results, buckets)
    if dry_run:
        print("\n(dry-run — no writes. Re-run without --dry-run to apply.)")


def _report_refetch(results: list[dict], buckets: dict[str, int]) -> None:
    total = sum(buckets.values())
    print(f"\nRefetch summary ({total} rows processed):")
    print(f"  ≤1 km delta  (low) : {buckets['low']}   # likely noise / already close")
    print(f"  1–10 km      (med) : {buckets['med']}   # town-scale correction")
    print(f"  >10 km       (high): {buckets['high']}  # wrong-city fix — the point of this pass")
    print(f"  no old coords      : {buckets['nocoord']}")
    print(f"  Google zero-results: {buckets['failed']}")
    print(f"  HTTP errors        : {buckets['http_err']}")

    high_deltas = sorted(
        [r for r in results if r["delta"] is not None and r["delta"] > 10.0],
        key=lambda r: -r["delta"],
    )
    if high_deltas:
        print(f"\nTop {min(20, len(high_deltas))} largest deltas (wrong-city fixes):")
        for r in high_deltas[:20]:
            row = r["row"]
            new = r["new"]
            print(f"  {r['delta']:>7.1f} km  "
                  f"{row['chain_id']}/{row['store_id']}  "
                  f"({row['lat']:.4f},{row['lng']:.4f}) → ({new[0]:.4f},{new[1]:.4f})  "
                  f"city={row['city']!r}  addr={row['address']!r}")

    failed = [r for r in results if r["new"] is None]
    if failed:
        print(f"\nZero-result queries ({len(failed)}):")
        for r in failed[:10]:
            row = r["row"]
            print(f"  {row['chain_id']}/{row['store_id']}  "
                  f"city={row['city']!r}  addr={row['address']!r}")


def main():
    parser = argparse.ArgumentParser(description="Geocode stores via Google Geocoding API")
    parser.add_argument("--limit", type=int, default=None, help="Max stores to process this run")
    parser.add_argument("--retry-failed", action="store_true", help="Re-try rows previously marked 'no_results'")
    parser.add_argument("--dry-run", action="store_true", help="Query but don't write to DB")
    parser.add_argument("--refetch-city-inferred", action="store_true",
                        help="Re-geocode rows whose city was backfilled by infer_city.py, "
                             "using city as administrative_area bias")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        sys.exit("ERROR: GOOGLE_MAPS_API_KEY not set (put it in .env at the repo root)")

    if args.refetch_city_inferred:
        refetch_city_inferred(api_key, args.limit, args.dry_run)
        return

    init_db()

    where = "lat IS NULL AND address != '' AND deleted_at IS NULL"
    if not args.retry_failed:
        where += " AND (geocode_status IS NULL OR geocode_status != 'no_results')"
    # COALESCE prefers city_resolved (looked-up name when raw city was a
    # numeric code) over raw city. Aliased to `city` so the loop below stays
    # the same shape.
    sql = f"""
        SELECT store_id, chain_id, address, COALESCE(city_resolved, city) AS city
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
