#!/usr/bin/env python3
"""Verify which chains actually operate at each duplicate-address cluster,
using Google Places API as ground truth.

Logic:
  1. Find clusters of stores sharing (address, city) — these are suspicious.
  2. For each unique cluster, query Places Text Search for supermarkets at
     that address.
  3. Match returned business names against our chain_id list.
  4. For each store in the cluster: mark 'verified' if its chain is in the
     Places result, 'not_at_address' if Places confirmed supermarkets there
     but this chain isn't among them, 'unknown' otherwise.

Usage:
    python verify_stores.py                   # never-checked clusters (original)
    python verify_stores.py --limit 20        # smoke test
    python verify_stores.py --dry-run         # query without writing
    python verify_stores.py --retry-unknown   # re-verify clusters that have any
                                              # rows currently marked 'unknown'
                                              # (e.g. previously failed because
                                              #  city was empty — now backfilled).
                                              # Overwrites ONLY unknown rows;
                                              # leaves 'verified' and
                                              # 'not_at_address' untouched.

On a --retry-unknown pass, clusters whose cities were just inferred today are
processed first (they're the likeliest to flip, since their previous failure
was the empty-city bug).
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

logger = logging.getLogger("verify_stores")

PLACES_TEXT_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
QPS_DELAY = 0.1  # Places allows much higher QPS than Geocoding
DEFAULT_NEARBY_RADIUS_M = 100

# Normalize common variants in Israeli city names before querying Places.
CITY_NORMALIZATIONS: dict[str, str] = {
    "קריתגת": "קרית גת",
    "קרית גת": "קרית גת",
    "פתח תקוה": "פתח תקווה",
    'פ"ת': "פתח תקווה",
    'ת"א': "תל אביב",
    "תל-אביב": "תל אביב",
    "תל אביב-יפו": "תל אביב",
    "ת״א": "תל אביב",
    "באר-שבע": "באר שבע",
    "ירושליים": "ירושלים",
    "בני-ברק": "בני ברק",
    "ראשון-לציון": "ראשון לציון",
    "רמת-גן": "רמת גן",
    "כפר-סבא": "כפר סבא",
}


def normalize_city(city: str) -> str:
    c = (city or "").strip()
    return CITY_NORMALIZATIONS.get(c, c)

# Map Google business-name keywords → our chain_id.
# Ordered: longer/more-specific keywords first so they win ties.
CHAIN_NAME_MAP: list[tuple[str, str]] = [
    # Shufersal family (umbrella of several store formats)
    ("shufersal", "shufersal"),
    ("שופרסל", "shufersal"),
    ("יש חסד", "shufersal"),
    ("yesh chesed", "shufersal"),
    ("יש ", "shufersal"),
    ("שלי ", "shufersal"),
    ("אקספרס ", "shufersal"),
    ("דיל ", "shufersal"),
    ("יוניברס", "shufersal"),
    ("גוד מרקט", "shufersal"),
    ("good market", "shufersal"),
    # Rami Levy family
    ("rami levy", "rami_levy"),
    ("rami-levy", "rami_levy"),
    ("רמי לוי", "rami_levy"),
    # Victory
    ("victory", "victory"),
    ("ויקטורי", "victory"),
    # Yohananof
    ("yohananof", "yohananof"),
    ("יוחננוף", "yohananof"),
    # Osher Ad
    ("osher ad", "osher_ad"),
    ("אושר עד", "osher_ad"),
    # Tiv Taam
    ("tiv taam", "tiv_taam"),
    ("tiv-taam", "tiv_taam"),
    ("טיב טעם", "tiv_taam"),
    # Yeinot Bitan + Carrefour
    ("yeinot bitan", "yeinot_bitan"),
    ("יינות ביתן", "yeinot_bitan"),
    ("carrefour", "yeinot_bitan"),
    ("קרפור", "yeinot_bitan"),
    # Hazi Hinam
    ("hazi hinam", "hazi_hinam"),
    ("חצי חינם", "hazi_hinam"),
    # Mahsani Hashuk
    ("mahsanei", "mahsani_hashuk"),
    ("mahsani", "mahsani_hashuk"),
    ("מחסני השוק", "mahsani_hashuk"),
    # Super-Pharm
    ("super-pharm", "super_pharm"),
    ("super pharm", "super_pharm"),
    ("superpharm", "super_pharm"),
    ("סופר-פארם", "super_pharm"),
    ("סופר פארם", "super_pharm"),
]
CHAIN_NAME_MAP.sort(key=lambda kv: -len(kv[0]))


def match_chain(business_name: str) -> str | None:
    """Return chain_id if name matches a known chain, else None."""
    if not business_name:
        return None
    lower = business_name.lower()
    for keyword, chain_id in CHAIN_NAME_MAP:
        if keyword.lower() in lower:
            return chain_id
    return None


def _parse_results(data: dict) -> list[dict]:
    status = data.get("status")
    if status == "OK":
        return [{"name": r.get("name", ""), "place_id": r.get("place_id", "")}
                for r in data.get("results", [])]
    if status == "ZERO_RESULTS":
        return []
    if status in ("OVER_QUERY_LIMIT", "REQUEST_DENIED", "INVALID_REQUEST"):
        raise RuntimeError(f"Places API error: {status} — {data.get('error_message', '')}")
    logger.warning("Places returned unexpected status %s", status)
    return []


def query_text(session: requests.Session, api_key: str, query: str) -> list[dict]:
    """Places Text Search by address string."""
    resp = session.get(PLACES_TEXT_URL, params={
        "query": query,
        "key": api_key,
        "region": "il",
        "language": "he",
        "type": "supermarket",
    }, timeout=15)
    resp.raise_for_status()
    return _parse_results(resp.json())


def query_nearby(session: requests.Session, api_key: str,
                 lat: float, lng: float, radius: int) -> list[dict]:
    """Places Nearby Search by lat/lng + radius."""
    resp = session.get(PLACES_NEARBY_URL, params={
        "location": f"{lat},{lng}",
        "radius": radius,
        "key": api_key,
        "language": "he",
        "type": "supermarket",
    }, timeout=15)
    resp.raise_for_status()
    return _parse_results(resp.json())


def main():
    parser = argparse.ArgumentParser(description="Verify stores via Google Places")
    parser.add_argument("--limit", type=int, default=None, help="Max clusters to query")
    parser.add_argument("--dry-run", action="store_true", help="Query without writing")
    parser.add_argument(
        "--retry-unknown", action="store_true",
        help="Re-verify clusters that contain any 'unknown' rows. Overwrites "
             "only the 'unknown' rows; keeps 'verified' and 'not_at_address' "
             "decisions intact. City-inferred-today clusters go first.",
    )
    parser.add_argument(
        "--only-all-unknown", action="store_true",
        help="Narrow --retry-unknown to clusters where EVERY row is unknown "
             "(skip partially-resolved clusters). Use with --nearby-radius to "
             "give Places a wider net on the hard cases.",
    )
    parser.add_argument(
        "--nearby-radius", type=int, default=DEFAULT_NEARBY_RADIUS_M,
        help=f"Radius in metres for Places Nearby Search (default {DEFAULT_NEARBY_RADIUS_M})",
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

    # Find duplicate-address clusters to verify.
    #   default mode    — clusters never checked (all rows have verified_by_places IS NULL).
    #   --retry-unknown — clusters where at least one row is currently 'unknown'.
    #                     We re-query because many of these failed the first time
    #                     with empty city, which today's backfill fixed. Order
    #                     city-inferred-today clusters first so the most-likely-
    #                     to-flip work gets done even under --limit.
    if args.retry_unknown:
        # --only-all-unknown narrows to clusters where every row is unknown.
        # These are the hardest cases (Places found nothing on the previous
        # pass) and usually worth pairing with a wider --nearby-radius.
        unknown_cond = (
            "SUM(CASE WHEN verified_by_places = 'unknown' THEN 1 ELSE 0 END) = COUNT(*)"
            if args.only_all_unknown else
            "SUM(CASE WHEN verified_by_places = 'unknown' THEN 1 ELSE 0 END) > 0"
        )
        clusters_sql = f"""
            SELECT address, city,
                   MAX(CASE WHEN city_inferred_at IS NOT NULL THEN 1 ELSE 0 END) AS city_new
            FROM stores
            WHERE address != ''
            GROUP BY address, city
            HAVING COUNT(*) > 1
               AND {unknown_cond}
            ORDER BY city_new DESC, address, city
        """
    else:
        clusters_sql = """
            SELECT address, city
            FROM stores
            WHERE address != ''
            GROUP BY address, city
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN verified_by_places IS NOT NULL THEN 1 ELSE 0 END) = 0
            ORDER BY address, city
        """
    with get_conn() as conn:
        if args.limit:
            clusters_sql += f" LIMIT {int(args.limit)}"
        clusters = conn.execute(clusters_sql).fetchall()

    if not clusters:
        print("No unverified duplicate-address clusters.")
        return

    print(f"Verifying {len(clusters)} clusters against Google Places ...")
    session = requests.Session()

    n_verified = n_dropped = n_unknown = 0

    for cluster in tqdm(clusters, unit="cluster"):
        address, city = cluster["address"], cluster["city"]
        norm_city = normalize_city(city)
        text_query = f"{address}, {norm_city}, Israel" if norm_city else f"{address}, Israel"

        # Fetch all stores in this cluster once; we also need lat/lng for Nearby Search.
        # In retry-unknown mode we still want ALL rows in the cluster (even verified
        # ones), because the Places result applies cluster-wide — but we'll filter
        # updates so we only touch rows currently 'unknown'.
        with get_conn() as conn:
            stores = conn.execute(
                "SELECT chain_id, store_id, lat, lng, verified_by_places FROM stores "
                "WHERE address = ? AND city = ?",
                (address, city),
            ).fetchall()

        # Query both Places endpoints and union the chain sets
        chains_at_address: set[str] = set()
        name_by_chain: dict[str, str] = {}
        had_any_result = False

        try:
            text_results = query_text(session, api_key, text_query)
        except requests.RequestException as e:
            logger.warning("Text Search error for %r: %s", text_query, e)
            text_results = []
        except RuntimeError as e:
            sys.exit(f"FATAL: {e}")

        if text_results:
            had_any_result = True
            for r in text_results:
                cid = match_chain(r["name"])
                if cid:
                    chains_at_address.add(cid)
                    name_by_chain.setdefault(cid, r["name"])

        # Nearby Search: use any store's lat/lng in the cluster (they all share the same coord)
        lat = next((s["lat"] for s in stores if s["lat"] is not None), None)
        lng = next((s["lng"] for s in stores if s["lng"] is not None), None)
        if lat is not None and lng is not None:
            time.sleep(QPS_DELAY)
            try:
                nearby_results = query_nearby(session, api_key, lat, lng, args.nearby_radius)
            except requests.RequestException as e:
                logger.warning("Nearby Search error at (%s,%s): %s", lat, lng, e)
                nearby_results = []
            except RuntimeError as e:
                sys.exit(f"FATAL: {e}")

            if nearby_results:
                had_any_result = True
                for r in nearby_results:
                    cid = match_chain(r["name"])
                    if cid:
                        chains_at_address.add(cid)
                        name_by_chain.setdefault(cid, r["name"])

        updates: list[tuple[str, str | None, str, str]] = []
        if not had_any_result:
            # Neither Google method returned anything — mark 'unknown'.
            for s in stores:
                if args.retry_unknown and s["verified_by_places"] != "unknown":
                    continue  # keep existing verified / not_at_address decisions
                updates.append(("unknown", None, s["chain_id"], s["store_id"]))
                n_unknown += 1
        else:
            for s in stores:
                if args.retry_unknown and s["verified_by_places"] != "unknown":
                    continue  # keep existing verified / not_at_address decisions
                if s["chain_id"] in chains_at_address:
                    updates.append(("verified", name_by_chain[s["chain_id"]],
                                    s["chain_id"], s["store_id"]))
                    n_verified += 1
                else:
                    updates.append(("not_at_address", None,
                                    s["chain_id"], s["store_id"]))
                    n_dropped += 1

        if not args.dry_run:
            with get_conn() as conn:
                conn.executemany(
                    "UPDATE stores SET verified_by_places = ?, places_name = ? "
                    "WHERE chain_id = ? AND store_id = ?",
                    updates,
                )
        if args.verbose or args.dry_run:
            for status, name, chain_id, store_id in updates:
                logger.info("  [%s] %s/%s → %s", status, chain_id, store_id, name)

        time.sleep(QPS_DELAY)

    print(f"\nDone.")
    print(f"  verified:       {n_verified}")
    print(f"  not_at_address: {n_dropped}")
    print(f"  unknown:        {n_unknown}")


if __name__ == "__main__":
    main()
