#!/usr/bin/env python3
"""Enrich products from Open Food Facts (OFF) by barcode.

Fetches brand, categories, and image URL for each barcoded product.
Fully resumable — tracks which barcodes have been checked so re-runs
skip already-processed products.

Usage:
    python enrich_off.py                  # process all unchecked products
    python enrich_off.py --limit 500      # cap for testing
    python enrich_off.py --dry-run        # query but don't write to DB
    python enrich_off.py --stats          # show coverage stats and exit
"""

import argparse
import logging
import time

import requests

from app.db import get_conn, init_db

logger = logging.getLogger("enrich_off")

OFF_API = "https://world.openfoodfacts.org/api/v2/product"
USER_AGENT = "FifthGrape/1.0 (grocery comparison app; contact: dev@fifth-grape.local)"
QPS_DELAY = 1.0  # ~1 req/sec, conservative for OFF rate limits

# OFF categories → emoji mapping (broad categories first)
CATEGORY_EMOJI: list[tuple[str, str]] = [
    ("milk", "🥛"),
    ("dairy", "🥛"),
    ("yogurt", "🥛"),
    ("cheese", "🧀"),
    ("butter", "🧈"),
    ("cream", "🥛"),
    ("egg", "🥚"),
    ("bread", "🍞"),
    ("pastri", "🥐"),
    ("chicken", "🍗"),
    ("poultry", "🍗"),
    ("beef", "🥩"),
    ("meat", "🥩"),
    ("pork", "🥩"),
    ("sausage", "🌭"),
    ("fish", "🐟"),
    ("seafood", "🐟"),
    ("salmon", "🐟"),
    ("tuna", "🐟"),
    ("fruit", "🍎"),
    ("vegetable", "🥬"),
    ("salad", "🥗"),
    ("beer", "🍺"),
    ("wine", "🍷"),
    ("spirit", "🥃"),
    ("coffee", "☕"),
    ("tea", "🍵"),
    ("juice", "🧃"),
    ("water", "💧"),
    ("soda", "🥤"),
    ("beverage", "🥤"),
    ("soft drink", "🥤"),
    ("chocolate", "🍫"),
    ("candy", "🍬"),
    ("sweet", "🍬"),
    ("cookie", "🍪"),
    ("biscuit", "🍪"),
    ("chip", "🍟"),
    ("crisp", "🍟"),
    ("snack", "🍿"),
    ("ice cream", "🍦"),
    ("frozen", "🧊"),
    ("rice", "🍚"),
    ("pasta", "🍝"),
    ("noodle", "🍝"),
    ("cereal", "🥣"),
    ("flour", "🌾"),
    ("sauce", "🥫"),
    ("ketchup", "🥫"),
    ("canned", "🥫"),
    ("preserve", "🥫"),
    ("spread", "🥜"),
    ("jam", "🍯"),
    ("honey", "🍯"),
    ("nut", "🥜"),
    ("oil", "🫒"),
    ("spice", "🧂"),
    ("seasoning", "🧂"),
    ("salt", "🧂"),
    ("baby", "👶"),
    ("infant", "👶"),
    ("pet food", "🐕"),
    ("cleaning", "🧹"),
    ("detergent", "🧺"),
    ("soap", "🧴"),
    ("shampoo", "🧴"),
    ("cosmetic", "🧴"),
    ("hygiene", "🧴"),
    ("toothpaste", "🪥"),
    ("paper", "🧻"),
    ("tissue", "🧻"),
    ("pizza", "🍕"),
]


def _ensure_off_table():
    """Create tracking table for OFF lookups."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS off_lookups (
                product_id TEXT PRIMARY KEY,
                status     TEXT NOT NULL,
                checked_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)


def _emoji_from_categories(categories_str: str) -> str | None:
    """Map OFF categories string to an emoji."""
    cats = categories_str.lower()
    for keyword, emoji in CATEGORY_EMOJI:
        if keyword in cats:
            return emoji
    return None


def _shorten_categories(categories_str: str) -> str:
    """Pick the most specific (last) category for storage."""
    parts = [c.strip() for c in categories_str.split(",")]
    # Remove language prefixes like "en:" "he:" "fr:"
    cleaned = []
    for p in parts:
        if ":" in p:
            p = p.split(":", 1)[1]
        cleaned.append(p.strip())
    return cleaned[-1] if cleaned else categories_str


# Fields we ask OFF to return. Trims response payload + ensures we get every
# image variant we know how to consume.
OFF_FIELDS = ",".join([
    "brands",
    "categories",
    "image_front_url",
    "image_front_he_url",
    "image_front_en_url",
    "image_url",
    "selected_images",
])


def _pick_image_url(product: dict) -> str | None:
    """Best front image URL from an OFF product dict, Hebrew-first cascade.

    Order: image_front_he_url → image_front_url (OFF's default-language pick)
    → image_front_en_url → selected_images.front.display.{he,en,*} → image_url
    (last resort; may be a non-front role).
    """
    candidates: list[str | None] = [
        product.get("image_front_he_url"),
        product.get("image_front_url"),
        product.get("image_front_en_url"),
    ]
    selected = product.get("selected_images") or {}
    front_display = (selected.get("front") or {}).get("display") or {}
    candidates.append(front_display.get("he"))
    candidates.append(front_display.get("en"))
    for lang, url in front_display.items():
        if lang not in ("he", "en"):
            candidates.append(url)
    candidates.append(product.get("image_url"))

    for c in candidates:
        if isinstance(c, str):
            c = c.strip()
            if c:
                return c
    return None


def fetch_product(session: requests.Session, barcode: str) -> dict | None:
    """Query OFF for a single barcode. Returns parsed fields or None."""
    url = f"{OFF_API}/{barcode}.json"
    resp = session.get(url, params={"fields": OFF_FIELDS}, timeout=15)
    if resp.status_code == 404:
        return None
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 5))
        logger.info("Rate limited, sleeping %ds", retry_after)
        time.sleep(retry_after)
        return fetch_product(session, barcode)  # single retry
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != 1:
        return None

    product = data.get("product", {})
    brand = product.get("brands", "").strip() or None
    categories = product.get("categories", "").strip() or None
    image_url = _pick_image_url(product)

    if not any([brand, categories, image_url]):
        return None

    return {
        "brand": brand.split(",")[0].strip() if brand else None,
        "categories": categories,
        "category": _shorten_categories(categories) if categories else None,
        "emoji": _emoji_from_categories(categories) if categories else None,
        "image_url": image_url,
    }


def show_stats():
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM products WHERE barcode IS NOT NULL").fetchone()[0]
        checked = conn.execute("SELECT COUNT(*) FROM off_lookups").fetchone()[0]
        found = conn.execute("SELECT COUNT(*) FROM off_lookups WHERE status='found'").fetchone()[0]
        not_found = conn.execute("SELECT COUNT(*) FROM off_lookups WHERE status='not_found'").fetchone()[0]

        has_brand = conn.execute("SELECT COUNT(*) FROM products WHERE brand IS NOT NULL AND brand != ''").fetchone()[0]
        has_emoji = conn.execute("SELECT COUNT(*) FROM products WHERE emoji IS NOT NULL").fetchone()[0]
        has_cat = conn.execute("SELECT COUNT(*) FROM products WHERE category IS NOT NULL").fetchone()[0]
        has_img = conn.execute("SELECT COUNT(*) FROM products WHERE image_url IS NOT NULL").fetchone()[0]

        # Popularity-weighted image coverage: of all (product, store) price rows,
        # how many resolve to a product that has an image? Better proxy for
        # "what fraction of basket-adds will show an image" than raw catalog %.
        weighted = conn.execute("""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN p.image_url IS NOT NULL THEN 1 ELSE 0 END) AS with_image
            FROM prices pr
            JOIN products p ON p.product_id = pr.product_id
        """).fetchone()
        total_rows = weighted["total_rows"] or 0
        with_image = weighted["with_image"] or 0

    print(f"OFF lookup progress: {checked}/{total} checked ({100*checked/total:.1f}%)")
    print(f"  found:     {found}")
    print(f"  not_found: {not_found}")
    print(f"  remaining: {total - checked}")
    print()
    print(f"Product coverage:")
    print(f"  brand:     {has_brand}/{total} ({100*has_brand/total:.1f}%)")
    print(f"  emoji:     {has_emoji}/{total} ({100*has_emoji/total:.1f}%)")
    print(f"  category:  {has_cat}/{total} ({100*has_cat/total:.1f}%)")
    print(f"  image:     {has_img}/{total} ({100*has_img/total:.1f}%)")
    if total_rows:
        print(f"  image (popularity-weighted): {with_image}/{total_rows} "
              f"({100*with_image/total_rows:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="Enrich products from Open Food Facts")
    parser.add_argument("--limit", type=int, default=None, help="Max products to query this run")
    parser.add_argument("--dry-run", action="store_true", help="Query but don't write")
    parser.add_argument("--stats", action="store_true", help="Show coverage stats and exit")
    parser.add_argument(
        "--refresh-images",
        action="store_true",
        help="Re-query OFF for already-found products that have no image_url yet (backfill).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    init_db()
    _ensure_off_table()

    if args.stats:
        show_stats()
        return

    # Pick rows to query.
    # Default: barcoded products never checked yet (no off_lookups row).
    # With --refresh-images: also include previously-found rows that still
    # have no image_url, so the backfill picks them up.
    # Order by store-count DESC so popular SKUs get covered first — even a
    # partial run hits the products users actually see.
    popularity_cte = """
        WITH popularity AS (
            SELECT product_id, COUNT(*) AS store_count
            FROM prices
            GROUP BY product_id
        )
    """
    if args.refresh_images:
        sql = popularity_cte + """
            SELECT p.product_id, p.barcode, p.brand, p.emoji, p.category, p.image_url,
                   COALESCE(pop.store_count, 0) AS store_count
            FROM products p
            LEFT JOIN off_lookups o ON p.product_id = o.product_id
            LEFT JOIN popularity pop ON p.product_id = pop.product_id
            WHERE p.barcode IS NOT NULL
              AND (
                  o.product_id IS NULL
                  OR (o.status = 'found' AND p.image_url IS NULL)
              )
            ORDER BY store_count DESC, p.product_id
        """
    else:
        sql = popularity_cte + """
            SELECT p.product_id, p.barcode, p.brand, p.emoji, p.category, p.image_url,
                   COALESCE(pop.store_count, 0) AS store_count
            FROM products p
            LEFT JOIN off_lookups o ON p.product_id = o.product_id
            LEFT JOIN popularity pop ON p.product_id = pop.product_id
            WHERE p.barcode IS NOT NULL AND o.product_id IS NULL
            ORDER BY store_count DESC, p.product_id
        """
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"

    with get_conn() as conn:
        rows = conn.execute(sql).fetchall()

    if not rows:
        print("All barcoded products have been checked.")
        show_stats()
        return

    print(f"Querying Open Food Facts for {len(rows)} products ...")
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    n_found = n_not_found = n_err = 0

    for i, row in enumerate(rows):
        barcode = row["barcode"]
        try:
            result = fetch_product(session, barcode)
        except requests.RequestException as e:
            logger.warning("HTTP error for %s: %s", barcode, e)
            n_err += 1
            time.sleep(2.0)
            continue

        if result is None:
            n_not_found += 1
            if not args.dry_run:
                with get_conn() as conn:
                    conn.execute(
                        "INSERT OR IGNORE INTO off_lookups (product_id, status) VALUES (?, 'not_found')",
                        (row["product_id"],),
                    )
                    conn.execute(
                        "UPDATE products SET image_tried_at = datetime('now') WHERE product_id = ?",
                        (row["product_id"],),
                    )
        else:
            n_found += 1
            if args.dry_run:
                logger.info("FOUND %s [%d stores]: brand=%s cat=%s emoji=%s img=%s",
                            barcode, row["store_count"], result["brand"], result["category"],
                            result["emoji"], bool(result["image_url"]))
            else:
                with get_conn() as conn:
                    # Only fill fields that are currently empty (image_tried_at
                    # is always refreshed below).
                    updates = {}
                    sets = ["image_tried_at = datetime('now')"]
                    if (not row["brand"]) and result["brand"]:
                        sets.append("brand = :brand")
                        updates["brand"] = result["brand"]
                    if (not row["emoji"]) and result["emoji"]:
                        sets.append("emoji = :emoji")
                        updates["emoji"] = result["emoji"]
                    if (not row["category"]) and result["category"]:
                        sets.append("category = :category")
                        updates["category"] = result["category"]
                    if result["image_url"] and (
                        "image_url" not in row.keys() or not row["image_url"]
                    ):
                        sets.append("image_url = :image_url")
                        updates["image_url"] = result["image_url"]

                    updates["product_id"] = row["product_id"]
                    conn.execute(
                        f"UPDATE products SET {', '.join(sets)} WHERE product_id = :product_id",
                        updates,
                    )

                    conn.execute(
                        "INSERT OR IGNORE INTO off_lookups (product_id, status) VALUES (?, 'found')",
                        (row["product_id"],),
                    )

        # Progress every 500
        total_done = n_found + n_not_found + n_err
        if total_done % 500 == 0:
            elapsed_pct = 100 * total_done / len(rows)
            print(f"  [{elapsed_pct:5.1f}%] checked={total_done}  found={n_found}  not_found={n_not_found}  errors={n_err}")

        time.sleep(QPS_DELAY)

    print(f"\nDone. found={n_found}  not_found={n_not_found}  errors={n_err}")
    if not args.dry_run:
        show_stats()


if __name__ == "__main__":
    main()
