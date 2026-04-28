"""SQLite database layer for storing scraped supermarket data."""

import json
import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH = os.environ.get("FIFTH_GRAPE_DB", "data/fifth_grape.db")

_CITY_CODES_SEED = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "scraper", "city_codes.json"
)


def _seed_city_codes() -> None:
    """Populate city_codes from the bundled CBS seed if the table is empty.
    `INSERT OR IGNORE` so calling this on a populated DB is a no-op except for
    new entries (e.g. if the seed file is updated)."""
    if not os.path.exists(_CITY_CODES_SEED):
        return
    with open(_CITY_CODES_SEED, encoding="utf-8") as f:
        rows = json.load(f)
    with get_conn() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO city_codes (code, name_he, name_en) VALUES (?, ?, ?)",
            [(r["code"], r["name_he"], r.get("name_en")) for r in rows],
        )


def _ensure_dir():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)


@contextmanager
def get_conn():
    _ensure_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if they don't exist."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS stores (
                store_id   TEXT NOT NULL,
                chain_id   TEXT NOT NULL,
                chain_name TEXT NOT NULL,
                branch_name TEXT NOT NULL,
                address    TEXT NOT NULL DEFAULT '',
                city       TEXT NOT NULL DEFAULT '',
                lat        REAL,
                lng        REAL,
                geocode_status TEXT,
                PRIMARY KEY (store_id, chain_id)
            );

            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                raw_name   TEXT,
                brand      TEXT,
                unit       TEXT,
                barcode    TEXT,
                emoji      TEXT,
                category   TEXT,
                image_url  TEXT
            );

            CREATE TABLE IF NOT EXISTS prices (
                store_id   TEXT NOT NULL,
                chain_id   TEXT NOT NULL,
                product_id TEXT NOT NULL,
                price      REAL NOT NULL,
                in_stock   INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (store_id, chain_id, product_id)
            );

            CREATE TABLE IF NOT EXISTS scrape_runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_id   TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status     TEXT NOT NULL DEFAULT 'running',
                error      TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_prices_product_id ON prices(product_id);

            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                distinct_id TEXT NOT NULL,
                event_name  TEXT NOT NULL,
                properties  TEXT NOT NULL DEFAULT '{}',
                client_ts   INTEGER NOT NULL,
                server_ts   INTEGER NOT NULL,
                app_version TEXT,
                platform    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_events_server_ts ON events(server_ts);
            CREATE INDEX IF NOT EXISTS idx_events_distinct_id ON events(distinct_id);
            CREATE INDEX IF NOT EXISTS idx_events_event_name ON events(event_name);

            CREATE TABLE IF NOT EXISTS promotions (
                promo_id         TEXT PRIMARY KEY,
                chain_id         TEXT NOT NULL,
                store_id         TEXT NOT NULL,
                promotion_id     TEXT NOT NULL,
                description      TEXT,
                start_at         TEXT,
                end_at           TEXT,
                reward_type      TEXT,
                discounted_price REAL,
                min_qty          REAL,
                min_purchase_amt REAL,
                update_date      TEXT,
                raw_json         TEXT,
                updated_at       TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_promotions_store ON promotions(store_id, chain_id);
            CREATE INDEX IF NOT EXISTS idx_promotions_end_at ON promotions(end_at);

            CREATE TABLE IF NOT EXISTS promotion_items (
                promo_id  TEXT NOT NULL,
                item_code TEXT NOT NULL,
                is_gift   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (promo_id, item_code),
                FOREIGN KEY (promo_id) REFERENCES promotions(promo_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_promotion_items_item ON promotion_items(item_code);

            -- Per-scrape per-CSV per-column counts of how the parser library
            -- handled each cell. rle_masked = library RLE-compressed it
            -- (recovered by our ffill), empty_count = source XML was empty,
            -- nonempty_count = literal value passed through. A regression in
            -- the upstream parser, or a feed format change, surfaces as a
            -- shift in these counts vs. the same chain's prior runs.
            CREATE TABLE IF NOT EXISTS parser_health (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                scrape_run_id  INTEGER NOT NULL,
                chain_id       TEXT NOT NULL,
                file_type      TEXT NOT NULL,
                csv_basename   TEXT NOT NULL,
                column_name    TEXT NOT NULL,
                rle_masked     INTEGER NOT NULL,
                empty_count    INTEGER NOT NULL,
                nonempty_count INTEGER NOT NULL,
                total_rows     INTEGER NOT NULL,
                recorded_at    TEXT NOT NULL,
                FOREIGN KEY (scrape_run_id) REFERENCES scrape_runs(id)
            );
            CREATE INDEX IF NOT EXISTS idx_parser_health_run ON parser_health(scrape_run_id);
            CREATE INDEX IF NOT EXISTS idx_parser_health_chain_type ON parser_health(chain_id, file_type);

            -- Israeli Central Bureau of Statistics locality codes ("סמלי
            -- יישובים"). Some chains (Hazi Hinam confirmed) ship <City> as a
            -- numeric code rather than a name; this lookup translates them.
            -- Seeded from app/scraper/city_codes.json by _seed_city_codes().
            CREATE TABLE IF NOT EXISTS city_codes (
                code    TEXT PRIMARY KEY,
                name_he TEXT NOT NULL,
                name_en TEXT
            );
        """)

        existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(stores)")}
        if "geocode_status" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN geocode_status TEXT")

        product_cols = {r[1] for r in conn.execute("PRAGMA table_info(products)")}
        if "raw_name" not in product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN raw_name TEXT")
        if "canonical_product_id" not in product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN canonical_product_id TEXT")
        if "image_url" not in product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN image_url TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_products_canonical "
            "ON products(canonical_product_id)"
        )

        if "verified_by_places" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN verified_by_places TEXT")
        if "places_name" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN places_name TEXT")
        # Forensic markers for the 2026-04-21 empty-city backfill + re-geocode.
        # Nullable everywhere; set only when inference / region-biased refetch
        # actually touched the row. Lets future debugging ask "did our inference
        # put this here?" without guessing.
        if "city_inferred_at" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN city_inferred_at TEXT")
        if "coords_refetched_at" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN coords_refetched_at TEXT")

        # 2026-04-28: city_resolved is the geocoder-friendly place name. For
        # rows whose source <City> is an Israeli locality code (e.g. Hazi
        # Hinam ships 8300 = ראשון לציון), this is the looked-up name.
        # NULL when no lookup match — geocoder falls back to raw `city`.
        if "city_resolved" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN city_resolved TEXT")

        # 2026-04-28: chain_id is per legal entity (corporate parent), but a
        # single chain_id often spans multiple consumer brands — Yeinot Bitan
        # publishes Carrefour, Sheli, Be'er, Quik etc. all under one chain_id.
        # The source XML carries the brand distinction in <SubChainId> and
        # <SubChainName>; we now persist both so the UI can color/filter by
        # actual brand instead of corporate parent.
        if "sub_chain_id" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN sub_chain_id TEXT")
        if "sub_chain_name" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN sub_chain_name TEXT")

        if "place_id" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN place_id TEXT")
        if "opening_hours_json" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN opening_hours_json TEXT")
        if "opening_hours_tz" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN opening_hours_tz TEXT")
        # Forensic marker: set only when fetch_hours.py touches the row, even
        # when Places returns no hours data (avoids re-querying unknowns).
        if "opening_hours_fetched_at" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN opening_hours_fetched_at TEXT")

    _seed_city_codes()


# ── Write operations (used by scraper) ──────────────────────────────

def upsert_store(conn: sqlite3.Connection, store: dict):
    # Resolve numeric municipal codes to place names. Caller passes raw <City>
    # in `city`; we look it up if it's digits-only and stash the resolved
    # name in city_resolved (NULL on lookup miss). Geocoder reads city_resolved.
    raw_city = (store.get("city") or "").strip()
    store["city_resolved"] = _resolve_city(conn, raw_city)
    store.setdefault("sub_chain_id", None)
    store.setdefault("sub_chain_name", None)
    conn.execute("""
        INSERT INTO stores (
            store_id, chain_id, chain_name, branch_name,
            sub_chain_id, sub_chain_name,
            address, city, city_resolved, lat, lng
        )
        VALUES (
            :store_id, :chain_id, :chain_name, :branch_name,
            :sub_chain_id, :sub_chain_name,
            :address, :city, :city_resolved, :lat, :lng
        )
        ON CONFLICT(store_id, chain_id) DO UPDATE SET
            chain_name     = excluded.chain_name,
            branch_name    = excluded.branch_name,
            sub_chain_id   = excluded.sub_chain_id,
            sub_chain_name = excluded.sub_chain_name,
            address        = excluded.address,
            city           = excluded.city,
            city_resolved  = excluded.city_resolved,
            lat = CASE
                WHEN stores.address = excluded.address AND stores.city = excluded.city
                THEN stores.lat ELSE NULL END,
            lng = CASE
                WHEN stores.address = excluded.address AND stores.city = excluded.city
                THEN stores.lng ELSE NULL END,
            geocode_status = CASE
                WHEN stores.address = excluded.address AND stores.city = excluded.city
                THEN stores.geocode_status ELSE NULL END
    """, store)


def _resolve_city(conn: sqlite3.Connection, raw_city: str) -> str | None:
    """If `raw_city` is digits-only (Israeli locality code), return its Hebrew
    name from city_codes; else return raw_city as-is. Empty input → None."""
    if not raw_city:
        return None
    if not raw_city.isdigit():
        return raw_city
    row = conn.execute(
        "SELECT name_he FROM city_codes WHERE code = ?", (raw_city,)
    ).fetchone()
    return row[0] if row else None


def upsert_product(conn: sqlite3.Connection, product: dict):
    # Sticky-good policy: never overwrite a non-empty name/brand/unit/barcode
    # with an empty or 'nan' value. Symptoms-level fix for parser data-loss for
    # specific chains (see data_source_issues.md §6) — keeps good upstream data
    # alive even when a later chain's pipeline path drops the field.
    conn.execute("""
        INSERT INTO products (product_id, name, brand, unit, barcode, emoji, category)
        VALUES (:product_id, :name, :brand, :unit, :barcode, :emoji, :category)
        ON CONFLICT(product_id) DO UPDATE SET
            name     = CASE WHEN excluded.name IN ('', 'nan')
                            THEN products.name
                            ELSE excluded.name END,
            brand    = COALESCE(NULLIF(excluded.brand, ''), products.brand),
            unit     = COALESCE(NULLIF(excluded.unit,  ''), products.unit),
            barcode  = COALESCE(excluded.barcode, products.barcode),
            emoji    = excluded.emoji,
            category = excluded.category
    """, product)


def upsert_price(conn: sqlite3.Connection, price: dict):
    conn.execute("""
        INSERT INTO prices (store_id, chain_id, product_id, price, in_stock, updated_at)
        VALUES (:store_id, :chain_id, :product_id, :price, :in_stock, :updated_at)
        ON CONFLICT(store_id, chain_id, product_id) DO UPDATE SET
            price      = excluded.price,
            in_stock   = excluded.in_stock,
            updated_at = excluded.updated_at
    """, price)


def upsert_promotion(conn: sqlite3.Connection, promo: dict):
    conn.execute("""
        INSERT INTO promotions (promo_id, chain_id, store_id, promotion_id,
                                description, start_at, end_at, reward_type,
                                discounted_price, min_qty, min_purchase_amt,
                                update_date, raw_json, updated_at)
        VALUES (:promo_id, :chain_id, :store_id, :promotion_id,
                :description, :start_at, :end_at, :reward_type,
                :discounted_price, :min_qty, :min_purchase_amt,
                :update_date, :raw_json, :updated_at)
        ON CONFLICT(promo_id) DO UPDATE SET
            description       = excluded.description,
            start_at          = excluded.start_at,
            end_at            = excluded.end_at,
            reward_type       = excluded.reward_type,
            discounted_price  = excluded.discounted_price,
            min_qty           = excluded.min_qty,
            min_purchase_amt  = excluded.min_purchase_amt,
            update_date       = excluded.update_date,
            raw_json          = excluded.raw_json,
            updated_at        = excluded.updated_at
    """, promo)


def replace_promotion_items(
    conn: sqlite3.Connection, promo_id: str, items: list[dict]
):
    """items: [{"item_code": str, "is_gift": int}]. Replaces all rows for the promo."""
    conn.execute("DELETE FROM promotion_items WHERE promo_id = ?", (promo_id,))
    if items:
        conn.executemany(
            "INSERT INTO promotion_items (promo_id, item_code, is_gift) VALUES (?, ?, ?)",
            [(promo_id, it["item_code"], int(it.get("is_gift", 0))) for it in items],
        )


# ── Read operations (used by API) ───────────────────────────────────

def search_products(
    conn: sqlite3.Connection, query: str, limit: int
) -> list[dict]:
    q = query.strip()
    if not q:
        return []
    terms = q.split()
    clauses = []
    params: list[str] = []
    for term in terms:
        like = f"%{term}%"
        clauses.append("(name LIKE ? OR brand LIKE ? OR barcode = ?)")
        params.extend([like, like, term])
    where = " AND ".join(clauses)
    params.append(limit)
    rows = conn.execute(
        f"SELECT * FROM products WHERE {where} ORDER BY name LIMIT ?",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def get_prices_for_products(
    conn: sqlite3.Connection, product_ids: list[str]
) -> list[dict]:
    if not product_ids:
        return []
    placeholders = ",".join("?" * len(product_ids))
    rows = conn.execute(
        f"SELECT * FROM prices WHERE product_id IN ({placeholders})",
        product_ids,
    ).fetchall()
    return [dict(r) for r in rows]


def get_products_by_ids(
    conn: sqlite3.Connection, product_ids: list[str]
) -> list[dict]:
    if not product_ids:
        return []
    placeholders = ",".join("?" * len(product_ids))
    rows = conn.execute(
        f"SELECT * FROM products WHERE product_id IN ({placeholders})",
        product_ids,
    ).fetchall()
    return [dict(r) for r in rows]


def get_stores_by_keys(
    conn: sqlite3.Connection, keys: list[tuple[str, str]]
) -> list[dict]:
    """keys: list of (store_id, chain_id) tuples.
    Excludes stores marked 'not_at_address' by Places verification.
    """
    if not keys:
        return []
    placeholders = ",".join("(?,?)" for _ in keys)
    flat = [v for pair in keys for v in pair]
    rows = conn.execute(
        f"""SELECT * FROM stores
            WHERE (store_id, chain_id) IN ({placeholders})
              AND (verified_by_places IS NULL OR verified_by_places != 'not_at_address')""",
        flat,
    ).fetchall()
    return [dict(r) for r in rows]


def get_canonical_groups(
    conn: sqlite3.Connection, product_ids: list[str]
) -> dict[str, list[str]]:
    """For each requested product_id, return the list of every product_id
    that shares its canonical group (the requested id is always included).

    A product_id whose canonical_product_id is NULL is its own group of one.
    A requested id that doesn't exist in the products table maps to [itself].
    """
    if not product_ids:
        return {}

    placeholders = ",".join("?" * len(product_ids))
    rows = conn.execute(
        f"""SELECT product_id,
                   COALESCE(canonical_product_id, product_id) AS canonical
              FROM products
             WHERE product_id IN ({placeholders})""",
        product_ids,
    ).fetchall()

    requested_to_canonical = {pid: pid for pid in product_ids}
    for r in rows:
        requested_to_canonical[r["product_id"]] = r["canonical"]

    canonicals = list(set(requested_to_canonical.values()))
    canon_placeholders = ",".join("?" * len(canonicals))
    expansion = conn.execute(
        f"""SELECT product_id,
                   COALESCE(canonical_product_id, product_id) AS canonical
              FROM products
             WHERE COALESCE(canonical_product_id, product_id)
                   IN ({canon_placeholders})""",
        canonicals,
    ).fetchall()

    members: dict[str, list[str]] = {}
    for r in expansion:
        members.setdefault(r["canonical"], []).append(r["product_id"])

    return {
        pid: members.get(canonical, [pid])
        for pid, canonical in requested_to_canonical.items()
    }


def get_last_scrape_time(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT finished_at FROM scrape_runs WHERE status='done' ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()
    return row["finished_at"] if row else None


def insert_events(
    conn: sqlite3.Connection,
    rows: list[tuple[str, str, str, int, int, str | None, str | None]],
) -> int:
    """Insert pre-validated event rows. Returns count inserted.

    Each row: (distinct_id, event_name, properties_json, client_ts, server_ts, app_version, platform).
    """
    if not rows:
        return 0
    conn.executemany(
        """INSERT INTO events
               (distinct_id, event_name, properties, client_ts, server_ts, app_version, platform)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


def prune_events_older_than(conn: sqlite3.Connection, cutoff_server_ts: int) -> int:
    cur = conn.execute(
        "DELETE FROM events WHERE server_ts < ?", (cutoff_server_ts,)
    )
    return cur.rowcount
