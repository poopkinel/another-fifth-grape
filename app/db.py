"""SQLite database layer for storing scraped supermarket data."""

import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH = os.environ.get("FIFTH_GRAPE_DB", "data/fifth_grape.db")


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


# ── Write operations (used by scraper) ──────────────────────────────

def upsert_store(conn: sqlite3.Connection, store: dict):
    conn.execute("""
        INSERT INTO stores (store_id, chain_id, chain_name, branch_name, address, city, lat, lng)
        VALUES (:store_id, :chain_id, :chain_name, :branch_name, :address, :city, :lat, :lng)
        ON CONFLICT(store_id, chain_id) DO UPDATE SET
            chain_name  = excluded.chain_name,
            branch_name = excluded.branch_name,
            address     = excluded.address,
            city        = excluded.city,
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


def upsert_product(conn: sqlite3.Connection, product: dict):
    conn.execute("""
        INSERT INTO products (product_id, name, brand, unit, barcode, emoji, category)
        VALUES (:product_id, :name, :brand, :unit, :barcode, :emoji, :category)
        ON CONFLICT(product_id) DO UPDATE SET
            name     = excluded.name,
            brand    = excluded.brand,
            unit     = excluded.unit,
            barcode  = excluded.barcode,
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
