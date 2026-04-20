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
                category   TEXT
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
        """)

        existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(stores)")}
        if "geocode_status" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN geocode_status TEXT")

        product_cols = {r[1] for r in conn.execute("PRAGMA table_info(products)")}
        if "raw_name" not in product_cols:
            conn.execute("ALTER TABLE products ADD COLUMN raw_name TEXT")

        if "verified_by_places" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN verified_by_places TEXT")
        if "places_name" not in existing_cols:
            conn.execute("ALTER TABLE stores ADD COLUMN places_name TEXT")


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


def get_last_scrape_time(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT finished_at FROM scrape_runs WHERE status='done' ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()
    return row["finished_at"] if row else None
