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
                PRIMARY KEY (store_id, chain_id)
            );

            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
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
                PRIMARY KEY (store_id, chain_id, product_id),
                FOREIGN KEY (store_id, chain_id) REFERENCES stores(store_id, chain_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            );

            CREATE TABLE IF NOT EXISTS scrape_runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_id   TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status     TEXT NOT NULL DEFAULT 'running',
                error      TEXT
            );
        """)


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
            lat         = excluded.lat,
            lng         = excluded.lng
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

def get_all_stores(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM stores").fetchall()
    return [dict(r) for r in rows]


def get_all_products(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM products").fetchall()
    return [dict(r) for r in rows]


def get_all_prices(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM prices").fetchall()
    return [dict(r) for r in rows]


def get_last_scrape_time(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT finished_at FROM scrape_runs WHERE status='done' ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()
    return row["finished_at"] if row else None
