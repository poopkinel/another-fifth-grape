"""Shared pytest fixtures.

A test DB path is set via FIFTH_GRAPE_DB before app.db is imported anywhere,
so tests can never accidentally touch the production DB. Each test that needs
a database gets its own fresh file in tmp_path via the `db` fixture.
"""

import os
import sys
import tempfile
from pathlib import Path

# Set the sentinel DB path BEFORE app.db is imported anywhere.
# app.db reads FIFTH_GRAPE_DB at module-load time.
_test_root = Path(tempfile.gettempdir()) / "fifth_grape_tests"
_test_root.mkdir(exist_ok=True)
os.environ.setdefault("FIFTH_GRAPE_DB", str(_test_root / "never_used.db"))

# Make the scripts/ directory importable so tests can reach backfill_canonical.
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root / "scripts"))
sys.path.insert(0, str(_repo_root))

import pytest


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A fresh, empty SQLite DB for each test, with the full schema applied."""
    db_path = tmp_path / "test.db"
    import app.db as app_db
    monkeypatch.setattr(app_db, "DB_PATH", str(db_path))
    app_db.init_db()
    yield db_path


@pytest.fixture
def seeded_db(db):
    """Seeds the `db` fixture with a compact dataset covering the canonical
    grouping edge cases used across tests.

    Group A  (canonical = milk_a):  milk_a ★, milk_b, milk_c
    Group   (singleton, no canonical): milk_other
    Stores:  store1/chain_x (passes), store2/chain_x ('not_at_address'),
             store3/chain_y ('verified')
    Prices:
      (store1,chain_x,milk_a) = 5.0  in_stock
      (store1,chain_x,milk_b) = 4.0  in_stock    ← dedup winner vs milk_a at store1
      (store3,chain_y,milk_c) = 6.0  in_stock
      (store2,chain_x,milk_a) = 3.0  in_stock   ← store2 should be filtered out
      (store1,chain_x,milk_other) = 10.0 in_stock
    """
    import app.db as app_db
    with app_db.get_conn() as conn:
        products = [
            ("milk_a",     "חלב 3% 1 ליטר", "טרה", "ליטר", "111", None, "milk 3%", "milk_a"),
            ("milk_b",     "טרה חלב 3% 1ל", "טרה", "ליטר", "222", None, "milk 3%", "milk_a"),
            ("milk_c",     "חלב בקרטון 3% 1", "טרה", "ליטר", "333", None, "milk 3%", "milk_a"),
            ("milk_other", "חלב אחר",        "תנובה", "ליטר", "444", None, None,    None),
        ]
        conn.executemany(
            "INSERT INTO products (product_id, name, brand, unit, barcode, emoji, "
            "category, canonical_product_id) VALUES (?,?,?,?,?,?,?,?)",
            products,
        )
        stores = [
            # (store_id, chain_id, chain_name, branch_name, address, city, lat, lng,
            #  geocode_status, verified_by_places, places_name)
            ("store1", "chain_x", "Chain X", "Branch 1", "Addr 1", "Tel Aviv", 32.0, 34.8, "ok", None,             None),
            ("store2", "chain_x", "Chain X", "Branch 2", "Addr 2", "Tel Aviv", 32.1, 34.8, "ok", "not_at_address", None),
            ("store3", "chain_y", "Chain Y", "Branch 3", "Addr 3", "Tel Aviv", 32.2, 34.8, "ok", "verified",       None),
        ]
        conn.executemany(
            "INSERT INTO stores (store_id, chain_id, chain_name, branch_name, "
            "address, city, lat, lng, geocode_status, verified_by_places, places_name) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            stores,
        )
        prices = [
            ("store1", "chain_x", "milk_a",     5.0,  1, "2026-04-20T12:00:00Z"),
            ("store1", "chain_x", "milk_b",     4.0,  1, "2026-04-20T12:00:00Z"),
            ("store3", "chain_y", "milk_c",     6.0,  1, "2026-04-20T12:00:00Z"),
            ("store2", "chain_x", "milk_a",     3.0,  1, "2026-04-20T12:00:00Z"),
            ("store1", "chain_x", "milk_other", 10.0, 1, "2026-04-20T12:00:00Z"),
        ]
        conn.executemany(
            "INSERT INTO prices (store_id, chain_id, product_id, price, in_stock, "
            "updated_at) VALUES (?,?,?,?,?,?)",
            prices,
        )
    return db
