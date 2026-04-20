"""End-to-end tests for POST /v1/prices/lookup.

Uses FastAPI's TestClient against a seeded in-memory-ish DB. Verifies the
canonical expansion, price relabelling/dedup, store filtering, and the
EXPAND_CANONICAL flag.
"""

import importlib

from fastapi.testclient import TestClient


def _make_client(seeded_db, monkeypatch, expand: bool):
    """Re-import app.main with EXPAND_CANONICAL set to the desired value,
    so we pick up the flag evaluation at module-load."""
    monkeypatch.setenv("EXPAND_CANONICAL", "true" if expand else "false")
    import app.main
    importlib.reload(app.main)
    return TestClient(app.main.app)


def _post_lookup(client, product_ids):
    r = client.post("/v1/prices/lookup", json={"productIds": product_ids})
    assert r.status_code == 200, r.text
    return r.json()


# ── With EXPAND_CANONICAL=true (default) ─────────────────────────────────────

def test_expand_child_sees_full_group_prices(seeded_db, monkeypatch):
    """Requesting a child id should return prices from all group members."""
    client = _make_client(seeded_db, monkeypatch, expand=True)
    data = _post_lookup(client, ["milk_b"])

    price_map = {p["storeId"]: p for p in data["prices"]}
    assert set(price_map) == {"store1", "store2", "store3"}
    # All returned prices are relabelled to the requested id.
    assert all(p["productId"] == "milk_b" for p in data["prices"])


def test_dedup_picks_cheapest_in_stock_at_same_store(seeded_db, monkeypatch):
    """store1 has milk_a@5.0 AND milk_b@4.0; dedup must keep the 4.0 row."""
    client = _make_client(seeded_db, monkeypatch, expand=True)
    data = _post_lookup(client, ["milk_a"])

    store1 = next(p for p in data["prices"] if p["storeId"] == "store1")
    assert store1["price"] == 4.0


def test_not_at_address_store_filtered_from_stores_response(seeded_db, monkeypatch):
    """store2 is marked not_at_address; it must NOT appear in stores."""
    client = _make_client(seeded_db, monkeypatch, expand=True)
    data = _post_lookup(client, ["milk_a"])

    store_ids = {s["storeId"] for s in data["stores"]}
    assert "store2" not in store_ids
    assert {"store1", "store3"}.issubset(store_ids)


def test_singleton_product_returns_only_its_own_prices(seeded_db, monkeypatch):
    """milk_other has no canonical group; expansion is a no-op for it."""
    client = _make_client(seeded_db, monkeypatch, expand=True)
    data = _post_lookup(client, ["milk_other"])

    assert len(data["prices"]) == 1
    assert data["prices"][0]["productId"] == "milk_other"
    assert data["prices"][0]["price"] == 10.0


# ── With EXPAND_CANONICAL=false ──────────────────────────────────────────────

def test_expand_false_child_sees_only_its_own_prices(seeded_db, monkeypatch):
    """Flag off: milk_b has a single direct price (store1 @ 4.0). No expansion
    means milk_c's store3 row is NOT pulled in."""
    client = _make_client(seeded_db, monkeypatch, expand=False)
    data = _post_lookup(client, ["milk_b"])

    prices = data["prices"]
    assert len(prices) == 1
    assert prices[0]["storeId"] == "store1"
    assert prices[0]["productId"] == "milk_b"
    assert prices[0]["price"] == 4.0
    store_ids = {s["storeId"] for s in data["stores"]}
    assert store_ids == {"store1"}


def test_expand_false_canonical_sees_only_its_own_prices(seeded_db, monkeypatch):
    """Flag off: milk_a has prices only at (store1) and (store2).
    store2 is filtered. So only store1 remains, and milk_b's price is NOT included."""
    client = _make_client(seeded_db, monkeypatch, expand=False)
    data = _post_lookup(client, ["milk_a"])

    price_by_store = {p["storeId"]: p for p in data["prices"]}
    # store1 price is milk_a's 5.0 (NOT milk_b's cheaper 4.0 — no dedup across group)
    assert price_by_store["store1"]["price"] == 5.0
    # milk_c's store3 row should NOT appear
    assert "store3" not in price_by_store
