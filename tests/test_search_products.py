import app.db as app_db


def test_search_orders_by_store_count_desc(seeded_db):
    """Most popular SKU surfaces first, regardless of insertion / name order.

    seeded_db setup: milk_a has 2 price rows (store1+store2), milk_b has 1,
    milk_c has 1, milk_other has 1. So milk_a should top the חלב search.
    Tie-break by name (alphabetical) for stable ordering.
    """
    with app_db.get_conn() as conn:
        rows = app_db.search_products(conn, "חלב", limit=10)

    ids = [r["product_id"] for r in rows]
    assert ids[0] == "milk_a", f"expected milk_a (2 stores) first, got {ids}"
    assert set(ids) == {"milk_a", "milk_b", "milk_c", "milk_other"}


def test_search_returns_zero_count_for_pricedless_product(db):
    """Products with no price rows still return (store_count = 0), just last."""
    with app_db.get_conn() as conn:
        conn.execute(
            "INSERT INTO products (product_id, name, brand, unit, barcode) "
            "VALUES (?, ?, ?, ?, ?)",
            ("ghost", "חלב פנטומי", "X", "ליטר", "999"),
        )
        rows = app_db.search_products(conn, "חלב", limit=10)

    ghost = next((r for r in rows if r["product_id"] == "ghost"), None)
    assert ghost is not None
    assert ghost["store_count"] == 0
