"""Integration tests for db.get_canonical_groups against a seeded temp DB."""


def test_child_input_expands_to_full_group(seeded_db):
    import app.db as app_db
    with app_db.get_conn() as conn:
        result = app_db.get_canonical_groups(conn, ["milk_b"])
    assert set(result["milk_b"]) == {"milk_a", "milk_b", "milk_c"}


def test_canonical_input_returns_same_group(seeded_db):
    """Symmetric: passing the canonical id yields the same member set."""
    import app.db as app_db
    with app_db.get_conn() as conn:
        from_child = app_db.get_canonical_groups(conn, ["milk_b"])
        from_canon = app_db.get_canonical_groups(conn, ["milk_a"])
    assert set(from_child["milk_b"]) == set(from_canon["milk_a"])


def test_singleton_product_is_its_own_group(seeded_db):
    import app.db as app_db
    with app_db.get_conn() as conn:
        result = app_db.get_canonical_groups(conn, ["milk_other"])
    assert result["milk_other"] == ["milk_other"]


def test_nonexistent_id_maps_to_itself(seeded_db):
    import app.db as app_db
    with app_db.get_conn() as conn:
        result = app_db.get_canonical_groups(conn, ["does_not_exist"])
    assert result["does_not_exist"] == ["does_not_exist"]


def test_mixed_inputs_each_resolve_correctly(seeded_db):
    import app.db as app_db
    with app_db.get_conn() as conn:
        result = app_db.get_canonical_groups(
            conn, ["milk_a", "milk_other", "does_not_exist"]
        )
    assert set(result["milk_a"]) == {"milk_a", "milk_b", "milk_c"}
    assert result["milk_other"] == ["milk_other"]
    assert result["does_not_exist"] == ["does_not_exist"]


def test_empty_input_returns_empty_dict(seeded_db):
    import app.db as app_db
    with app_db.get_conn() as conn:
        assert app_db.get_canonical_groups(conn, []) == {}
