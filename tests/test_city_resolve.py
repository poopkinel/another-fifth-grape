"""Verify CBS locality-code resolution end-to-end:
(1) init_db seeds the city_codes lookup from the bundled JSON,
(2) upsert_store populates city_resolved when raw <City> is a numeric code,
(3) geocoder SELECTs prefer city_resolved over the raw code.

Motivating case: Hazi Hinam ships <City>8300</City> = ראשון לציון."""

import app.db as app_db


def test_seed_loaded(db):
    """init_db should have populated city_codes from the bundled CBS file."""
    with app_db.get_conn() as conn:
        n = conn.execute("SELECT COUNT(*) FROM city_codes").fetchone()[0]
        assert n > 1000, "expected the full CBS list — got too few rows"
        # spot-check a few well-known mappings
        for code, expected_he in [
            ("8300", "ראשון לציון"),
            ("6600", "חולון"),
            ("8400", "רחובות"),
            ("5000", "תל אביב -יפו"),
            ("3000", "ירושלים"),
        ]:
            row = conn.execute(
                "SELECT name_he FROM city_codes WHERE code = ?", (code,)
            ).fetchone()
            assert row is not None, f"code {code} missing from seed"
            assert row[0] == expected_he, f"{code}: got {row[0]!r}, want {expected_he!r}"


def test_numeric_city_resolved_to_name_on_upsert(db):
    """A raw numeric <City> should be looked up and stored in city_resolved.
    The raw value stays in `city` for forensics."""
    with app_db.get_conn() as conn:
        app_db.upsert_store(conn, {
            "store_id": "hazi_hinam_207",
            "chain_id": "hazi_hinam",
            "chain_name": "Hazi Hinam",
            "branch_name": "Test branch",
            "address": "Some street 5",
            "city": "8300",
            "lat": None,
            "lng": None,
        })
        row = conn.execute(
            "SELECT city, city_resolved FROM stores WHERE store_id = ?",
            ("hazi_hinam_207",),
        ).fetchone()
    assert row[0] == "8300"
    assert row[1] == "ראשון לציון"


def test_place_name_city_passes_through(db):
    """A non-numeric <City> already has a place name — city_resolved stores
    the same string verbatim (no lookup)."""
    with app_db.get_conn() as conn:
        app_db.upsert_store(conn, {
            "store_id": "shufersal_001",
            "chain_id": "shufersal",
            "chain_name": "Shufersal",
            "branch_name": "Test branch",
            "address": "Dizengoff 50",
            "city": "תל אביב",
            "lat": None,
            "lng": None,
        })
        row = conn.execute(
            "SELECT city, city_resolved FROM stores WHERE store_id = ?",
            ("shufersal_001",),
        ).fetchone()
    assert row[0] == "תל אביב"
    assert row[1] == "תל אביב"


def test_unknown_numeric_code_leaves_resolved_null(db):
    """Numeric value not in the lookup → city_resolved is NULL. Geocoder will
    COALESCE down to raw `city`, where _is_numeric_city defangs it."""
    with app_db.get_conn() as conn:
        app_db.upsert_store(conn, {
            "store_id": "test_999",
            "chain_id": "test_chain",
            "chain_name": "Test",
            "branch_name": "Branch",
            "address": "Some address",
            "city": "9999999",  # not a real CBS code
            "lat": None,
            "lng": None,
        })
        row = conn.execute(
            "SELECT city, city_resolved FROM stores WHERE store_id = ?",
            ("test_999",),
        ).fetchone()
    assert row[0] == "9999999"
    assert row[1] is None


def test_geocoder_query_uses_resolved_name(db):
    """The geocoder reads `COALESCE(city_resolved, city) AS city`. Verify by
    running that exact SELECT against a row with a numeric raw city."""
    with app_db.get_conn() as conn:
        app_db.upsert_store(conn, {
            "store_id": "hazi_hinam_207",
            "chain_id": "hazi_hinam",
            "chain_name": "Hazi Hinam",
            "branch_name": "X",
            "address": "Some Addr 5",
            "city": "8300",
            "lat": None,
            "lng": None,
        })
        row = conn.execute(
            "SELECT address, COALESCE(city_resolved, city) AS city "
            "FROM stores WHERE store_id = ?", ("hazi_hinam_207",),
        ).fetchone()
    assert row["city"] == "ראשון לציון"
