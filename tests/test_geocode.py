"""Verify the geocoder treats numeric-only city values as missing for query
construction and the components bias, while still letting place-name cities
through. The motivating case is Hazi Hinam's `<City>8300</City>` (a 4-digit
municipal code, not a name) — passing those to Google as
administrative_area would silently zero-result the lookup.
"""

import sys
from pathlib import Path

# make backend root importable so `import geocode` works
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from geocode import _is_numeric_city, build_query


def test_numeric_city_detected():
    assert _is_numeric_city("8300")
    assert _is_numeric_city("6600")
    assert _is_numeric_city("  8300  ")  # surrounding whitespace
    assert _is_numeric_city("0")


def test_place_name_city_not_numeric():
    assert not _is_numeric_city("תל אביב")
    assert not _is_numeric_city("Tel Aviv")
    assert not _is_numeric_city("Beit-Shemesh")
    assert not _is_numeric_city("בית שמש")
    assert not _is_numeric_city("")
    assert not _is_numeric_city(None)


def test_query_omits_numeric_city():
    """Numeric city goes nowhere in the textual query — address + Israel only."""
    q = build_query("רחוב הרצל 5", "8300")
    assert q == "רחוב הרצל 5, Israel"


def test_query_includes_place_name_city():
    q = build_query("רחוב הרצל 5", "תל אביב")
    assert q == "רחוב הרצל 5, תל אביב, Israel"


def test_query_with_empty_city():
    q = build_query("רחוב הרצל 5", "")
    assert q == "רחוב הרצל 5, Israel"
