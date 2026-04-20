"""Unit tests for the fingerprint function in scripts/backfill_canonical.py.

These lock the grouping behavior that matters most for coverage/correctness:
- milk-case merges (the original user-reported bug)
- size/unit semantics that prevent wrong merges (1L vs 2L, carton vs bag)
- orphan-numeric as variant code (cosmetic shade lines, bed sizes)
- Hebrew-specific quirks (abbreviations, attached prefixes, unit scaling)
"""

from backfill_canonical import fingerprint


# ── Positive merges: should share a fingerprint ──────────────────────────────

def test_milk_case_three_tara_variants_merge():
    fps = [
        fingerprint("טרה חלב בקרטון 3% 1ל",  "טרה", "ליטר"),
        fingerprint("טרה חלב בקרטון 3% 1",   "טרה", "ליטר"),
        fingerprint("חלב בקרטון 3% 1 ליטר",  "טרה", "ליטר"),
    ]
    assert fps[0] == fps[1] == fps[2]
    # And the size resolved to 1L, spec is carton+3%
    brand, base, spec, size, unit = fps[0]
    assert brand == "טרה"
    assert size == 1.0
    assert unit == "L"
    assert "3%" in spec
    # "בקרטון" should have its Heb prefix stripped to "קרטון"
    assert "קרטון" in spec


def test_hebrew_prefix_stripping_bekartn_equals_kartn():
    a = fingerprint("חלב בקרטון 3% 1 ליטר", "טרה", "ליטר")
    b = fingerprint("חלב קרטון 3% 1 ליטר",  "טרה", "ליטר")
    assert a == b


def test_unit_scaling_ml_to_liters():
    # 500 מ"ל should canonicalise to 0.5 L
    a = fingerprint("מרסס מבשם בדים 500 מ\"ל", "", None)
    b = fingerprint("מרסס מבשם בדים 0.5 ליטר", "", None)
    assert a[3] == 0.5
    assert a[4] == "L"
    assert a == b


def test_bare_name_number_plus_unit_column_fallback():
    # Name has a bare "1", unit column = "ליטר" → should resolve to 1L
    # (same group as a name that spells out "1 ליטר")
    a = fingerprint("טרה חלב בקרטון 3% 1", "טרה", "ליטר")
    b = fingerprint("חלב בקרטון 3% 1 ליטר", "טרה", "ליטר")
    assert a == b
    assert a[3] == 1.0


def test_bare_name_number_plus_ml_unit_scales_correctly():
    # The exact bug fixed today: name "500" + unit "100 מ\"ל" → 0.5 L, not 500 L
    fp = fingerprint("מרסס מבשם בדים 500", "", "100 מ\"ל")
    assert fp[3] == 0.5
    assert fp[4] == "L"


def test_grams_scale_to_kg():
    a = fingerprint("חומוס אסלי 400 גרם", "", None)
    b = fingerprint("חומוס אסלי 400 גר", "", None)
    assert a == b
    assert a[3] == 0.4
    assert a[4] == "kg"


# ── Negative separations: must NOT share a fingerprint ───────────────────────

def test_one_liter_vs_two_liter_stay_separate():
    a = fingerprint("חלב בקרטון 3% 1 ליטר", "טרה", "ליטר")
    b = fingerprint("חלב בקרטון 3% 2 ליטר", "טרה", "ליטר")
    assert a != b


def test_carton_vs_bag_stay_separate():
    a = fingerprint("טרה חלב בקרטון 3% 1 ליטר", "טרה", "ליטר")
    b = fingerprint("טרה חלב בשקית 3% 1 ליטר",  "טרה", "ליטר")
    assert a != b


def test_different_fat_percentages_stay_separate():
    a = fingerprint("חלב בקרטון 1% 1 ליטר", "טרה", "ליטר")
    b = fingerprint("חלב בקרטון 3% 1 ליטר", "טרה", "ליטר")
    assert a != b


def test_multipack_vs_single_stay_separate():
    # מארז (multipack) is a packaging spec, not a stopword
    a = fingerprint("מארז אוכמניות", "", None)
    b = fingerprint("אוכמניות", "", None)
    assert a != b


def test_orphan_numeric_keeps_lipstick_shades_distinct():
    # Two lipstick shades with identical names except trailing shade code.
    # Orphan numerics become spec variant codes, preventing the merge.
    a = fingerprint("שפתון 60", "", None)
    b = fingerprint("שפתון 20", "", None)
    c = fingerprint("שפתון 60", "", None)
    assert a != b
    assert a == c


def test_bed_sizes_stay_distinct_via_variant_code():
    a = fingerprint("מיטה עץ אורן 80", "", None)
    b = fingerprint("מיטה עץ אורן 90", "", None)
    assert a != b


# ── Normalisation edge cases ─────────────────────────────────────────────────

def test_abbreviation_mahad_expands_to_stopword():
    # "מהד" should expand to "מהדורה" and then be dropped as a stopword,
    # so a name with/without it yields the same base tokens (given same size).
    a = fingerprint("חלב בקרטון 3% 1 ליטר מהד", "טרה", "ליטר")
    b = fingerprint("חלב בקרטון 3% 1 ליטר",    "טרה", "ליטר")
    assert a == b


def test_brand_tokens_stripped_from_name():
    # Brand appearing IN the name should not pollute base tokens.
    a = fingerprint("טרה חלב בקרטון 3% 1 ליטר", "טרה", "ליטר")
    b = fingerprint("חלב בקרטון 3% 1 ליטר",    "טרה", "ליטר")
    assert a == b
