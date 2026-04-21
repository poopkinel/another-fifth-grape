"""Unit tests for scripts/infer_city.infer_city.

Lock in the behavior that matters for the 2026-04-21 empty-city backfill:
- Abbreviations with gershayim resolve to the right city
- Full-city substring match in branch_name uses word boundaries (single-word
  cities don't latch onto longer words — e.g. "נשר" inside "כנפי נשרים")
- Address matching only trusts canonical positions (comma-delimited or
  exact-match), so "שדרות דב יוסף" (boulevard) is NOT read as city Sderot
- Hebrew spelling variants normalise (קריית ↔ קרית)
- Genuine unresolvable cases (neighborhoods, online stores) return None
"""

from infer_city import infer_city


# ── Abbreviations ────────────────────────────────────────────────────────────

def test_abbrev_tel_aviv_in_branch():
    assert infer_city('אקספרס ת"א- פלורנטין', 'פלורנטין 27') == ("תל אביב", 'abbrev:ת"א')


def test_abbrev_petah_tikva_in_branch():
    assert infer_city('קרפור עמישב פ"ת (2750)', "")[0] == "פתח תקווה"


def test_abbrev_petah_tikva_after_comma_in_address():
    # Address is a street named "ראשון לציון"; the real city is after the comma.
    # The pre-fix inference would have latched onto ראשון לציון; the fix picks פ"ת.
    city, source = infer_city("כל בו חצי חינם אם המושבות", 'ראשון לציון 1 , פ"ת , ישראל')
    assert city == "פתח תקווה"
    assert source.startswith("abbrev_addr:")


def test_abbrev_ramat_hasharon_compound():
    assert infer_city('קרפור סיטי  זרובבל רמה"ש (4200)', "33 סוקולוב")[0] == "רמת השרון"


def test_abbrev_kiryat_ata_dotted():
    # ק.אתא with a dot (not gershayim) — seen in real branch names.
    assert infer_city('קרפור מרקט  בית וגן ק.אתא (2240)', "10 העצמאות")[0] == "קרית אתא"


def test_ambiguous_abbrev_not_used():
    # ק"א could mean Kiryat Ata OR Kiryat Ono — deliberately excluded from the map.
    city, _ = infer_city('קרפור ק"א', "")
    assert city is None


# ── Full city in branch_name (word-boundary) ─────────────────────────────────

def test_single_word_city_in_branch():
    assert infer_city("דיל אילת נחל אורה", "ירושלים השלמה 52")[0] == "אילת"


def test_multi_word_city_in_branch():
    assert infer_city("BE באר שבע", "") == ("באר שבע", "city_branch:באר שבע")


def test_city_as_suffix_in_branch():
    assert infer_city("מבקיעים אשקלון", "מתחם גלובוס סנטר, מבקיעים")[0] == "אשקלון"


def test_city_with_street_prefix_in_branch():
    # "ביאליק רמת גן" — street "Bialik" in city "Ramat Gan"
    assert infer_city("ביאליק רמת גן", "ביאליק 79")[0] == "רמת גן"


def test_word_boundary_prevents_nesher_false_positive():
    # The original false positive: "נשר" (city Nesher) inside "נשרים" (eagles).
    # Must NOT resolve.
    assert infer_city("כנפי נשרים", "כנפי נשרים 26")[0] is None


def test_longest_city_wins():
    # "נצרת עילית" must win over "נצרת" when both could match.
    assert infer_city("שופרסל נצרת עילית", "")[0] == "נצרת עילית"


# ── Variant spellings ───────────────────────────────────────────────────────

def test_kriyat_double_yod_variant():
    # Raw data uses קריית שמונה; lexicon is קרית שמונה.
    assert infer_city("קריית שמונה", "")[0] == "קרית שמונה"


def test_typo_modiin():
    assert infer_city("מודעין ישפרו", "החרט 1")[0] == "מודיעין"


def test_petah_tikva_alternate_spelling():
    # Lexicon has both פתח תקווה and פתח תקוה; either spelling in source should match.
    assert infer_city("שופרסל פתח תקוה", "")[0] == "פתח תקווה"


def test_dash_city_name():
    # "באר-שבע" with dash should equate to "באר שבע".
    assert infer_city("קרפור באר-שבע", "")[0] == "באר שבע"


# ── Address matching — safe positions only ──────────────────────────────────

def test_address_comma_delimited():
    city, source = infer_city("", "כישור 22, חולון, ישראל")
    assert city == "חולון"
    assert source == "city_address:חולון"


def test_address_exact_city():
    assert infer_city("קרפור מרקט בית אליעזר (3020)", "חדרה")[0] == "חדרה"


def test_address_multi_word_at_end():
    assert infer_city("קרפור מרקט  מעלות", "שלמה שרירא 3 מעלות תרשיחא")[0] == "מעלות תרשיחא"


def test_address_boulevard_hebrew_not_matched_as_sderot():
    # "שדרות דב יוסף" = Dov Yosef Boulevard (street type), NOT city Sderot.
    # Was the most dangerous false positive caught during audit.
    assert infer_city("בית הפירות פת", "שדרות דב יוסף")[0] is None


def test_address_eilat_street_not_matched_as_city():
    # "אילת 36" = Eilat street (a common street name), NOT the city Eilat.
    # Single-word city names in non-canonical address position must be rejected.
    assert infer_city("קרפור סיטי  נאות רחל", "אילת 36")[0] is None


def test_address_rishon_street_rejected_when_wrong_position():
    # The address starts with "ראשון לציון" as a STREET, then the real city פ"ת
    # comes after the comma. The fix must pick פ"ת, not ראשון לציון.
    city, _ = infer_city("כל בו חצי חינם אם המושבות", 'ראשון לציון 1 , פ"ת , ישראל')
    assert city == "פתח תקווה"


# ── Unresolved cases (left alone, safer than wrong guess) ───────────────────

def test_tel_aviv_neighborhood_alone_unresolved():
    # Neighborhoods like Florentin are deliberately NOT mapped — too brittle.
    assert infer_city("אקספרס דניה", "חיים הזז 5")[0] is None


def test_online_store_unresolved():
    assert infer_city("שופרסל ONLINE", "WWW.SHUFERSAL.CO.IL")[0] is None


def test_empty_inputs():
    assert infer_city("", "") == (None, "none")
    assert infer_city(None, None) == (None, "none")


# ── Branch beats address ─────────────────────────────────────────────────────

def test_branch_resolves_before_address():
    # If branch_name gives a confident city, don't fall through to address.
    city, source = infer_city("שוקן תל אביב", "שוקן 1")
    assert city == "תל אביב"
    assert source.startswith("city_branch:")  # not city_address
