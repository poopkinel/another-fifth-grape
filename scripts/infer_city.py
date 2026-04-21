"""Infer `city` for stores where it's empty, from branch_name + address.

Default (no flags)   : DRY-RUN. Prints source distribution, top inferred cities,
                       sample decisions, and unresolved counts. Writes nothing.
With --commit        : Backs up the DB, then writes `city` inside a single
                       transaction. Only updates rows where city is NULL or ''.

Inference is intentionally conservative. Sources (highest confidence first):

  1. `abbrev`       — Hebrew abbreviation with explicit gershayim, e.g. ת"א,
                      פ"ת, כ"ס, רא"ל, ב"ש, ר"ג, ב"ב. Very reliable.
  2. `city_branch`  — Full city name appears as substring of branch_name, e.g.
                      "דיל חיפה- גרנד קניון" → חיפה. Longest-first match.
  3. `city_address` — Full city name as substring of address (suffix patterns
                      like "הרקון 2 הוד השרון").

No neighborhood guessing — a bare "פלורנטין" stays unresolved. Leaving a row
unchanged is always safer than writing a wrong city (which would then bias the
geocoder in the wrong direction).

Why the backfill matters:
  When city is '' the geocoder falls back to same-named streets in whichever
  city Google returns first — frequently Tel Aviv (lots of common streets).
  A branch labelled "קרפור עמישב פ"ת" would geocode to Begin 96 Tel Aviv
  instead of Petah Tikva. Filling city first, then re-geocoding with
  components=administrative_area, resolves the street to the correct city.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

DB = Path("/home/fifth-grape/backend/data/fifth_grape.db")

RTL_MARKS = "\u200f\u200e\u202b\u202c\u202a\u202d\u202e\ufeff"
GERSHAYIM = "\u05f4"  # Hebrew punctuation gershayim (looks like ")
GERESH = "\u05f3"     # Hebrew punctuation geresh (looks like ')

# ── Abbreviation map ──────────────────────────────────────────────────────────
# Keys use ASCII " — callers normalize gershayim → " before lookup.
ABBREV = {
    'ת"א':   'תל אביב',
    'פ"ת':   'פתח תקווה',
    'כ"ס':   'כפר סבא',
    'רא"ל':  'ראשון לציון',
    'ר"ל':   'ראשון לציון',
    'ב"ש':   'באר שבע',
    'ר"ג':   'רמת גן',
    'ב"ב':   'בני ברק',
    'ק"ש':   'קרית שמונה',
    'ק"ג':   'קרית גת',
    'ק"מ':   'קרית מוצקין',
    'ק"ב':   'קרית ביאליק',
    'ק"י':   'קרית ים',
    # ק"א is ambiguous (Kiryat Ata vs Kiryat Ono) — deliberately excluded.
    'נ"ע':   'נצרת עילית',
    'מ"ע':   'מעלה אדומים',
    'ז"י':   'זכרון יעקב',
    'א"ע':   'אור עקיבא',
    'א"י':   'אור יהודה',
    'ה"ש':   'הוד השרון',
    'ר"ש':   'רמת השרון',
    'רמה"ש': 'רמת השרון',
    'ק.אתא': 'קרית אתא',
}

# ── Israeli city lexicon ──────────────────────────────────────────────────────
# Keys are the canonical city name we write to the DB. Values list all variant
# spellings the scraper might produce (dashed, ktiv haser vs malé, Hebrew
# combined forms like "תל אביב יפו"). This structure guarantees the DB stores
# a single canonical label per city regardless of which source form matched.
CITY_VARIANTS = {
    "תל אביב":      ["תל אביב", "תל-אביב", "תל אביב יפו"],
    "ירושלים":      ["ירושלים"],
    "חיפה":         ["חיפה"],
    "ראשון לציון":  ["ראשון לציון"],
    "פתח תקווה":    ["פתח תקווה", "פתח תקוה"],
    "אשדוד":        ["אשדוד"],
    "נתניה":        ["נתניה"],
    "באר שבע":      ["באר שבע", "באר-שבע"],
    "חולון":        ["חולון"],
    "בני ברק":      ["בני ברק"],
    "רמת גן":       ["רמת גן"],
    "רחובות":       ["רחובות"],
    "אשקלון":       ["אשקלון"],
    "בת ים":        ["בת ים", "בת-ים"],
    "הרצליה":       ["הרצליה"],
    "כפר סבא":      ["כפר סבא"],
    "חדרה":         ["חדרה"],
    "מודיעין":      ["מודיעין"],
    "רעננה":        ["רעננה"],
    "נהריה":        ["נהריה"],
    "לוד":          ["לוד"],
    "רמלה":         ["רמלה"],
    "גבעתיים":      ["גבעתיים"],
    "הוד השרון":    ["הוד השרון"],
    "בית שמש":      ["בית שמש", "בית-שמש"],
    "אילת":         ["אילת"],
    "טבריה":        ["טבריה"],
    "עכו":          ["עכו"],
    "פרדס חנה":     ["פרדס חנה"],
    "כרמיאל":       ["כרמיאל"],
    "נצרת עילית":   ["נצרת עילית"],
    "נצרת":         ["נצרת"],
    "קרית גת":      ["קרית גת"],
    "קרית שמונה":   ["קרית שמונה"],
    "קרית אתא":     ["קרית אתא"],
    "קרית מוצקין":  ["קרית מוצקין"],
    "קרית ביאליק":  ["קרית ביאליק"],
    "קרית ים":      ["קרית ים"],
    "קרית אונו":    ["קרית אונו"],
    "קרית מלאכי":   ["קרית מלאכי"],
    "רמת השרון":    ["רמת השרון"],
    "יבנה":         ["יבנה"],
    "ראש העין":     ["ראש העין"],
    "דימונה":       ["דימונה"],
    "טירת כרמל":    ["טירת כרמל"],
    "עפולה":        ["עפולה"],
    "אור יהודה":    ["אור יהודה"],
    "אור עקיבא":    ["אור עקיבא"],
    "מעלה אדומים":  ["מעלה אדומים"],
    "ביתר עילית":   ["ביתר עילית", "ביתר"],
    "אלעד":         ["אלעד"],
    "נס ציונה":     ["נס ציונה"],
    "זכרון יעקב":   ["זכרון יעקב"],
    "מצפה רמון":    ["מצפה רמון"],
    "ערד":          ["ערד"],
    "שדרות":        ["שדרות"],
    "נתיבות":       ["נתיבות"],
    "אופקים":       ["אופקים"],
    "טמרה":         ["טמרה"],
    "סחנין":        ["סחנין"],
    "אום אל פחם":   ["אום אל פחם"],
    "טירה":         ["טירה"],
    "קלנסווה":      ["קלנסווה"],
    "יהוד":         ["יהוד", "יהוד-מונוסון"],
    "קדימה":        ["קדימה"],
    "צורן":         ["צורן"],
    "פרדסיה":       ["פרדסיה"],
    "כפר יונה":     ["כפר יונה"],
    "תל מונד":      ["תל מונד"],
    "אבן יהודה":    ["אבן יהודה"],
    "קיסריה":       ["קיסריה"],
    "בנימינה":      ["בנימינה"],
    "רכסים":        ["רכסים"],
    "קרית טבעון":   ["קרית טבעון"],
    "נשר":          ["נשר"],
    "יקנעם":        ["יקנעם"],
    "מגדל העמק":    ["מגדל העמק"],
    "בית שאן":      ["בית שאן", "בית-שאן"],
    "צפת":          ["צפת"],
    "גדרה":         ["גדרה"],
    "חצור הגלילית": ["חצור הגלילית"],
    "ירוחם":        ["ירוחם"],
    "גבעת שמואל":   ["גבעת שמואל"],
    "נוף הגליל":    ["נוף הגליל"],
    "מעלות תרשיחא": ["מעלות תרשיחא", "מעלות-תרשיחא"],
    "אפרת":         ["אפרת"],
    "אריאל":        ["אריאל"],
    "עמנואל":       ["עמנואל"],
    "סביון":        ["סביון"],
    "מטולה":        ["מטולה"],
    "גן יבנה":      ["גן יבנה"],
    "גני תקווה":    ["גני תקווה"],
    "ראש פינה":     ["ראש פינה"],
    "עתלית":        ["עתלית"],
    "גבעת זאב":     ["גבעת זאב"],
    "כפר ורדים":    ["כפר ורדים"],
}

# Common variant spellings → canonical form (applied during substring match).
# Hebrew place names are often written with either `קרית` or `קריית` and
# sometimes `תקוה` vs `תקווה`. We rewrite to the lexicon form so matching works
# against a single canonical string.
VARIANT_FIX = {
    "מודעין": "מודיעין",    # typo
    "קריית":  "קרית",       # ktiv haser vs malé
    "קירית":  "קרית",       # common typo
}

RE_SPACE = re.compile(r"\s+")
RE_PUNCT = re.compile(r"[,.()\[\]\"'\u05f3\u05f4]+")
RE_PUNCT_KEEP_COMMA = re.compile(r"[.()\[\]\"'\u05f3\u05f4]+")


def norm(s: str | None) -> str:
    """NFKC, strip RTL marks, replace gershayim→" and geresh→', collapse spaces.

    Leaves Hebrew letters and the ASCII `"` / `'` / `-` intact so abbreviation
    patterns (e.g. ת"א) still match after this pass.
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    for ch in RTL_MARKS:
        s = s.replace(ch, "")
    s = s.replace(GERSHAYIM, '"').replace(GERESH, "'")
    return RE_SPACE.sub(" ", s).strip()


def _strip_for_substring(s: str) -> str:
    """Version used for substring matching: drop punct so `באר-שבע` matches
    the lexicon's `באר שבע` and vice versa."""
    s = RE_PUNCT.sub(" ", s)
    s = s.replace("-", " ")
    return RE_SPACE.sub(" ", s).strip().lower()


def _strip_for_address(s: str) -> str:
    """Like _strip_for_substring but keeps commas, since they signal the
    canonical Hebrew address format 'Street N, City, Country'."""
    s = RE_PUNCT_KEEP_COMMA.sub(" ", s)
    s = s.replace("-", " ")
    # Normalise ", " / " ," / " , " to a single ", " for reliable matching.
    s = re.sub(r"\s*,\s*", ", ", s)
    return RE_SPACE.sub(" ", s).strip().lower()


def _city_in_address_safe(addr: str, city_key: str) -> bool:
    """Only accept city matches at canonical positions inside an address.

    Safe positions:
      1. Preceded by a comma (", חולון" or ", חולון, ישראל")
      2. The address IS the city alone ("חדרה")
      3. The city is multi-word and sits at the end of the string

    Rejects the common trap where a street is named after another city
    ("שדרות דב יוסף" = Dov Yosef Boulevard, not Sderot; "אילת 36" = Eilat
    street, not Eilat), or where a full city name is used as a street name
    ("ראשון לציון 1 , פ"ת , ישראל" — real city is Petah Tikva, after comma).
    """
    if not addr:
        return False
    if addr == city_key:
        return True
    # Pattern: ", <city>" followed by end/space/comma.
    if re.search(rf",\s*{re.escape(city_key)}(?:\s|,|$)", addr):
        return True
    # Multi-word city at end of string (single-word too risky — streets often
    # share names with cities).
    if " " in city_key and addr.endswith(city_key):
        # require a space immediately before (so it's not glued to a street).
        idx = len(addr) - len(city_key)
        if idx > 0 and addr[idx - 1] == " ":
            return True
    return False


# Pre-compute the stripped lexicon once, longest-first.
_CITY_INDEX: list[tuple[str, str]] | None = None


def _city_index() -> list[tuple[str, str]]:
    """Return [(variant_stripped_for_matching, canonical_name), ...] sorted
    longest-first so that multi-word cities win over their single-word
    prefixes ("נצרת עילית" before "נצרת")."""
    global _CITY_INDEX
    if _CITY_INDEX is None:
        seen: dict[str, str] = {}
        for canonical, variants in CITY_VARIANTS.items():
            for v in variants:
                key = _strip_for_substring(v)
                # First writer wins (canonical is written alongside each variant,
                # so this is always safe), and the canonical name is what we
                # ultimately write to the DB regardless of which variant matched.
                seen.setdefault(key, canonical)
        _CITY_INDEX = sorted(seen.items(), key=lambda kv: -len(kv[0]))
    return _CITY_INDEX


def _abbrev_index() -> list[tuple[str, str]]:
    """Return abbreviation patterns sorted longest-first (רא"ל before ר"ל)."""
    return sorted(ABBREV.items(), key=lambda kv: -len(kv[0]))


def infer_city(branch_name: str | None, address: str | None) -> tuple[str | None, str]:
    """Infer a city from branch_name and (fallback) address.

    Returns (city_or_None, source). `source` is one of:
      - "abbrev:<abbrev>"          e.g. 'abbrev:פ"ת'
      - "city_branch:<city>"       full city name matched inside branch_name
      - "city_address:<city>"      full city name matched inside address
      - "none"                     unresolved
    """
    b = norm(branch_name)
    a = norm(address)

    # 1. Abbreviation in branch_name (very high confidence).
    for abbr, full in _abbrev_index():
        # Must be surrounded by non-letter chars to avoid false positives.
        # Hebrew letters aren't matched by \b, so build an explicit boundary.
        pat = rf"(?:^|[^\w\"'])({re.escape(abbr)})(?:$|[^\w\"'])"
        if re.search(pat, b):
            return full, f"abbrev:{abbr}"

    # 2. Full city as a standalone token in branch_name.
    # Word-boundary match is required so single-word cities don't latch onto
    # longer words (e.g. "נשר" in "כנפי נשרים" = "eagles").
    b_stripped = _strip_for_substring(b)
    for typo, fix in VARIANT_FIX.items():
        b_stripped = b_stripped.replace(_strip_for_substring(typo), _strip_for_substring(fix))
    for key, canon in _city_index():
        if re.search(rf"(?:^|\s){re.escape(key)}(?:$|\s)", b_stripped):
            return canon, f"city_branch:{canon}"

    # 3. Abbreviation in address, canonical-position only (after comma).
    for abbr, full in _abbrev_index():
        if re.search(rf",\s*{re.escape(abbr)}(?:\s|,|$)", a):
            return full, f"abbrev_addr:{abbr}"

    # 4. Full city in address — only in canonical positions (see
    #    _city_in_address_safe for why). Single-word cities are particularly
    #    treacherous because many streets are named after cities.
    a_stripped = _strip_for_address(a)
    for typo, fix in VARIANT_FIX.items():
        a_stripped = a_stripped.replace(_strip_for_address(typo), _strip_for_address(fix))
    for key, canon in _city_index():
        if _city_in_address_safe(a_stripped, key):
            return canon, f"city_address:{canon}"

    return None, "none"


# ── Script driver ─────────────────────────────────────────────────────────────


def iter_empty_city_rows(conn: sqlite3.Connection):
    return conn.execute(
        """
        SELECT chain_id, store_id, branch_name, address, lat, lng, geocode_status
        FROM stores
        WHERE city IS NULL OR city = ''
        ORDER BY chain_id, store_id
        """
    ).fetchall()


def summarize(rows) -> tuple[list[dict], Counter, Counter]:
    """Run inference over rows, return (decisions, source_counts, city_counts)."""
    decisions = []
    src_counts: Counter = Counter()
    city_counts: Counter = Counter()
    for r in rows:
        city, source = infer_city(r["branch_name"], r["address"])
        decisions.append({
            "chain_id": r["chain_id"],
            "store_id": r["store_id"],
            "branch_name": r["branch_name"],
            "address": r["address"],
            "inferred_city": city,
            "source": source,
        })
        # Bucket source by prefix (abbrev / city_branch / city_address / none)
        src_counts[source.split(":", 1)[0]] += 1
        if city:
            city_counts[city] += 1
    return decisions, src_counts, city_counts


def print_audit(decisions, src_counts, city_counts, sample_size: int = 30):
    n = len(decisions)
    resolved = sum(1 for d in decisions if d["inferred_city"])
    print(f"Total empty-city rows scanned : {n}")
    print(f"  Resolved                    : {resolved}  ({resolved / n * 100:.1f}%)")
    print(f"  Unresolved                  : {n - resolved}")
    print()
    print("By source:")
    for src, cnt in src_counts.most_common():
        print(f"  {src:<14} {cnt}")
    print()
    print("Top 20 inferred cities:")
    for city, cnt in city_counts.most_common(20):
        print(f"  {city:<20} {cnt}")
    print()
    print(f"Sample decisions ({sample_size}):")
    step = max(1, n // sample_size)
    for d in decisions[::step][:sample_size]:
        tag = d["inferred_city"] or "—"
        print(f"  [{d['source']:<22}] {tag:<15} "
              f"{d['chain_id']}/{d['store_id']}  "
              f"branch={d['branch_name']!r} addr={d['address']!r}")
    print()
    print("Sample unresolved (20):")
    unresolved = [d for d in decisions if not d["inferred_city"]]
    step = max(1, len(unresolved) // 20) if unresolved else 1
    for d in unresolved[::step][:20]:
        print(f"  {d['chain_id']}/{d['store_id']}  "
              f"branch={d['branch_name']!r}  addr={d['address']!r}")


def backup_db() -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    dst = DB.with_suffix(DB.suffix + f".bak.{ts}")
    shutil.copy2(DB, dst)
    print(f"DB backed up to {dst}")
    return dst


def write_city(conn: sqlite3.Connection, decisions) -> int:
    """Apply inferred cities inside a single transaction. Returns rows updated.

    Also stamps `city_inferred_at` with the current UTC timestamp so future
    debugging can distinguish scraper-supplied cities from inferred ones.
    """
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    updates = [(d["inferred_city"], now, d["store_id"], d["chain_id"])
               for d in decisions if d["inferred_city"]]
    if not updates:
        return 0
    conn.executemany(
        "UPDATE stores SET city = ?, city_inferred_at = ? "
        "WHERE store_id = ? AND chain_id = ? AND (city IS NULL OR city = '')",
        updates,
    )
    return conn.total_changes


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--commit", action="store_true",
                   help="Write to DB (default is dry-run)")
    p.add_argument("--sample", type=int, default=30,
                   help="Number of sample decisions to print (default 30)")
    args = p.parse_args()

    if not DB.exists():
        sys.exit(f"DB not found: {DB}")

    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row
        rows = iter_empty_city_rows(conn)

    decisions, src_counts, city_counts = summarize(rows)
    print_audit(decisions, src_counts, city_counts, sample_size=args.sample)

    if not args.commit:
        print("\n(dry-run — no writes. Pass --commit to apply.)")
        return

    backup_db()
    with sqlite3.connect(DB) as conn:
        try:
            n = write_city(conn, decisions)
            conn.commit()
            print(f"\nWrote city to {n} rows.")
        except Exception:
            conn.rollback()
            raise


if __name__ == "__main__":
    main()
