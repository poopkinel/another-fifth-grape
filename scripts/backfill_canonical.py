"""HIGH-tier-only equivalence-grouping backfill for products.canonical_product_id.

Default (no flags)        : DRY-RUN. Prints summary + sample of planned writes.
                            Writes nothing.
With --commit             : Backs up the DB, then runs the writes inside a
                            single transaction. Aborts (and rolls back) on any
                            error. Skips rows that already have a non-null
                            canonical_product_id (idempotent re-runs).

Only HIGH-confidence groups are touched: size resolved + no orphan variant
codes. MEDIUM and LOW tiers stay untouched (separate decision).
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

# ── Lexicons ──────────────────────────────────────────────────────────────────
RTL_MARKS = "\u200f\u200e\u202b\u202c\u202a\u202d\u202e\ufeff"

PACKAGING = {
    "בקרטון", "קרטון", "בשקית", "שקית", "בקבוק", "בבקבוק",
    "פחית", "בפחית", "קופסה", "בקופסה", "כוס", "בכוס",
    "מארז", "במארז",  # multipack — distinguishes from singles
    "צנצנת", "בצנצנת", "שפופרת", "בשפופרת",
}
COLOR = {"אדום", "ירוק", "צהוב", "שחור", "לבן", "כחול", "ורוד", "סגול", "כתום"}
DIET = {"אורגני", "אורגנית", "כשר", "כשרה", "טבעוני", "טבעונית",
        "ללא", "דל", "לקטוז", "גלוטן", "סוכר", "מלח", "שומן",
        "מועשר", "מחוזק", "טבעי", "טבעית"}
STOPWORDS = {"של", "עם", "בטעם", "טעם", "חדש", "חדשה", "מהדורה",
             "יחידה", "יחידות", "גרם", "ליטר", "מ", "ל", "ג",
             "מ\"ל", "ק\"ג", "ק\"מ", "מל", "קג", "ml", "kg", "l", "g",
             "וכו", "ועוד", "פרימיום", "premium"}

# Abbreviation → full form, applied before tokenization (word-boundary replace)
ABBREV = {
    r"\bמהד\b": "מהדורה",
    r"\bגר\b": "גרם",
    r"\bמל\b": "מ\"ל",
    r"\bקג\b": "ק\"ג",
    r"\bיח\b": "יחידה",
}

# Hebrew single-letter attached prefixes we strip from spec tokens so that
# "בקרטון" ≡ "קרטון", "בשקית" ≡ "שקית", "ללא לקטוז" ≡ "לא לקטוז"
HEB_PREFIXES = ("ב", "ה", "ל", "מ", "ו")

# Size extraction (Hebrew + Latin units → normalised (value, canonical_unit))
# Order matters: more-specific patterns first.
SIZE_PATTERNS = [
    (re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:מ\"?ל|מל|ml)\b", re.I), "ml"),
    (re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:ל(?:יטר)?|l)\b",  re.I), "L"),
    (re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:ק\"?ג|קג|kg)\b",  re.I), "kg"),
    (re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:גרם|גר|ג|g)\b",   re.I), "g"),
    (re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:יח'?|יחידות|יחידה)\b"), "u"),
]
PERCENT_RE = re.compile(r"\d+%")
PUNCT_RE = re.compile(r"[\"',./()\-_\u05f3\u05f4]+")  # +Hebrew geresh/gershayim


def norm(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    for ch in RTL_MARKS:
        s = s.replace(ch, "")
    return s.strip().lower()


def extract_size(name: str) -> tuple[float | None, str | None, str]:
    """Return (value, canonical_unit, name_with_match_removed)."""
    for rx, unit in SIZE_PATTERNS:
        m = rx.search(name)
        if m:
            try:
                v = float(m.group(1).replace(",", "."))
                if unit == "ml":
                    v, unit = v / 1000.0, "L"
                elif unit == "g":
                    v, unit = v / 1000.0, "kg"
                stripped = (name[:m.start()] + " " + name[m.end():]).strip()
                return (v, unit, stripped)
            except ValueError:
                pass
    return (None, None, name)


# (canonical_unit, multiplier_for_bare_number) by unit-suffix detection.
# Most-specific (sub-units like ml/g) first.
UNIT_DETECTORS = [
    (re.compile(r"(?:מ\"?ל|מל|\bml\b)", re.I), "L", 0.001),
    (re.compile(r"(?:ק\"?ג|\bקג\b|\bkg\b)", re.I), "kg", 1.0),
    (re.compile(r"(?:ליטר|\bל\b|\bl\b)", re.I), "L", 1.0),
    (re.compile(r"(?:גרם|\bגר\b|\bג\b|\bg\b)", re.I), "kg", 0.001),
    (re.compile(r"(?:יחידות|יחידה|יח'?)"), "u", 1.0),
]


def detect_unit_kind(unit_str: str | None) -> tuple[str | None, float]:
    """Return (canonical_unit, multiplier) for a bare name number to be in.

    Looks at the unit STRING regardless of any quantity in it; e.g.
    "מ\"ל", "100 מ\"ל" both yield ("L", 0.001).
    """
    if not unit_str:
        return (None, 1.0)
    n = norm(unit_str)
    for rx, unit, mult in UNIT_DETECTORS:
        if rx.search(n):
            return (unit, mult)
    return (None, 1.0)


def normalize_unit_column(unit_str: str | None) -> tuple[float | None, str | None, float]:
    """Returns (size_value_if_unit_string_encodes_quantity, canonical_unit, multiplier).

    Size is filled when the unit string itself includes a number; otherwise None.
    Multiplier always reflects the unit kind (so a bare name number can be scaled).
    """
    if not unit_str:
        return (None, None, 1.0)
    n = norm(unit_str)
    canon_unit, mult = detect_unit_kind(n)
    v, _u, _ = extract_size(n)
    return (v, canon_unit, mult)


def strip_heb_prefix(tok: str) -> str:
    """Strip a single-letter Hebrew attached prefix when stripping leaves a >=3-char stem."""
    if len(tok) >= 4 and tok[0] in HEB_PREFIXES:
        return tok[1:]
    return tok


def tokenize(text: str) -> list[str]:
    text = PUNCT_RE.sub(" ", text)
    return [t for t in text.split() if t]


def fingerprint(name: str, brand: str | None, unit: str | None):
    n = norm(name)
    b = norm(brand)
    # Apply abbreviation expansion before extraction
    for pat, repl in ABBREV.items():
        n = re.sub(pat, repl, n)

    # 1. Try to extract size from the name (with unit-word in the name itself)
    size, size_unit, n_after_size = extract_size(n)

    # 2. Strip brand tokens from the (size-stripped) name
    name_after_brand = n_after_size
    if b:
        for bt in tokenize(b):
            name_after_brand = re.sub(rf"\b{re.escape(bt)}\b", " ", name_after_brand)

    tokens = tokenize(name_after_brand)
    # Drop bare numeric tokens that the size match should have eaten
    bare_numerics = [t for t in tokens if re.fullmatch(r"\d+(?:[.,]\d+)?", t)]
    tokens = [t for t in tokens if not re.fullmatch(r"\d+(?:[.,]\d+)?", t)]
    tokens = [t for t in tokens if t not in STOPWORDS]

    # 3. Fallback: if name had no size match BUT has exactly one bare numeric
    #    AND the unit column is a known unit → compose them as the size,
    #    applying the multiplier (e.g. unit="מ\"ל" → ÷1000 to get liters).
    if size is None and len(bare_numerics) == 1:
        _, canon_unit, mult = normalize_unit_column(unit)
        if canon_unit in ("L", "kg", "u"):
            try:
                size = float(bare_numerics[0].replace(",", ".")) * mult
                size_unit = canon_unit
                bare_numerics = []  # consumed
            except ValueError:
                pass

    # 4. Classify remaining tokens
    spec = []
    base = []
    for t in tokens:
        if PERCENT_RE.fullmatch(t):
            spec.append(t)
            continue
        stripped = strip_heb_prefix(t)
        if stripped in PACKAGING or stripped in COLOR or stripped in DIET \
                or t in PACKAGING or t in COLOR or t in DIET:
            spec.append(stripped if stripped in PACKAGING|COLOR|DIET else t)
        else:
            base.append(t)

    # 5. Any remaining bare numeric is a "variant code" → goes into spec.
    #    This is what keeps lipstick shade 230 ≠ shade 100, bed 80 ≠ bed 90, etc.
    for nt in bare_numerics:
        spec.append(f"#{nt}")

    # 6. Resolve final unit
    final_unit = size_unit
    if final_unit is None:
        _, u, _ = normalize_unit_column(unit)
        final_unit = u

    return (
        b,
        tuple(sorted(base)),
        tuple(sorted(spec)),
        size,
        final_unit,
    )


def confidence(fp, rows):
    if fp[0] in ("__singleton__", "__garbage__"):
        return "n/a"
    size = fp[3]
    variant_codes = sum(1 for s in fp[2] if s.startswith("#"))
    if size is not None and variant_codes == 0:
        return "HIGH"
    if size is not None and variant_codes == 1:
        return "MEDIUM"
    if size is None and variant_codes == 0:
        return "MEDIUM"
    return "LOW"


def compute_high_tier_writes(conn) -> tuple[list[tuple[str, str]], dict]:
    """Return [(child_id, canonical_id), ...] for HIGH-tier groups, plus stats."""
    products = list(conn.execute(
        "SELECT product_id, name, brand, unit, canonical_product_id FROM products"
    ).fetchall())

    price_count = dict(conn.execute(
        "SELECT product_id, COUNT(*) FROM prices GROUP BY product_id"
    ).fetchall())
    latest_ts = dict(conn.execute(
        "SELECT product_id, MAX(updated_at) FROM prices GROUP BY product_id"
    ).fetchall())

    groups: dict[tuple, list[sqlite3.Row]] = defaultdict(list)
    for p in products:
        raw_name = (p["name"] or "").strip().lower()
        if raw_name in ("", "nan") or raw_name.startswith("nan"):
            continue
        fp = fingerprint(p["name"] or "", p["brand"], p["unit"])
        if not fp[1] and not fp[2]:
            continue
        groups[fp].append(p)

    def winner(group):
        return max(group, key=lambda p: (
            price_count.get(p["product_id"], 0),
            latest_ts.get(p["product_id"], ""),
            -int(p["product_id"]) if (p["product_id"] or "").isdigit() else 0,
        ))

    writes: list[tuple[str, str]] = []
    high_groups = 0
    skipped_already_set = 0
    for fp, rows in groups.items():
        if len(rows) < 2 or confidence(fp, rows) != "HIGH":
            continue
        high_groups += 1
        w = winner(rows)
        for p in rows:
            if p["canonical_product_id"]:
                skipped_already_set += 1
                continue
            writes.append((p["product_id"], w["product_id"]))

    stats = {
        "total_products": len(products),
        "high_groups": high_groups,
        "writes_planned": len(writes),
        "self_pointers": sum(1 for c, w in writes if c == w),
        "child_pointers": sum(1 for c, w in writes if c != w),
        "skipped_already_set": skipped_already_set,
    }
    return writes, stats


def write_canonical(conn, writes: list[tuple[str, str]]) -> int:
    """Apply (child_id, canonical_id) writes inside a single transaction."""
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        cur.executemany(
            "UPDATE products SET canonical_product_id=? "
            "WHERE product_id=? AND canonical_product_id IS NULL",
            [(canonical, child) for child, canonical in writes],
        )
        n = cur.rowcount
        conn.commit()
        return n
    except Exception:
        conn.rollback()
        raise


def backup_db(src: Path) -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    dst = src.with_suffix(f".db.bak.{ts}")
    shutil.copy2(src, dst)
    return dst


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--commit", action="store_true",
                    help="Apply the writes. Without this flag, the script is a dry run.")
    ap.add_argument("--show-samples", type=int, default=10,
                    help="Sample N HIGH-tier groups to print for review (default 10).")
    args = ap.parse_args()

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    writes, stats = compute_high_tier_writes(conn)

    print("── HIGH-tier backfill plan ──")
    for k, v in stats.items():
        print(f"  {k:<22} {v:>7,}")
    print()

    # Sample output: show N HIGH groups with their proposed writes
    if args.show_samples > 0:
        print(f"── Sample of {args.show_samples} HIGH-tier groups (★ = canonical) ──")
        # Re-derive groups for sampling (cheaper than re-loading)
        products = list(conn.execute(
            "SELECT product_id, name, brand, unit FROM products"
        ).fetchall())
        price_count = dict(conn.execute(
            "SELECT product_id, COUNT(*) FROM prices GROUP BY product_id"
        ).fetchall())
        groups = defaultdict(list)
        for p in products:
            raw = (p["name"] or "").strip().lower()
            if raw in ("", "nan") or raw.startswith("nan"):
                continue
            fp = fingerprint(p["name"] or "", p["brand"], p["unit"])
            if not fp[1] and not fp[2]:
                continue
            groups[fp].append(p)
        high = [(fp, rows) for fp, rows in groups.items()
                if len(rows) > 1 and confidence(fp, rows) == "HIGH"]
        # sort by total prices in group (descending) so we eyeball the most-impactful first
        high.sort(key=lambda kv: -sum(price_count.get(p["product_id"], 0) for p in kv[1]))
        for fp, rows in high[:args.show_samples]:
            w = max(rows, key=lambda p: price_count.get(p["product_id"], 0))
            print(f"\n  brand={fp[0]!r}  base={list(fp[1])}  spec={list(fp[2])}  size={fp[3]} {fp[4]}")
            for p in sorted(rows, key=lambda r: -price_count.get(r["product_id"], 0))[:6]:
                mark = " ★" if p["product_id"] == w["product_id"] else "  "
                print(f"   {mark} {p['product_id']:<14} prices={price_count.get(p['product_id'],0):>5}  {p['name']!r}")
            if len(rows) > 6:
                print(f"      … and {len(rows) - 6} more")
        print()

    if not args.commit:
        print("DRY RUN — no writes performed. Re-run with --commit to apply.")
        return

    # Commit path
    print("── COMMIT ──")
    backup = backup_db(DB)
    print(f"  backup: {backup}")
    print(f"  applying {len(writes):,} updates inside one transaction…")
    n = write_canonical(conn, writes)
    print(f"  rows updated: {n:,}")
    # Sanity: count non-null canonical
    after = conn.execute(
        "SELECT COUNT(*) FROM products WHERE canonical_product_id IS NOT NULL"
    ).fetchone()[0]
    print(f"  products with non-null canonical_product_id (after): {after:,}")


if __name__ == "__main__":
    main()
