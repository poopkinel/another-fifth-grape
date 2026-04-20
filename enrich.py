#!/usr/bin/env python3
"""Enrich product data: assign emojis, extract brands, clean names.

Usage:
    python enrich.py                 # run all enrichment steps
    python enrich.py --emoji         # only assign emojis
    python enrich.py --brand         # only extract brands
    python enrich.py --name          # only clean names
    python enrich.py --dry-run       # preview changes without writing
"""

import argparse
import logging
import re

from app.db import get_conn, init_db

logger = logging.getLogger("enrich")

# ── Step 1: Emoji assignment ──────────────────────────────────────────
# Ordered list: first match wins. More specific patterns before general ones.

EMOJI_RULES: list[tuple[str, str]] = [
    # Dairy
    ("יוגורט", "🥛"),
    ("קוטג", "🧀"),
    ("לבנה", "🧀"),
    ("שמנת", "🥛"),
    ("חמאה", "🧈"),
    ("גבינ", "🧀"),
    ("חלב", "🥛"),
    ("מילקי", "🥛"),
    # Bread & Bakery
    ("באגט", "🥖"),
    ("לחמני", "🍞"),
    ("פיתה", "🍞"),
    ("חלה", "🍞"),
    ("לחם", "🍞"),
    ("טורטי", "🍞"),
    ("קרואסון", "🥐"),
    ("עוג", "🍰"),
    ("מאפה", "🍞"),
    # Meat
    ("שניצל", "🍗"),
    ("חזה עוף", "🍗"),
    ("כרעיים", "🍗"),
    ("עוף", "🍗"),
    ("הודו", "🍗"),
    ("סטייק", "🥩"),
    ("קבב", "🥩"),
    ("אנטריקוט", "🥩"),
    ("צלי", "🥩"),
    ("טחון", "🥩"),
    ("בשר", "🥩"),
    ("המבורגר", "🍔"),
    ("נקניק", "🌭"),
    ("נקניקי", "🌭"),
    ("סלמי", "🌭"),
    ("פסטרמה", "🌭"),
    # Fish
    ("סלמון", "🐟"),
    ("טונה", "🐟"),
    ("דג ", "🐟"),
    ("דגים", "🐟"),
    ("פילה", "🐟"),
    ("שרימפס", "🦐"),
    # Eggs
    ("ביצ", "🥚"),
    # Fruits
    ("בננ", "🍌"),
    ("תפוז", "🍊"),
    ("קלמנטינ", "🍊"),
    ("לימון", "🍋"),
    ("תות", "🍓"),
    ("ענב", "🍇"),
    ("אבטיח", "🍉"),
    ("מלון", "🍈"),
    ("אפרסק", "🍑"),
    ("שזיף", "🍑"),
    ("אגס", "🍐"),
    ("תפוח", "🍎"),
    ("מנגו", "🥭"),
    ("אבוקדו", "🥑"),
    ("קיווי", "🥝"),
    ("אננס", "🍍"),
    # Vegetables
    ("עגבני", "🍅"),
    ("מלפפון", "🥒"),
    ("גזר", "🥕"),
    ("בצל", "🧅"),
    ("שום", "🧄"),
    ("תפו\"א", "🥔"),
    ("תפוח אדמה", "🥔"),
    ("ברוקולי", "🥦"),
    ("כרובית", "🥦"),
    ("חסה", "🥬"),
    ("פטריו", "🍄"),
    ("תירס", "🌽"),
    ("פלפל", "🌶️"),
    ("ירקות", "🥬"),
    ("סלט", "🥗"),
    # Drinks - alcoholic
    ("בירה", "🍺"),
    ("יין ", "🍷"),
    ("וודקה", "🍸"),
    ("ויסקי", "🥃"),
    ("ערק", "🥃"),
    # Drinks - non-alcoholic
    ("קפה", "☕"),
    ("נספרסו", "☕"),
    ("אספרסו", "☕"),
    ("תה ", "🍵"),
    ("מיץ", "🧃"),
    ("פריגת", "🧃"),
    ("קולה", "🥤"),
    ("ספרייט", "🥤"),
    ("פאנטה", "🥤"),
    ("משקה", "🥤"),
    ("סודה", "🥤"),
    ("מים ", "💧"),
    ("מים\n", "💧"),
    ("נביעות", "💧"),
    # Snacks & Sweets
    ("שוקולד", "🍫"),
    ("במבה", "🍿"),
    ("ביסלי", "🍿"),
    ("חטיף", "🍫"),
    ("גלידה", "🍦"),
    ("ארטיק", "🍦"),
    ("ממתק", "🍬"),
    ("סוכרי", "🍬"),
    ("מסטיק", "🍬"),
    ("עוגי", "🍪"),
    ("ביסקוי", "🍪"),
    ("קרקר", "🍘"),
    ("צ'יפס", "🍟"),
    ("פופקורן", "🍿"),
    # Grains & Staples
    ("אורז", "🍚"),
    ("פסטה", "🍝"),
    ("ספגטי", "🍝"),
    ("פנה", "🍝"),
    ("קמח", "🌾"),
    ("דגנ", "🥣"),
    ("קורנפלקס", "🥣"),
    ("גרנולה", "🥣"),
    ("סוכר", "🍬"),
    # Canned & Sauces
    ("שימור", "🥫"),
    ("רוטב", "🥫"),
    ("קטשופ", "🥫"),
    ("מיונז", "🥫"),
    ("חרדל", "🥫"),
    # Spreads & Dips
    ("חומוס", "🫘"),
    ("טחינ", "🥜"),
    ("ריבה", "🍯"),
    ("דבש", "🍯"),
    ("ממרח", "🥜"),
    ("שוקו", "🍫"),
    # Nuts
    ("אגוז", "🥜"),
    ("בוטנ", "🥜"),
    ("שקד", "🥜"),
    ("פיסטוק", "🥜"),
    ("פיצוח", "🥜"),
    ("גרעינ", "🥜"),
    # Oil & Cooking
    ("שמן זית", "🫒"),
    ("שמן", "🍳"),
    ("חומץ", "🍶"),
    # Spices
    ("תבלין", "🧂"),
    ("מלח", "🧂"),
    ("פפריקה", "🧂"),
    ("כורכום", "🧂"),
    ("קינמון", "🧂"),
    # Frozen
    ("קפוא", "🧊"),
    ("קפואה", "🧊"),
    ("קפואים", "🧊"),
    # Baby
    ("חיתול", "👶"),
    ("תינוק", "👶"),
    ("מזון תינוק", "👶"),
    ("סימילאק", "👶"),
    ("מטרנה", "👶"),
    # Cleaning & Home
    ("אקונומיקה", "🧹"),
    ("ניקוי", "🧹"),
    ("מנקה", "🧹"),
    ("כביס", "🧺"),
    ("אריאל", "🧺"),
    ("כלים ", "🧽"),
    ("ספוג", "🧽"),
    ("שמפו", "🧴"),
    ("מרכך", "🧴"),
    ("סבון", "🧴"),
    ("ג'ל רחצה", "🧴"),
    ("דאודורנט", "🧴"),
    ("משחת שיניים", "🪥"),
    ("מברשת שינ", "🪥"),
    # Paper
    ("נייר טואלט", "🧻"),
    ("מגבות נייר", "🧻"),
    ("טישו", "🧻"),
    ("מפיות", "🧻"),
    # Pet
    ("לכלב", "🐕"),
    ("לחתול", "🐈"),
    # Pizza & Ready meals
    ("פיצה", "🍕"),
    # Rice-related
    ("סושי", "🍣"),
]


def assign_emojis(dry_run: bool) -> int:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT product_id, name FROM products WHERE emoji IS NULL"
        ).fetchall()

    if not rows:
        print("All products already have emojis.")
        return 0

    updates: list[tuple[str, str]] = []
    for row in rows:
        name_lower = row["name"].lower() if row["name"] else ""
        for keyword, emoji in EMOJI_RULES:
            if keyword in name_lower:
                updates.append((emoji, row["product_id"]))
                break

    if dry_run:
        for emoji, pid in updates[:20]:
            name = next(r["name"] for r in rows if r["product_id"] == pid)
            print(f"  {emoji}  {name}")
        print(f"  ... {len(updates)} total (showing first 20)")
        return len(updates)

    with get_conn() as conn:
        conn.executemany(
            "UPDATE products SET emoji = ? WHERE product_id = ?",
            updates,
        )

    unmatched = len(rows) - len(updates)
    print(f"Assigned emojis to {len(updates)} products. {unmatched} unmatched.")
    return len(updates)


# ── Step 2: Brand extraction ─────────────────────────────────────────
# Known Israeli grocery brands — matched against product names when brand is null.

KNOWN_BRANDS: list[str] = [
    # Dairy
    "תנובה", "טרה", "יטבתה", "שטראוס", "גד", "מעדנות", "דנונה",
    "אקטיביה", "מולר", "יופלה", "פרי הגליל",
    # Food producers
    "אסם", "עלית", "תלמה", "סוגת", "אחוה", "אנגל", "זוגלובק",
    "עוף טוב", "עוף העמק", "טיבול", "סוגת", "של ערב",
    "אורגניקלין", "סנפרוסט",
    # Snacks
    "במבה", "ביסלי", "תפוצ'יפס", "דוריטוס", "פרינגלס",
    # Drinks
    "נביעות", "מי עדן", "פריגת", "קוקה קולה", "פפסי",
    "שוופס", "נסטלה", "ספרינג", "ריו",
    # International food
    "הינץ", "ברילה", "דה צ'צ'ו", "קנור", "מגי", "קלוגס",
    "פילדלפיה", "פרסידנט", "קרפט", "נוטלה", "בן אנד ג'ריס",
    # Cleaning
    "סנו", "כיף", "מר מוסקיטו", "פיירי",
    "אריאל", "פרסיל", "סופט",
    # Personal care
    "דאב", "ניבאה", "פנטן", "הד אנד שולדרס",
    "קולגייט", "אורל בי", "לנקום", "לוריאל",
    # Baby
    "סימילאק", "מטרנה", "האגיס", "פמפרס",
    # Private labels
    "שופרסל", "רמי לוי", "ויקטורי", "אושר עד",
    "מחסני השוק", "חצי חינם",
    # More food
    "עדיף", "פלדמן", "וילי פוד", "פנדה", "סופרוג'ם",
    "כרמל", "גלידות שטראוס", "נסטלה",
    "ליפטון", "רביולי", "קנור",
]

# Sort longest-first so "קוקה קולה" matches before a hypothetical "קוקה"
KNOWN_BRANDS.sort(key=len, reverse=True)

# Junk values to treat as null
JUNK_BRANDS = {"לא ידוע", "כללי", "---", "משתנה", "שונות", "לא", "אחר"}


def extract_brands(dry_run: bool) -> int:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT product_id, name, brand FROM products "
            "WHERE brand IS NULL OR brand = '' OR brand IN ({})".format(
                ",".join("?" for _ in JUNK_BRANDS)
            ),
            list(JUNK_BRANDS),
        ).fetchall()

    if not rows:
        print("All products already have brands.")
        return 0

    # Pre-compile word-boundary patterns for each brand
    brand_patterns = [
        (brand, re.compile(r"(?:^|[\s(*/\-])(" + re.escape(brand) + r")(?:[\s)*/\-.,]|$)"))
        for brand in KNOWN_BRANDS
    ]

    updates: list[tuple[str, str]] = []
    for row in rows:
        name = row["name"] or ""
        for brand, pattern in brand_patterns:
            if pattern.search(name):
                updates.append((brand, row["product_id"]))
                break

    if dry_run:
        for brand, pid in updates[:20]:
            name = next(r["name"] for r in rows if r["product_id"] == pid)
            print(f"  [{brand:12}]  {name}")
        print(f"  ... {len(updates)} total (showing first 20)")
        return len(updates)

    with get_conn() as conn:
        conn.executemany(
            "UPDATE products SET brand = ? WHERE product_id = ?",
            updates,
        )

    unmatched = len(rows) - len(updates)
    print(f"Extracted brands for {len(updates)} products. {unmatched} still unbranded.")
    return len(updates)


# ── Step 3: Name cleanup ─────────────────────────────────────────────

def clean_name(raw: str) -> str:
    """Normalize a raw product name."""
    s = raw.strip()
    # Fix missing space before/after parentheses: "ליטר)פיקו" → "ליטר) פיקו"
    s = re.sub(r"\)(\S)", r") \1", s)
    s = re.sub(r"(\S)\(", r"\1 (", s)
    # Collapse multiple spaces
    s = re.sub(r"\s{2,}", " ", s)
    # Strip leading/trailing *
    s = s.strip("* ")
    return s


def clean_names(dry_run: bool) -> int:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT product_id, name, raw_name FROM products WHERE raw_name IS NULL"
        ).fetchall()

    if not rows:
        print("All products already have raw_name saved.")
        return 0

    updates: list[tuple[str, str, str]] = []
    changed = 0
    for row in rows:
        original = row["name"]
        cleaned = clean_name(original)
        updates.append((cleaned, original, row["product_id"]))
        if cleaned != original:
            changed += 1

    if dry_run:
        shown = 0
        for cleaned, original, pid in updates:
            if cleaned != original and shown < 20:
                print(f"  {original}")
                print(f"  → {cleaned}")
                print()
                shown += 1
        print(f"  {changed} names would change out of {len(updates)} total")
        return changed

    with get_conn() as conn:
        conn.executemany(
            "UPDATE products SET name = ?, raw_name = ? WHERE product_id = ?",
            updates,
        )

    print(f"Saved raw_name for {len(updates)} products. Cleaned {changed} names.")
    return changed


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich product data")
    parser.add_argument("--emoji", action="store_true", help="Only assign emojis")
    parser.add_argument("--brand", action="store_true", help="Only extract brands")
    parser.add_argument("--name", action="store_true", help="Only clean names")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    init_db()

    run_all = not (args.emoji or args.brand or args.name)

    if run_all or args.emoji:
        print("═══ Step 1: Emoji assignment ═══")
        assign_emojis(args.dry_run)
        print()

    if run_all or args.brand:
        print("═══ Step 2: Brand extraction ═══")
        extract_brands(args.dry_run)
        print()

    if run_all or args.name:
        print("═══ Step 3: Name cleanup ═══")
        clean_names(args.dry_run)
        print()


if __name__ == "__main__":
    main()
