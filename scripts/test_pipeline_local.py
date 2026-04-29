"""Run our parse → load pipeline against a single locally-provided source file
and verify the result. Used to test the per-chain parsed_dir fix without
needing to scrape from the VPS (most chain portals are geo-blocked).

Usage:
    python scripts/test_pipeline_local.py \
        --gz /path/to/StoresFull.xml.gz \
        --chain-id hazi_hinam \
        --db data/test_pipeline.db

The script wipes the target DB, drops the file into the layout the parser
expects (dump_dir/<DumpFolderName>/<file>.xml), runs the pipeline, and prints
verification queries.
"""

import argparse
import gzip
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gz", required=True, help="path to .gz or .xml source file")
    ap.add_argument("--chain-id", required=True, help="our internal chain_id (e.g. hazi_hinam)")
    ap.add_argument("--db", default="data/test_pipeline.db", help="test SQLite DB path")
    ap.add_argument("--file-type", default="STORE_FILE",
                    choices=["STORE_FILE", "PRICE_FULL_FILE", "PROMO_FULL_FILE"])
    args = ap.parse_args()

    src = Path(args.gz).resolve()
    if not src.exists():
        sys.exit(f"file not found: {src}")

    # Override DB path BEFORE importing app modules
    db_path = Path(args.db).resolve()
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["FIFTH_GRAPE_DB"] = str(db_path)

    from app.db import init_db
    from app.scraper.chains import CHAINS
    from app.scraper.runner import _parse_chain, _load_chain

    if args.chain_id not in CHAINS:
        sys.exit(f"unknown chain_id: {args.chain_id}; known: {list(CHAINS)}")

    scraper_name, display_name = CHAINS[args.chain_id]

    from il_supermarket_scarper.utils import DumpFolderNames
    folder_name = DumpFolderNames[scraper_name].value

    init_db()

    dump_dir = tempfile.mkdtemp(prefix="test_pipeline_dump_")
    parsed_dir = tempfile.mkdtemp(prefix="test_pipeline_parsed_")
    try:
        # Place gunzipped XML at dump_dir/<DumpFolderName>/<basename>.xml
        chain_dump_dir = os.path.join(dump_dir, folder_name)
        os.makedirs(chain_dump_dir)

        # Parser library splits filename on '-' and expects ≥2 components, so
        # rename to a parser-compatible pattern: <Type><ChainId>-<StoreNum>-<TS>.xml
        # Type prefix triggers FileTypesFilters detection (Stores → STORE_FILE,
        # PriceFull → PRICE_FULL_FILE, PromoFull → PROMO_FULL_FILE).
        type_prefix = {
            "STORE_FILE": "Stores",
            "PRICE_FULL_FILE": "PriceFull",
            "PROMO_FULL_FILE": "PromoFull",
        }[args.file_type]
        # Trailing digit is required: parser's filename_to_file_type_and_chain_id
        # does re.search(r"\d", prefix) and crashes on None — needs a digit anchor.
        synthetic_name = f"{type_prefix}0-000-202604271200.xml"
        xml_path = os.path.join(chain_dump_dir, synthetic_name)

        if src.suffix == ".gz":
            with gzip.open(src, "rb") as gin, open(xml_path, "wb") as fout:
                shutil.copyfileobj(gin, fout)
        else:
            shutil.copy(src, xml_path)

        print(f"Placed source at: {xml_path}")
        print(f"  size: {os.path.getsize(xml_path):,} bytes")

        print(f"\n→ _parse_chain({scraper_name})")
        _parse_chain(scraper_name, dump_dir, parsed_dir)
        parsed_files = sorted(os.listdir(parsed_dir))
        print(f"  parsed CSVs: {parsed_files}")
        for f in parsed_files:
            n = sum(1 for _ in open(os.path.join(parsed_dir, f), encoding="utf-8")) - 1
            print(f"    {f}: {n} data rows")

        print(f"\n→ _load_chain({args.chain_id}, {scraper_name})")
        _load_chain(args.chain_id, scraper_name, parsed_dir)

        # Verification queries
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        print("\n=== row counts by chain_id ===")
        for r in cur.execute("SELECT chain_id, COUNT(*) AS n FROM stores GROUP BY chain_id"):
            print(f"  {r['chain_id']:20s} {r['n']}")

        print(f"\n=== sample branch_names for {args.chain_id} ===")
        for r in cur.execute(
            "SELECT store_id, branch_name, address, city FROM stores "
            "WHERE chain_id=? ORDER BY store_id LIMIT 15",
            (args.chain_id,),
        ):
            print(f"  {r['store_id']:25s}  {(r['branch_name'] or '')[:40]:40s}  "
                  f"addr={(r['address'] or '')[:30]:30s}  city={(r['city'] or '')[:15]}")

        print(f"\n=== empty-city / URL-address split ===")
        for r in cur.execute("""
            SELECT
                SUM(CASE WHEN city = '' OR city IS NULL THEN 1 ELSE 0 END) AS empty_city,
                SUM(CASE WHEN address LIKE '%http%' OR address LIKE '%www.%' OR address LIKE '%.co.il%' THEN 1 ELSE 0 END) AS url_address,
                COUNT(*) AS total
            FROM stores WHERE chain_id=?
        """, (args.chain_id,)):
            print(f"  total={r['total']}  empty_city={r['empty_city']}  url_address={r['url_address']}")

        print(f"\n=== other tables (sanity) ===")
        for table in ("products", "prices", "promotions", "promotion_items"):
            try:
                n = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {n}")
            except sqlite3.Error as e:
                print(f"  {table}: <{e}>")

        conn.close()
    finally:
        shutil.rmtree(dump_dir, ignore_errors=True)
        shutil.rmtree(parsed_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
