"""Scraper runner: downloads XMLs, parses them, loads into SQLite."""

import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone

import pandas as pd
from il_supermarket_scarper import ScarpingTask
from il_supermarket_scarper.utils import FileTypesFilters
from il_supermarket_parsers import ConvertingTask

from app.db import (
    get_conn,
    init_db,
    replace_promotion_items,
    upsert_price,
    upsert_product,
    upsert_promotion,
    upsert_store,
)
from app.scraper.chains import CHAINS, scraper_name_to_chain_id

logger = logging.getLogger(__name__)

# Limit files per chain to keep scrape fast (full price + stores is enough)
DEFAULT_FILE_LIMIT = None  # None = download all available files

# STORE_FILE is not in all_full_files(), so we combine manually
SCRAPE_FILE_TYPES = ["PRICE_FULL_FILE", "STORE_FILE", "PROMO_FULL_FILE"]


def run_scrape(chain_ids: list[str] | None = None, file_limit: int = DEFAULT_FILE_LIMIT):
    """Run full scrape→parse→load pipeline for specified chains (or all)."""
    init_db()

    if chain_ids is None:
        chain_ids = list(CHAINS.keys())

    dump_dir = tempfile.mkdtemp(prefix="fifth_grape_dumps_")
    parsed_dir = tempfile.mkdtemp(prefix="fifth_grape_parsed_")

    try:
        for chain_id in chain_ids:
            if chain_id not in CHAINS:
                logger.warning("Unknown chain_id: %s, skipping", chain_id)
                continue

            scraper_name, display_name = CHAINS[chain_id]
            logger.info("═══ Scraping %s (%s) ═══", display_name, chain_id)

            now = datetime.now(timezone.utc).isoformat()
            with get_conn() as conn:
                cur = conn.execute(
                    "INSERT INTO scrape_runs (chain_id, started_at, status) VALUES (?, ?, 'running')",
                    (chain_id, now),
                )
                run_id = cur.lastrowid

            try:
                _scrape_chain(scraper_name, dump_dir, file_limit)
                _parse_chain(scraper_name, dump_dir, parsed_dir)
                _load_chain(chain_id, scraper_name, parsed_dir)

                with get_conn() as conn:
                    conn.execute(
                        "UPDATE scrape_runs SET status='done', finished_at=? WHERE id=?",
                        (datetime.now(timezone.utc).isoformat(), run_id),
                    )
                logger.info("✓ %s done", display_name)

            except Exception as e:
                logger.error("✗ %s failed: %s", display_name, e, exc_info=True)
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE scrape_runs SET status='error', finished_at=?, error=? WHERE id=?",
                        (datetime.now(timezone.utc).isoformat(), str(e), run_id),
                    )
    finally:
        shutil.rmtree(dump_dir, ignore_errors=True)
        shutil.rmtree(parsed_dir, ignore_errors=True)


def _scrape_chain(scraper_name: str, dump_dir: str, file_limit: int):
    """Download XML files for one chain. Scrape stores and prices separately
    to ensure both file types are downloaded (combined requests may skip one)."""
    for file_type in SCRAPE_FILE_TYPES:
        scraper = ScarpingTask(
            enabled_scrapers=[scraper_name],
            files_types=[file_type],
            output_configuration={
                "output_mode": "disk",
                "base_storage_path": dump_dir,
            },
            multiprocessing=1,
        )
        thread = scraper.start(limit=file_limit)
        thread.join()


def _parse_chain(scraper_name: str, dump_dir: str, parsed_dir: str):
    """Parse downloaded XMLs into CSVs."""
    task = ConvertingTask(
        data_folder=dump_dir,
        enabled_parsers=[scraper_name],
        files_types=SCRAPE_FILE_TYPES,
        output_folder=parsed_dir,
    )
    task.start()


def _load_chain(chain_id: str, scraper_name: str, parsed_dir: str):
    """Load parsed CSVs into SQLite."""
    _load_stores(chain_id, parsed_dir)
    _load_prices(chain_id, parsed_dir)
    _load_promotions(chain_id, parsed_dir)


def _load_stores(chain_id: str, parsed_dir: str):
    """Load store data from parsed CSVs."""
    display_name = CHAINS[chain_id][1]

    store_files = [
        f for f in os.listdir(parsed_dir)
        if f.startswith("store") and f.endswith(".csv")
    ]

    if not store_files:
        logger.warning("No store files found for %s", chain_id)
        return

    with get_conn() as conn:
        for sf in store_files:
            path = os.path.join(parsed_dir, sf)
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception as e:
                logger.warning("Failed to read %s: %s", sf, e)
                continue

            # Normalize column names to lowercase
            df.columns = [c.strip().lower() for c in df.columns]

            # Forward-fill root-level fields the parser only sets on the first row
            root_cols = [c for c in ("chainid", "chainname", "subchainid", "subchainname") if c in df.columns]
            if root_cols:
                df[root_cols] = df[root_cols].ffill()

            for _, row in df.iterrows():
                store_id = str(row.get("storeid", "")).strip()
                if not store_id or store_id == "nan":
                    continue

                upsert_store(conn, {
                    "store_id": f"{chain_id}_{store_id}",
                    "chain_id": chain_id,
                    "chain_name": display_name,
                    "branch_name": str(row.get("storename", "")).strip(),
                    "address": _clean_address(row.get("address")),
                    "city": _clean_city(row.get("city")),
                    "lat": None,  # Filled by geocode.py
                    "lng": None,
                })


def _load_prices(chain_id: str, parsed_dir: str):
    """Load product + price data from parsed price CSVs."""
    price_files = [
        f for f in os.listdir(parsed_dir)
        if f.startswith("price") and f.endswith(".csv")
    ]

    if not price_files:
        logger.warning("No price files found for %s", chain_id)
        return

    now = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        for pf in price_files:
            path = os.path.join(parsed_dir, pf)
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception as e:
                logger.warning("Failed to read %s: %s", pf, e)
                continue

            df.columns = [c.strip().lower() for c in df.columns]

            # Parser only sets root-level fields (chainid, storeid, etc.)
            # on the first row per file — forward-fill them across all items
            root_cols = [c for c in ("chainid", "subchainid", "storeid", "bikoretno", "itemstatus") if c in df.columns]
            if root_cols:
                df[root_cols] = df[root_cols].ffill()

            for _, row in df.iterrows():
                item_code = str(row.get("itemcode", "")).strip()
                store_id = str(row.get("storeid", "")).strip()
                store_id = store_id.lstrip("0") or "0"  # Match store CSV format (no leading zeros)
                if not item_code or not store_id or item_code == "nan" or store_id == "nan":
                    continue

                # Price parsing — handle commas, empty strings, NaN
                raw_price = str(row.get("itemprice", "")).strip().replace(",", "")
                if not raw_price or raw_price == "nan":
                    continue
                try:
                    price_val = float(raw_price)
                except ValueError:
                    continue

                # ItemStatus per gov spec: 0 = removed from sale, 1 = active
                item_status = str(row.get("itemstatus", "1")).strip()
                in_stock = item_status != "0"

                # Upsert product
                barcode = item_code if len(item_code) >= 8 else None
                upsert_product(conn, {
                    "product_id": item_code,
                    "name": str(row.get("itemname", "")).strip(),
                    "brand": _nullable(row.get("manufacturername")),
                    "unit": _nullable(row.get("unitofmeasure")),
                    "barcode": barcode,
                    "emoji": None,
                    "category": None,
                })

                # Upsert price
                upsert_price(conn, {
                    "store_id": f"{chain_id}_{store_id}",
                    "chain_id": chain_id,
                    "product_id": item_code,
                    "price": price_val,
                    "in_stock": 1 if in_stock else 0,
                    "updated_at": now,
                })


# Columns we don't want to dump into raw_json (already promoted to typed columns
# or noisy XML metadata). Lowercased.
_PROMO_RAW_DROP = {
    "chainid", "subchainid", "storeid", "bikoretno",
    "promotionid", "promotiondescription", "promotionupdatedate",
    "promotionstartdate", "promotionstarthour",
    "promotionenddate", "promotionendhour",
    "rewardtype", "discountedprice", "minqty", "minpurchaseamnt",
    "itemcode", "isgiftitem",
}


def _combine_date_hour(date_val, hour_val) -> str | None:
    """CPFTA promo dates and hours are separate fields. Combine to ISO when both
    are present; fall back to date alone."""
    d = _nullable(date_val)
    if not d:
        return None
    h = _nullable(hour_val)
    if not h:
        return d
    return f"{d}T{h}"


def _to_float(val) -> float | None:
    s = _nullable(val)
    if s is None:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _load_promotions(chain_id: str, parsed_dir: str):
    """Load promotions + their item lists from parsed promofull CSVs.

    The CSV is denormalized: one row per (promotion x item). We group by
    (storeid, promotionid) and write one promotions row plus N promotion_items rows.
    """
    promo_files = [
        f for f in os.listdir(parsed_dir)
        if f.startswith("promo") and f.endswith(".csv")
    ]
    if not promo_files:
        logger.info("No promo files found for %s", chain_id)
        return

    now = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        for pf in promo_files:
            path = os.path.join(parsed_dir, pf)
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception as e:
                logger.warning("Failed to read %s: %s", pf, e)
                continue

            df.columns = [c.strip().lower() for c in df.columns]

            root_cols = [
                c for c in ("chainid", "subchainid", "storeid", "bikoretno")
                if c in df.columns
            ]
            if root_cols:
                df[root_cols] = df[root_cols].ffill()

            if "promotionid" not in df.columns or "storeid" not in df.columns:
                logger.warning(
                    "promofull %s missing promotionid/storeid columns; skipping", pf
                )
                continue

            # Group: one promotion may span many rows (one per item)
            grouped = df.groupby(["storeid", "promotionid"], sort=False, dropna=False)
            for (storeid_raw, promotion_id_raw), group in grouped:
                store_id = str(storeid_raw or "").strip().lstrip("0") or "0"
                promotion_id = str(promotion_id_raw or "").strip()
                if not promotion_id or promotion_id == "nan":
                    continue
                if store_id == "nan":
                    continue

                head = group.iloc[0]
                promo_id = f"{chain_id}_{store_id}_{promotion_id}"

                start_at = _combine_date_hour(
                    head.get("promotionstartdate"), head.get("promotionstarthour")
                )
                end_at = _combine_date_hour(
                    head.get("promotionenddate"), head.get("promotionendhour")
                )

                # raw_json: keep any feed columns we didn't promote so the data
                # isn't lost if the regulator adds fields later.
                extras = {
                    col: _nullable(head.get(col))
                    for col in group.columns
                    if col not in _PROMO_RAW_DROP
                }
                extras = {k: v for k, v in extras.items() if v is not None}

                upsert_promotion(conn, {
                    "promo_id":         promo_id,
                    "chain_id":         chain_id,
                    "store_id":         f"{chain_id}_{store_id}",
                    "promotion_id":     promotion_id,
                    "description":      _nullable(head.get("promotiondescription")),
                    "start_at":         start_at,
                    "end_at":           end_at,
                    "reward_type":      _nullable(head.get("rewardtype")),
                    "discounted_price": _to_float(head.get("discountedprice")),
                    "min_qty":          _to_float(head.get("minqty")),
                    "min_purchase_amt": _to_float(head.get("minpurchaseamnt")),
                    "update_date":      _nullable(head.get("promotionupdatedate")),
                    "raw_json":         json.dumps(extras, ensure_ascii=False) if extras else None,
                    "updated_at":       now,
                })

                items: list[dict] = []
                seen: set[str] = set()
                for _, row in group.iterrows():
                    item_code = _nullable(row.get("itemcode"))
                    if not item_code or item_code in seen:
                        continue
                    seen.add(item_code)
                    is_gift_raw = _nullable(row.get("isgiftitem"))
                    items.append({
                        "item_code": item_code,
                        "is_gift": 1 if is_gift_raw == "1" else 0,
                    })
                replace_promotion_items(conn, promo_id, items)


def _nullable(val) -> str | None:
    """Return None for NaN/empty/sentinel values."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "nan", "None", "{}"):
        return None
    return s


def _clean_address(val) -> str:
    return _nullable(val) or ""


def _clean_city(val) -> str:
    s = _nullable(val)
    if s is None:
        return ""
    if s.isdigit():
        return ""
    return s
