"""Kaggle source mode for the scraper.

Populates parsed_dir with chain CSVs from the daily-updated Kaggle dataset
`erlichsefi/israeli-supermarkets-2024`. The dataset publishes pre-parsed
CSVs in the same format il_supermarket_parsers produces, so the existing
_load_chain consumes them unchanged once we undo the upstream parser's
RLE compression on metadata columns (see _patch_kaggle_csv).

Bypasses the geo-blocked chain portals — anonymous kagglehub downloads
work from the VPS without API keys or VPN.
"""

import logging
from pathlib import Path

import pandas as pd

from app.scraper.chains import KAGGLE_FILE_STEM

logger = logging.getLogger(__name__)

KAGGLE_DATASET = "erlichsefi/israeli-supermarkets-2024"

# Columns the upstream parser RLE-collapses to '' in rows where the value
# equals the previous row's. Without ffill, _load_prices drops every row past
# the first per source XML because storeid is empty. Different sets per
# file kind: store files have one row per store (so storeid varies row-to-
# row legitimately), but price/promo files concatenate per-store XMLs (one
# storeid each), so storeid IS RLE-collapsed there.
_RLE_METADATA_STORE = (
    "found_folder", "file_name", "chainid", "lastupdatedate", "chainname",
)
_RLE_METADATA_MULTI_STORE = (
    "found_folder", "file_name", "chainid",
    "subchainid", "storeid", "bikoretno",
)

_dataset_path: str | None = None


def _download_dataset() -> str:
    """Download (or use cached) latest Kaggle snapshot. Cached per-process so
    multiple chains in one scrape share a single download."""
    global _dataset_path
    if _dataset_path is not None:
        return _dataset_path
    import kagglehub
    _dataset_path = kagglehub.dataset_download(KAGGLE_DATASET)
    logger.info("Kaggle dataset %s available at %s", KAGGLE_DATASET, _dataset_path)
    return _dataset_path


def _ffill_metadata(df: pd.DataFrame, columns) -> pd.DataFrame:
    """Forward-fill the named columns. '' → previous row's value; cells
    outside the named set are left untouched (genuine empties stay empty)."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].replace("", pd.NA).ffill().fillna("")
    return df


def _patch_kaggle_csv(src: Path, dst: Path, file_kind: str) -> None:
    """Read a Kaggle CSV and rewrite it to dst with RLE-collapsed metadata
    columns ffilled. file_kind is 'store' | 'price' | 'promo'."""
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]
    cols = _RLE_METADATA_STORE if file_kind == "store" else _RLE_METADATA_MULTI_STORE
    df = _ffill_metadata(df, cols)
    df.to_csv(dst, index=False)


# (Kaggle filename-prefix, our internal file kind). The prefixes match
# what _load_stores / _load_prices / _load_promotions glob for, so we
# preserve them when copying into parsed_dir.
_FILE_TYPES = (
    ("store_file_", "store"),
    ("price_full_file_", "price"),
    ("promo_full_file_", "promo"),
)


def populate_parsed_dir(chain_id: str, parsed_dir: str) -> None:
    """Copy this chain's store/price/promo CSVs from the Kaggle snapshot
    into parsed_dir, ffilling RLE-collapsed metadata so _load_chain can
    consume them unchanged.

    Raises KeyError if chain_id has no Kaggle mapping."""
    if chain_id not in KAGGLE_FILE_STEM:
        raise KeyError(f"chain_id={chain_id!r} has no Kaggle source mapping")
    stem = KAGGLE_FILE_STEM[chain_id]
    src_root = Path(_download_dataset())
    parsed = Path(parsed_dir)

    for prefix, kind in _FILE_TYPES:
        src = src_root / f"{prefix}{stem}.csv"
        if not src.exists():
            logger.warning(
                "Kaggle %s CSV for %s missing at %s — skipping (chain may "
                "not publish this file type, or upstream parser dropped it).",
                kind, chain_id, src.name,
            )
            continue
        dst = parsed / f"{prefix}{stem}.csv"
        _patch_kaggle_csv(src, dst, kind)
        logger.info("Kaggle %s CSV for %s → %s", kind, chain_id, dst.name)
