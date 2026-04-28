"""Verify the il_supermarket_parser RLE patch produces sentinel-tagged output
and that downstream sentinel→ffill recovery is correct.

If this test fails after a library upgrade, the patch needs review — the
underlying reduce_size signature or behavior may have changed."""

import pandas as pd
import pytest

from il_supermarket_parsers.documents.xml_dataframe_parser import (
    XmlDataFrameConverter,
)

from app.scraper.parser_patch import (
    RLE_SENTINEL,
    _reduce_size_with_sentinel,
    apply_parser_patch,
)


def test_apply_is_idempotent():
    apply_parser_patch()
    apply_parser_patch()
    assert XmlDataFrameConverter.reduce_size is _reduce_size_with_sentinel


def test_first_row_never_masked():
    """Library compares each cell to shift()'s previous, which yields NaN at
    row 0; NaN-vs-string never matches, so row 0 is always preserved. Our
    recovery logic depends on this — a sentinel can never appear at row 0."""
    apply_parser_patch()
    df = pd.DataFrame([{"city": ""}, {"city": ""}])
    out = XmlDataFrameConverter(list_key="Store", id_field="StoreId").reduce_size(df)
    assert out.loc[0, "city"] == ""
    assert out.loc[1, "city"] == RLE_SENTINEL


def test_hazi_hinam_repro():
    """The motivating case (project_parser_rle_bug.md): two consecutive stores
    with the same City. Pre-patch, row 1's City was indistinguishable from a
    genuine empty. Post-patch it's a sentinel, so the loader can recover it."""
    apply_parser_patch()
    df = pd.DataFrame([
        {"storeid": "207", "city": "8300", "address": "Foo St 1"},
        {"storeid": "208", "city": "8300", "address": "Bar St 2"},
        {"storeid": "209", "city": "6600", "address": ""},
        {"storeid": "210", "city": "",     "address": "Online"},
    ])
    out = XmlDataFrameConverter(list_key="Store", id_field="StoreId").reduce_size(df)

    assert out.loc[1, "city"] == RLE_SENTINEL          # RLE-masked
    assert out.loc[1, "address"] == "Bar St 2"         # not masked (differs from prev)
    assert out.loc[2, "city"] == "6600"                # not masked (differs from prev)
    assert out.loc[2, "address"] == ""                 # genuine empty (differs from prev "Bar St 2")
    assert out.loc[3, "city"] == ""                    # genuine empty (differs from prev "6600")
    assert out.loc[3, "address"] == "Online"


def test_recovery_via_replace_and_ffill():
    """The downstream contract: replace sentinel with NA and ffill to recover
    RLE-masked values without touching genuine empties."""
    apply_parser_patch()
    df = pd.DataFrame([
        {"city": "8300"},
        {"city": "8300"},  # → sentinel
        {"city": "6600"},
        {"city": ""},      # genuine empty (differs from prev "6600")
    ])
    out = XmlDataFrameConverter(list_key="Store", id_field="StoreId").reduce_size(df)

    recovered = out.replace(RLE_SENTINEL, pd.NA).ffill()
    assert recovered["city"].tolist() == ["8300", "8300", "6600", ""]


def test_csv_roundtrip_preserves_distinction(tmp_path):
    """Patch is only useful if the sentinel survives a to_csv → read_csv
    roundtrip identically. Verifies the CSV consumer contract used by
    runner._read_parser_csv."""
    apply_parser_patch()
    df = pd.DataFrame([
        {"city": "8300"},
        {"city": "8300"},  # → sentinel
        {"city": ""},      # genuine empty
    ])
    out = XmlDataFrameConverter(list_key="Store", id_field="StoreId").reduce_size(df)
    csv_path = tmp_path / "store.csv"
    out.to_csv(csv_path, index=False)

    re_read = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    re_read.columns = [c.strip().lower() for c in re_read.columns]
    assert re_read["city"].tolist() == ["8300", RLE_SENTINEL, ""]

    recovered = re_read.replace(RLE_SENTINEL, pd.NA).ffill()
    assert recovered["city"].tolist() == ["8300", "8300", ""]


def test_empty_dataframe_handled():
    apply_parser_patch()
    df = pd.DataFrame()
    out = XmlDataFrameConverter(list_key="Store", id_field="StoreId").reduce_size(df)
    assert len(out) == 0
