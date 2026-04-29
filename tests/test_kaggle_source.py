"""Unit tests for the Kaggle CSV RLE-fix.

Network-dependent paths (download, populate_parsed_dir end-to-end) are
exercised in the manual smoke test under scripts/, not here.
"""

import csv

import pytest

from app.scraper.kaggle_source import _patch_kaggle_csv


def _read_rows(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_price_csv_storeid_ffilled_within_file(tmp_path):
    """One source XML = one store, so storeid is RLE-collapsed to '' on
    every row past row 1. Without ffill, _load_prices drops 99% of rows."""
    src = tmp_path / "price_full_file_x.csv"
    src.write_text(
        "found_folder,file_name,chainid,storeid,bikoretno,itemcode,itemprice\n"
        "f,A.xml,7290,182,3,11111,5.00\n"
        ",,,,,22222,6.00\n"
        ",,,,,33333,7.00\n"
        "f,B.xml,7290,200,4,44444,8.00\n"
        ",,,,,55555,9.00\n"
    )
    dst = tmp_path / "out.csv"
    _patch_kaggle_csv(src, dst, "price")

    rows = _read_rows(dst)
    assert [r["storeid"] for r in rows] == ["182", "182", "182", "200", "200"]
    assert [r["chainid"] for r in rows] == ["7290"] * 5
    assert [r["bikoretno"] for r in rows] == ["3", "3", "3", "4", "4"]
    # Per-row data preserved — itemcode is never RLE-masked.
    assert [r["itemcode"] for r in rows] == ["11111", "22222", "33333", "44444", "55555"]


def test_store_csv_does_not_ffill_subchainid(tmp_path):
    """Store files have one row per store; subchainid varies row-to-row.
    A genuinely-empty subchainid (store has no sub-brand) must NOT inherit
    the previous row's value."""
    src = tmp_path / "store_file_x.csv"
    src.write_text(
        "found_folder,file_name,chainid,subchainid,storeid,storename\n"
        "f,A.xml,7290,1,1,store_one\n"
        ",,,,2,store_two\n"   # subchainid empty — could be RLE OR genuine
        ",,,3,3,store_three\n"
        ",,,,4,store_four\n"  # subchainid empty
    )
    dst = tmp_path / "out.csv"
    _patch_kaggle_csv(src, dst, "store")

    rows = _read_rows(dst)
    # Chain-level metadata IS ffilled.
    assert [r["chainid"] for r in rows] == ["7290"] * 4
    # But subchainid is NOT — preserves the structural ambiguity rather
    # than risk corrupting genuine empties. Loader treats '' as None.
    assert [r["subchainid"] for r in rows] == ["1", "", "3", ""]
    # Per-row data preserved.
    assert [r["storeid"] for r in rows] == ["1", "2", "3", "4"]
    assert [r["storename"] for r in rows] == ["store_one", "store_two", "store_three", "store_four"]


def test_genuine_empties_in_non_metadata_columns_preserved(tmp_path):
    """A row with no manufacturername in the source XML must stay empty —
    must not inherit the previous row's manufacturer."""
    src = tmp_path / "price_full_file_x.csv"
    src.write_text(
        "found_folder,file_name,chainid,storeid,bikoretno,itemcode,itemname,manufacturername\n"
        "f,A.xml,7290,1,1,11111,Apple,Tnuva\n"
        ",,,,,22222,Bread,\n"
        ",,,,,33333,Cookie,Osem\n"
    )
    dst = tmp_path / "out.csv"
    _patch_kaggle_csv(src, dst, "price")

    rows = _read_rows(dst)
    assert [r["manufacturername"] for r in rows] == ["Tnuva", "", "Osem"]
