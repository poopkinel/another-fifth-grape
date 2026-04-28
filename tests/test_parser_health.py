"""Verify per-(run, chain, file_type, column) cell counts are recorded into
the parser_health table when run_id is supplied to the loaders. This is the
regression alarm for the upstream parser library — drift in these counts
across runs of the same chain is the signal something changed."""

import pandas as pd
import pytest

import app.db as app_db
from app.scraper.parser_patch import RLE_SENTINEL
from app.scraper.runner import _read_parser_csv


@pytest.fixture
def run_id(db):
    """Create a scrape_runs row for the test and return its id."""
    with app_db.get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO scrape_runs (chain_id, started_at, status) VALUES (?, ?, 'running')",
            ("test_chain", "2026-04-28T00:00:00+00:00"),
        )
        return cur.lastrowid


def _write_csv(path, rows, columns):
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)


def test_records_per_column_counts(tmp_path, db, run_id):
    """Synthetic CSV with a mix of sentinel/empty/value cells in each column;
    the recorded counts must match what's actually in the file."""
    csv_path = tmp_path / "store.csv"
    _write_csv(csv_path, [
        ("207", "8300",       "Foo St 1"),
        ("208", RLE_SENTINEL, "Bar St 2"),    # city RLE-masked
        ("209", "6600",       ""),            # address genuinely empty
        ("210", "",           "Online"),      # city genuinely empty
    ], columns=["storeid", "city", "address"])

    with app_db.get_conn() as conn:
        _read_parser_csv(
            str(csv_path),
            health_ctx=(conn, run_id, "test_chain", "store"),
        )
        rows = list(conn.execute("""
            SELECT column_name, rle_masked, empty_count, nonempty_count, total_rows
            FROM parser_health
            WHERE scrape_run_id=? AND chain_id=? AND file_type=?
            ORDER BY column_name
        """, (run_id, "test_chain", "store")))

    by_col = {r[0]: tuple(r) for r in rows}
    # storeid: 4 values, no sentinel/empty
    assert by_col["storeid"] == ("storeid", 0, 0, 4, 4)
    # city: 1 sentinel (row 1), 1 empty (row 3), 2 values
    assert by_col["city"] == ("city", 1, 1, 2, 4)
    # address: 1 empty (row 2), 3 values
    assert by_col["address"] == ("address", 0, 1, 3, 4)


def test_no_recording_without_run_id(tmp_path, db):
    """Loaders called without a run_id (e.g. test_pipeline_local.py) must NOT
    write parser_health rows — recording is opt-in to keep ad-hoc runs clean."""
    csv_path = tmp_path / "store.csv"
    _write_csv(csv_path, [("207", "8300"), ("208", RLE_SENTINEL)],
               columns=["storeid", "city"])

    _read_parser_csv(str(csv_path))  # no health_ctx

    with app_db.get_conn() as conn:
        n = conn.execute("SELECT COUNT(*) FROM parser_health").fetchone()[0]
    assert n == 0


def test_recovery_still_works_with_recording(tmp_path, db, run_id):
    """Recording is a side-effect; the returned DataFrame must still be the
    fully-recovered one (sentinels replaced and ffilled)."""
    csv_path = tmp_path / "store.csv"
    _write_csv(csv_path, [
        ("207", "8300"),
        ("208", RLE_SENTINEL),
        ("209", RLE_SENTINEL),
    ], columns=["storeid", "city"])

    with app_db.get_conn() as conn:
        df = _read_parser_csv(
            str(csv_path),
            health_ctx=(conn, run_id, "test_chain", "store"),
        )

    assert df["city"].tolist() == ["8300", "8300", "8300"]
