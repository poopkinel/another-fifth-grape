"""Report drift in parser_health metrics — flags columns where the RLE-mask
rate, source-empty rate, or row count shifted vs. the historical baseline.

Run after a scrape (or on a schedule) to catch signs that the upstream parser
library has regressed or that a chain's feed format has changed. Spikes in
empty rate or sudden disappearance of RLE compression both surface here
without needing the source XMLs to reverify.

Usage:
    python scripts/parser_health_report.py
    python scripts/parser_health_report.py --chain hazi_hinam
    python scripts/parser_health_report.py --window 5 --threshold 0.10

Flagging rules:
  - rle_rate    : |latest - baseline| ≥ --threshold (absolute pct points)
  - empty_rate  : |latest - baseline| ≥ --threshold (absolute pct points)
  - total_rows  : |latest - baseline| / baseline ≥ --rows-threshold (relative)

Baseline = mean of the prior --window runs (excluding latest). Columns with
fewer than 2 runs of history are skipped.
"""

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from typing import NamedTuple

# Make app.* importable.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

from app.db import DB_PATH


class Run(NamedTuple):
    run_id: int
    rle: int
    empty: int
    total: int

    @property
    def rle_rate(self) -> float:
        return self.rle / self.total if self.total else 0.0

    @property
    def empty_rate(self) -> float:
        return self.empty / self.total if self.total else 0.0


def fetch_history(
    db_path: str,
    chain: str | None,
    file_type: str | None,
) -> dict[tuple[str, str, str], list[Run]]:
    """Returns {(chain, file_type, column): [Run, ...]} ordered DESC by run_id.
    Aggregates across csv_basenames within a (run, chain, file_type, column)."""
    where = []
    args: list = []
    if chain:
        where.append("chain_id = ?")
        args.append(chain)
    if file_type:
        where.append("file_type = ?")
        args.append(file_type)
    sql = f"""
        SELECT chain_id, file_type, column_name, scrape_run_id,
               SUM(rle_masked), SUM(empty_count), SUM(total_rows)
        FROM parser_health
        {('WHERE ' + ' AND '.join(where)) if where else ''}
        GROUP BY chain_id, file_type, column_name, scrape_run_id
        ORDER BY chain_id, file_type, column_name, scrape_run_id DESC
    """
    out: dict[tuple[str, str, str], list[Run]] = defaultdict(list)
    conn = sqlite3.connect(db_path)
    try:
        for chain_id, ft, col, run_id, rle, empty, total in conn.execute(sql, args):
            out[(chain_id, ft, col)].append(
                Run(run_id, int(rle or 0), int(empty or 0), int(total or 0))
            )
    finally:
        conn.close()
    return out


def flag_drift(
    history: dict[tuple[str, str, str], list[Run]],
    window: int,
    threshold: float,
    rows_threshold: float,
) -> list[dict]:
    flagged: list[dict] = []
    for (chain, ft, col), runs in history.items():
        if len(runs) < 2:
            continue
        latest = runs[0]
        baseline = runs[1 : 1 + window]
        if not baseline:
            continue
        b_total = sum(p.total for p in baseline) / len(baseline)
        b_rle_rate = sum(p.rle_rate for p in baseline) / len(baseline)
        b_empty_rate = sum(p.empty_rate for p in baseline) / len(baseline)

        reasons: list[str] = []
        if abs(latest.rle_rate - b_rle_rate) >= threshold:
            reasons.append(f"rle {b_rle_rate:.1%} → {latest.rle_rate:.1%}")
        if abs(latest.empty_rate - b_empty_rate) >= threshold:
            reasons.append(f"empty {b_empty_rate:.1%} → {latest.empty_rate:.1%}")
        if b_total > 0 and abs(latest.total - b_total) / b_total >= rows_threshold:
            reasons.append(f"rows {b_total:,.0f} → {latest.total:,}")

        if reasons:
            flagged.append({
                "chain": chain,
                "file_type": ft,
                "column": col,
                "latest_run": latest.run_id,
                "baseline_runs": len(baseline),
                "reasons": reasons,
            })
    flagged.sort(key=lambda r: (r["chain"], r["file_type"], r["column"]))
    return flagged


def render(flagged: list[dict], coverage: dict) -> str:
    lines: list[str] = []
    lines.append("# parser_health drift report")
    lines.append(
        f"_{coverage['n_runs']} runs across {coverage['n_chains']} chain(s); "
        f"{coverage['n_columns']} (chain, file_type, column) tracked._"
    )
    lines.append("")
    if not flagged:
        lines.append("No columns flagged. Nothing has drifted past the threshold.")
        return "\n".join(lines) + "\n"

    lines.append(f"## Flagged ({len(flagged)})")
    lines.append("| chain | file_type | column | latest_run | baseline | reasons |")
    lines.append("|---|---|---|---|---|---|")
    for f in flagged:
        lines.append(
            f"| {f['chain']} | {f['file_type']} | {f['column']} | "
            f"{f['latest_run']} | n={f['baseline_runs']} | "
            f"{'; '.join(f['reasons'])} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Flag drift in parser_health metrics vs. baseline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--chain", help="filter to one chain_id")
    ap.add_argument("--file-type", choices=("store", "price", "promo"),
                    help="filter to one file type")
    ap.add_argument("--window", type=int, default=5,
                    help="prior runs to use as baseline (default 5)")
    ap.add_argument("--threshold", type=float, default=0.10,
                    help="abs pct-point shift in rle_rate or empty_rate to flag (default 0.10 = 10pp)")
    ap.add_argument("--rows-threshold", type=float, default=0.20,
                    help="relative shift in total_rows to flag (default 0.20 = 20%%)")
    ap.add_argument("--db", default=DB_PATH, help=f"SQLite DB path (default {DB_PATH})")
    ap.add_argument("--exit-nonzero-on-flags", action="store_true",
                    help="exit 1 if any column is flagged (for CI/cron alerts)")
    args = ap.parse_args()

    history = fetch_history(args.db, args.chain, args.file_type)
    if not history:
        print("No parser_health rows match. Has a scrape with run_id ever happened yet?",
              file=sys.stderr)
        return 0

    flagged = flag_drift(history, args.window, args.threshold, args.rows_threshold)

    coverage = {
        "n_runs": len({p.run_id for runs in history.values() for p in runs}),
        "n_chains": len({k[0] for k in history}),
        "n_columns": len(history),
    }
    print(render(flagged, coverage))
    return 1 if (flagged and args.exit_nonzero_on_flags) else 0


if __name__ == "__main__":
    sys.exit(main())
