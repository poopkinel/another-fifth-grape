"""Hard-delete stores soft-deleted more than --age-days ago.

Stage 6 of the prune-on-scrape rollout. Runs after the recovery window
on rows that stage 5 soft-deleted and stage 3 never restored.

Default: dry-run. Pass --commit to actually delete. Cascades:
- prices / promotions: explicit DELETE keyed on (store_id, chain_id)
- promotion_items: cascades from promotions via FK
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import get_conn, init_db


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--age-days", type=int, default=30,
        help="Sweep stores soft-deleted more than this many days ago (default 30).",
    )
    ap.add_argument(
        "--commit", action="store_true",
        help="Actually delete. Without this, prints counts only.",
    )
    args = ap.parse_args()

    init_db()
    cutoff = f"datetime('now', '-{args.age_days} days')"

    with get_conn() as conn:
        target_keys = conn.execute(
            f"SELECT store_id, chain_id FROM stores "
            f"WHERE deleted_at IS NOT NULL AND deleted_at < {cutoff}"
        ).fetchall()

        if not target_keys:
            print(f"No stores soft-deleted more than {args.age_days} days ago.")
            return

        n_stores = len(target_keys)
        # Pre-count cascades so dry-run is informative.
        placeholders = ",".join("(?,?)" for _ in target_keys)
        flat = [v for pair in target_keys for v in pair]
        n_prices = conn.execute(
            f"SELECT COUNT(*) FROM prices WHERE (store_id, chain_id) IN ({placeholders})",
            flat,
        ).fetchone()[0]
        n_promos = conn.execute(
            f"SELECT COUNT(*) FROM promotions WHERE (store_id, chain_id) IN ({placeholders})",
            flat,
        ).fetchone()[0]
        n_promo_items = conn.execute(
            f"SELECT COUNT(*) FROM promotion_items "
            f"WHERE promo_id IN (SELECT promo_id FROM promotions "
            f"WHERE (store_id, chain_id) IN ({placeholders}))",
            flat,
        ).fetchone()[0]

        verb = "Will delete" if not args.commit else "Deleting"
        print(f"{verb} (age > {args.age_days} days):")
        print(f"  stores:          {n_stores}")
        print(f"  prices:          {n_prices}")
        print(f"  promotions:      {n_promos}")
        print(f"  promotion_items: {n_promo_items}  (cascades from promotions)")

        if not args.commit:
            print("\nDry run — pass --commit to actually delete.")
            return

        conn.execute(
            f"DELETE FROM prices WHERE (store_id, chain_id) IN ({placeholders})",
            flat,
        )
        conn.execute(
            f"DELETE FROM promotions WHERE (store_id, chain_id) IN ({placeholders})",
            flat,
        )
        conn.execute(
            f"DELETE FROM stores WHERE (store_id, chain_id) IN ({placeholders})",
            flat,
        )
        print("Done.")


if __name__ == "__main__":
    main()
