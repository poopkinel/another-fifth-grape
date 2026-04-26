"""Delete analytics events older than RETENTION_DAYS (default 90).

Run via cron / systemd timer. Idempotent.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import get_conn, init_db, prune_events_older_than

RETENTION_DAYS = int(os.environ.get("EVENTS_RETENTION_DAYS", "90"))


def main() -> None:
    init_db()
    cutoff = int(time.time()) - RETENTION_DAYS * 86400
    with get_conn() as conn:
        deleted = prune_events_older_than(conn, cutoff)
    print(f"pruned {deleted} events older than {RETENTION_DAYS} days")


if __name__ == "__main__":
    main()
