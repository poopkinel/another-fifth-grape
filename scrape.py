#!/usr/bin/env python3
"""CLI entry point: run the scraper for all or specific chains."""

import argparse
import logging
import sys

from app.scraper.chains import CHAINS
from app.scraper.runner import run_scrape


def main():
    parser = argparse.ArgumentParser(description="Fifth Grape – scrape Israeli supermarket data")
    parser.add_argument(
        "--chains",
        nargs="+",
        choices=list(CHAINS.keys()),
        help="Specific chains to scrape (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max files to download per chain per file type (default: all)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    run_scrape(chain_ids=args.chains, file_limit=args.limit)


if __name__ == "__main__":
    main()
