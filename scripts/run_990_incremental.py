#!/usr/bin/env python3
"""Incrementally process new 990 XML filings.

This script:
  1. Re-downloads the index files (they grow as the IRS adds new filings)
  2. Downloads only NEW XML files (checkpoint-aware)
  3. Re-parses all XMLs (or only new ones if you pass --parse-new-only)
  4. Reloads the filings + schedule_m tables in BigQuery

Usage:
    python -m scripts.run_990_incremental [--skip-bigquery] [--xml-limit N]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

logger = logging.getLogger("pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental 990 filing update")
    parser.add_argument("--skip-bigquery", action="store_true")
    parser.add_argument("--xml-limit", type=int, default=None,
                        help="Limit XML downloads (for testing)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    t0 = time.time()

    # Step 1: Refresh index (force re-download to pick up new filings)
    logger.info("═══ Step 1/4: Refresh 990 index ═══")
    from pipeline.download_index import build_filtered_index
    build_filtered_index(force=True)

    # Step 2: Download new XMLs only (checkpoint-aware)
    logger.info("═══ Step 2/4: Download new XML files ═══")
    from pipeline.download_xml import run as download_xmls
    new_count = download_xmls(force=False, limit=args.xml_limit)
    logger.info("Downloaded %d new XML files.", new_count)

    # Step 3: Re-parse all XMLs (idempotent)
    logger.info("═══ Step 3/4: Parse XML files ═══")
    from pipeline.parse_990 import parse_all_xmls
    parse_all_xmls(force=True)

    if not args.skip_bigquery:
        # Step 4: Reload filings + schedule_m (and refresh view)
        logger.info("═══ Step 4/4: Load into BigQuery ═══")
        from pipeline.load_bigquery import load_filings, load_schedule_m
        load_filings()
        load_schedule_m()

        from pipeline.views import create_prospecting_view
        create_prospecting_view()
    else:
        logger.info("BigQuery upload skipped (--skip-bigquery)")

    elapsed = time.time() - t0
    logger.info("Incremental update complete in %.1f minutes.", elapsed / 60)


if __name__ == "__main__":
    sys.exit(main() or 0)
