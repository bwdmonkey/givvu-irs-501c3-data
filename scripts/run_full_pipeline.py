#!/usr/bin/env python3
"""Run the full IRS 501(c)(3) data pipeline end-to-end.

Steps:
  1. Download + filter EO BMF CSVs (organisations)
  2. Download IRS 990 e-file index from AWS S3
  3. Download 990 XML files in parallel
  4. Parse XMLs → filings.jsonl + schedule_m.jsonl
  5. Upload JSONL to GCS and load into BigQuery
  6. Create/refresh the prospecting view

Usage:
    python -m scripts.run_full_pipeline [--force] [--xml-limit N]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

logger = logging.getLogger("pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full IRS 501(c)(3) pipeline")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download and re-parse everything, ignoring caches",
    )
    parser.add_argument(
        "--xml-limit", type=int, default=None,
        help="Limit the number of XML files to download (for testing)",
    )
    parser.add_argument(
        "--skip-bigquery", action="store_true",
        help="Skip BigQuery upload (useful for local-only runs)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    t0 = time.time()

    # ── Step 1: EO BMF ────────────────────────────────────────────────
    logger.info("═══ Step 1/6: Download + filter EO BMF (organisations) ═══")
    from pipeline.download_bmf import download_and_parse_all
    download_and_parse_all(force_download=args.force)

    # ── Step 2: 990 Index ─────────────────────────────────────────────
    logger.info("═══ Step 2/6: Download + filter 990 e-file index ═══")
    from pipeline.download_index import build_filtered_index
    build_filtered_index(force=args.force)

    # ── Step 3: Download XMLs ─────────────────────────────────────────
    logger.info("═══ Step 3/6: Download 990 XML files ═══")
    from pipeline.download_xml import run as download_xmls
    download_xmls(force=args.force, limit=args.xml_limit)

    # ── Step 4: Parse XMLs ────────────────────────────────────────────
    logger.info("═══ Step 4/6: Parse XML files ═══")
    from pipeline.parse_990 import parse_all_xmls
    parse_all_xmls(force=args.force)

    if not args.skip_bigquery:
        # ── Step 5: Load into BigQuery ────────────────────────────────
        logger.info("═══ Step 5/6: Upload to GCS + load into BigQuery ═══")
        from pipeline.load_bigquery import load_all
        load_all()

        # ── Step 6: Create prospecting view ───────────────────────────
        logger.info("═══ Step 6/6: Create prospecting view ═══")
        from pipeline.views import create_prospecting_view
        create_prospecting_view()
    else:
        logger.info("═══ Steps 5-6 skipped (--skip-bigquery) ═══")

    elapsed = time.time() - t0
    logger.info("Pipeline complete in %.1f minutes.", elapsed / 60)


if __name__ == "__main__":
    sys.exit(main() or 0)
