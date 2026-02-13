#!/usr/bin/env python3
"""Refresh only the EO BMF organisations table.

This is a lightweight run that downloads the latest IRS EO BMF data,
filters for 501(c)(3) organisations, and reloads the BigQuery table.
Useful as a monthly refresh since the IRS updates the BMF monthly.

Usage:
    python -m scripts.run_bmf_only [--force] [--skip-bigquery]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

logger = logging.getLogger("pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh EO BMF organisations only")
    parser.add_argument("--force", action="store_true", help="Re-download everything")
    parser.add_argument("--skip-bigquery", action="store_true", help="Skip BigQuery upload")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    t0 = time.time()

    logger.info("═══ Downloading + filtering EO BMF ═══")
    from pipeline.download_bmf import download_and_parse_all
    download_and_parse_all(force_download=args.force)

    if not args.skip_bigquery:
        logger.info("═══ Loading organisations into BigQuery ═══")
        from pipeline.load_bigquery import load_organizations
        load_organizations()
    else:
        logger.info("BigQuery upload skipped (--skip-bigquery)")

    elapsed = time.time() - t0
    logger.info("BMF refresh complete in %.1f seconds.", elapsed)


if __name__ == "__main__":
    sys.exit(main() or 0)
