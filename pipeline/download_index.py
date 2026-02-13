#!/usr/bin/env python3
"""Download IRS 990 e-file index files from AWS S3 and filter for full-990 filers.

Produces a CSV at ``data/index/filtered_index.csv`` containing only Form 990
(full) filers that are 501(c)(3) organizations.  The optional EIN filter is
built from a previously-downloaded organizations JSONL file.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from pipeline.config import (
    INDEX_DIR,
    PARSED_DIR,
    TAX_YEARS,
    s3_index_url,
)

logger = logging.getLogger(__name__)

# Columns in the S3 index CSV
INDEX_COLUMNS = [
    "RETURN_ID",
    "FILING_TYPE",
    "EIN",
    "TAX_PERIOD",
    "SUB_DATE",
    "TAXPAYER_NAME",
    "RETURN_TYPE",
    "DLN",
    "OBJECT_ID",
]


def download_index_csv(year: int) -> Path:
    """Download the S3 index CSV for the given filing year."""
    dest = INDEX_DIR / f"index_{year}.csv"
    if dest.exists() and dest.stat().st_size > 0:
        logger.debug("Index for %d already downloaded.", year)
        return dest

    url = s3_index_url(year)
    logger.info("Downloading index for %d from %s", year, url)
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()

    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    logger.info("Downloaded %s (%d MB)", dest.name, dest.stat().st_size // (1024 * 1024))
    return dest


def load_501c3_eins() -> set[str] | None:
    """Load the set of 501(c)(3) EINs from the parsed organizations JSONL.

    Returns None if the file does not exist (skip EIN filter).
    """
    orgs_path = PARSED_DIR / "organizations.jsonl"
    if not orgs_path.exists():
        logger.warning(
            "organizations.jsonl not found; skipping 501(c)(3) EIN filter. "
            "Run download_bmf.py first for a tighter filter."
        )
        return None

    eins: set[str] = set()
    with open(orgs_path, "r", encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            ein = row.get("ein")
            if ein:
                # Pad to 9 digits for matching
                eins.add(str(ein).zfill(9))
    logger.info("Loaded %d 501(c)(3) EINs for filtering.", len(eins))
    return eins


def build_filtered_index(
    force: bool = False,
) -> Path:
    """Download index files for configured TAX_YEARS and produce a filtered CSV.

    Returns path to ``data/index/filtered_index.csv``.
    """
    output_path = INDEX_DIR / "filtered_index.csv"

    if output_path.exists() and not force:
        logger.info("filtered_index.csv already exists. Use force=True to rebuild.")
        return output_path

    # Download indexes
    index_paths: list[Path] = []
    for year in TAX_YEARS:
        try:
            index_paths.append(download_index_csv(year))
        except Exception:
            logger.exception("Failed to download index for %d", year)

    # Load 501(c)(3) EINs (optional filter)
    ein_filter = load_501c3_eins()

    # Read, filter, and combine
    all_rows: list[dict] = []
    for path in tqdm(index_paths, desc="Filtering indexes"):
        df = pd.read_csv(path, dtype=str)
        # Normalize column names
        df.columns = [c.strip().upper() for c in df.columns]

        # Filter for Form 990 (full) only — not 990EZ, 990PF, 990O, etc.
        df = df[df["RETURN_TYPE"] == "990"]

        # Filter by EIN if we have the 501(c)(3) list
        if ein_filter is not None:
            df["EIN_PADDED"] = df["EIN"].str.zfill(9)
            df = df[df["EIN_PADDED"].isin(ein_filter)]
            df = df.drop(columns=["EIN_PADDED"])

        for _, row in df.iterrows():
            all_rows.append({
                "object_id": str(row.get("OBJECT_ID", "")).strip(),
                "ein": str(row.get("EIN", "")).strip().zfill(9),
                "tax_period": str(row.get("TAX_PERIOD", "")).strip(),
                "taxpayer_name": str(row.get("TAXPAYER_NAME", "")).strip(),
                "return_type": str(row.get("RETURN_TYPE", "")).strip(),
                "sub_date": str(row.get("SUB_DATE", "")).strip(),
                "dln": str(row.get("DLN", "")).strip(),
            })

    # Write filtered index
    logger.info("Writing %d filtered index rows to %s", len(all_rows), output_path)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "object_id", "ein", "tax_period", "taxpayer_name",
            "return_type", "sub_date", "dln",
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info("Filtered index complete: %d Form 990 filings", len(all_rows))
    return output_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_filtered_index(force=True)
