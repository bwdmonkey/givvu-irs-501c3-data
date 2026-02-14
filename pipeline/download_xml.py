#!/usr/bin/env python3
"""Download 990 XML files from the IRS TEOS ZIP bundles.

The IRS distributes e-filed 990 returns as monthly ZIP archives.  Each ZIP
contains thousands of individual XML files.  This module:

1. Reads the filtered index to determine which ZIP bundles are needed.
2. Downloads each unique ZIP bundle (skipping already-downloaded ones).
3. Extracts only the XML files for object IDs in our filtered index.

A checkpoint file tracks which bundles have been fully processed.
"""

from __future__ import annotations

import csv
import logging
import zipfile
from collections import defaultdict
from pathlib import Path

import requests
from tqdm import tqdm

from pipeline.config import (
    INDEX_DIR,
    TAX_YEARS,
    XML_DIR,
    ZIP_DIR,
    irs_zip_url,
)

logger = logging.getLogger(__name__)

CHECKPOINT_FILE = ZIP_DIR / ".processed_batches.txt"


def _load_checkpoint() -> set[str]:
    if not CHECKPOINT_FILE.exists():
        return set()
    return set(CHECKPOINT_FILE.read_text().splitlines())


def _append_checkpoint(batch_id: str) -> None:
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        f.write(batch_id + "\n")


def load_index() -> list[dict]:
    """Read the filtered index and return a list of dicts."""
    path = INDEX_DIR / "filtered_index.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run download_index.py first.")
    rows: list[dict] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def _group_by_batch(index_rows: list[dict]) -> dict[tuple[int, str], set[str]]:
    """Group object IDs by (year, batch_id) for targeted extraction."""
    groups: dict[tuple[int, str], set[str]] = defaultdict(set)
    for row in index_rows:
        batch_id = row.get("xml_batch_id", "").strip()
        oid = row.get("object_id", "").strip()
        if not batch_id or not oid:
            continue
        # Extract year from batch_id (e.g., "2025_TEOS_XML_01A" → 2025)
        try:
            year = int(batch_id[:4])
        except (ValueError, IndexError):
            # Fall back to TAX_YEARS[0]
            year = TAX_YEARS[0] if TAX_YEARS else 2025
        groups[(year, batch_id)].add(oid)
    return groups


def download_and_extract_batch(
    year: int,
    batch_id: str,
    target_oids: set[str] | None = None,
) -> int:
    """Download a ZIP bundle and extract target XMLs.

    Parameters
    ----------
    year : int
        Filing year (used to construct the URL path).
    batch_id : str
        The ZIP batch identifier (e.g., ``2025_TEOS_XML_01A``).
    target_oids : set[str] | None
        If provided, only extract XMLs whose object_id is in this set.
        If None, extract all XMLs.

    Returns
    -------
    int
        Number of XML files extracted.
    """
    zip_path = ZIP_DIR / f"{batch_id}.zip"

    # Download ZIP if not already present
    if not zip_path.exists() or zip_path.stat().st_size == 0:
        url = irs_zip_url(batch_id, year)
        logger.info("Downloading %s ...", url)
        resp = requests.get(url, timeout=600, stream=True)
        resp.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
        logger.debug("Saved %s (%d MB)", zip_path.name, zip_path.stat().st_size // (1024 * 1024))

    # Extract XMLs
    extracted = 0
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".xml"):
                    continue

                # Determine object_id from filename (e.g., "202543569349100509_public.xml")
                stem = Path(name).stem  # e.g., "202543569349100509_public"
                oid = stem.replace("_public", "")

                # If we have a target set, only extract matching files
                if target_oids is not None and oid not in target_oids:
                    continue

                dest = XML_DIR / f"{oid}.xml"
                if dest.exists():
                    extracted += 1
                    continue

                data = zf.read(name)
                dest.write_bytes(data)
                extracted += 1
    except zipfile.BadZipFile:
        logger.error("Bad ZIP file: %s — deleting and will retry next run", zip_path)
        zip_path.unlink(missing_ok=True)
        return 0

    return extracted


def download_xmls(
    force: bool = False,
    limit_batches: int | None = None,
) -> int:
    """Download and extract all needed ZIP bundles.

    Parameters
    ----------
    force : bool
        If True, re-process already-completed batches.
    limit_batches : int | None
        If set, only process this many batches (useful for testing).

    Returns
    -------
    int
        Total number of XML files extracted.
    """
    index_rows = load_index()
    if not index_rows:
        logger.warning("Filtered index is empty. Nothing to download.")
        return 0

    batch_groups = _group_by_batch(index_rows)
    if not batch_groups:
        logger.warning("No batch IDs found in index. Nothing to download.")
        return 0

    done = set() if force else _load_checkpoint()
    to_process = {k: v for k, v in batch_groups.items() if k[1] not in done}

    if limit_batches is not None:
        keys = list(to_process.keys())[:limit_batches]
        to_process = {k: to_process[k] for k in keys}

    if not to_process:
        logger.info("All %d batches already processed.", len(batch_groups))
        return 0

    logger.info(
        "Processing %d ZIP batches (%d already done, %d total) ...",
        len(to_process), len(done), len(batch_groups),
    )

    total_extracted = 0
    for (year, batch_id), target_oids in tqdm(to_process.items(), desc="Downloading ZIPs"):
        try:
            n = download_and_extract_batch(year, batch_id, target_oids)
            total_extracted += n
            _append_checkpoint(batch_id)
            logger.debug("Batch %s: extracted %d XMLs", batch_id, n)
        except Exception:
            logger.exception("Failed to process batch %s", batch_id)

    logger.info("Downloaded and extracted %d XML files from %d batches.",
                total_extracted, len(to_process))
    return total_extracted


def run(force: bool = False, limit: int | None = None) -> int:
    """Synchronous entry point."""
    return download_xmls(force=force, limit_batches=limit)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(force=False)
