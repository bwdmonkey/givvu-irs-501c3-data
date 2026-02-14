#!/usr/bin/env python3
"""Download IRS EO BMF CSV files and filter for 501(c)(3) organizations.

Produces a single JSONL file at ``data/parsed/organizations.jsonl`` containing
all 501(c)(3) records from all 52 state/territory files.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

from pipeline.config import (
    BMF_COLUMN_MAP,
    BMF_DIR,
    BMF_URLS,
    PARSED_DIR,
)

logger = logging.getLogger(__name__)

# Integer columns in the BMF CSV that should be cast
_INT_COLUMNS = {
    "SUBSECTION", "FOUNDATION", "AFFILIATION", "ORGANIZATION",
    "STATUS", "DEDUCTIBILITY", "ASSET_CD", "INCOME_CD",
    "ASSET_AMT", "INCOME_AMT", "REVENUE_AMT", "TAX_PERIOD",
    "ACCT_PD",
}


def _safe_int(value: str) -> int | None:
    """Convert a string to int, returning None for blanks / non-numeric."""
    if not value or not value.strip():
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


def download_bmf_csv(state_code: str, url: str, dest_dir: Path) -> Path:
    """Download a single BMF CSV file. Returns the local file path."""
    dest = dest_dir / f"eo_{state_code}.csv"
    if dest.exists() and dest.stat().st_size > 0:
        logger.debug("Already downloaded %s", dest.name)
        return dest

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    logger.debug("Downloaded %s (%d bytes)", dest.name, len(resp.content))
    return dest


def parse_bmf_csv(path: Path) -> list[dict]:
    """Read a BMF CSV and return 501(c)(3) records as dicts."""
    records: list[dict] = []
    raw = path.read_bytes()
    text = raw.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    for row in reader:
        # Filter: SUBSECTION == 03 → 501(c)(3)
        # The BMF CSV stores this as "03" (zero-padded)
        sub = row.get("SUBSECTION", "").strip()
        if sub not in ("3", "03"):
            continue

        record: dict = {}
        for csv_col, bq_col in BMF_COLUMN_MAP.items():
            value = row.get(csv_col, "")
            if value is not None:
                value = value.strip()
            if csv_col in _INT_COLUMNS:
                record[bq_col] = _safe_int(value)
            else:
                record[bq_col] = value if value else None
        records.append(record)

    return records


def download_and_parse_all(
    max_workers: int = 10,
    force_download: bool = False,
) -> Path:
    """Download all BMF CSVs in parallel, parse, and write JSONL.

    Returns the path to the output JSONL file.
    """
    output_path = PARSED_DIR / "organizations.jsonl"

    if output_path.exists() and not force_download:
        logger.info("organizations.jsonl already exists. Use force_download=True to re-create.")
        return output_path

    # Step 1: Download CSV files in parallel
    logger.info("Downloading %d BMF CSV files ...", len(BMF_URLS))
    csv_paths: dict[str, Path] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(download_bmf_csv, code, url, BMF_DIR): code
            for code, url in BMF_URLS.items()
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading BMF"):
            code = futures[future]
            try:
                csv_paths[code] = future.result()
            except Exception:
                logger.exception("Failed to download %s", code)

    # Step 2: Parse and filter each CSV
    logger.info("Parsing %d CSV files for 501(c)(3) records ...", len(csv_paths))
    all_records: list[dict] = []

    for code in tqdm(sorted(csv_paths), desc="Parsing BMF"):
        try:
            records = parse_bmf_csv(csv_paths[code])
            all_records.extend(records)
        except Exception:
            logger.exception("Failed to parse %s", code)

    # Step 3: Write JSONL
    logger.info("Writing %d records to %s", len(all_records), output_path)
    with open(output_path, "w", encoding="utf-8") as f:
        for rec in all_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    logger.info("BMF download complete: %d 501(c)(3) organizations", len(all_records))
    return output_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    download_and_parse_all(force_download=True)
