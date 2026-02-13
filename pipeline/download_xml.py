#!/usr/bin/env python3
"""Asynchronously download 990 XML files from AWS S3 in parallel.

Reads the filtered index CSV produced by ``download_index.py`` and downloads
each XML return into ``data/xml/<object_id>.xml``.  A checkpoint file tracks
which object IDs have already been downloaded so the process can be resumed.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path

import aiohttp
from tqdm import tqdm

from pipeline.config import (
    INDEX_DIR,
    XML_DIR,
    XML_DOWNLOAD_CONCURRENCY,
    s3_xml_url,
)

logger = logging.getLogger(__name__)

CHECKPOINT_FILE = XML_DIR / ".downloaded.txt"


def _load_checkpoint() -> set[str]:
    """Load the set of already-downloaded object IDs."""
    if not CHECKPOINT_FILE.exists():
        return set()
    return set(CHECKPOINT_FILE.read_text().splitlines())


def _append_checkpoint(object_ids: list[str]) -> None:
    """Append newly-downloaded object IDs to the checkpoint file."""
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        for oid in object_ids:
            f.write(oid + "\n")


def load_index() -> list[dict]:
    """Read the filtered index and return a list of dicts."""
    path = INDEX_DIR / "filtered_index.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run download_index.py first."
        )
    rows: list[dict] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


async def _download_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    object_id: str,
    dest: Path,
) -> str | None:
    """Download a single XML file. Returns object_id on success, None on failure."""
    url = s3_xml_url(object_id)
    async with semaphore:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    dest.write_bytes(content)
                    return object_id
                else:
                    logger.warning("HTTP %d for %s", resp.status, object_id)
                    return None
        except Exception:
            logger.debug("Error downloading %s", object_id, exc_info=True)
            return None


async def download_xmls(
    max_concurrent: int = XML_DOWNLOAD_CONCURRENCY,
    force: bool = False,
    limit: int | None = None,
) -> int:
    """Download 990 XML files for all filings in the filtered index.

    Parameters
    ----------
    max_concurrent : int
        Maximum number of simultaneous HTTP connections.
    force : bool
        If True, re-download even if the checkpoint says it is done.
    limit : int | None
        If set, only download this many files (useful for testing).

    Returns
    -------
    int
        Number of newly-downloaded files.
    """
    index_rows = load_index()
    if not index_rows:
        logger.warning("Filtered index is empty. Nothing to download.")
        return 0

    # Determine which files still need downloading
    if force:
        done: set[str] = set()
    else:
        done = _load_checkpoint()

    to_download = [
        row for row in index_rows
        if row["object_id"] not in done
    ]

    if limit is not None:
        to_download = to_download[:limit]

    if not to_download:
        logger.info("All %d XML files already downloaded.", len(index_rows))
        return 0

    logger.info(
        "Downloading %d XML files (%d already done, %d total) ...",
        len(to_download), len(done), len(index_rows),
    )

    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=60, connect=15)

    downloaded: list[str] = []
    batch_size = 500  # Checkpoint every N downloads

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        pbar = tqdm(total=len(to_download), desc="Downloading XMLs")
        tasks: list[asyncio.Task] = []

        for row in to_download:
            oid = row["object_id"]
            dest = XML_DIR / f"{oid}.xml"
            task = asyncio.create_task(
                _download_one(session, semaphore, oid, dest)
            )
            tasks.append(task)

        for coro in asyncio.as_completed(tasks):
            result = await coro
            pbar.update(1)
            if result:
                downloaded.append(result)
                # Periodic checkpoint
                if len(downloaded) % batch_size == 0:
                    _append_checkpoint(downloaded[-batch_size:])

        pbar.close()

    # Final checkpoint write
    remainder = len(downloaded) % batch_size
    if remainder:
        _append_checkpoint(downloaded[-remainder:])

    logger.info("Downloaded %d new XML files.", len(downloaded))
    return len(downloaded)


def run(force: bool = False, limit: int | None = None) -> int:
    """Synchronous entry point for the async downloader."""
    return asyncio.run(download_xmls(force=force, limit=limit))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(force=False)
