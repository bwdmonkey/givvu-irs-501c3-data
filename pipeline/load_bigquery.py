#!/usr/bin/env python3
"""Upload parsed JSONL files to GCS and load into BigQuery tables.

Handles the three main tables: organizations, filings, and schedule_m.
Files are first uploaded to a GCS staging bucket, then loaded into BigQuery
using a load job (WRITE_TRUNCATE mode for idempotent full refreshes).
"""

from __future__ import annotations

import logging
from pathlib import Path

from google.cloud import bigquery, storage

from pipeline.config import (
    BQ_DATASET,
    BQ_TABLE_FILINGS,
    BQ_TABLE_ORGANIZATIONS,
    BQ_TABLE_SCHEDULE_M,
    GCP_PROJECT_ID,
    GCS_BUCKET,
    PARSED_DIR,
)

logger = logging.getLogger(__name__)


# ── GCS upload ────────────────────────────────────────────────────────────


def upload_to_gcs(local_path: Path, gcs_blob_name: str) -> str:
    """Upload a local file to GCS and return the gs:// URI."""
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_blob_name)

    logger.info("Uploading %s → gs://%s/%s ...", local_path.name, GCS_BUCKET, gcs_blob_name)
    blob.upload_from_filename(str(local_path), timeout=600)
    uri = f"gs://{GCS_BUCKET}/{gcs_blob_name}"
    logger.info("Upload complete: %s", uri)
    return uri


# ── BigQuery load ─────────────────────────────────────────────────────────


def load_jsonl_to_bq(
    gcs_uri: str,
    table_name: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> bigquery.LoadJob:
    """Load a JSONL file from GCS into a BigQuery table.

    Uses schema auto-detection disabled — the table must already exist
    (created by ``setup_bigquery.py``).
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        # Don't auto-detect — use the schema already on the table
        autodetect=False,
        # Tolerate a few bad rows (e.g., schema mismatches from edge cases)
        max_bad_records=100,
        ignore_unknown_values=True,
    )

    logger.info("Loading %s → %s (mode=%s) ...", gcs_uri, table_ref, write_disposition)
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Block until complete

    table = client.get_table(table_ref)
    logger.info(
        "Loaded %d rows into %s.%s",
        table.num_rows, table.dataset_id, table.table_id,
    )
    return load_job


# ── Convenience: upload + load for each table ─────────────────────────────


def load_organizations() -> None:
    """Upload and load organizations.jsonl into BigQuery."""
    local = PARSED_DIR / "organizations.jsonl"
    if not local.exists():
        logger.error("organizations.jsonl not found. Run download_bmf.py first.")
        return
    gcs_uri = upload_to_gcs(local, "staging/organizations.jsonl")
    load_jsonl_to_bq(gcs_uri, BQ_TABLE_ORGANIZATIONS)


def load_filings() -> None:
    """Upload and load filings.jsonl into BigQuery."""
    local = PARSED_DIR / "filings.jsonl"
    if not local.exists():
        logger.error("filings.jsonl not found. Run parse_990.py first.")
        return
    gcs_uri = upload_to_gcs(local, "staging/filings.jsonl")
    load_jsonl_to_bq(gcs_uri, BQ_TABLE_FILINGS)


def load_schedule_m() -> None:
    """Upload and load schedule_m.jsonl into BigQuery."""
    local = PARSED_DIR / "schedule_m.jsonl"
    if not local.exists():
        logger.error("schedule_m.jsonl not found. Run parse_990.py first.")
        return
    gcs_uri = upload_to_gcs(local, "staging/schedule_m.jsonl")
    load_jsonl_to_bq(gcs_uri, BQ_TABLE_SCHEDULE_M)


def load_all() -> None:
    """Upload and load all three tables."""
    load_organizations()
    load_filings()
    load_schedule_m()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_all()
