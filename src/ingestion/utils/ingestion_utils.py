"""
utils/ingestion_utils.py — Shared utilities for all ingestion jobs.

Provides:
  - Structured logging with run_id for observability
  - Bronze S3 path builder (consistent partitioning)
  - Ingestion metadata tagging
  - Record counter with stats
"""

import uuid
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from loguru import logger


# ── Run context ────────────────────────────────────────────────────────────────

class IngestionContext:
    """
    Holds metadata for a single ingestion run.
    Passed through all ingestion functions to ensure consistent logging.
    """

    def __init__(self, source: str, job_name: str):
        self.run_id    = str(uuid.uuid4())
        self.source    = source
        self.job_name  = job_name
        self.started_at = datetime.now(timezone.utc)

        # Counters
        self.records_read      = 0
        self.records_written   = 0
        self.records_rejected  = 0
        self.files_processed   = 0
        self.bytes_written     = 0

    def tag_record(self, record: Dict, source_file: str = "") -> Dict:
        """Inject pipeline metadata into a record before writing to Bronze."""
        record["batch_id"]   = self.run_id
        record["ingested_at"] = datetime.now(timezone.utc).isoformat()
        if source_file:
            record["source_file"] = source_file
        return record

    def log_start(self) -> None:
        logger.info(
            f"[{self.run_id[:8]}] Starting ingestion | "
            f"job={self.job_name} | source={self.source}"
        )

    def log_progress(self, every: int = 10_000) -> None:
        if self.records_written % every == 0 and self.records_written > 0:
            logger.info(
                f"[{self.run_id[:8]}] Progress | "
                f"written={self.records_written:,} | "
                f"rejected={self.records_rejected:,} | "
                f"files={self.files_processed}"
            )

    def log_finish(self) -> None:
        elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        rate = self.records_written / elapsed if elapsed > 0 else 0
        logger.info(
            f"[{self.run_id[:8]}] Ingestion complete | "
            f"job={self.job_name} | "
            f"read={self.records_read:,} | "
            f"written={self.records_written:,} | "
            f"rejected={self.records_rejected:,} | "
            f"files={self.files_processed} | "
            f"duration={elapsed:.1f}s | "
            f"rate={rate:.0f} rec/s"
        )

    def summary(self) -> Dict:
        return {
            "run_id":           self.run_id,
            "job_name":         self.job_name,
            "source":           self.source,
            "started_at":       self.started_at.isoformat(),
            "finished_at":      datetime.now(timezone.utc).isoformat(),
            "records_read":     self.records_read,
            "records_written":  self.records_written,
            "records_rejected": self.records_rejected,
            "files_processed":  self.files_processed,
        }


# ── S3 / Bronze path builder ───────────────────────────────────────────────────

def build_bronze_key(
    source: str,
    entity: str,
    run_id: str,
    chunk_idx: int = 0,
    extension: str = "json",
) -> str:
    """
    Build a consistent Bronze S3 key with Hive-style partitioning.

    Pattern:
        {source}/{entity}/year=YYYY/month=MM/day=DD/hour=HH/
            {source}_{entity}_{YYYYMMDD_HH}_{run_id}_{chunk:03d}.{ext}

    Args:
        source:    Data source (twitter, youtube, reddit)
        entity:    Entity type (tweets, comments, posts, etc.)
        run_id:    Pipeline run UUID
        chunk_idx: File chunk index (for large batches split into multiple files)
        extension: File extension (json, parquet)

    Returns:
        Full S3 key string
    """
    now = datetime.now(timezone.utc)
    short_run = run_id[:8]
    filename = (
        f"{source}_{entity}_{now.strftime('%Y%m%d_%H')}_{short_run}_{chunk_idx:03d}.{extension}"
    )
    return (
        f"{source}/{entity}/"
        f"year={now.year:04d}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"{filename}"
    )


# ── Chunked writer ─────────────────────────────────────────────────────────────

def chunk_records(records: List[Dict], chunk_size: int = 50_000):
    """Split a list of records into chunks for multi-file upload."""
    for i in range(0, len(records), chunk_size):
        yield i // chunk_size, records[i:i + chunk_size]


def records_to_ndjson(records: List[Dict]) -> str:
    """
    Serialize records to Newline-Delimited JSON (NDJSON).
    NDJSON is the standard format for Bronze layer JSON files —
    each line is a complete JSON object, making it easy to
    process line-by-line without loading the entire file.
    """
    return "\n".join(json.dumps(r, default=str) for r in records)


# ── Landing zone file scanner ──────────────────────────────────────────────────

def list_landing_files(
    s3_client,
    bucket: str,
    prefix: str,
    processed_keys: set = None,
) -> List[str]:
    """
    List all files in a MinIO landing zone prefix that haven't been processed.

    Args:
        s3_client:      boto3 S3 client
        bucket:         Landing zone bucket name
        prefix:         Prefix to scan (e.g., "youtube/videos/")
        processed_keys: Set of already-processed S3 keys to skip

    Returns:
        List of unprocessed S3 keys
    """
    if processed_keys is None:
        processed_keys = set()

    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key not in processed_keys and not key.endswith("/"):
                keys.append(key)

    return keys


def read_json_from_s3(s3_client, bucket: str, key: str) -> List[Dict]:
    """Download and parse a JSON file from S3/MinIO."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def mark_as_processed(
    s3_client,
    bucket: str,
    key: str,
    run_id: str,
) -> None:
    """
    Mark a landing zone file as processed by adding metadata tag.
    This prevents double-ingestion on pipeline re-runs.
    """
    try:
        s3_client.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={
                "TagSet": [
                    {"Key": "processed", "Value": "true"},
                    {"Key": "processed_by_run_id", "Value": run_id},
                    {"Key": "processed_at", "Value": datetime.now(timezone.utc).isoformat()},
                ]
            },
        )
    except Exception as e:
        logger.warning(f"Could not tag {key} as processed: {e}")
