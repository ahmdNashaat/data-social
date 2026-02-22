"""
batch_ingestion.py — Landing zone → Bronze layer batch ingestion.

Scans the MinIO landing zone for new files from YouTube and Reddit simulators,
validates them, tags with pipeline metadata, and writes to the Bronze layer
with consistent Hive-style partitioning.

Design:
  - Idempotent: processed files are tagged in S3 metadata to prevent re-ingestion.
  - Chunked: large files are split into 50K-record chunks to control memory usage.
  - Schema-permissive: Bronze accepts all records; strict validation at Silver.
  - Observable: every run produces a JSON summary written to MinIO.

Usage:
    python batch_ingestion.py                    # process all sources
    python batch_ingestion.py --source youtube   # only YouTube
    python batch_ingestion.py --source reddit    # only Reddit
    python batch_ingestion.py --dry-run          # no MinIO writes
    python batch_ingestion.py --reprocess        # ignore processed tags
"""

import argparse
import json
import sys
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from loguru import logger

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../simulators"))

from ingestion.utils.ingestion_utils import (
    IngestionContext,
    build_bronze_key,
    records_to_ndjson,
    chunk_records,
    list_landing_files,
    read_json_from_s3,
    mark_as_processed,
)
from ingestion.schemas.bronze_schemas import validate_record

try:
    import boto3
    from botocore.client import Config
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    from config import CONFIG
except ImportError:
    class CONFIG:
        minio_endpoint = "http://localhost:9000"
        minio_access_key = "minioadmin"
        minio_secret_key = "minioadmin"
        bucket_landing = "landing-zone"


# ── Source definitions ─────────────────────────────────────────────────────────
# Each source maps a landing zone prefix to a Bronze entity

SOURCE_CONFIG = {
    "youtube": [
        {
            "landing_prefix": "youtube/videos/",
            "bronze_source":  "youtube",
            "bronze_entity":  "videos",
            "schema_key":     "youtube.videos",
        },
        {
            "landing_prefix": "youtube/comments/",
            "bronze_source":  "youtube",
            "bronze_entity":  "comments",
            "schema_key":     "youtube.comments",
        },
        {
            "landing_prefix": "youtube/snapshots/",
            "bronze_source":  "youtube",
            "bronze_entity":  "snapshots",
            "schema_key":     "youtube.snapshots",
        },
    ],
    "reddit": [
        {
            "landing_prefix": "reddit/posts/",
            "bronze_source":  "reddit",
            "bronze_entity":  "posts",
            "schema_key":     "reddit.posts",
        },
        {
            "landing_prefix": "reddit/comments/",
            "bronze_source":  "reddit",
            "bronze_entity":  "comments",
            "schema_key":     "reddit.comments",
        },
        {
            "landing_prefix": "reddit/metadata/",
            "bronze_source":  "reddit",
            "bronze_entity":  "subreddits",
            "schema_key":     None,   # No schema validation for reference data
        },
    ],
}


# ── S3 client factory ──────────────────────────────────────────────────────────

def _create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=CONFIG.minio_endpoint,
        aws_access_key_id=CONFIG.minio_access_key,
        aws_secret_access_key=CONFIG.minio_secret_key,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
        region_name="us-east-1",
    )


# ── Core ingestion logic ───────────────────────────────────────────────────────

def _get_processed_keys(s3, bucket: str, prefix: str) -> set:
    """
    Return set of S3 keys already tagged as processed.
    This enables idempotent re-runs without double-ingestion.
    """
    processed = set()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                try:
                    tags = s3.get_object_tagging(Bucket=bucket, Key=key)
                    tag_dict = {t["Key"]: t["Value"] for t in tags.get("TagSet", [])}
                    if tag_dict.get("processed") == "true":
                        processed.add(key)
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"Could not check processed keys for {prefix}: {e}")
    return processed


def ingest_file(
    s3,
    landing_bucket: str,
    bronze_bucket: str,
    landing_key: str,
    ctx: IngestionContext,
    source: str,
    entity: str,
    schema_key: Optional[str],
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Ingest a single file from landing zone → Bronze.

    Returns:
        (records_written, records_rejected)
    """
    logger.info(f"[{ctx.run_id[:8]}] Processing s3://{landing_bucket}/{landing_key}")

    # ── Read ────────────────────────────────────────────────────────────────
    try:
        records = read_json_from_s3(s3, landing_bucket, landing_key)
        ctx.records_read += len(records)
    except Exception as e:
        logger.error(f"Failed to read {landing_key}: {e}")
        return 0, 0

    # ── Validate + tag ──────────────────────────────────────────────────────
    valid_records = []
    rejected = 0

    for record in records:
        # Inject pipeline metadata
        record["batch_id"]    = ctx.run_id
        record["source_file"] = landing_key
        if "ingested_at" not in record:
            record["ingested_at"] = datetime.now(timezone.utc).isoformat()

        # Schema validation (permissive at Bronze — only check required fields)
        if schema_key:
            is_valid, errors = validate_record(record, schema_key)
            if not is_valid:
                rejected += 1
                logger.debug(f"Rejected record: {errors}")
                continue

        valid_records.append(record)

    logger.info(
        f"[{ctx.run_id[:8]}] {landing_key} | "
        f"read={len(records):,} | valid={len(valid_records):,} | "
        f"rejected={rejected:,}"
    )

    if dry_run:
        ctx.records_written  += len(valid_records)
        ctx.records_rejected += rejected
        ctx.files_processed  += 1
        return len(valid_records), rejected

    # ── Write to Bronze in chunks ───────────────────────────────────────────
    written = 0
    for chunk_idx, chunk in chunk_records(valid_records, chunk_size=50_000):
        bronze_key = build_bronze_key(source, entity, ctx.run_id, chunk_idx)
        ndjson = records_to_ndjson(chunk)

        try:
            s3.put_object(
                Bucket=bronze_bucket,
                Key=bronze_key,
                Body=ndjson.encode("utf-8"),
                ContentType="application/x-ndjson",
                Metadata={
                    "batch_id":       ctx.run_id,
                    "record_count":   str(len(chunk)),
                    "source_landing": landing_key,
                },
            )
            written += len(chunk)
            ctx.bytes_written += len(ndjson)
            logger.debug(f"Wrote {len(chunk):,} records → s3://bronze/{bronze_key}")

        except Exception as e:
            logger.error(f"Failed to write Bronze chunk {chunk_idx}: {e}")

    ctx.records_written  += written
    ctx.records_rejected += rejected
    ctx.files_processed  += 1

    # ── Mark source file as processed ──────────────────────────────────────
    mark_as_processed(s3, landing_bucket, landing_key, ctx.run_id)

    return written, rejected


def run_source_ingestion(
    s3,
    source_conf: Dict,
    ctx: IngestionContext,
    reprocess: bool = False,
    dry_run: bool = False,
) -> None:
    """Ingest all unprocessed files for a single source/entity pair."""
    landing_prefix = source_conf["landing_prefix"]
    source         = source_conf["bronze_source"]
    entity         = source_conf["bronze_entity"]
    schema_key     = source_conf["schema_key"]

    # Find unprocessed files
    if reprocess:
        processed_keys = set()
    else:
        processed_keys = _get_processed_keys(s3, CONFIG.bucket_landing, landing_prefix)

    all_keys = list_landing_files(
        s3, CONFIG.bucket_landing, landing_prefix, processed_keys
    )

    if not all_keys:
        logger.info(f"No new files in landing zone: {landing_prefix}")
        return

    logger.info(f"Found {len(all_keys)} new file(s) in {landing_prefix}")

    for key in all_keys:
        ingest_file(
            s3=s3,
            landing_bucket=CONFIG.bucket_landing,
            bronze_bucket="bronze",
            landing_key=key,
            ctx=ctx,
            source=source,
            entity=entity,
            schema_key=schema_key,
            dry_run=dry_run,
        )


# ── Main runner ────────────────────────────────────────────────────────────────

def run(
    sources: Optional[List[str]] = None,
    reprocess: bool = False,
    dry_run: bool = False,
) -> Dict:
    """
    Run batch ingestion for one or all sources.

    Args:
        sources:    List of source names to ingest (default: all).
        reprocess:  Re-ingest already-processed files.
        dry_run:    Validate and count without writing to Bronze.

    Returns:
        Ingestion summary dict.
    """
    ctx = IngestionContext(source="batch", job_name="batch_ingestion")
    ctx.log_start()

    if sources is None:
        sources = list(SOURCE_CONFIG.keys())

    if dry_run:
        logger.info("DRY RUN MODE — no writes to Bronze")

    s3 = None if dry_run else (_create_s3_client() if HAS_BOTO3 else None)

    for source_name in sources:
        if source_name not in SOURCE_CONFIG:
            logger.warning(f"Unknown source: {source_name} — skipping")
            continue

        logger.info(f"--- Ingesting source: {source_name} ---")
        for source_conf in SOURCE_CONFIG[source_name]:
            if dry_run or s3:
                run_source_ingestion(
                    s3=s3,
                    source_conf=source_conf,
                    ctx=ctx,
                    reprocess=reprocess,
                    dry_run=dry_run,
                )

    ctx.log_finish()
    summary = ctx.summary()

    # Write run summary to MinIO
    if not dry_run and s3:
        summary_key = (
            f"_ingestion_runs/"
            f"year={datetime.now(timezone.utc).year:04d}/"
            f"month={datetime.now(timezone.utc).month:02d}/"
            f"day={datetime.now(timezone.utc).day:02d}/"
            f"batch_ingestion_{ctx.run_id}.json"
        )
        try:
            s3.put_object(
                Bucket="bronze",
                Key=summary_key,
                Body=json.dumps(summary, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as e:
            logger.warning(f"Could not write run summary: {e}")

    return summary


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Landing zone → Bronze batch ingestion")
    parser.add_argument(
        "--source",
        choices=list(SOURCE_CONFIG.keys()),
        default=None,
        help="Ingest a single source (default: all)",
    )
    parser.add_argument("--reprocess", action="store_true", help="Re-ingest already processed files")
    parser.add_argument("--dry-run",   action="store_true", help="Count/validate without writing")
    args = parser.parse_args()

    sources = [args.source] if args.source else None
    summary = run(sources=sources, reprocess=args.reprocess, dry_run=args.dry_run)
    print(json.dumps(summary, indent=2))
