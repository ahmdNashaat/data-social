"""
clients.py — Shared Kafka producer and MinIO client factory.

Centralizes connection logic so all simulators use identical retry/error handling.
"""

import io
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.client import Config
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging

from config import CONFIG


# ── Kafka Producer ─────────────────────────────────────────────────────────────

def _json_serializer(data: Any) -> bytes:
    return json.dumps(data, default=str).encode("utf-8")


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(NoBrokersAvailable),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
)
def create_kafka_producer() -> KafkaProducer:
    """
    Create a KafkaProducer with retry logic.
    Retries up to 10 times with exponential backoff.
    """
    producer = KafkaProducer(
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        value_serializer=_json_serializer,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",                         # Wait for all replicas
        retries=3,
        retry_backoff_ms=500,
        compression_type="gzip",
        linger_ms=5,                        # Small batching window for throughput
        batch_size=16_384,
        max_block_ms=30_000,
    )
    logger.info(f"Kafka producer connected to {CONFIG.kafka_bootstrap_servers}")
    return producer


def send_to_kafka(
    producer: KafkaProducer,
    topic: str,
    message: Dict,
    key: Optional[str] = None,
) -> bool:
    """
    Send a single message to Kafka with error handling and DLQ fallback.

    Returns True on success, False on failure.
    """
    try:
        future = producer.send(topic, value=message, key=key)
        future.get(timeout=10)
        return True
    except KafkaError as e:
        logger.error(f"Failed to send to {topic}: {e}")
        # Try to send to DLQ
        try:
            dlq_msg = {
                "original_topic": topic,
                "original_message": message,
                "error": str(e),
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(CONFIG.topic_dlq, value=dlq_msg)
        except Exception:
            pass
        return False


# ── MinIO / S3 Client ──────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
)
def create_minio_client():
    """
    Create a boto3 S3 client configured for MinIO.
    Uses path-style addressing required by MinIO.
    """
    client = boto3.client(
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
    # Verify connection
    client.list_buckets()
    logger.info(f"MinIO client connected to {CONFIG.minio_endpoint}")
    return client


def upload_to_minio(
    client,
    bucket: str,
    key: str,
    data: str,
    content_type: str = "application/json",
) -> bool:
    """
    Upload string data (JSON or CSV) to MinIO.

    Args:
        client:       boto3 S3 client
        bucket:       Bucket name
        key:          Object key (full path within bucket)
        data:         String content to upload
        content_type: MIME type

    Returns:
        True on success, False on failure.
    """
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data.encode("utf-8"),
            ContentType=content_type,
        )
        logger.debug(f"Uploaded s3://{bucket}/{key}")
        return True
    except Exception as e:
        logger.error(f"MinIO upload failed for s3://{bucket}/{key}: {e}")
        return False


def build_landing_key(source: str, filename: str) -> str:
    """
    Build a consistent landing zone S3 key.
    Pattern: landing-zone/{source}/year=YYYY/month=MM/day=DD/{filename}
    """
    now = datetime.now(timezone.utc)
    return (
        f"{source}/"
        f"year={now.year:04d}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{filename}"
    )
