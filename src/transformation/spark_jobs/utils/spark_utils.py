"""
spark_jobs/utils/spark_utils.py — Shared PySpark utilities.

Provides:
  - SparkSession factory with MinIO/S3A config
  - Deduplication helper (window-based, idempotent)
  - Schema enforcement
  - Partitioned Parquet writer
  - Structured job logging
"""

import uuid
from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType,
    BooleanType, DoubleType, TimestampType, ArrayType,
)
from loguru import logger


# ── SparkSession factory ───────────────────────────────────────────────────────

def create_spark_session(
    app_name: str,
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin",
) -> SparkSession:
    """
    Create a SparkSession pre-configured for:
      - MinIO / S3A access
      - Kafka source (for streaming jobs)
      - Parquet optimizations
      - Snappy compression

    Args:
        app_name:         Spark application name (shown in Spark UI)
        minio_endpoint:   MinIO server URL
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key

    Returns:
        Configured SparkSession
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # ── S3A / MinIO ───────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",             minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",           minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key",           minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ── Parquet ───────────────────────────────────────────────────────
        .config("spark.sql.parquet.compression.codec",      "snappy")
        .config("spark.sql.parquet.mergeSchema",            "false")
        .config("spark.sql.parquet.filterPushdown",         "true")
        # ── Performance ───────────────────────────────────────────────────
        .config("spark.sql.shuffle.partitions",             "8")
        .config("spark.serializer",                         "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled",               "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ── Logging ───────────────────────────────────────────────────────
        .config("spark.eventLog.enabled",                   "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: app={app_name} | version={spark.version}")
    return spark


# ── Deduplication ──────────────────────────────────────────────────────────────

def deduplicate(
    df: DataFrame,
    primary_keys: List[str],
    order_by: str = "ingested_at",
    desc: bool = True,
) -> DataFrame:
    """
    Deduplicate a DataFrame using window functions.

    Keeps the LATEST record per primary key combination
    (ordered by order_by column, descending by default).

    This is idempotent — running on already-deduplicated data
    produces the same result.

    Args:
        df:           Input DataFrame
        primary_keys: Columns that together uniquely identify a record
        order_by:     Column to use for ordering within duplicates
        desc:         True = keep latest (desc order), False = keep earliest

    Returns:
        Deduplicated DataFrame
    """
    window = Window.partitionBy(*primary_keys).orderBy(
        F.col(order_by).desc() if desc else F.col(order_by).asc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


# ── Timestamp standardization ──────────────────────────────────────────────────

def normalize_timestamps(df: DataFrame, timestamp_cols: List[str]) -> DataFrame:
    """
    Parse and normalize timestamp columns to UTC TimestampType.

    Handles common formats:
      - ISO 8601 with timezone (2024-01-15T14:30:00+00:00)
      - ISO 8601 without timezone (assumed UTC)
      - Unix epoch (integer seconds)

    Args:
        df:             Input DataFrame
        timestamp_cols: List of column names to normalize

    Returns:
        DataFrame with timestamp columns converted to TimestampType (UTC)
    """
    result = df
    for col_name in timestamp_cols:
        if col_name in df.columns:
            result = result.withColumn(
                col_name,
                F.to_utc_timestamp(
                    F.to_timestamp(F.col(col_name)),
                    "UTC"
                )
            )
    return result


# ── Null handling ──────────────────────────────────────────────────────────────

def fill_nulls(df: DataFrame, defaults: dict) -> DataFrame:
    """
    Fill null values with sensible defaults per column.

    Args:
        df:       Input DataFrame
        defaults: Dict mapping column name → default value

    Returns:
        DataFrame with nulls filled
    """
    return df.fillna(defaults)


def reject_null_keys(
    df: DataFrame,
    required_cols: List[str],
) -> tuple:
    """
    Split DataFrame into valid records and rejected records (null required fields).

    Returns:
        (valid_df, rejected_df) tuple
    """
    null_condition = F.lit(False)
    for col_name in required_cols:
        null_condition = null_condition | F.col(col_name).isNull()

    rejected = df.filter(null_condition)
    valid    = df.filter(~null_condition)
    return valid, rejected


# ── Silver Parquet writer ──────────────────────────────────────────────────────

def write_to_silver(
    df: DataFrame,
    bucket: str,
    entity: str,
    partition_cols: Optional[List[str]] = None,
    mode: str = "overwrite",
) -> int:
    """
    Write a DataFrame to the Silver layer as partitioned Parquet.

    Partition pattern: s3a://{bucket}/{entity}/year=YYYY/month=MM/day=DD/

    Args:
        df:             DataFrame to write
        bucket:         Silver bucket name
        entity:         Entity name (e.g., "posts", "engagements")
        partition_cols: Columns to partition by (default: year, month, day)
        mode:           Write mode (overwrite | append)

    Returns:
        Number of records written
    """
    if partition_cols is None:
        partition_cols = ["year", "month", "day"]

    # Add partition columns from ingested_at if not already present
    write_df = df
    if "year" not in df.columns and "ingested_at" in df.columns:
        write_df = (
            write_df
            .withColumn("year",  F.year(F.col("ingested_at")))
            .withColumn("month", F.month(F.col("ingested_at")))
            .withColumn("day",   F.dayofmonth(F.col("ingested_at")))
        )

    path = f"s3a://{bucket}/{entity}/"
    count = write_df.count()

    (
        write_df
        .write
        .mode(mode)
        .partitionBy(*partition_cols)
        .parquet(path)
    )

    logger.info(f"Wrote {count:,} records → s3a://{bucket}/{entity}/ (mode={mode})")
    return count


# ── Job context ────────────────────────────────────────────────────────────────

class SparkJobContext:
    """Tracks Spark job metadata for structured logging and observability."""

    def __init__(self, job_name: str, source_layer: str, target_layer: str):
        self.job_name     = job_name
        self.run_id       = str(uuid.uuid4())
        self.source_layer = source_layer
        self.target_layer = target_layer
        self.started_at   = datetime.now(timezone.utc)
        self.records_read    = 0
        self.records_written = 0
        self.records_rejected = 0

    def log_start(self):
        logger.info(
            f"[{self.run_id[:8]}] Spark job starting | "
            f"job={self.job_name} | "
            f"{self.source_layer} → {self.target_layer}"
        )

    def log_finish(self):
        elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        logger.info(
            f"[{self.run_id[:8]}] Spark job complete | "
            f"job={self.job_name} | "
            f"read={self.records_read:,} | "
            f"written={self.records_written:,} | "
            f"rejected={self.records_rejected:,} | "
            f"duration={elapsed:.1f}s"
        )
