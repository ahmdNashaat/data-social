"""
spark_jobs/bronze_to_silver.py — Bronze → Silver transformation.

Reads raw NDJSON data from Bronze layer, applies cleaning and standardization,
and writes deduplicated Parquet files to the Silver layer.

Transformations applied:
  - Schema enforcement (cast to correct types, drop unknown columns)
  - Null rejection for primary key columns
  - Deduplication using window functions (keep latest by ingested_at)
  - Timestamp normalization to UTC
  - String trimming and lowercasing for categorical fields
  - Derived columns (platform_normalized, content_length, etc.)
  - Pipeline metadata injection (silver_batch_id, processed_at)

Design:
  - Idempotent: overwrites Silver partitions — safe to re-run
  - Modular: each entity has its own transform function
  - Observable: logs record counts at each stage

Usage (via spark-submit):
    spark-submit \\
        --master spark://spark-master:7077 \\
        --executor-memory 2g \\
        src/transformation/spark_jobs/bronze_to_silver.py \\
        --date 2024-01-15

Usage (local test):
    python bronze_to_silver.py --local --date 2024-01-15
"""

import argparse
import os
import sys
from datetime import datetime, date, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType,
    BooleanType, DoubleType, TimestampType, ArrayType,
)
from loguru import logger

sys.path.insert(0, os.path.dirname(__file__))
from utils.spark_utils import (
    create_spark_session,
    deduplicate,
    normalize_timestamps,
    fill_nulls,
    reject_null_keys,
    write_to_silver,
    SparkJobContext,
)


# ── Bronze Schemas (for enforced reads) ───────────────────────────────────────

TWEET_BRONZE_SCHEMA = StructType([
    StructField("tweet_id",             StringType(),    False),
    StructField("user_id",              StringType(),    False),
    StructField("platform",             StringType(),    False),
    StructField("content",              StringType(),    True),
    StructField("hashtags",             ArrayType(StringType()), True),
    StructField("mentions",             ArrayType(StringType()), True),
    StructField("language",             StringType(),    True),
    StructField("is_retweet",           BooleanType(),   True),
    StructField("is_reply",             BooleanType(),   True),
    StructField("media_type",           StringType(),    True),
    StructField("likes_count",          LongType(),      True),
    StructField("retweets_count",       LongType(),      True),
    StructField("replies_count",        LongType(),      True),
    StructField("impressions_count",    LongType(),      True),
    StructField("quote_count",          LongType(),      True),
    StructField("user_follower_count",  LongType(),      True),
    StructField("user_account_type",    StringType(),    True),
    StructField("user_is_verified",     BooleanType(),   True),
    StructField("created_at",           StringType(),    True),
    StructField("ingested_at",          StringType(),    True),
    StructField("batch_id",             StringType(),    True),
    StructField("source_system",        StringType(),    True),
    StructField("schema_version",       StringType(),    True),
])

ENGAGEMENT_BRONZE_SCHEMA = StructType([
    StructField("engagement_id",        StringType(),    False),
    StructField("tweet_id",             StringType(),    True),
    StructField("post_id",              StringType(),    False),
    StructField("engaging_user_id",     StringType(),    False),
    StructField("post_author_id",       StringType(),    True),
    StructField("platform",             StringType(),    False),
    StructField("engagement_type",      StringType(),    False),
    StructField("content",              StringType(),    True),
    StructField("created_at",           StringType(),    True),
    StructField("ingested_at",          StringType(),    True),
    StructField("batch_id",             StringType(),    True),
    StructField("source_system",        StringType(),    True),
    StructField("schema_version",       StringType(),    True),
])


# ── Transform functions ────────────────────────────────────────────────────────

def transform_tweets(
    spark: SparkSession,
    bronze_path: str,
    silver_batch_id: str,
) -> DataFrame:
    """
    Transform raw tweets from Bronze → Silver.

    Steps:
      1. Read NDJSON with enforced schema
      2. Reject records with null primary keys
      3. Normalize timestamps to UTC
      4. Deduplicate by tweet_id (keep latest ingested)
      5. Add derived columns
      6. Add Silver metadata
    """
    logger.info(f"Transforming tweets from {bronze_path}")

    # ── Read ────────────────────────────────────────────────────────────
    raw_df = (
        spark.read
        .schema(TWEET_BRONZE_SCHEMA)
        .json(bronze_path)
    )
    total_read = raw_df.count()
    logger.info(f"  Read {total_read:,} raw tweets")

    # ── Reject null PKs ─────────────────────────────────────────────────
    valid_df, rejected_df = reject_null_keys(raw_df, ["tweet_id", "user_id"])
    rejected_count = rejected_df.count()
    if rejected_count > 0:
        logger.warning(f"  Rejected {rejected_count:,} tweets with null PKs")
        # In production: write rejected to quarantine bucket

    # ── Normalize timestamps ─────────────────────────────────────────────
    ts_df = normalize_timestamps(valid_df, ["created_at", "ingested_at"])

    # ── Clean & standardize ──────────────────────────────────────────────
    clean_df = (
        ts_df
        # Trim whitespace from string fields
        .withColumn("content",              F.trim(F.col("content")))
        .withColumn("platform",             F.lower(F.trim(F.col("platform"))))
        .withColumn("language",             F.lower(F.trim(F.col("language"))))
        .withColumn("user_account_type",    F.upper(F.trim(F.col("user_account_type"))))
        .withColumn("media_type",           F.lower(F.trim(F.col("media_type"))))

        # Fill nulls with defaults
        .fillna({
            "likes_count":       0,
            "retweets_count":    0,
            "replies_count":     0,
            "impressions_count": 0,
            "quote_count":       0,
            "is_retweet":        False,
            "is_reply":          False,
        })

        # Clip negative counts to 0
        .withColumn("likes_count",       F.greatest(F.col("likes_count"),       F.lit(0)))
        .withColumn("retweets_count",    F.greatest(F.col("retweets_count"),     F.lit(0)))
        .withColumn("impressions_count", F.greatest(F.col("impressions_count"),  F.lit(0)))

        # Derived columns
        .withColumn("content_length",    F.length(F.col("content")))
        .withColumn("hashtag_count",     F.size(F.col("hashtags")))
        .withColumn("mention_count",     F.size(F.col("mentions")))
        .withColumn("has_media",         F.col("media_type").isNotNull())
        .withColumn("engagement_total",
                    F.col("likes_count") + F.col("retweets_count") + F.col("replies_count"))
    )

    # ── Deduplicate ──────────────────────────────────────────────────────
    dedup_df = deduplicate(clean_df, primary_keys=["tweet_id"], order_by="ingested_at")
    dedup_count = dedup_df.count()
    dups_removed = total_read - rejected_count - dedup_count
    logger.info(
        f"  After dedup: {dedup_count:,} tweets "
        f"(removed {dups_removed:,} duplicates, {rejected_count:,} rejected)"
    )

    # ── Add Silver metadata ──────────────────────────────────────────────
    silver_df = (
        dedup_df
        .withColumn("silver_batch_id",    F.lit(silver_batch_id))
        .withColumn("silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("silver_layer",        F.lit("silver"))
    )

    return silver_df


def transform_engagements(
    spark: SparkSession,
    bronze_path: str,
    silver_batch_id: str,
) -> DataFrame:
    """Transform raw engagement events from Bronze → Silver."""
    logger.info(f"Transforming engagements from {bronze_path}")

    raw_df = (
        spark.read
        .schema(ENGAGEMENT_BRONZE_SCHEMA)
        .json(bronze_path)
    )
    total_read = raw_df.count()

    valid_df, _ = reject_null_keys(
        raw_df, ["engagement_id", "post_id", "engaging_user_id", "engagement_type"]
    )
    ts_df = normalize_timestamps(valid_df, ["created_at", "ingested_at"])

    clean_df = (
        ts_df
        .withColumn("platform",        F.lower(F.trim(F.col("platform"))))
        .withColumn("engagement_type", F.upper(F.trim(F.col("engagement_type"))))
        # Validate engagement_type is one of known values
        .withColumn(
            "engagement_type",
            F.when(
                F.col("engagement_type").isin(["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW"]),
                F.col("engagement_type")
            ).otherwise(F.lit("UNKNOWN"))
        )
        # Engagement value weight
        .withColumn(
            "engagement_value",
            F.when(F.col("engagement_type") == "LIKE",    F.lit(1.0))
             .when(F.col("engagement_type") == "REPLY",   F.lit(2.0))
             .when(F.col("engagement_type") == "RETWEET", F.lit(3.0))
             .when(F.col("engagement_type") == "QUOTE",   F.lit(4.0))
             .when(F.col("engagement_type") == "VIEW",    F.lit(0.1))
             .otherwise(F.lit(1.0))
        )
    )

    dedup_df = deduplicate(
        clean_df,
        primary_keys=["engagement_id"],
        order_by="ingested_at",
    )

    logger.info(f"  Engagements: {total_read:,} raw → {dedup_df.count():,} clean")

    return (
        dedup_df
        .withColumn("silver_batch_id",     F.lit(silver_batch_id))
        .withColumn("silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )


def transform_posts_unified(
    spark: SparkSession,
    tweets_silver: DataFrame,
    youtube_bronze_path: str,
    reddit_bronze_path: str,
    silver_batch_id: str,
) -> DataFrame:
    """
    Build a unified 'posts' Silver table from Twitter, YouTube, and Reddit.

    This creates a platform-agnostic posts table with common fields,
    enabling cross-platform analytics in the Gold layer.
    """
    # Twitter posts (from already-transformed tweets)
    twitter_posts = (
        tweets_silver.select(
            F.col("tweet_id").alias("post_id"),
            F.col("user_id"),
            F.lit("twitter").alias("platform"),
            F.col("content"),
            F.col("hashtags"),
            F.col("created_at").alias("posted_at"),
            F.col("likes_count"),
            F.col("retweets_count").alias("shares_count"),
            F.col("replies_count").alias("comments_count"),
            F.col("impressions_count"),
            F.col("ingested_at"),
            F.col("silver_batch_id"),
        )
    )

    # YouTube posts (videos)
    yt_schema = StructType([
        StructField("video_id",      StringType(),  False),
        StructField("channel_id",    StringType(),  False),
        StructField("platform",      StringType(),  True),
        StructField("title",         StringType(),  True),
        StructField("view_count",    LongType(),    True),
        StructField("like_count",    LongType(),    True),
        StructField("comment_count", LongType(),    True),
        StructField("published_at",  StringType(),  True),
        StructField("ingested_at",   StringType(),  True),
        StructField("tags",          ArrayType(StringType()), True),
    ])

    youtube_raw = spark.read.schema(yt_schema).json(youtube_bronze_path)
    youtube_posts = (
        normalize_timestamps(youtube_raw, ["published_at", "ingested_at"])
        .select(
            F.col("video_id").alias("post_id"),
            F.col("channel_id").alias("user_id"),
            F.lit("youtube").alias("platform"),
            F.col("title").alias("content"),
            F.col("tags").alias("hashtags"),
            F.col("published_at").alias("posted_at"),
            F.col("like_count").alias("likes_count"),
            F.lit(0).cast(LongType()).alias("shares_count"),
            F.col("comment_count").alias("comments_count"),
            F.col("view_count").alias("impressions_count"),
            F.col("ingested_at"),
            F.lit(silver_batch_id).alias("silver_batch_id"),
        )
    )

    # Reddit posts
    reddit_schema = StructType([
        StructField("post_id",        StringType(),  False),
        StructField("author_user_id", StringType(),  False),
        StructField("platform",       StringType(),  True),
        StructField("title",          StringType(),  True),
        StructField("score",          LongType(),    True),
        StructField("num_comments",   LongType(),    True),
        StructField("tags",           ArrayType(StringType()), True),
        StructField("posted_at",      StringType(),  True),
        StructField("ingested_at",    StringType(),  True),
    ])

    reddit_raw = spark.read.schema(reddit_schema).json(reddit_bronze_path)
    reddit_posts = (
        normalize_timestamps(reddit_raw, ["posted_at", "ingested_at"])
        .select(
            F.col("post_id"),
            F.col("author_user_id").alias("user_id"),
            F.lit("reddit").alias("platform"),
            F.col("title").alias("content"),
            F.col("tags").alias("hashtags"),
            F.col("posted_at"),
            F.col("score").alias("likes_count"),
            F.lit(0).cast(LongType()).alias("shares_count"),
            F.col("num_comments").alias("comments_count"),
            F.lit(0).cast(LongType()).alias("impressions_count"),
            F.col("ingested_at"),
            F.lit(silver_batch_id).alias("silver_batch_id"),
        )
    )

    # Union all platforms
    unified = twitter_posts.unionByName(youtube_posts).unionByName(reddit_posts)

    # Dedup across all platforms
    return deduplicate(unified, primary_keys=["post_id", "platform"], order_by="ingested_at")


# ── Main job ───────────────────────────────────────────────────────────────────

def run(
    process_date: Optional[str] = None,
    local_mode: bool = False,
) -> None:
    """
    Run the Bronze → Silver transformation for a given date.

    Args:
        process_date: Date to process in YYYY-MM-DD format (default: yesterday)
        local_mode:   Run with local[*] master for testing
    """
    ctx = SparkJobContext(
        job_name="bronze_to_silver",
        source_layer="bronze",
        target_layer="silver",
    )
    ctx.log_start()

    if process_date is None:
        process_date = (date.today() - timedelta(days=1)).isoformat()

    dt = datetime.strptime(process_date, "%Y-%m-%d")
    year, month, day = dt.year, dt.month, dt.day

    logger.info(f"Processing date: {process_date}")

    # ── Create Spark session ──────────────────────────────────────────────
    if local_mode:
        spark = SparkSession.builder.appName("bronze_to_silver_local").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
    else:
        spark = create_spark_session("bronze_to_silver")

    # ── Bronze paths for this date ────────────────────────────────────────
    date_prefix = f"year={year:04d}/month={month:02d}/day={day:02d}"
    bronze_base = "s3a://bronze"

    tweet_path      = f"{bronze_base}/twitter/tweets/{date_prefix}/"
    engagement_path = f"{bronze_base}/twitter/engagements/{date_prefix}/"
    youtube_path    = f"{bronze_base}/youtube/videos/{date_prefix}/"
    reddit_path     = f"{bronze_base}/reddit/posts/{date_prefix}/"

    silver_base = "s3a://silver"

    try:
        # ── Tweets ───────────────────────────────────────────────────────
        tweets_silver = transform_tweets(spark, tweet_path, ctx.run_id)
        write_count = write_to_silver(tweets_silver, "silver", "tweets")
        ctx.records_written += write_count

        # ── Engagements ───────────────────────────────────────────────────
        engagements_silver = transform_engagements(spark, engagement_path, ctx.run_id)
        write_count = write_to_silver(engagements_silver, "silver", "engagements")
        ctx.records_written += write_count

        # ── Unified posts (cross-platform) ────────────────────────────────
        posts_silver = transform_posts_unified(
            spark, tweets_silver, youtube_path, reddit_path, ctx.run_id
        )
        write_count = write_to_silver(posts_silver, "silver", "posts")
        ctx.records_written += write_count

        logger.info(f"Bronze → Silver complete for {process_date}")

    except Exception as e:
        logger.error(f"Bronze → Silver failed: {e}")
        raise
    finally:
        spark.stop()
        ctx.log_finish()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver PySpark transformation")
    parser.add_argument("--date",  type=str, default=None, help="Date to process (YYYY-MM-DD)")
    parser.add_argument("--local", action="store_true",    help="Run with local Spark master")
    args = parser.parse_args()

    run(process_date=args.date, local_mode=args.local)
