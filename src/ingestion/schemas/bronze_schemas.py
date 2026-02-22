"""
schemas/bronze_schemas.py — Bronze layer schema definitions.

Defines the expected schema for every source hitting the Bronze layer.
Used by ingestion jobs to validate before writing and by PySpark
transformation jobs to enforce schema on read.

Design principles:
  - Bronze schemas are PERMISSIVE — we prefer to capture data over rejecting it.
  - Type coercion is minimal — keep source types as close to origin as possible.
  - All schemas include mandatory pipeline metadata columns.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Pipeline metadata (appended to every Bronze record) ───────────────────────

PIPELINE_METADATA_FIELDS = {
    "ingested_at":      str,    # ISO 8601 UTC timestamp of ingestion
    "batch_id":         str,    # UUID for the pipeline run
    "source_file":      str,    # Source file path or Kafka topic+offset
    "source_system":    str,    # e.g. "twitter_simulator_v1"
    "schema_version":   str,    # e.g. "1.0"
}


# ── Twitter / X ────────────────────────────────────────────────────────────────

BRONZE_TWEET_SCHEMA: Dict[str, Any] = {
    # Identifiers
    "tweet_id":              str,
    "user_id":               str,
    "platform":              str,

    # Content
    "content":               str,
    "hashtags":              list,       # List[str]
    "mentions":              list,       # List[str]
    "language":              str,
    "is_retweet":            bool,
    "is_reply":              bool,
    "media_type":            Optional[str],

    # Metrics
    "likes_count":           int,
    "retweets_count":        int,
    "replies_count":         int,
    "impressions_count":     int,
    "quote_count":           int,

    # User snapshot
    "user_follower_count":   int,
    "user_account_type":     str,
    "user_is_verified":      bool,

    # Timestamps
    "created_at":            str,       # ISO 8601 UTC
    "ingested_at":           str,

    # Pipeline
    "source_system":         str,
    "schema_version":        str,
}

BRONZE_ENGAGEMENT_SCHEMA: Dict[str, Any] = {
    "engagement_id":         str,
    "tweet_id":              str,
    "post_id":               str,
    "engaging_user_id":      str,
    "post_author_id":        str,
    "platform":              str,
    "engagement_type":       str,       # LIKE | RETWEET | REPLY | QUOTE
    "content":               Optional[str],
    "created_at":            str,
    "ingested_at":           str,
    "source_system":         str,
    "schema_version":        str,
}


# ── YouTube ────────────────────────────────────────────────────────────────────

BRONZE_YOUTUBE_VIDEO_SCHEMA: Dict[str, Any] = {
    "video_id":              str,
    "channel_id":            str,
    "platform":              str,

    # Content
    "title":                 str,
    "description":           Optional[str],
    "category":              str,
    "tags":                  list,
    "language":              str,
    "duration_seconds":      int,
    "thumbnail_url":         Optional[str],
    "is_short":              bool,
    "is_live_stream":        bool,
    "age_restricted":        bool,

    # Metrics
    "view_count":            int,
    "like_count":            int,
    "dislike_count":         int,
    "comment_count":         int,
    "share_count":           int,
    "subscriber_gain":       int,

    # Channel snapshot
    "channel_name":          str,
    "channel_subscriber_count": int,
    "channel_account_type":  str,

    # Timestamps
    "published_at":          str,
    "ingested_at":           str,
    "source_system":         str,
    "schema_version":        str,
}

BRONZE_YOUTUBE_COMMENT_SCHEMA: Dict[str, Any] = {
    "comment_id":            str,
    "video_id":              str,
    "parent_comment_id":     Optional[str],
    "author_user_id":        str,
    "author_username":       str,
    "platform":              str,
    "content":               str,
    "is_reply":              bool,
    "is_pinned":             bool,
    "is_hearted":            bool,
    "like_count":            int,
    "published_at":          str,
    "ingested_at":           str,
    "source_system":         str,
    "schema_version":        str,
}

BRONZE_YOUTUBE_SNAPSHOT_SCHEMA: Dict[str, Any] = {
    "snapshot_id":           str,
    "video_id":              str,
    "snapshot_date":         str,       # YYYY-MM-DD
    "daily_views":           int,
    "cumulative_views":      int,
    "daily_likes":           int,
    "daily_comments":        int,
    "ingested_at":           str,
    "source_system":         str,
}


# ── Reddit ─────────────────────────────────────────────────────────────────────

BRONZE_REDDIT_POST_SCHEMA: Dict[str, Any] = {
    "post_id":               str,
    "author_user_id":        str,
    "author_username":       str,
    "subreddit":             str,
    "subreddit_category":    str,
    "platform":              str,

    # Content
    "title":                 str,
    "body":                  Optional[str],
    "url":                   Optional[str],
    "post_type":             str,
    "flair":                 Optional[str],
    "tags":                  list,
    "is_nsfw":               bool,
    "is_spoiler":            bool,
    "is_oc":                 bool,
    "is_crosspost":          bool,

    # Metrics
    "score":                 int,
    "upvotes":               int,
    "downvotes":             int,
    "upvote_ratio":          float,
    "num_comments":          int,
    "num_awards":            int,
    "is_pinned":             bool,

    # Author snapshot
    "author_karma":          int,
    "author_account_age_days": int,

    # Timestamps
    "posted_at":             str,
    "ingested_at":           str,
    "source_system":         str,
    "schema_version":        str,
}

BRONZE_REDDIT_COMMENT_SCHEMA: Dict[str, Any] = {
    "comment_id":            str,
    "post_id":               str,
    "parent_comment_id":     Optional[str],
    "author_user_id":        str,
    "author_username":       str,
    "subreddit":             str,
    "platform":              str,
    "body":                  str,
    "depth":                 int,
    "is_edited":             bool,
    "is_deleted":            bool,
    "is_mod_comment":        bool,
    "contains_url":          bool,
    "score":                 int,
    "upvotes":               int,
    "downvotes":             int,
    "num_awards":            int,
    "posted_at":             str,
    "ingested_at":           str,
    "source_system":         str,
    "schema_version":        str,
}


# ── Schema registry ────────────────────────────────────────────────────────────

SCHEMA_REGISTRY: Dict[str, Dict] = {
    "twitter.tweets":         BRONZE_TWEET_SCHEMA,
    "twitter.engagements":    BRONZE_ENGAGEMENT_SCHEMA,
    "youtube.videos":         BRONZE_YOUTUBE_VIDEO_SCHEMA,
    "youtube.comments":       BRONZE_YOUTUBE_COMMENT_SCHEMA,
    "youtube.snapshots":      BRONZE_YOUTUBE_SNAPSHOT_SCHEMA,
    "reddit.posts":           BRONZE_REDDIT_POST_SCHEMA,
    "reddit.comments":        BRONZE_REDDIT_COMMENT_SCHEMA,
}


# ── Required fields (must never be null in Bronze) ─────────────────────────────

REQUIRED_FIELDS: Dict[str, List[str]] = {
    "twitter.tweets":      ["tweet_id", "user_id", "platform", "created_at"],
    "twitter.engagements": ["engagement_id", "post_id", "engaging_user_id", "engagement_type"],
    "youtube.videos":      ["video_id", "channel_id", "platform", "published_at"],
    "youtube.comments":    ["comment_id", "video_id", "author_user_id"],
    "youtube.snapshots":   ["snapshot_id", "video_id", "snapshot_date"],
    "reddit.posts":        ["post_id", "author_user_id", "subreddit", "posted_at"],
    "reddit.comments":     ["comment_id", "post_id", "author_user_id"],
}


def validate_record(record: Dict, schema_key: str) -> tuple[bool, List[str]]:
    """
    Lightweight Bronze validation — checks required fields are not null.
    Full schema validation happens at Silver layer with Great Expectations.

    Returns:
        (is_valid, list_of_errors)
    """
    errors = []
    required = REQUIRED_FIELDS.get(schema_key, [])
    for field_name in required:
        if field_name not in record or record[field_name] is None:
            errors.append(f"Required field missing or null: {field_name}")
    return len(errors) == 0, errors
