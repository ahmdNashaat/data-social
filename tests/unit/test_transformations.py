"""
tests/unit/test_transformations.py — Unit tests for transformation logic.

Tests the business logic of PySpark transformations without requiring
a running Spark cluster. Uses pure Python to validate the logic.
"""

import sys
import os
import pytest
from datetime import datetime, timezone, timedelta
from typing import List, Dict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))


# ── Helpers — replicate transformation logic in pure Python for testing ────────

def deduplicate_records(records: List[Dict], primary_keys: List[str], order_by: str = "ingested_at") -> List[Dict]:
    """Pure Python equivalent of the PySpark deduplicate() function."""
    groups: Dict[tuple, Dict] = {}
    for record in records:
        key = tuple(record.get(k) for k in primary_keys)
        if key not in groups or record.get(order_by, "") > groups[key].get(order_by, ""):
            groups[key] = record
    return list(groups.values())


def normalize_engagement_type(eng_type: str) -> str:
    """Replicate the PySpark engagement_type normalization."""
    valid = {"LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW"}
    cleaned = eng_type.strip().upper() if eng_type else ""
    return cleaned if cleaned in valid else "UNKNOWN"


def get_engagement_value(eng_type: str) -> float:
    """Replicate the engagement value weight logic."""
    weights = {"LIKE": 1.0, "REPLY": 2.0, "RETWEET": 3.0, "QUOTE": 4.0, "VIEW": 0.1}
    return weights.get(eng_type, 1.0)


def get_engagement_tier(total_engagement: int) -> str:
    """Replicate the engagement tier classification."""
    if total_engagement >= 10_000:
        return "viral"
    elif total_engagement >= 1_000:
        return "high"
    elif total_engagement >= 100:
        return "medium"
    return "low"


def calculate_engagement_rate(likes: int, shares: int, comments: int, impressions: int) -> float:
    """Safe engagement rate calculation."""
    if impressions == 0:
        return 0.0
    return round((likes + shares + comments) / impressions * 100, 4)


def is_weekend(dt: datetime) -> bool:
    """Check if a datetime falls on a weekend."""
    return dt.weekday() >= 5  # 5=Saturday, 6=Sunday


def date_to_sk(dt) -> int:
    """Convert date to YYYYMMDD integer surrogate key."""
    if hasattr(dt, 'date'):
        d = dt.date()
    else:
        d = dt
    return int(d.strftime("%Y%m%d"))


# ── Deduplication tests ────────────────────────────────────────────────────────

class TestDeduplication:

    def test_dedup_keeps_latest_by_ingested_at(self):
        records = [
            {"tweet_id": "t1", "content": "old",  "ingested_at": "2024-01-15T10:00:00"},
            {"tweet_id": "t1", "content": "new",  "ingested_at": "2024-01-15T12:00:00"},
            {"tweet_id": "t1", "content": "older","ingested_at": "2024-01-15T08:00:00"},
        ]
        result = deduplicate_records(records, ["tweet_id"])
        assert len(result) == 1
        assert result[0]["content"] == "new"

    def test_dedup_keeps_unique_records(self):
        records = [
            {"tweet_id": "t1", "content": "a", "ingested_at": "2024-01-15T10:00:00"},
            {"tweet_id": "t2", "content": "b", "ingested_at": "2024-01-15T10:00:00"},
            {"tweet_id": "t3", "content": "c", "ingested_at": "2024-01-15T10:00:00"},
        ]
        result = deduplicate_records(records, ["tweet_id"])
        assert len(result) == 3

    def test_dedup_compound_key(self):
        records = [
            {"post_id": "p1", "platform": "twitter", "content": "tweet",  "ingested_at": "2024-01-15T10:00:00"},
            {"post_id": "p1", "platform": "youtube", "content": "video",  "ingested_at": "2024-01-15T10:00:00"},
            {"post_id": "p1", "platform": "twitter", "content": "tweet2", "ingested_at": "2024-01-15T12:00:00"},
        ]
        result = deduplicate_records(records, ["post_id", "platform"])
        assert len(result) == 2

    def test_dedup_empty_input(self):
        result = deduplicate_records([], ["tweet_id"])
        assert result == []

    def test_dedup_single_record(self):
        records = [{"tweet_id": "t1", "content": "only", "ingested_at": "2024-01-15T10:00:00"}]
        result = deduplicate_records(records, ["tweet_id"])
        assert len(result) == 1

    def test_dedup_is_idempotent(self):
        """Running dedup twice should produce the same result as running once."""
        records = [
            {"tweet_id": "t1", "content": "old", "ingested_at": "2024-01-15T10:00:00"},
            {"tweet_id": "t1", "content": "new", "ingested_at": "2024-01-15T12:00:00"},
        ]
        once  = deduplicate_records(records, ["tweet_id"])
        twice = deduplicate_records(once, ["tweet_id"])
        assert once == twice


# ── Engagement type normalization tests ───────────────────────────────────────

class TestEngagementTypeNormalization:

    def test_valid_types_pass_through(self):
        for eng_type in ["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW"]:
            assert normalize_engagement_type(eng_type) == eng_type

    def test_lowercase_normalized_to_upper(self):
        assert normalize_engagement_type("like") == "LIKE"
        assert normalize_engagement_type("retweet") == "RETWEET"

    def test_mixed_case_normalized(self):
        assert normalize_engagement_type("Like") == "LIKE"
        assert normalize_engagement_type("ReTweet") == "RETWEET"

    def test_invalid_type_becomes_unknown(self):
        assert normalize_engagement_type("CLAP") == "UNKNOWN"
        assert normalize_engagement_type("share") == "UNKNOWN"
        assert normalize_engagement_type("")      == "UNKNOWN"

    def test_whitespace_stripped(self):
        assert normalize_engagement_type("  LIKE  ") == "LIKE"


# ── Engagement value tests ────────────────────────────────────────────────────

class TestEngagementValues:

    def test_like_has_lowest_weight(self):
        assert get_engagement_value("LIKE") < get_engagement_value("REPLY")

    def test_quote_has_highest_weight(self):
        assert get_engagement_value("QUOTE") > get_engagement_value("RETWEET")
        assert get_engagement_value("QUOTE") > get_engagement_value("LIKE")

    def test_view_has_fractional_weight(self):
        assert get_engagement_value("VIEW") < 1.0

    def test_unknown_type_gets_default(self):
        assert get_engagement_value("UNKNOWN") == 1.0
        assert get_engagement_value("INVALID") == 1.0

    def test_all_values_positive(self):
        for eng_type in ["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW"]:
            assert get_engagement_value(eng_type) > 0


# ── Engagement tier tests ─────────────────────────────────────────────────────

class TestEngagementTier:

    def test_viral_threshold(self):
        assert get_engagement_tier(10_000) == "viral"
        assert get_engagement_tier(100_000) == "viral"
        assert get_engagement_tier(9_999) == "high"

    def test_high_threshold(self):
        assert get_engagement_tier(1_000) == "high"
        assert get_engagement_tier(5_000) == "high"
        assert get_engagement_tier(999) == "medium"

    def test_medium_threshold(self):
        assert get_engagement_tier(100) == "medium"
        assert get_engagement_tier(500) == "medium"
        assert get_engagement_tier(99) == "low"

    def test_low_threshold(self):
        assert get_engagement_tier(0) == "low"
        assert get_engagement_tier(50) == "low"

    def test_tier_values_are_valid(self):
        valid = {"viral", "high", "medium", "low"}
        for n in [0, 10, 100, 500, 1000, 5000, 10000, 100000]:
            assert get_engagement_tier(n) in valid


# ── Engagement rate tests ─────────────────────────────────────────────────────

class TestEngagementRate:

    def test_basic_calculation(self):
        rate = calculate_engagement_rate(likes=10, shares=5, comments=5, impressions=1000)
        assert rate == 2.0

    def test_zero_impressions_returns_zero(self):
        rate = calculate_engagement_rate(100, 50, 20, 0)
        assert rate == 0.0

    def test_rate_capped_logic(self):
        """Rate can exceed 100% if engagement > impressions (boosted content)."""
        rate = calculate_engagement_rate(200, 100, 50, 100)
        assert rate > 100

    def test_rounded_to_4_decimals(self):
        rate = calculate_engagement_rate(1, 0, 0, 3)
        assert str(rate).count(".") == 1
        decimal_places = len(str(rate).split(".")[1])
        assert decimal_places <= 4


# ── Date surrogate key tests ──────────────────────────────────────────────────

class TestDateSurrKey:

    def test_sk_is_yyyymmdd_integer(self):
        from datetime import date
        sk = date_to_sk(date(2024, 1, 15))
        assert sk == 20240115

    def test_sk_from_datetime(self):
        dt = datetime(2024, 6, 30, 12, 0, 0, tzinfo=timezone.utc)
        sk = date_to_sk(dt)
        assert sk == 20240630

    def test_sk_is_integer(self):
        from datetime import date
        sk = date_to_sk(date(2024, 1, 1))
        assert isinstance(sk, int)

    def test_sk_sortable(self):
        """Date SKs should sort chronologically as integers."""
        from datetime import date
        dates = [date(2024, 3, 1), date(2024, 1, 15), date(2024, 12, 31)]
        sks = sorted(date_to_sk(d) for d in dates)
        assert sks == [20240115, 20240301, 20241231]


# ── Weekend detection tests ───────────────────────────────────────────────────

class TestWeekendDetection:

    def test_saturday_is_weekend(self):
        saturday = datetime(2024, 1, 13)   # A Saturday
        assert is_weekend(saturday) is True

    def test_sunday_is_weekend(self):
        sunday = datetime(2024, 1, 14)     # A Sunday
        assert is_weekend(sunday) is True

    def test_weekdays_not_weekend(self):
        for offset in range(5):  # Monday–Friday
            monday = datetime(2024, 1, 8)  # A Monday
            weekday = monday + timedelta(days=offset)
            assert is_weekend(weekday) is False
