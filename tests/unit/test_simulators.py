"""
tests/unit/test_simulators.py — Unit tests for all simulator modules.

Tests run without Kafka or MinIO — verifies schema, data types, and business logic.
"""

import sys
import os
import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src/simulators"))

from user_pool import generate_user_pool, User
from twitter_simulator import _build_tweet, _build_engagement
from youtube_simulator import _build_video, _build_comment, _build_view_snapshot
from reddit_simulator import _build_post, _build_comment_thread, SUBREDDITS


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def user_pool():
    """Generate a small user pool for testing."""
    return generate_user_pool(num_users=100, num_brands=5)


@pytest.fixture
def brand_user(user_pool):
    return next(u for u in user_pool if u.account_type == "BRAND")


@pytest.fixture
def regular_user(user_pool):
    return next(u for u in user_pool if u.account_type == "USER")


# ── User Pool Tests ────────────────────────────────────────────────────────────

class TestUserPool:
    def test_generates_correct_count(self, user_pool):
        assert len(user_pool) == 100

    def test_has_brands(self, user_pool):
        brands = [u for u in user_pool if u.is_brand]
        assert len(brands) == 5

    def test_user_ids_are_unique(self, user_pool):
        ids = [u.user_id for u in user_pool]
        assert len(set(ids)) == len(ids)

    def test_user_id_is_valid_uuid(self, user_pool):
        for user in user_pool[:10]:
            uuid.UUID(user.user_id)  # Raises ValueError if invalid

    def test_follower_count_non_negative(self, user_pool):
        for user in user_pool:
            assert user.follower_count >= 0

    def test_brand_has_high_follower_count(self, brand_user):
        assert brand_user.follower_count >= 10_000

    def test_created_at_is_valid_iso(self, user_pool):
        for user in user_pool[:10]:
            dt = datetime.fromisoformat(user.created_at)
            assert dt <= datetime.now(timezone.utc)

    def test_all_account_types_present(self, user_pool):
        types = {u.account_type for u in user_pool}
        assert "BRAND" in types
        assert "USER" in types


# ── Twitter Simulator Tests ────────────────────────────────────────────────────

class TestTwitterSimulator:
    def test_tweet_has_required_fields(self, regular_user, user_pool):
        tweet = _build_tweet(regular_user, user_pool)
        required = [
            "tweet_id", "user_id", "platform", "content",
            "hashtags", "likes_count", "retweets_count",
            "created_at", "ingested_at", "schema_version",
        ]
        for field in required:
            assert field in tweet, f"Missing field: {field}"

    def test_tweet_platform_is_twitter(self, regular_user, user_pool):
        tweet = _build_tweet(regular_user, user_pool)
        assert tweet["platform"] == "twitter"

    def test_tweet_id_is_uuid(self, regular_user, user_pool):
        tweet = _build_tweet(regular_user, user_pool)
        uuid.UUID(tweet["tweet_id"])

    def test_tweet_user_id_matches(self, regular_user, user_pool):
        tweet = _build_tweet(regular_user, user_pool)
        assert tweet["user_id"] == regular_user.user_id

    def test_tweet_likes_non_negative(self, regular_user, user_pool):
        for _ in range(20):
            tweet = _build_tweet(regular_user, user_pool)
            assert tweet["likes_count"] >= 0
            assert tweet["retweets_count"] >= 0

    def test_tweet_timestamps_are_valid_iso(self, regular_user, user_pool):
        tweet = _build_tweet(regular_user, user_pool)
        datetime.fromisoformat(tweet["created_at"])
        datetime.fromisoformat(tweet["ingested_at"])

    def test_brand_tweet_has_higher_engagement(self, brand_user, regular_user, user_pool):
        """Brands should statistically get more engagement."""
        brand_likes = [_build_tweet(brand_user, user_pool)["likes_count"] for _ in range(50)]
        user_likes  = [_build_tweet(regular_user, user_pool)["likes_count"] for _ in range(50)]
        assert sum(brand_likes) > sum(user_likes)

    def test_engagement_event_structure(self, regular_user, user_pool):
        tweet = _build_tweet(regular_user, user_pool)
        eng = _build_engagement(tweet, regular_user)
        required = [
            "engagement_id", "tweet_id", "post_id",
            "engaging_user_id", "platform", "engagement_type",
            "created_at", "ingested_at",
        ]
        for field in required:
            assert field in eng, f"Missing field: {field}"

    def test_engagement_type_is_valid(self, regular_user, user_pool):
        valid_types = {"LIKE", "RETWEET", "REPLY", "QUOTE"}
        tweet = _build_tweet(regular_user, user_pool)
        for _ in range(20):
            eng = _build_engagement(tweet, regular_user)
            assert eng["engagement_type"] in valid_types

    def test_engagement_timestamp_after_tweet(self, regular_user, user_pool):
        tweet = _build_tweet(regular_user, user_pool)
        eng = _build_engagement(tweet, regular_user)
        tweet_ts = datetime.fromisoformat(tweet["created_at"])
        eng_ts   = datetime.fromisoformat(eng["created_at"])
        assert eng_ts >= tweet_ts


# ── YouTube Simulator Tests ────────────────────────────────────────────────────

class TestYouTubeSimulator:
    def test_video_has_required_fields(self, regular_user):
        video = _build_video(regular_user)
        required = [
            "video_id", "channel_id", "platform", "title",
            "category", "duration_seconds", "view_count",
            "like_count", "published_at", "ingested_at",
        ]
        for field in required:
            assert field in video, f"Missing field: {field}"

    def test_video_platform_is_youtube(self, regular_user):
        video = _build_video(regular_user)
        assert video["platform"] == "youtube"

    def test_video_duration_positive(self, regular_user):
        for _ in range(20):
            video = _build_video(regular_user)
            assert video["duration_seconds"] > 0

    def test_view_count_non_negative(self, regular_user):
        for _ in range(20):
            video = _build_video(regular_user)
            assert video["view_count"] >= 0

    def test_like_count_less_than_views(self, regular_user):
        for _ in range(20):
            video = _build_video(regular_user)
            assert video["like_count"] <= video["view_count"]

    def test_comment_has_required_fields(self, regular_user):
        video = _build_video(regular_user)
        comment = _build_comment(video, regular_user)
        required = ["comment_id", "video_id", "author_user_id", "content", "posted_at"]
        for field in required:
            assert field in comment, f"Missing field: {field}"

    def test_view_snapshots_are_chronological(self, regular_user):
        video = _build_video(regular_user)
        snapshots = _build_view_snapshot(video)
        if len(snapshots) > 1:
            dates = [s["snapshot_date"] for s in snapshots]
            assert dates == sorted(dates)

    def test_cumulative_views_non_decreasing(self, regular_user):
        video = _build_video(regular_user)
        snapshots = _build_view_snapshot(video)
        if len(snapshots) > 1:
            for i in range(1, len(snapshots)):
                assert snapshots[i]["cumulative_views"] >= snapshots[i-1]["cumulative_views"]


# ── Reddit Simulator Tests ─────────────────────────────────────────────────────

class TestRedditSimulator:
    def test_post_has_required_fields(self, regular_user):
        subreddit = SUBREDDITS[0]
        post = _build_post(regular_user, subreddit)
        required = [
            "post_id", "author_user_id", "subreddit", "platform",
            "title", "score", "upvotes", "posted_at", "ingested_at",
        ]
        for field in required:
            assert field in post, f"Missing field: {field}"

    def test_post_platform_is_reddit(self, regular_user):
        post = _build_post(regular_user, SUBREDDITS[0])
        assert post["platform"] == "reddit"

    def test_upvote_ratio_valid_range(self, regular_user):
        for _ in range(20):
            post = _build_post(regular_user, SUBREDDITS[0])
            assert 0.0 <= post["upvote_ratio"] <= 1.0

    def test_comment_thread_depth_limit(self, regular_user, user_pool):
        subreddit = SUBREDDITS[0]
        post = _build_post(regular_user, subreddit)
        comments = _build_comment_thread(post, user_pool, num_top_level=5, max_depth=3)
        for comment in comments:
            assert comment["depth"] <= 3

    def test_comment_has_post_id(self, regular_user, user_pool):
        post = _build_post(regular_user, SUBREDDITS[0])
        comments = _build_comment_thread(post, user_pool, num_top_level=3)
        for comment in comments:
            assert comment["post_id"] == post["post_id"]

    def test_score_non_negative(self, regular_user):
        for _ in range(20):
            post = _build_post(regular_user, SUBREDDITS[0])
            assert post["score"] >= 0
