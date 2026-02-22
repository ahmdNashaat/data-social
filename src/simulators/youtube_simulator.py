"""
youtube_simulator.py — Simulates YouTube batch data.

Generates video metadata + time-series view snapshots + comments.
Uploads JSON files to MinIO landing zone for batch ingestion.

Usage:
    python youtube_simulator.py --videos 500
    python youtube_simulator.py --videos 100 --comments-per-video 20
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from faker import Faker
from loguru import logger

from config import CONFIG
from clients import create_minio_client, upload_to_minio, build_landing_key
from user_pool import User, generate_user_pool, load_user_pool

fake = Faker()
random.seed()   # Non-deterministic after pool generation


# ── Video categories & content ─────────────────────────────────────────────────

VIDEO_CATEGORIES = [
    "Technology", "Education", "Entertainment", "Gaming",
    "Music", "News", "Sports", "Travel", "Food", "Fitness",
    "Business", "Science", "Comedy", "Film", "Fashion",
]

DURATION_RANGES = {
    "short":    (60,   600),      # 1–10 min (Shorts-like)
    "standard": (600,  1800),     # 10–30 min
    "long":     (1800, 7200),     # 30–120 min
}


def _video_duration() -> int:
    """Return video duration in seconds with realistic distribution."""
    category = random.choices(
        ["short", "standard", "long"],
        weights=[0.25, 0.55, 0.20],
    )[0]
    low, high = DURATION_RANGES[category]
    return random.randint(low, high)


def _view_velocity(account_type: str, days_since_publish: int) -> int:
    """Simulate realistic view growth — fast early, slow later."""
    base_views = {
        "BRAND":      random.randint(5_000,   500_000),
        "INFLUENCER": random.randint(1_000,   200_000),
        "USER":       random.randint(0,       10_000),
    }.get(account_type, 100)

    # Decay factor: most views in first 7 days
    decay = max(0.1, 1 - (days_since_publish / 365) * 0.6)
    return int(base_views * decay * random.uniform(0.8, 1.2))


# ── Schema builders ────────────────────────────────────────────────────────────

def _build_video(user: User) -> Dict:
    """Build a YouTube video metadata record."""
    category = random.choice(VIDEO_CATEGORIES)
    hashtags = random.sample(CONFIG.all_hashtags, k=random.randint(1, 5))

    published_at = datetime.now(timezone.utc) - timedelta(
        days=random.randint(1, 365 * 2)
    )
    days_since = (datetime.now(timezone.utc) - published_at).days
    views = _view_velocity(user.account_type, days_since)
    likes = int(views * random.uniform(0.02, 0.08))
    dislikes = int(likes * random.uniform(0.01, 0.05))
    comments = int(views * random.uniform(0.001, 0.01))

    return {
        "video_id":          str(uuid.uuid4()),
        "channel_id":        user.user_id,
        "platform":          "youtube",

        # Content metadata
        "title":             fake.sentence(nb_words=random.randint(5, 12)).rstrip("."),
        "description":       fake.paragraph(nb_sentences=3),
        "category":          category,
        "tags":              hashtags,
        "language":          random.choices(["en", "es", "fr", "de", "pt"], weights=[0.7, 0.1, 0.08, 0.06, 0.06])[0],
        "duration_seconds":  _video_duration(),
        "thumbnail_url":     f"https://img.youtube.com/vi/{uuid.uuid4()}/maxresdefault.jpg",
        "is_short":          random.random() < 0.2,
        "is_live_stream":    random.random() < 0.05,
        "age_restricted":    False,

        # Engagement metrics
        "view_count":        views,
        "like_count":        likes,
        "dislike_count":     dislikes,
        "comment_count":     comments,
        "share_count":       int(views * random.uniform(0.001, 0.005)),
        "subscriber_gain":   int(views * random.uniform(0.0001, 0.002)),

        # Channel snapshot
        "channel_name":      user.display_name,
        "channel_subscriber_count": user.follower_count,
        "channel_account_type": user.account_type,

        # Timestamps
        "published_at":      published_at.isoformat(),
        "ingested_at":       datetime.now(timezone.utc).isoformat(),

        # Pipeline metadata
        "source_system":     "youtube_simulator_v1",
        "schema_version":    "1.0",
    }


def _build_comment(video: Dict, user: User) -> Dict:
    """Build a YouTube comment for a video."""
    published_at = datetime.fromisoformat(video["published_at"])
    max_offset = (datetime.now(timezone.utc) - published_at).total_seconds()
    offset = random.uniform(0, min(max_offset, 86400 * 30))
    comment_ts = published_at + timedelta(seconds=offset)

    likes = max(0, int(random.expovariate(0.5)))
    is_reply = random.random() < 0.3

    return {
        "comment_id":        str(uuid.uuid4()),
        "video_id":          video["video_id"],
        "parent_comment_id": str(uuid.uuid4()) if is_reply else None,
        "author_user_id":    user.user_id,
        "author_username":   user.username,
        "platform":          "youtube",

        # Content
        "content":           fake.sentence(nb_words=random.randint(5, 30)),
        "is_reply":          is_reply,
        "is_pinned":         random.random() < 0.02,
        "is_hearted":        random.random() < 0.05,

        # Engagement
        "like_count":        likes,

        # Timestamps
        "published_at":      comment_ts.isoformat(),
        "ingested_at":       datetime.now(timezone.utc).isoformat(),

        # Pipeline metadata
        "source_system":     "youtube_simulator_v1",
        "schema_version":    "1.0",
    }


def _build_view_snapshot(video: Dict) -> List[Dict]:
    """
    Build daily view count time-series snapshots for a video.
    Shows realistic growth curve over the video's lifetime.
    """
    published_at = datetime.fromisoformat(video["published_at"])
    days_since = (datetime.now(timezone.utc) - published_at).days
    snapshots = []

    cumulative_views = 0
    for day in range(min(days_since, 90)):  # Max 90 days of history
        # Day 1 gets the biggest spike
        daily_multiplier = max(0.01, 1 / (day + 1) ** 0.5)
        daily_views = int(video["view_count"] * daily_multiplier * random.uniform(0.8, 1.2) / 10)
        cumulative_views += daily_views

        snap_date = published_at + timedelta(days=day)
        snapshots.append({
            "snapshot_id":        str(uuid.uuid4()),
            "video_id":           video["video_id"],
            "snapshot_date":      snap_date.date().isoformat(),
            "daily_views":        daily_views,
            "cumulative_views":   cumulative_views,
            "daily_likes":        int(daily_views * random.uniform(0.02, 0.08)),
            "daily_comments":     int(daily_views * random.uniform(0.001, 0.01)),
            "ingested_at":        datetime.now(timezone.utc).isoformat(),
            "source_system":      "youtube_simulator_v1",
        })

    return snapshots


# ── Main simulator ─────────────────────────────────────────────────────────────

def run(
    num_videos: int = 500,
    comments_per_video: int = 10,
    include_snapshots: bool = True,
    dry_run: bool = False,
) -> None:
    """
    Run the YouTube simulator. Generates video metadata, comments, and
    view snapshots, then uploads JSON files to MinIO landing zone.

    Args:
        num_videos:          Number of videos to generate.
        comments_per_video:  Average comments per video.
        include_snapshots:   Whether to generate view time-series data.
        dry_run:             Print stats without uploading to MinIO.
    """
    logger.info("Initializing YouTube Simulator...")

    try:
        users = load_user_pool()
    except FileNotFoundError:
        users = generate_user_pool()

    minio = None if dry_run else create_minio_client()
    run_id = str(uuid.uuid4())[:8]

    videos = []
    all_comments = []
    all_snapshots = []

    logger.info(f"Generating {num_videos} videos...")

    for i in range(num_videos):
        user = random.choice(users)
        video = _build_video(user)
        videos.append(video)

        # Comments
        n_comments = max(0, int(random.gauss(comments_per_video, comments_per_video / 2)))
        for _ in range(n_comments):
            commenter = random.choice(users)
            all_comments.append(_build_comment(video, commenter))

        # View snapshots
        if include_snapshots:
            all_snapshots.extend(_build_view_snapshot(video))

        if (i + 1) % 100 == 0:
            logger.info(f"Generated {i+1}/{num_videos} videos...")

    logger.info(
        f"Generated: {len(videos)} videos | "
        f"{len(all_comments)} comments | "
        f"{len(all_snapshots)} view snapshots"
    )

    if dry_run:
        logger.info("Dry run — skipping MinIO upload.")
        logger.info(f"Sample video:\n{json.dumps(videos[0], indent=2)}")
        return

    # ── Upload to MinIO landing zone ────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Videos file
    videos_key = build_landing_key("youtube/videos", f"videos_{ts}_{run_id}.json")
    upload_to_minio(
        minio, CONFIG.bucket_landing, videos_key,
        json.dumps(videos, default=str), "application/json"
    )

    # Comments file (split into chunks of 10K)
    chunk_size = 10_000
    for idx, start in enumerate(range(0, len(all_comments), chunk_size)):
        chunk = all_comments[start:start + chunk_size]
        key = build_landing_key("youtube/comments", f"comments_{ts}_{run_id}_{idx:03d}.json")
        upload_to_minio(
            minio, CONFIG.bucket_landing, key,
            json.dumps(chunk, default=str), "application/json"
        )

    # Snapshots file
    if all_snapshots:
        snaps_key = build_landing_key("youtube/snapshots", f"snapshots_{ts}_{run_id}.json")
        upload_to_minio(
            minio, CONFIG.bucket_landing, snaps_key,
            json.dumps(all_snapshots, default=str), "application/json"
        )

    logger.info(
        f"YouTube simulator complete: "
        f"{len(videos)} videos, {len(all_comments)} comments, "
        f"{len(all_snapshots)} snapshots uploaded to MinIO."
    )


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YouTube Batch Data Simulator")
    parser.add_argument("--videos",             type=int,  default=500,  help="Number of videos to generate")
    parser.add_argument("--comments-per-video", type=int,  default=10,   help="Avg comments per video")
    parser.add_argument("--no-snapshots",       action="store_true",     help="Skip view snapshot generation")
    parser.add_argument("--dry-run",            action="store_true",     help="Print without uploading")
    args = parser.parse_args()

    run(
        num_videos=args.videos,
        comments_per_video=args.comments_per_video,
        include_snapshots=not args.no_snapshots,
        dry_run=args.dry_run,
    )
