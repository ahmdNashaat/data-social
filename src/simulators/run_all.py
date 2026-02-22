"""
run_all.py — Master simulator runner.

Generates the user pool once, then runs all three simulators.
This is what Airflow calls via the `simulate` Makefile target.

Usage:
    python run_all.py                        # default settings
    python run_all.py --scale small          # quick test: 1K tweets
    python run_all.py --scale medium         # moderate: 50K tweets
    python run_all.py --scale large          # full: 500K tweets
    python run_all.py --dry-run              # no Kafka/MinIO needed
"""

import argparse
import time
from loguru import logger

from user_pool import generate_user_pool, save_user_pool
import twitter_simulator
import youtube_simulator
import reddit_simulator

# ── Scale presets ──────────────────────────────────────────────────────────────
SCALE_PRESETS = {
    "small": {
        "description": "Quick test run (~1 min)",
        "tweet_events": 1_000,
        "tweet_rate": 50,
        "youtube_videos": 50,
        "youtube_comments": 5,
        "reddit_posts": 200,
        "reddit_comments": 5,
    },
    "medium": {
        "description": "Moderate pipeline test (~5 min)",
        "tweet_events": 50_000,
        "tweet_rate": 100,
        "youtube_videos": 500,
        "youtube_comments": 10,
        "reddit_posts": 2_000,
        "reddit_comments": 8,
    },
    "large": {
        "description": "Full production-like run (~20 min)",
        "tweet_events": 500_000,
        "tweet_rate": 200,
        "youtube_videos": 2_000,
        "youtube_comments": 20,
        "reddit_posts": 10_000,
        "reddit_comments": 15,
    },
}


def main(scale: str = "medium", dry_run: bool = False) -> None:
    preset = SCALE_PRESETS[scale]
    total_start = time.time()

    logger.info("=" * 60)
    logger.info(f"Social Media Analytics Platform — Data Simulator")
    logger.info(f"Scale: {scale.upper()} — {preset['description']}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("=" * 60)

    # ── Step 1: Generate shared user pool ─────────────────────────────────
    logger.info("\n[1/4] Generating user pool...")
    t = time.time()
    users = generate_user_pool()
    save_user_pool(users)
    logger.info(f"User pool ready ({len(users):,} users) in {time.time()-t:.1f}s")

    # ── Step 2: Twitter/X streaming events → Kafka ─────────────────────────
    logger.info(f"\n[2/4] Running Twitter simulator ({preset['tweet_events']:,} events)...")
    t = time.time()
    twitter_simulator.run(
        num_events=preset["tweet_events"],
        rate=preset["tweet_rate"],
        dry_run=dry_run,
    )
    logger.info(f"Twitter simulator done in {time.time()-t:.1f}s")

    # ── Step 3: YouTube batch files → MinIO ───────────────────────────────
    logger.info(f"\n[3/4] Running YouTube simulator ({preset['youtube_videos']:,} videos)...")
    t = time.time()
    youtube_simulator.run(
        num_videos=preset["youtube_videos"],
        comments_per_video=preset["youtube_comments"],
        dry_run=dry_run,
    )
    logger.info(f"YouTube simulator done in {time.time()-t:.1f}s")

    # ── Step 4: Reddit batch files → MinIO ───────────────────────────────
    logger.info(f"\n[4/4] Running Reddit simulator ({preset['reddit_posts']:,} posts)...")
    t = time.time()
    reddit_simulator.run(
        num_posts=preset["reddit_posts"],
        comments_per_post=preset["reddit_comments"],
        dry_run=dry_run,
    )
    logger.info(f"Reddit simulator done in {time.time()-t:.1f}s")

    # ── Summary ────────────────────────────────────────────────────────────
    elapsed = time.time() - total_start
    logger.info("\n" + "=" * 60)
    logger.info(f"All simulators complete in {elapsed:.1f}s ({elapsed/60:.1f} min)")
    logger.info(f"Scale: {scale.upper()}")
    logger.info(
        f"Estimated events generated:\n"
        f"  Twitter events : ~{preset['tweet_events']:,}\n"
        f"  YouTube videos : ~{preset['youtube_videos']:,}\n"
        f"  Reddit posts   : ~{preset['reddit_posts']:,}\n"
        f"  Total          : ~{preset['tweet_events'] + preset['youtube_videos'] + preset['reddit_posts']:,}"
    )
    if not dry_run:
        logger.info("Data is available in:")
        logger.info("  Kafka  → topics: social.tweets, social.engagements")
        logger.info("  MinIO  → bucket: landing-zone/youtube/, landing-zone/reddit/")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all social media simulators")
    parser.add_argument(
        "--scale",
        choices=["small", "medium", "large"],
        default="medium",
        help="Scale preset (default: medium)"
    )
    parser.add_argument("--dry-run", action="store_true", help="No Kafka/MinIO needed")
    args = parser.parse_args()

    main(scale=args.scale, dry_run=args.dry_run)
