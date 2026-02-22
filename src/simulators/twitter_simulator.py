"""
twitter_simulator.py — Simulates Twitter/X streaming events.

Produces tweet and engagement events to Kafka topics.
Mimics realistic patterns: bursts, trending hashtags, engagement ratios.

Usage:
    python twitter_simulator.py --events 50000
    python twitter_simulator.py --events 1000 --rate 10  # 10 tweets/sec
    python twitter_simulator.py --continuous             # Run forever
"""

import argparse
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from faker import Faker
from loguru import logger

from config import CONFIG
from clients import create_kafka_producer, send_to_kafka
from user_pool import User, generate_user_pool, load_user_pool

fake = Faker()


# ── Schema builders ────────────────────────────────────────────────────────────

def _build_tweet(user: User, all_users: List[User]) -> Dict:
    """Build a single tweet event matching the Bronze schema."""

    # Pick 0–3 hashtags weighted toward trending
    num_tags = random.choices([0, 1, 2, 3], weights=[0.3, 0.4, 0.2, 0.1])[0]
    hashtags = random.sample(CONFIG.all_hashtags, k=min(num_tags, len(CONFIG.all_hashtags)))

    # Mentions (0–2 random users)
    num_mentions = random.choices([0, 1, 2], weights=[0.6, 0.3, 0.1])[0]
    mentions = [
        random.choice(all_users).username
        for _ in range(num_mentions)
    ]

    # Tweet content
    content_templates = [
        fake.sentence(nb_words=random.randint(5, 20)),
        f"Just published: {fake.catch_phrase()} {' '.join(hashtags)}",
        f"Interesting thread on {fake.bs()} {' '.join(hashtags)}",
        f"Thoughts on {fake.catch_phrase()}? {' '.join(hashtags)}",
    ]
    content = random.choice(content_templates)
    if mentions:
        content = f"@{mentions[0]} {content}"

    # Engagement counts — brands/influencers get more
    multiplier = 10 if user.account_type == "BRAND" else (
        5 if user.account_type == "INFLUENCER" else 1
    )
    likes     = max(0, int(random.expovariate(0.1) * multiplier))
    retweets  = max(0, int(likes * random.uniform(0.05, 0.3)))
    replies   = max(0, int(likes * random.uniform(0.02, 0.15)))
    impressions = max(likes, int(user.follower_count * random.uniform(0.01, 0.4)))

    # Slight timestamp jitter (simulate delayed delivery)
    jitter_seconds = random.randint(0, 60)
    ts = datetime.now(timezone.utc) - timedelta(seconds=jitter_seconds)

    return {
        # Identifiers
        "tweet_id":        str(uuid.uuid4()),
        "user_id":         user.user_id,
        "platform":        "twitter",

        # Content
        "content":         content,
        "hashtags":        hashtags,
        "mentions":        mentions,
        "language":        random.choices(["en", "es", "fr", "de", "pt"], weights=[0.7, 0.1, 0.08, 0.06, 0.06])[0],
        "is_retweet":      random.random() < 0.15,
        "is_reply":        random.random() < 0.20,
        "media_type":      random.choices([None, "photo", "video", "gif"], weights=[0.6, 0.25, 0.1, 0.05])[0],

        # Metrics (snapshot at ingestion time)
        "likes_count":       likes,
        "retweets_count":    retweets,
        "replies_count":     replies,
        "impressions_count": impressions,
        "quote_count":       max(0, int(retweets * random.uniform(0.1, 0.3))),

        # User snapshot
        "user_follower_count": user.follower_count,
        "user_account_type":   user.account_type,
        "user_is_verified":    user.is_verified,

        # Timestamps
        "created_at":      ts.isoformat(),
        "ingested_at":     datetime.now(timezone.utc).isoformat(),

        # Pipeline metadata
        "source_system":   "twitter_simulator_v1",
        "schema_version":  "1.0",
    }


def _build_engagement(tweet: Dict, engaging_user: User) -> Dict:
    """Build an engagement event for a tweet."""
    eng_type = random.choices(
        ["LIKE", "RETWEET", "REPLY", "QUOTE"],
        weights=[0.65, 0.20, 0.12, 0.03],
    )[0]

    ts = datetime.fromisoformat(tweet["created_at"])
    # Engagement happens after the tweet (up to 24h later)
    eng_ts = ts + timedelta(seconds=random.randint(1, 86400))

    return {
        "engagement_id":   str(uuid.uuid4()),
        "tweet_id":        tweet["tweet_id"],
        "post_id":         tweet["tweet_id"],    # Unified field across platforms
        "engaging_user_id": engaging_user.user_id,
        "post_author_id":  tweet["user_id"],
        "platform":        "twitter",
        "engagement_type": eng_type,
        "content":         fake.sentence() if eng_type in ("REPLY", "QUOTE") else None,
        "created_at":      eng_ts.isoformat(),
        "ingested_at":     datetime.now(timezone.utc).isoformat(),
        "source_system":   "twitter_simulator_v1",
        "schema_version":  "1.0",
    }


# ── Main simulator ─────────────────────────────────────────────────────────────

def run(
    num_events: int,
    rate: int = CONFIG.tweet_rate,
    continuous: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Run the Twitter simulator.

    Args:
        num_events:  Number of tweets to produce (ignored if continuous=True).
        rate:        Tweets per second.
        continuous:  Run indefinitely.
        dry_run:     Print events without sending to Kafka.
    """
    logger.info("Initializing Twitter/X Simulator...")

    # Load or generate user pool
    try:
        users = load_user_pool()
    except FileNotFoundError:
        logger.info("No user pool found — generating...")
        users = generate_user_pool()

    producer = None if dry_run else create_kafka_producer()

    tweet_count = 0
    engagement_count = 0
    batch_size = min(rate, 100)
    sleep_interval = batch_size / rate if rate > 0 else 0

    logger.info(
        f"Starting Twitter simulator | "
        f"target: {'continuous' if continuous else num_events} tweets | "
        f"rate: {rate}/sec | "
        f"users: {len(users)} | "
        f"dry_run: {dry_run}"
    )

    try:
        while continuous or tweet_count < num_events:
            batch_tweets = []

            for _ in range(batch_size):
                if not continuous and tweet_count >= num_events:
                    break

                user = random.choice(users)
                tweet = _build_tweet(user, users)
                batch_tweets.append(tweet)

                if dry_run:
                    print(tweet)
                else:
                    send_to_kafka(
                        producer,
                        topic=CONFIG.topic_tweets,
                        message=tweet,
                        key=tweet["user_id"],
                    )
                tweet_count += 1

                # Generate engagements for some tweets
                if random.random() < CONFIG.engagement_ratio:
                    num_engs = random.randint(1, 5)
                    for _ in range(num_engs):
                        engaging_user = random.choice(users)
                        engagement = _build_engagement(tweet, engaging_user)
                        if not dry_run:
                            send_to_kafka(
                                producer,
                                topic=CONFIG.topic_engagements,
                                message=engagement,
                                key=engagement["tweet_id"],
                            )
                        engagement_count += 1

            # Progress log every 1000 tweets
            if tweet_count % 1000 == 0 and tweet_count > 0:
                logger.info(
                    f"Progress: {tweet_count:,} tweets | "
                    f"{engagement_count:,} engagements"
                )

            if sleep_interval > 0:
                time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Simulator stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()

    logger.info(
        f"Twitter simulator complete: "
        f"{tweet_count:,} tweets, {engagement_count:,} engagements sent"
    )


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter/X Social Media Simulator")
    parser.add_argument("--events",     type=int,  default=10_000, help="Number of tweets to generate")
    parser.add_argument("--rate",       type=int,  default=CONFIG.tweet_rate, help="Tweets per second")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely")
    parser.add_argument("--dry-run",    action="store_true", help="Print events without sending to Kafka")
    args = parser.parse_args()

    run(
        num_events=args.events,
        rate=args.rate,
        continuous=args.continuous,
        dry_run=args.dry_run,
    )
