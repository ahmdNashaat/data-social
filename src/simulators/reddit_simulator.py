"""
reddit_simulator.py — Simulates Reddit batch data.

Generates posts, comments (threaded), and subreddit metadata.
Uploads JSON files to MinIO landing zone for batch ingestion.

Usage:
    python reddit_simulator.py --posts 2000
    python reddit_simulator.py --posts 500 --comments-per-post 15
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from faker import Faker
from loguru import logger

from config import CONFIG
from clients import create_minio_client, upload_to_minio, build_landing_key
from user_pool import User, generate_user_pool, load_user_pool

fake = Faker()
random.seed()


# ── Subreddit definitions ──────────────────────────────────────────────────────

SUBREDDITS = [
    {"name": "r/technology",        "category": "tech",      "subscribers": 14_000_000},
    {"name": "r/datascience",       "category": "tech",      "subscribers": 1_200_000},
    {"name": "r/MachineLearning",   "category": "tech",      "subscribers": 3_100_000},
    {"name": "r/programming",       "category": "tech",      "subscribers": 6_200_000},
    {"name": "r/Python",            "category": "tech",      "subscribers": 1_600_000},
    {"name": "r/worldnews",         "category": "news",      "subscribers": 31_000_000},
    {"name": "r/news",              "category": "news",      "subscribers": 26_000_000},
    {"name": "r/business",          "category": "business",  "subscribers": 1_800_000},
    {"name": "r/startups",          "category": "business",  "subscribers": 1_100_000},
    {"name": "r/marketing",         "category": "business",  "subscribers": 520_000},
    {"name": "r/AskReddit",         "category": "general",   "subscribers": 42_000_000},
    {"name": "r/gaming",            "category": "gaming",    "subscribers": 38_000_000},
    {"name": "r/sports",            "category": "sports",    "subscribers": 2_900_000},
    {"name": "r/movies",            "category": "entertainment", "subscribers": 29_000_000},
    {"name": "r/music",             "category": "entertainment", "subscribers": 31_000_000},
    {"name": "r/food",              "category": "lifestyle", "subscribers": 22_000_000},
    {"name": "r/travel",            "category": "lifestyle", "subscribers": 8_900_000},
    {"name": "r/fitness",           "category": "lifestyle", "subscribers": 11_000_000},
]

POST_TYPES = ["text", "link", "image", "video", "poll"]
FLAIR_TAGS = {
    "tech":         ["Discussion", "Project", "Question", "Research", "Tutorial", "News"],
    "news":         ["Breaking", "Analysis", "Opinion", "Developing"],
    "business":     ["Discussion", "Advice", "Question", "News", "Success Story"],
    "gaming":       ["Discussion", "Question", "Recommendation", "Review"],
    "entertainment":["Discussion", "Review", "Question", "Fan Theory"],
    "lifestyle":    ["Discussion", "Advice", "Question", "Recipe", "Tip"],
    "general":      ["Serious", "Fun", "Meta"],
}


# ── Schema builders ────────────────────────────────────────────────────────────

def _karma_score(account_type: str) -> int:
    """Upvote/downvote net score by account type."""
    if account_type in ("BRAND", "INFLUENCER"):
        return max(1, int(random.expovariate(0.0005)))
    return max(0, int(random.expovariate(0.01)))


def _build_post(user: User, subreddit: Dict) -> Dict:
    """Build a Reddit post record."""
    post_type = random.choices(
        POST_TYPES,
        weights=[0.45, 0.25, 0.20, 0.07, 0.03],
    )[0]

    category = subreddit["category"]
    flair = random.choice(FLAIR_TAGS.get(category, ["Discussion"]))
    hashtags = random.sample(CONFIG.all_hashtags, k=random.randint(0, 3))

    # Title — more serious for news/tech, casual for general
    title_templates = [
        fake.sentence(nb_words=random.randint(6, 15)).rstrip("."),
        f"[{flair}] {fake.catch_phrase()}",
        f"Anyone else noticed {fake.bs()}?",
        f"My experience with {fake.company()} — {fake.sentence(nb_words=5)}",
    ]
    title = random.choice(title_templates)

    score = _karma_score(user.account_type)
    upvote_ratio = random.uniform(0.5, 0.99)
    upvotes = int(score / upvote_ratio) if upvote_ratio > 0 else score
    downvotes = upvotes - score
    num_comments = max(0, int(score * random.uniform(0.01, 0.3)))

    posted_at = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, 365)
    )

    return {
        "post_id":           str(uuid.uuid4()),
        "author_user_id":    user.user_id,
        "author_username":   user.username,
        "subreddit":         subreddit["name"],
        "subreddit_category": category,
        "platform":          "reddit",

        # Content
        "title":             title,
        "body":              fake.paragraph(nb_sentences=random.randint(1, 8)) if post_type == "text" else None,
        "url":               fake.url() if post_type == "link" else None,
        "post_type":         post_type,
        "flair":             flair,
        "tags":              hashtags,
        "is_nsfw":           False,
        "is_spoiler":        random.random() < 0.03,
        "is_oc":             random.random() < 0.15,    # Original Content
        "is_crosspost":      random.random() < 0.10,

        # Engagement metrics
        "score":             score,
        "upvotes":           upvotes,
        "downvotes":         max(0, downvotes),
        "upvote_ratio":      round(upvote_ratio, 4),
        "num_comments":      num_comments,
        "num_awards":        random.choices([0, 1, 2, 3, 5], weights=[0.7, 0.15, 0.08, 0.05, 0.02])[0],
        "is_pinned":         random.random() < 0.01,

        # Author snapshot
        "author_karma":      random.randint(100, 500_000),
        "author_account_age_days": random.randint(30, 3650),

        # Timestamps
        "posted_at":         posted_at.isoformat(),
        "ingested_at":       datetime.now(timezone.utc).isoformat(),

        # Pipeline metadata
        "source_system":     "reddit_simulator_v1",
        "schema_version":    "1.0",
    }


def _build_comment(
    post: Dict,
    user: User,
    depth: int = 0,
    parent_comment_id: Optional[str] = None,
) -> Dict:
    """Build a Reddit comment (supports nested replies up to depth 3)."""
    posted_at = datetime.fromisoformat(post["posted_at"])
    max_offset = (datetime.now(timezone.utc) - posted_at).total_seconds()
    offset = random.uniform(0, min(max_offset, 86400 * 7))
    comment_ts = posted_at + timedelta(seconds=offset)

    score = max(0, int(random.expovariate(0.05)))

    return {
        "comment_id":        str(uuid.uuid4()),
        "post_id":           post["post_id"],
        "parent_comment_id": parent_comment_id,
        "author_user_id":    user.user_id,
        "author_username":   user.username,
        "subreddit":         post["subreddit"],
        "platform":          "reddit",

        # Content
        "body":              fake.paragraph(nb_sentences=random.randint(1, 5)),
        "depth":             depth,
        "is_edited":         random.random() < 0.10,
        "is_deleted":        random.random() < 0.02,
        "is_mod_comment":    random.random() < 0.01,
        "contains_url":      random.random() < 0.15,

        # Engagement
        "score":             score,
        "upvotes":           score + random.randint(0, max(1, score // 5)),
        "downvotes":         random.randint(0, max(1, score // 10)),
        "num_awards":        random.choices([0, 1, 2], weights=[0.9, 0.08, 0.02])[0],

        # Timestamps
        "posted_at":         comment_ts.isoformat(),
        "ingested_at":       datetime.now(timezone.utc).isoformat(),

        # Pipeline metadata
        "source_system":     "reddit_simulator_v1",
        "schema_version":    "1.0",
    }


def _build_comment_thread(
    post: Dict,
    users: List[User],
    num_top_level: int = 5,
    max_depth: int = 3,
) -> List[Dict]:
    """Build a realistic threaded comment structure for a post."""
    all_comments: List[Dict] = []

    for _ in range(num_top_level):
        top = _build_comment(post, random.choice(users), depth=0)
        all_comments.append(top)

        # Replies to top-level (depth 1–3)
        if random.random() < 0.6:
            num_replies = random.randint(1, 4)
            parent_id = top["comment_id"]
            for d in range(1, min(max_depth + 1, num_replies + 1)):
                reply = _build_comment(
                    post, random.choice(users),
                    depth=d, parent_comment_id=parent_id
                )
                all_comments.append(reply)
                parent_id = reply["comment_id"]
                if random.random() > 0.4:
                    break

    return all_comments


# ── Main simulator ─────────────────────────────────────────────────────────────

def run(
    num_posts: int = 2000,
    comments_per_post: int = 8,
    dry_run: bool = False,
) -> None:
    """
    Run the Reddit simulator.

    Args:
        num_posts:          Number of posts to generate.
        comments_per_post:  Average comments per post.
        dry_run:            Print stats without uploading to MinIO.
    """
    logger.info("Initializing Reddit Simulator...")

    try:
        users = load_user_pool()
    except FileNotFoundError:
        users = generate_user_pool()

    minio = None if dry_run else create_minio_client()
    run_id = str(uuid.uuid4())[:8]

    all_posts = []
    all_comments = []

    logger.info(f"Generating {num_posts} posts...")

    for i in range(num_posts):
        user = random.choice(users)
        subreddit = random.choice(SUBREDDITS)
        post = _build_post(user, subreddit)
        all_posts.append(post)

        # Generate threaded comments
        n_top = max(0, int(random.gauss(comments_per_post, comments_per_post / 2)))
        thread = _build_comment_thread(post, users, num_top_level=n_top)
        all_comments.extend(thread)

        if (i + 1) % 500 == 0:
            logger.info(f"Generated {i+1}/{num_posts} posts | {len(all_comments)} comments so far")

    logger.info(
        f"Generated: {len(all_posts)} posts | {len(all_comments)} comments"
    )

    if dry_run:
        logger.info("Dry run — skipping MinIO upload.")
        logger.info(f"Sample post:\n{json.dumps(all_posts[0], indent=2)}")
        return

    # ── Upload to MinIO ─────────────────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    posts_key = build_landing_key("reddit/posts", f"posts_{ts}_{run_id}.json")
    upload_to_minio(
        minio, CONFIG.bucket_landing, posts_key,
        json.dumps(all_posts, default=str), "application/json"
    )

    # Comments in chunks
    chunk_size = 10_000
    for idx, start in enumerate(range(0, len(all_comments), chunk_size)):
        chunk = all_comments[start:start + chunk_size]
        key = build_landing_key("reddit/comments", f"comments_{ts}_{run_id}_{idx:03d}.json")
        upload_to_minio(
            minio, CONFIG.bucket_landing, key,
            json.dumps(chunk, default=str), "application/json"
        )

    # Subreddit metadata (static reference)
    meta_key = build_landing_key("reddit/metadata", f"subreddits_{ts}.json")
    upload_to_minio(
        minio, CONFIG.bucket_landing, meta_key,
        json.dumps(SUBREDDITS, default=str), "application/json"
    )

    logger.info(
        f"Reddit simulator complete: "
        f"{len(all_posts)} posts, {len(all_comments)} comments uploaded to MinIO."
    )


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Batch Data Simulator")
    parser.add_argument("--posts",              type=int, default=2000, help="Number of posts")
    parser.add_argument("--comments-per-post",  type=int, default=8,    help="Avg comments per post")
    parser.add_argument("--dry-run",            action="store_true",    help="Print without uploading")
    args = parser.parse_args()

    run(
        num_posts=args.posts,
        comments_per_post=args.comments_per_post,
        dry_run=args.dry_run,
    )
