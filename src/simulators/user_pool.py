"""
user_pool.py — Generates and caches a realistic pool of users and brands.

All simulators share the same user pool to ensure referential consistency
(e.g., the same user_id appears across tweets, engagements, and YouTube).
"""

import json
import random
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from faker import Faker
from loguru import logger

from config import CONFIG

fake = Faker()
Faker.seed(42)          # Reproducible pool across simulator runs
random.seed(42)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class User:
    user_id: str
    username: str
    display_name: str
    platform: str
    account_type: str           # USER | BRAND | INFLUENCER
    is_brand: bool
    is_verified: bool
    follower_count: int
    following_count: int
    post_count: int
    bio: str
    location: str
    created_at: str             # ISO 8601 UTC
    profile_url: str


# ── Generator ─────────────────────────────────────────────────────────────────

def _random_follower_count(account_type: str) -> int:
    """Realistic follower distributions by account type."""
    if account_type == "BRAND":
        return random.randint(10_000, 2_000_000)
    elif account_type == "INFLUENCER":
        return random.randint(5_000, 500_000)
    else:
        # Power-law distribution — most users have few followers
        base = random.expovariate(0.0003)
        return max(0, min(int(base), 50_000))


def _created_at() -> str:
    """Random account creation date in the last 5 years."""
    days_ago = random.randint(30, 5 * 365)
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.isoformat()


def generate_user_pool(
    num_users: int = CONFIG.num_users,
    num_brands: int = CONFIG.num_brands,
    platforms: Optional[List[str]] = None,
) -> List[User]:
    """
    Generate a mixed pool of regular users, brands, and influencers.

    Args:
        num_users:  Total number of regular users to generate.
        num_brands: Number of brand accounts (subset of total).
        platforms:  Platforms to distribute users across.

    Returns:
        List of User dataclass instances.
    """
    if platforms is None:
        platforms = CONFIG.platforms

    users: List[User] = []

    # ── Brand accounts ────────────────────────────────────────────────────
    brand_names = [fake.company() for _ in range(num_brands)]
    for brand_name in brand_names:
        platform = random.choice(platforms)
        slug = brand_name.lower().replace(" ", "").replace(",", "")[:20]
        users.append(User(
            user_id=str(uuid.uuid4()),
            username=f"{slug}_official",
            display_name=brand_name,
            platform=platform,
            account_type="BRAND",
            is_brand=True,
            is_verified=True,
            follower_count=_random_follower_count("BRAND"),
            following_count=random.randint(100, 2000),
            post_count=random.randint(500, 10000),
            bio=fake.catch_phrase(),
            location=fake.city(),
            created_at=_created_at(),
            profile_url=f"https://{platform}.com/{slug}_official",
        ))

    # ── Influencer accounts ────────────────────────────────────────────────
    num_influencers = max(10, num_users // 100)
    for _ in range(num_influencers):
        platform = random.choice(platforms)
        username = fake.user_name()
        users.append(User(
            user_id=str(uuid.uuid4()),
            username=username,
            display_name=fake.name(),
            platform=platform,
            account_type="INFLUENCER",
            is_brand=False,
            is_verified=random.random() < 0.4,
            follower_count=_random_follower_count("INFLUENCER"),
            following_count=random.randint(200, 5000),
            post_count=random.randint(200, 5000),
            bio=fake.sentence(nb_words=10),
            location=fake.city(),
            created_at=_created_at(),
            profile_url=f"https://{platform}.com/{username}",
        ))

    # ── Regular users ──────────────────────────────────────────────────────
    remaining = num_users - num_brands - num_influencers
    for _ in range(remaining):
        platform = random.choice(platforms)
        username = fake.user_name()
        users.append(User(
            user_id=str(uuid.uuid4()),
            username=username,
            display_name=fake.name(),
            platform=platform,
            account_type="USER",
            is_brand=False,
            is_verified=False,
            follower_count=_random_follower_count("USER"),
            following_count=random.randint(10, 500),
            post_count=random.randint(0, 500),
            bio=fake.sentence(nb_words=8) if random.random() < 0.6 else "",
            location=fake.city() if random.random() < 0.7 else "",
            created_at=_created_at(),
            profile_url=f"https://{platform}.com/{username}",
        ))

    logger.info(
        f"Generated user pool: {num_brands} brands, "
        f"{num_influencers} influencers, {remaining} users "
        f"(total: {len(users)})"
    )
    return users


def save_user_pool(users: List[User], path: str = "/tmp/user_pool.json") -> None:
    """Persist user pool to JSON for reuse across simulator runs."""
    with open(path, "w") as f:
        json.dump([asdict(u) for u in users], f, indent=2)
    logger.info(f"User pool saved to {path}")


def load_user_pool(path: str = "/tmp/user_pool.json") -> List[User]:
    """Load previously generated user pool."""
    with open(path) as f:
        data = json.load(f)
    users = [User(**u) for u in data]
    logger.info(f"Loaded {len(users)} users from {path}")
    return users
