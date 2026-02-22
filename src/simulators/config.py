"""
config.py — Shared configuration and constants for all simulators.
Loads from .env file or environment variables.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SimulatorConfig:
    # ── Kafka ──────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic_tweets: str            = os.getenv("KAFKA_TOPIC_TWEETS",       "social.tweets")
    topic_engagements: str       = os.getenv("KAFKA_TOPIC_ENGAGEMENTS",  "social.engagements")
    topic_youtube: str           = os.getenv("KAFKA_TOPIC_YOUTUBE",      "social.youtube")
    topic_reddit: str            = os.getenv("KAFKA_TOPIC_REDDIT",       "social.reddit")
    topic_dlq: str               = os.getenv("KAFKA_TOPIC_DLQ",          "social.events.dlq")

    # ── MinIO / S3 ─────────────────────────────────────────────────────────
    minio_endpoint: str          = os.getenv("MINIO_ENDPOINT",           "http://localhost:9000")
    minio_access_key: str        = os.getenv("MINIO_ROOT_USER",          "minioadmin")
    minio_secret_key: str        = os.getenv("MINIO_ROOT_PASSWORD",      "minioadmin")
    bucket_landing: str          = os.getenv("BUCKET_LANDING",           "landing-zone")

    # ── Scale ──────────────────────────────────────────────────────────────
    num_users: int               = int(os.getenv("SIMULATOR_NUM_USERS",   "10000"))
    num_brands: int              = int(os.getenv("SIMULATOR_NUM_BRANDS",  "50"))
    tweet_rate: int              = int(os.getenv("SIMULATOR_TWEET_RATE",  "100"))
    engagement_ratio: float      = float(os.getenv("SIMULATOR_ENGAGEMENT_RATIO", "0.05"))

    # ── Platforms ──────────────────────────────────────────────────────────
    platforms: list = field(default_factory=lambda: ["twitter", "youtube", "reddit"])

    # ── Hashtag categories ─────────────────────────────────────────────────
    hashtag_categories: dict = field(default_factory=lambda: {
        "tech":     ["#AI", "#MachineLearning", "#DataEngineering", "#Python", "#Spark",
                     "#Kafka", "#CloudComputing", "#DevOps", "#OpenSource", "#BigData"],
        "business": ["#Marketing", "#Startup", "#Entrepreneurship", "#Growth",
                     "#Leadership", "#Innovation", "#B2B", "#SaaS", "#ProductLaunch"],
        "lifestyle": ["#Travel", "#Food", "#Fitness", "#Wellness", "#Photography",
                      "#Fashion", "#Music", "#Gaming", "#Sports", "#Movies"],
        "news":     ["#BreakingNews", "#Politics", "#Economy", "#Climate",
                     "#Technology", "#Science", "#Health", "#Education"],
    })

    @property
    def all_hashtags(self) -> list:
        tags = []
        for lst in self.hashtag_categories.values():
            tags.extend(lst)
        return tags


# Singleton config instance
CONFIG = SimulatorConfig()
