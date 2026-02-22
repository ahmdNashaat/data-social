"""
quality/generate_expectations.py — Generate all Great Expectations suites.

Generates expectation suite JSON files for Bronze, Silver, and Gold layers.
Run this script once to create/update all expectation files:

    python quality/generate_expectations.py

Design philosophy:
  - Bronze: PERMISSIVE — catch schema drift and missing PKs only
  - Silver: STRICT    — enforce types, ranges, uniqueness, completeness
  - Gold:   BUSINESS  — validate metric ranges and business rule compliance
"""

import json
import os
from pathlib import Path

EXPECTATIONS_DIR = Path(__file__).parent / "expectations"
EXPECTATIONS_DIR.mkdir(exist_ok=True)


def write_suite(suite: dict, filename: str) -> None:
    path = EXPECTATIONS_DIR / filename
    with open(path, "w") as f:
        json.dump(suite, f, indent=2)
    print(f"  Written: {filename}")


def exp(expectation_type: str, **kwargs) -> dict:
    """Helper to build an expectation dict."""
    notes = kwargs.pop("notes", "")
    return {
        "expectation_type": expectation_type,
        "kwargs": kwargs,
        "meta": {"notes": notes} if notes else {},
    }


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE SUITES — Permissive
# ══════════════════════════════════════════════════════════════════════════════

def build_bronze_tweets() -> dict:
    return {
        "expectation_suite_name": "bronze.tweets",
        "expectations": [
            # Row count
            exp("expect_table_row_count_to_be_between",
                min_value=1, max_value=10_000_000,
                notes="At least 1 record per batch"),

            # Schema — required columns exist
            exp("expect_column_to_exist", column="tweet_id"),
            exp("expect_column_to_exist", column="user_id"),
            exp("expect_column_to_exist", column="platform"),
            exp("expect_column_to_exist", column="created_at"),
            exp("expect_column_to_exist", column="ingested_at"),
            exp("expect_column_to_exist", column="batch_id"),

            # Null checks (primary keys)
            exp("expect_column_values_to_not_be_null",
                column="tweet_id", mostly=1.0,
                notes="PK must never be null"),
            exp("expect_column_values_to_not_be_null",
                column="user_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="platform", mostly=0.99),
            exp("expect_column_values_to_not_be_null",
                column="ingested_at", mostly=1.0),

            # Platform values (permissive — accepts mixed case)
            exp("expect_column_values_to_be_in_set",
                column="platform",
                value_set=["twitter", "Twitter", "TWITTER"],
                mostly=0.99,
                notes="Bronze accepts mixed case"),

            # Metric ranges (no negatives)
            exp("expect_column_values_to_be_between",
                column="likes_count", min_value=0, max_value=100_000_000, mostly=0.99),
            exp("expect_column_values_to_be_between",
                column="retweets_count", min_value=0, max_value=50_000_000, mostly=0.99),

            # UUID format
            exp("expect_column_values_to_match_regex",
                column="tweet_id",
                regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                mostly=0.99),
        ],
        "meta": {
            "layer": "bronze", "table": "tweets",
            "blocking": False,
            "description": "Permissive Bronze checks. Catches schema drift and missing PKs.",
        },
    }


def build_bronze_engagements() -> dict:
    return {
        "expectation_suite_name": "bronze.engagements",
        "expectations": [
            exp("expect_table_row_count_to_be_between",
                min_value=0, max_value=50_000_000,
                notes="Can be 0 if no interactions occurred"),

            exp("expect_column_to_exist", column="engagement_id"),
            exp("expect_column_to_exist", column="post_id"),
            exp("expect_column_to_exist", column="engaging_user_id"),
            exp("expect_column_to_exist", column="engagement_type"),
            exp("expect_column_to_exist", column="platform"),

            exp("expect_column_values_to_not_be_null",
                column="engagement_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="post_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="engagement_type", mostly=0.99),

            exp("expect_column_values_to_be_in_set",
                column="engagement_type",
                value_set=["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW",
                           "like", "retweet", "reply", "quote", "view"],
                mostly=0.95,
                notes="Bronze accepts mixed case — Silver normalizes"),

            exp("expect_column_values_to_be_in_set",
                column="platform",
                value_set=["twitter", "youtube", "reddit",
                           "Twitter", "YouTube", "Reddit"],
                mostly=0.99),
        ],
        "meta": {"layer": "bronze", "table": "engagements", "blocking": False},
    }


# ══════════════════════════════════════════════════════════════════════════════
# SILVER SUITES — Strict + Blocking
# ══════════════════════════════════════════════════════════════════════════════

def build_silver_tweets() -> dict:
    return {
        "expectation_suite_name": "silver.tweets",
        "expectations": [
            exp("expect_table_row_count_to_be_between",
                min_value=1, max_value=10_000_000,
                notes="Empty Silver = ingestion failure"),

            # Required columns present
            exp("expect_column_to_exist", column="tweet_id"),
            exp("expect_column_to_exist", column="user_id"),
            exp("expect_column_to_exist", column="platform"),
            exp("expect_column_to_exist", column="content"),
            exp("expect_column_to_exist", column="created_at"),
            exp("expect_column_to_exist", column="ingested_at"),
            exp("expect_column_to_exist", column="silver_batch_id"),
            exp("expect_column_to_exist", column="engagement_total"),

            # Null checks — strict
            exp("expect_column_values_to_not_be_null",
                column="tweet_id", mostly=1.0,
                notes="CRITICAL: PK must never be null"),
            exp("expect_column_values_to_not_be_null",
                column="user_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="platform", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="created_at", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="silver_batch_id", mostly=1.0),

            # Uniqueness — dedup verification
            exp("expect_column_values_to_be_unique",
                column="tweet_id", mostly=0.999,
                notes="Dedup rate must be < 0.1%"),
            exp("expect_column_proportion_of_unique_values_to_be_between",
                column="tweet_id", min_value=0.999, max_value=1.0),

            # Platform — must be normalized to lowercase
            exp("expect_column_values_to_be_in_set",
                column="platform",
                value_set=["twitter"],
                mostly=1.0,
                notes="Silver must be lowercase normalized"),

            # Account type — must be UPPER
            exp("expect_column_values_to_be_in_set",
                column="user_account_type",
                value_set=["USER", "BRAND", "INFLUENCER"],
                mostly=0.99,
                notes="Silver must have UPPER normalized account types"),

            # Metric ranges — no negatives in Silver
            exp("expect_column_values_to_be_between",
                column="likes_count", min_value=0, max_value=100_000_000, mostly=1.0),
            exp("expect_column_values_to_be_between",
                column="retweets_count", min_value=0, max_value=50_000_000, mostly=1.0),
            exp("expect_column_values_to_be_between",
                column="impressions_count", min_value=0, max_value=1_000_000_000, mostly=1.0),
            exp("expect_column_values_to_be_between",
                column="engagement_total", min_value=0, max_value=200_000_000, mostly=1.0),
            exp("expect_column_values_to_be_between",
                column="content_length", min_value=0, max_value=280, mostly=0.95,
                notes="Twitter max = 280 chars"),

            # UUID format
            exp("expect_column_values_to_match_regex",
                column="tweet_id",
                regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                mostly=1.0),
            exp("expect_column_values_to_match_regex",
                column="silver_batch_id",
                regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                mostly=1.0),
        ],
        "meta": {
            "layer": "silver", "table": "tweets",
            "blocking": True,
            "description": "BLOCKING Silver gate. Strict null/unique/range/format checks.",
        },
    }


def build_silver_engagements() -> dict:
    return {
        "expectation_suite_name": "silver.engagements",
        "expectations": [
            exp("expect_table_row_count_to_be_between",
                min_value=0, max_value=50_000_000),

            exp("expect_column_to_exist", column="engagement_id"),
            exp("expect_column_to_exist", column="post_id"),
            exp("expect_column_to_exist", column="engaging_user_id"),
            exp("expect_column_to_exist", column="engagement_type"),
            exp("expect_column_to_exist", column="engagement_value"),
            exp("expect_column_to_exist", column="engaged_at"),

            exp("expect_column_values_to_not_be_null",
                column="engagement_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="post_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="engaging_user_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="engagement_type", mostly=1.0),

            exp("expect_column_values_to_be_unique",
                column="engagement_id", mostly=0.999),

            # Silver: UPPER normalized, with UNKNOWN as catch-all
            exp("expect_column_values_to_be_in_set",
                column="engagement_type",
                value_set=["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW", "UNKNOWN"],
                mostly=1.0,
                notes="UNKNOWN is valid — assigned to unrecognized types"),

            exp("expect_column_values_to_be_between",
                column="engagement_value", min_value=0.0, max_value=10.0, mostly=1.0,
                notes="Weighted scores range 0.1 (VIEW) to 4.0 (QUOTE)"),

            exp("expect_column_values_to_be_in_set",
                column="platform",
                value_set=["twitter", "youtube", "reddit"],
                mostly=1.0,
                notes="Silver must be lowercase"),
        ],
        "meta": {
            "layer": "silver", "table": "engagements", "blocking": True,
        },
    }


def build_silver_posts() -> dict:
    return {
        "expectation_suite_name": "silver.posts",
        "expectations": [
            exp("expect_table_row_count_to_be_between",
                min_value=1, max_value=50_000_000),

            exp("expect_column_to_exist", column="post_id"),
            exp("expect_column_to_exist", column="user_id"),
            exp("expect_column_to_exist", column="platform"),
            exp("expect_column_to_exist", column="posted_at"),

            exp("expect_column_values_to_not_be_null",
                column="post_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="user_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="platform", mostly=1.0),

            exp("expect_column_values_to_be_in_set",
                column="platform",
                value_set=["twitter", "youtube", "reddit"],
                mostly=1.0,
                notes="Unified posts table covers all 3 platforms"),

            exp("expect_column_values_to_be_between",
                column="likes_count", min_value=0, max_value=100_000_000, mostly=1.0),
            exp("expect_column_values_to_be_between",
                column="shares_count", min_value=0, max_value=50_000_000, mostly=1.0),
            exp("expect_column_values_to_be_between",
                column="comments_count", min_value=0, max_value=10_000_000, mostly=1.0),
        ],
        "meta": {
            "layer": "silver", "table": "posts", "blocking": True,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# GOLD SUITES — Business rules
# ══════════════════════════════════════════════════════════════════════════════

def build_gold_fact_engagements() -> dict:
    return {
        "expectation_suite_name": "gold.fact_engagements",
        "expectations": [
            exp("expect_table_row_count_to_be_between",
                min_value=1, max_value=100_000_000,
                notes="Gold must have engagement records"),

            exp("expect_column_to_exist", column="engagement_sk"),
            exp("expect_column_to_exist", column="engagement_id"),
            exp("expect_column_to_exist", column="post_id"),
            exp("expect_column_to_exist", column="engaging_user_id"),
            exp("expect_column_to_exist", column="platform"),
            exp("expect_column_to_exist", column="date_sk"),
            exp("expect_column_to_exist", column="engagement_type"),
            exp("expect_column_to_exist", column="engagement_value"),
            exp("expect_column_to_exist", column="gold_loaded_at"),

            # No null foreign keys in Gold
            exp("expect_column_values_to_not_be_null",
                column="engagement_sk", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="engagement_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="post_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="platform", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="date_sk", mostly=1.0,
                notes="All facts must join to dim_date"),

            # Surrogate key uniqueness
            exp("expect_column_values_to_be_unique",
                column="engagement_sk", mostly=0.9999,
                notes="SK uniqueness — fails if surrogate key collision"),

            # Business rules
            exp("expect_column_values_to_be_in_set",
                column="platform",
                value_set=["twitter", "youtube", "reddit"],
                mostly=1.0),

            exp("expect_column_values_to_be_in_set",
                column="engagement_type",
                value_set=["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW", "UNKNOWN"],
                mostly=1.0),

            exp("expect_column_values_to_be_between",
                column="engagement_value", min_value=0.0, max_value=10.0, mostly=1.0),

            # Date SK must be a valid YYYYMMDD integer
            exp("expect_column_values_to_be_between",
                column="date_sk",
                min_value=20200101,
                max_value=20301231,
                mostly=1.0,
                notes="date_sk must be valid YYYYMMDD in range 2020-2030"),
        ],
        "meta": {
            "layer": "gold", "table": "fact_engagements",
            "blocking": False,
            "description": "Alert-only Gold validation. Business rule checks on fact table.",
        },
    }


def build_gold_fact_post_metrics() -> dict:
    return {
        "expectation_suite_name": "gold.fact_post_metrics",
        "expectations": [
            exp("expect_table_row_count_to_be_between",
                min_value=1, max_value=100_000_000),

            exp("expect_column_to_exist", column="metric_sk"),
            exp("expect_column_to_exist", column="post_id"),
            exp("expect_column_to_exist", column="platform"),
            exp("expect_column_to_exist", column="date_sk"),
            exp("expect_column_to_exist", column="engagement_rate_pct"),
            exp("expect_column_to_exist", column="engagement_tier"),

            exp("expect_column_values_to_not_be_null",
                column="metric_sk", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="post_id", mostly=1.0),
            exp("expect_column_values_to_not_be_null",
                column="date_sk", mostly=1.0),

            exp("expect_column_values_to_be_unique",
                column="metric_sk", mostly=0.9999),

            # Business rule: engagement_rate must be 0-100%
            exp("expect_column_values_to_be_between",
                column="engagement_rate_pct",
                min_value=0.0,
                max_value=100.0,
                mostly=0.99,
                notes="Most posts have rate 0-100%, some viral posts exceed 100%"),

            exp("expect_column_values_to_be_in_set",
                column="engagement_tier",
                value_set=["viral", "high", "medium", "low"],
                mostly=1.0),

            exp("expect_column_values_to_be_between",
                column="date_sk", min_value=20200101, max_value=20301231, mostly=1.0),
        ],
        "meta": {
            "layer": "gold", "table": "fact_post_metrics", "blocking": False,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("Generating Great Expectations suites...\n")

    suites = {
        "bronze.tweets.json":           build_bronze_tweets(),
        "bronze.engagements.json":      build_bronze_engagements(),
        "silver.tweets.json":           build_silver_tweets(),
        "silver.engagements.json":      build_silver_engagements(),
        "silver.posts.json":            build_silver_posts(),
        "gold.fact_engagements.json":   build_gold_fact_engagements(),
        "gold.fact_post_metrics.json":  build_gold_fact_post_metrics(),
    }

    for filename, suite in suites.items():
        write_suite(suite, filename)

    print(f"\nGenerated {len(suites)} expectation suites in {EXPECTATIONS_DIR}")
    print("\nLayer summary:")
    print("  Bronze (2 suites) — NON-BLOCKING: schema drift + null PKs")
    print("  Silver (3 suites) — BLOCKING:     strict null/unique/range/format")
    print("  Gold   (2 suites) — ALERT-ONLY:   business rules + metric ranges")


if __name__ == "__main__":
    main()
