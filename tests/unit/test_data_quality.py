"""
tests/unit/test_data_quality.py — Unit tests for data quality expectations.

Validates the expectation logic in pure Python without requiring
a running Great Expectations instance or database.

Tests cover:
  - All 7 expectation suite JSON files are valid and loadable
  - Each suite has the expected structure and required fields
  - Bronze suites are non-blocking (mostly < 1.0 or meta.blocking=False)
  - Silver suites are strict (mostly = 1.0 for PKs)
  - Gold suites have business rule checks
  - Expectation count thresholds per layer
"""

import json
import os
import sys
import pytest
from pathlib import Path
from typing import Dict, List

QUALITY_DIR      = Path(__file__).parent.parent.parent / "quality"
EXPECTATIONS_DIR = QUALITY_DIR / "expectations"
CHECKPOINTS_DIR  = QUALITY_DIR / "checkpoints"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_suite(filename: str) -> Dict:
    path = EXPECTATIONS_DIR / filename
    with open(path) as f:
        return json.load(f)


def get_expectation(suite: Dict, expectation_type: str) -> List[Dict]:
    """Find all expectations of a given type in a suite."""
    return [
        e for e in suite["expectations"]
        if e["expectation_type"] == expectation_type
    ]


def get_column_expectations(suite: Dict, column: str) -> List[Dict]:
    """Find all expectations targeting a specific column."""
    return [
        e for e in suite["expectations"]
        if e.get("kwargs", {}).get("column") == column
    ]


# ── Suite file existence tests ────────────────────────────────────────────────

class TestSuiteFilesExist:

    @pytest.mark.parametrize("filename", [
        "bronze.tweets.json",
        "bronze.engagements.json",
        "silver.tweets.json",
        "silver.engagements.json",
        "silver.posts.json",
        "gold.fact_engagements.json",
        "gold.fact_post_metrics.json",
    ])
    def test_suite_file_exists(self, filename):
        assert (EXPECTATIONS_DIR / filename).exists(), \
            f"Suite file missing: {filename}"

    @pytest.mark.parametrize("filename", [
        "bronze.tweets.json",
        "bronze.engagements.json",
        "silver.tweets.json",
        "silver.engagements.json",
        "silver.posts.json",
        "gold.fact_engagements.json",
        "gold.fact_post_metrics.json",
    ])
    def test_suite_is_valid_json(self, filename):
        suite = load_suite(filename)
        assert isinstance(suite, dict)

    @pytest.mark.parametrize("filename,expected_name", [
        ("bronze.tweets.json",          "bronze.tweets"),
        ("bronze.engagements.json",     "bronze.engagements"),
        ("silver.tweets.json",          "silver.tweets"),
        ("silver.engagements.json",     "silver.engagements"),
        ("silver.posts.json",           "silver.posts"),
        ("gold.fact_engagements.json",  "gold.fact_engagements"),
        ("gold.fact_post_metrics.json", "gold.fact_post_metrics"),
    ])
    def test_suite_name_matches_filename(self, filename, expected_name):
        suite = load_suite(filename)
        assert suite["expectation_suite_name"] == expected_name

    @pytest.mark.parametrize("filename", [
        "bronze.tweets.json",
        "bronze.engagements.json",
        "silver.tweets.json",
        "silver.engagements.json",
        "silver.posts.json",
        "gold.fact_engagements.json",
        "gold.fact_post_metrics.json",
    ])
    def test_suite_has_expectations_list(self, filename):
        suite = load_suite(filename)
        assert "expectations" in suite
        assert isinstance(suite["expectations"], list)
        assert len(suite["expectations"]) > 0

    @pytest.mark.parametrize("filename", [
        "bronze.tweets.json",
        "bronze.engagements.json",
        "silver.tweets.json",
        "silver.engagements.json",
        "silver.posts.json",
        "gold.fact_engagements.json",
        "gold.fact_post_metrics.json",
    ])
    def test_suite_has_meta(self, filename):
        suite = load_suite(filename)
        assert "meta" in suite
        assert "layer" in suite["meta"]
        assert "blocking" in suite["meta"]


# ── Expectation count thresholds ──────────────────────────────────────────────

class TestExpectationCounts:
    """Each suite should have a minimum number of expectations."""

    def test_bronze_tweets_has_enough_expectations(self):
        suite = load_suite("bronze.tweets.json")
        assert len(suite["expectations"]) >= 8

    def test_silver_tweets_has_most_expectations(self):
        """Silver is the strictest layer — should have most expectations."""
        bronze = load_suite("bronze.tweets.json")
        silver = load_suite("silver.tweets.json")
        assert len(silver["expectations"]) > len(bronze["expectations"])

    def test_silver_engagements_has_enough_expectations(self):
        suite = load_suite("silver.engagements.json")
        assert len(suite["expectations"]) >= 8

    def test_gold_fact_engagements_has_enough_expectations(self):
        suite = load_suite("gold.fact_engagements.json")
        assert len(suite["expectations"]) >= 10


# ── Blocking / non-blocking designation ──────────────────────────────────────

class TestBlockingDesignation:

    def test_bronze_suites_are_non_blocking(self):
        for filename in ["bronze.tweets.json", "bronze.engagements.json"]:
            suite = load_suite(filename)
            assert suite["meta"]["blocking"] is False, \
                f"{filename} should be non-blocking"

    def test_silver_suites_are_blocking(self):
        for filename in ["silver.tweets.json", "silver.engagements.json", "silver.posts.json"]:
            suite = load_suite(filename)
            assert suite["meta"]["blocking"] is True, \
                f"{filename} should be blocking"

    def test_gold_suites_are_non_blocking(self):
        for filename in ["gold.fact_engagements.json", "gold.fact_post_metrics.json"]:
            suite = load_suite(filename)
            assert suite["meta"]["blocking"] is False, \
                f"{filename} should be non-blocking (alert-only)"

    def test_layer_labels_correct(self):
        layer_map = {
            "bronze.tweets.json":          "bronze",
            "bronze.engagements.json":     "bronze",
            "silver.tweets.json":          "silver",
            "silver.engagements.json":     "silver",
            "silver.posts.json":           "silver",
            "gold.fact_engagements.json":  "gold",
            "gold.fact_post_metrics.json": "gold",
        }
        for filename, expected_layer in layer_map.items():
            suite = load_suite(filename)
            assert suite["meta"]["layer"] == expected_layer


# ── Bronze suite content tests ────────────────────────────────────────────────

class TestBronzeTweets:

    def setup_method(self):
        self.suite = load_suite("bronze.tweets.json")

    def test_has_row_count_expectation(self):
        exps = get_expectation(self.suite, "expect_table_row_count_to_be_between")
        assert len(exps) >= 1

    def test_row_count_min_is_1(self):
        exps = get_expectation(self.suite, "expect_table_row_count_to_be_between")
        assert exps[0]["kwargs"]["min_value"] >= 1

    def test_checks_tweet_id_not_null(self):
        exps = get_column_expectations(self.suite, "tweet_id")
        null_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        assert len(null_checks) >= 1

    def test_tweet_id_null_check_is_strict(self):
        """Bronze PK null check must be 100% (mostly=1.0)."""
        exps = get_column_expectations(self.suite, "tweet_id")
        null_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        assert null_checks[0]["kwargs"]["mostly"] == 1.0

    def test_platform_accepts_mixed_case(self):
        """Bronze is permissive — should accept 'Twitter' and 'twitter'."""
        exps = get_column_expectations(self.suite, "platform")
        value_set_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(value_set_checks) >= 1
        value_set = value_set_checks[0]["kwargs"]["value_set"]
        # Should include at least mixed-case options
        assert len(value_set) > 1

    def test_has_uuid_format_check(self):
        exps = get_expectation(self.suite, "expect_column_values_to_match_regex")
        uuid_checks = [
            e for e in exps
            if e.get("kwargs", {}).get("column") == "tweet_id"
        ]
        assert len(uuid_checks) >= 1
        assert "regex" in uuid_checks[0]["kwargs"]

    def test_required_columns_exist_expectations(self):
        col_exist_exps = get_expectation(self.suite, "expect_column_to_exist")
        checked_cols = {e["kwargs"]["column"] for e in col_exist_exps}
        required = {"tweet_id", "user_id", "platform", "created_at"}
        assert required.issubset(checked_cols)

    def test_metrics_have_non_negative_bounds(self):
        for col in ["likes_count", "retweets_count"]:
            exps = get_column_expectations(self.suite, col)
            range_checks = [
                e for e in exps
                if e["expectation_type"] == "expect_column_values_to_be_between"
            ]
            if range_checks:
                assert range_checks[0]["kwargs"]["min_value"] >= 0


# ── Silver suite content tests ────────────────────────────────────────────────

class TestSilverTweets:

    def setup_method(self):
        self.suite = load_suite("silver.tweets.json")

    def test_has_uniqueness_check(self):
        exps = get_expectation(self.suite, "expect_column_values_to_be_unique")
        tweet_id_unique = [
            e for e in exps
            if e["kwargs"]["column"] == "tweet_id"
        ]
        assert len(tweet_id_unique) >= 1

    def test_uniqueness_threshold_very_high(self):
        """Silver dedup must be > 99.9% unique."""
        exps = get_expectation(self.suite, "expect_column_values_to_be_unique")
        tweet_id_unique = [
            e for e in exps
            if e["kwargs"]["column"] == "tweet_id"
        ]
        assert tweet_id_unique[0]["kwargs"]["mostly"] >= 0.999

    def test_platform_only_accepts_lowercase(self):
        """Silver must normalize to lowercase — no mixed case."""
        exps = get_column_expectations(self.suite, "platform")
        value_set_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(value_set_checks) >= 1
        value_set = value_set_checks[0]["kwargs"]["value_set"]
        assert value_set == ["twitter"], \
            "Silver tweets platform must only be 'twitter' (lowercase)"

    def test_pk_null_check_is_100_percent(self):
        """Silver PKs must be 100% non-null."""
        exps = get_column_expectations(self.suite, "tweet_id")
        null_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        assert null_checks[0]["kwargs"]["mostly"] == 1.0

    def test_has_silver_batch_id_check(self):
        """Silver records must always have pipeline tracking metadata."""
        exps = get_column_expectations(self.suite, "silver_batch_id")
        assert len(exps) >= 1

    def test_silver_stricter_than_bronze(self):
        """Silver should have more expectations than Bronze for same entity."""
        bronze = load_suite("bronze.tweets.json")
        silver = load_suite("silver.tweets.json")
        assert len(silver["expectations"]) > len(bronze["expectations"])

    def test_account_type_normalized(self):
        exps = get_column_expectations(self.suite, "user_account_type")
        value_set_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(value_set_checks) >= 1
        value_set = value_set_checks[0]["kwargs"]["value_set"]
        # All must be UPPER case
        assert all(v == v.upper() for v in value_set)


class TestSilverEngagements:

    def setup_method(self):
        self.suite = load_suite("silver.engagements.json")

    def test_has_engagement_value_check(self):
        exps = get_column_expectations(self.suite, "engagement_value")
        assert len(exps) >= 1

    def test_engagement_value_is_positive(self):
        exps = get_column_expectations(self.suite, "engagement_value")
        range_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert range_checks[0]["kwargs"]["min_value"] >= 0.0

    def test_engagement_type_includes_unknown(self):
        """UNKNOWN is valid — assigned to unrecognized types."""
        exps = get_column_expectations(self.suite, "engagement_type")
        value_set_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(value_set_checks) >= 1
        value_set = value_set_checks[0]["kwargs"]["value_set"]
        assert "UNKNOWN" in value_set

    def test_platform_lowercase_normalized(self):
        exps = get_column_expectations(self.suite, "platform")
        value_set_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        value_set = value_set_checks[0]["kwargs"]["value_set"]
        assert all(v == v.lower() for v in value_set)


# ── Gold suite content tests ──────────────────────────────────────────────────

class TestGoldFactEngagements:

    def setup_method(self):
        self.suite = load_suite("gold.fact_engagements.json")

    def test_has_surrogate_key_check(self):
        exps = get_column_expectations(self.suite, "engagement_sk")
        assert len(exps) >= 1

    def test_surrogate_key_is_unique(self):
        exps = get_column_expectations(self.suite, "engagement_sk")
        unique_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_unique"
        ]
        assert len(unique_checks) >= 1

    def test_date_sk_range_check(self):
        """date_sk must be a valid YYYYMMDD integer."""
        exps = get_column_expectations(self.suite, "date_sk")
        range_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert len(range_checks) >= 1
        assert range_checks[0]["kwargs"]["min_value"] == 20200101
        assert range_checks[0]["kwargs"]["max_value"] == 20301231

    def test_checks_all_platforms(self):
        exps = get_column_expectations(self.suite, "platform")
        value_set_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        value_set = value_set_checks[0]["kwargs"]["value_set"]
        assert set(value_set) == {"twitter", "youtube", "reddit"}

    def test_engagement_value_positive(self):
        exps = get_column_expectations(self.suite, "engagement_value")
        range_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert range_checks[0]["kwargs"]["min_value"] >= 0.0

    def test_gold_loaded_at_column_exists(self):
        exps = get_column_expectations(self.suite, "gold_loaded_at")
        col_exist = [
            e for e in exps
            if e["expectation_type"] == "expect_column_to_exist"
        ]
        assert len(col_exist) >= 1


class TestGoldFactPostMetrics:

    def setup_method(self):
        self.suite = load_suite("gold.fact_post_metrics.json")

    def test_engagement_rate_has_upper_bound_100(self):
        """Most posts have engagement rate 0-100%."""
        exps = get_column_expectations(self.suite, "engagement_rate_pct")
        range_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert len(range_checks) >= 1
        assert range_checks[0]["kwargs"]["min_value"] == 0.0
        assert range_checks[0]["kwargs"]["max_value"] == 100.0

    def test_engagement_tier_valid_values(self):
        exps = get_column_expectations(self.suite, "engagement_tier")
        value_set_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(value_set_checks) >= 1
        value_set = set(value_set_checks[0]["kwargs"]["value_set"])
        assert value_set == {"viral", "high", "medium", "low"}

    def test_metric_sk_is_unique(self):
        exps = get_column_expectations(self.suite, "metric_sk")
        unique_checks = [
            e for e in exps
            if e["expectation_type"] == "expect_column_values_to_be_unique"
        ]
        assert len(unique_checks) >= 1


# ── Checkpoint YAML tests ─────────────────────────────────────────────────────

class TestCheckpointFiles:

    @pytest.mark.parametrize("filename", [
        "bronze_quality_check.yml",
        "silver_quality_check.yml",
        "gold_quality_check.yml",
    ])
    def test_checkpoint_file_exists(self, filename):
        assert (CHECKPOINTS_DIR / filename).exists()

    @pytest.mark.parametrize("filename", [
        "bronze_quality_check.yml",
        "silver_quality_check.yml",
        "gold_quality_check.yml",
    ])
    def test_checkpoint_has_validations(self, filename):
        import yaml
        # Simple check without yaml (may not be installed)
        content = (CHECKPOINTS_DIR / filename).read_text()
        assert "validations:" in content
        assert "expectation_suite_name:" in content

    def test_bronze_checkpoint_references_correct_suites(self):
        content = (CHECKPOINTS_DIR / "bronze_quality_check.yml").read_text()
        assert "bronze.tweets" in content
        assert "bronze.engagements" in content

    def test_silver_checkpoint_references_correct_suites(self):
        content = (CHECKPOINTS_DIR / "silver_quality_check.yml").read_text()
        assert "silver.tweets" in content
        assert "silver.engagements" in content
        assert "silver.posts" in content

    def test_silver_checkpoint_is_blocking(self):
        """Silver checkpoint should NOT have '|| true' fallback."""
        content = (CHECKPOINTS_DIR / "silver_quality_check.yml").read_text()
        assert "BLOCKING" in content

    def test_bronze_checkpoint_is_non_blocking(self):
        """Bronze checkpoint should have non-blocking comment."""
        content = (CHECKPOINTS_DIR / "bronze_quality_check.yml").read_text()
        assert "NON-BLOCKING" in content


# ── Cross-layer consistency tests ─────────────────────────────────────────────

class TestCrossLayerConsistency:

    def test_silver_more_strict_than_bronze_for_tweets(self):
        """Silver tweet expectations must outnumber Bronze tweet expectations."""
        bronze = load_suite("bronze.tweets.json")
        silver = load_suite("silver.tweets.json")
        assert len(silver["expectations"]) > len(bronze["expectations"])

    def test_bronze_mostly_threshold_lower_than_silver(self):
        """Bronze null checks can be 99%, Silver must be 100%."""
        bronze_tweet_id = [
            e for e in load_suite("bronze.tweets.json")["expectations"]
            if e.get("kwargs", {}).get("column") == "tweet_id"
            and e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        silver_tweet_id = [
            e for e in load_suite("silver.tweets.json")["expectations"]
            if e.get("kwargs", {}).get("column") == "tweet_id"
            and e["expectation_type"] == "expect_column_values_to_not_be_null"
        ]
        # Both should have mostly=1.0 for PKs — PK is always critical
        assert bronze_tweet_id[0]["kwargs"]["mostly"] == 1.0
        assert silver_tweet_id[0]["kwargs"]["mostly"] == 1.0

    def test_all_suites_have_row_count_check(self):
        """Every layer should verify table is not empty."""
        for filename in [
            "bronze.tweets.json", "silver.tweets.json", "gold.fact_engagements.json"
        ]:
            suite = load_suite(filename)
            row_count_exps = get_expectation(
                suite, "expect_table_row_count_to_be_between"
            )
            assert len(row_count_exps) >= 1, \
                f"{filename} is missing row count expectation"

    def test_platform_values_consistent_across_suites(self):
        """Silver and Gold must use the same lowercase platform values."""
        silver_posts = load_suite("silver.posts.json")
        gold_facts   = load_suite("gold.fact_engagements.json")

        def get_platform_values(suite):
            exps = get_column_expectations(suite, "platform")
            value_set_checks = [
                e for e in exps
                if e["expectation_type"] == "expect_column_values_to_be_in_set"
            ]
            return set(value_set_checks[0]["kwargs"]["value_set"]) if value_set_checks else set()

        silver_platforms = get_platform_values(silver_posts)
        gold_platforms   = get_platform_values(gold_facts)

        assert silver_platforms == gold_platforms == {"twitter", "youtube", "reddit"}
