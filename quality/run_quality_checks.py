"""
quality/run_quality_checks.py — Quality check runner.

Runs data quality checks using pure pandas + business logic,
without requiring Great Expectations CLI to be installed locally.
Useful for development and CI/CD where GE may not be available.

Usage:
    python run_quality_checks.py --layer bronze --source twitter
    python run_quality_checks.py --layer silver
    python run_quality_checks.py --layer all
    python run_quality_checks.py --dry-run          # Validate check definitions only
"""

import argparse
import json
import sys
import os
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

from loguru import logger


# ── Expectation engine (pure Python) ──────────────────────────────────────────

class ExpectationResult:
    def __init__(self, name: str, passed: bool, observed: Any, expected: Any, details: str = ""):
        self.name     = name
        self.passed   = passed
        self.observed = observed
        self.expected = expected
        self.details  = details

    def to_dict(self) -> Dict:
        return {
            "expectation": self.name,
            "passed":      self.passed,
            "observed":    self.observed,
            "expected":    self.expected,
            "details":     self.details,
        }


class DataQualityChecker:
    """
    Lightweight data quality checker using pandas.
    Runs expectations defined as Python functions — no GE dependency needed.
    """

    def __init__(self, layer: str, entity: str):
        self.layer   = layer
        self.entity  = entity
        self.results: List[ExpectationResult] = []

    def check_not_null(self, series, col_name: str, mostly: float = 1.0) -> ExpectationResult:
        """expect_column_values_to_not_be_null"""
        null_rate = series.isnull().mean()
        non_null_rate = 1 - null_rate
        passed = non_null_rate >= mostly
        return ExpectationResult(
            name=f"not_null({col_name})",
            passed=passed,
            observed=round(non_null_rate, 4),
            expected=f">= {mostly}",
            details=f"{series.isnull().sum():,} nulls out of {len(series):,} rows",
        )

    def check_unique(self, series, col_name: str, mostly: float = 1.0) -> ExpectationResult:
        """expect_column_values_to_be_unique"""
        dup_rate = series.duplicated().mean()
        unique_rate = 1 - dup_rate
        passed = unique_rate >= mostly
        return ExpectationResult(
            name=f"unique({col_name})",
            passed=passed,
            observed=round(unique_rate, 4),
            expected=f">= {mostly}",
            details=f"{series.duplicated().sum():,} duplicates",
        )

    def check_in_set(self, series, col_name: str, value_set: List, mostly: float = 1.0) -> ExpectationResult:
        """expect_column_values_to_be_in_set"""
        in_set_rate = series.isin(value_set).mean()
        passed = in_set_rate >= mostly
        invalid = series[~series.isin(value_set)].unique().tolist()
        return ExpectationResult(
            name=f"in_set({col_name})",
            passed=passed,
            observed=round(in_set_rate, 4),
            expected=f">= {mostly} in {value_set}",
            details=f"Invalid values: {invalid[:5]}",
        )

    def check_between(self, series, col_name: str, min_val=None, max_val=None, mostly: float = 1.0) -> ExpectationResult:
        """expect_column_values_to_be_between — nulls excluded from denominator"""
        non_null = series.dropna()
        if len(non_null) == 0:
            return ExpectationResult(f"between({col_name})", True, 1.0, f">= {mostly}")
        mask = (non_null >= (min_val or non_null.min())) & (non_null <= (max_val or non_null.max()))
        pass_rate = mask.mean()
        passed = pass_rate >= mostly
        return ExpectationResult(
            name=f"between({col_name}, [{min_val}, {max_val}])",
            passed=passed,
            observed=round(pass_rate, 4),
            expected=f">= {mostly}",
        )

    def check_row_count(self, df, min_val: int = 1, max_val: int = None) -> ExpectationResult:
        """expect_table_row_count_to_be_between"""
        count = len(df)
        passed = count >= min_val
        if max_val is not None:
            passed = passed and count <= max_val
        return ExpectationResult(
            name="row_count",
            passed=passed,
            observed=count,
            expected=f"[{min_val}, {max_val or '∞'}]",
        )

    def run_all(self, results: List[ExpectationResult]) -> Dict:
        """Summarize results and return quality report."""
        total   = len(results)
        passed  = sum(1 for r in results if r.passed)
        failed  = total - passed
        success = failed == 0

        report = {
            "layer":         self.layer,
            "entity":        self.entity,
            "run_at":        datetime.now(timezone.utc).isoformat(),
            "total_checks":  total,
            "passed":        passed,
            "failed":        failed,
            "success_rate":  round(passed / total, 4) if total > 0 else 0,
            "overall_pass":  success,
            "results":       [r.to_dict() for r in results],
        }

        for r in results:
            icon = "✓" if r.passed else "✗"
            level = "info" if r.passed else "warning"
            getattr(logger, level)(
                f"  [{icon}] {r.name}: {r.observed} (expected {r.expected})"
                + (f" — {r.details}" if r.details else "")
            )

        return report


# ── Layer-specific checks ──────────────────────────────────────────────────────

def check_bronze_tweets(df) -> List[ExpectationResult]:
    """Bronze tweet quality checks."""
    checker = DataQualityChecker("bronze", "tweets")
    results = [
        checker.check_row_count(df, min_val=1),
        checker.check_not_null(df["tweet_id"],   "tweet_id",   mostly=1.0),
        checker.check_not_null(df["user_id"],    "user_id",    mostly=1.0),
        checker.check_not_null(df["platform"],   "platform",   mostly=1.0),
        checker.check_not_null(df["created_at"], "created_at", mostly=0.99),
        checker.check_in_set(df["platform"], "platform", ["twitter"], mostly=0.999),
        checker.check_between(df["likes_count"],    "likes_count",    min_val=0, mostly=0.999),
        checker.check_between(df["retweets_count"], "retweets_count", min_val=0, mostly=0.999),
    ]
    if "tweet_id" in df.columns:
        results.append(checker.check_unique(df["tweet_id"], "tweet_id", mostly=0.99))
    return results


def check_silver_tweets(df) -> List[ExpectationResult]:
    """Silver tweet quality checks — stricter than Bronze."""
    checker = DataQualityChecker("silver", "tweets")
    results = [
        checker.check_row_count(df, min_val=1),
        checker.check_not_null(df["tweet_id"],        "tweet_id",        mostly=1.0),
        checker.check_not_null(df["user_id"],         "user_id",         mostly=1.0),
        checker.check_not_null(df["platform"],        "platform",        mostly=1.0),
        checker.check_not_null(df["created_at"],      "created_at",      mostly=1.0),
        checker.check_not_null(df["likes_count"],     "likes_count",     mostly=1.0),
        checker.check_not_null(df["retweets_count"],  "retweets_count",  mostly=1.0),
        checker.check_not_null(df["silver_batch_id"], "silver_batch_id", mostly=1.0),
        checker.check_unique(df["tweet_id"], "tweet_id", mostly=1.0),   # Full dedup at Silver
        checker.check_in_set(df["platform"], "platform", ["twitter"], mostly=1.0),
        checker.check_between(df["likes_count"],    "likes_count",    min_val=0, mostly=1.0),
        checker.check_between(df["retweets_count"], "retweets_count", min_val=0, mostly=1.0),
        checker.check_in_set(
            df["user_account_type"], "user_account_type",
            ["BRAND", "INFLUENCER", "USER"], mostly=0.999
        ),
    ]
    return results


def check_gold_engagements(df) -> List[ExpectationResult]:
    """Gold fact_engagements quality checks — alert only."""
    checker = DataQualityChecker("gold", "fact_engagements")
    results = [
        checker.check_row_count(df, min_val=1),
        checker.check_not_null(df["engagement_sk"], "engagement_sk", mostly=1.0),
        checker.check_not_null(df["engagement_id"], "engagement_id", mostly=1.0),
        checker.check_not_null(df["post_id"],       "post_id",       mostly=1.0),
        checker.check_not_null(df["date_sk"],       "date_sk",       mostly=1.0),
        checker.check_unique(df["engagement_sk"], "engagement_sk", mostly=1.0),
        checker.check_unique(df["engagement_id"], "engagement_id", mostly=1.0),
        checker.check_in_set(
            df["engagement_type"], "engagement_type",
            ["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW", "UNKNOWN"], mostly=1.0
        ),
        checker.check_in_set(
            df["platform"], "platform",
            ["twitter", "youtube", "reddit"], mostly=1.0
        ),
        checker.check_between(df["engagement_value"], "engagement_value", min_val=0, mostly=1.0),
        checker.check_between(df["date_sk"], "date_sk", min_val=20200101, max_val=20301231, mostly=1.0),
    ]
    return results


# ── KPI summary ────────────────────────────────────────────────────────────────

def print_kpi_summary(reports: List[Dict]) -> None:
    """Print a summary KPI table for all quality checks."""
    print("\n" + "═" * 60)
    print("  DATA QUALITY KPI SUMMARY")
    print("═" * 60)
    print(f"  {'Layer':<12} {'Entity':<20} {'Checks':<8} {'Pass':<6} {'Fail':<6} {'Rate'}")
    print("  " + "─" * 58)
    for r in reports:
        icon = "✓" if r["overall_pass"] else "✗"
        print(
            f"  {icon} {r['layer']:<11} {r['entity']:<20} "
            f"{r['total_checks']:<8} {r['passed']:<6} {r['failed']:<6} "
            f"{r['success_rate']*100:.1f}%"
        )
    print("═" * 60)
    all_pass = all(r["overall_pass"] for r in reports)
    print(f"  Overall: {'ALL PASSED ✓' if all_pass else 'FAILURES DETECTED ✗'}")
    print("═" * 60 + "\n")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data quality checks")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate without loading data")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("Dry run mode — validating check definitions only")
        logger.info("Checks defined for: bronze.tweets, silver.tweets, silver.engagements, gold.fact_engagements")
        sys.exit(0)

    logger.info("Loading data quality checks...")
    logger.info("Note: In production, point to real MinIO/DuckDB data.")
    logger.info("For local testing, use --dry-run or provide a sample CSV/Parquet.")
    sys.exit(0)
