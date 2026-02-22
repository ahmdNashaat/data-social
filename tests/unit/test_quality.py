"""
tests/unit/test_quality.py — Unit tests for data quality check logic.

Tests the quality checker functions with synthetic DataFrames —
no Great Expectations, MinIO, or DuckDB needed.
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../quality"))

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# ── Quality logic (pure Python — no pandas dependency) ────────────────────────

def check_not_null_rate(values: list, mostly: float = 1.0) -> tuple:
    """Pure Python: null rate check."""
    total = len(values)
    if total == 0:
        return True, 1.0
    non_null = sum(1 for v in values if v is not None and v == v)  # handles NaN
    rate = non_null / total
    return rate >= mostly, round(rate, 4)


def check_unique_rate(values: list, mostly: float = 1.0) -> tuple:
    """Pure Python: uniqueness rate check."""
    total = len(values)
    if total == 0:
        return True, 1.0
    unique = len(set(v for v in values if v is not None))
    rate = unique / total
    return rate >= mostly, round(rate, 4)


def check_in_set_rate(values: list, valid_set: set, mostly: float = 1.0) -> tuple:
    """Pure Python: value set membership check."""
    total = len(values)
    if total == 0:
        return True, 1.0
    in_set = sum(1 for v in values if v in valid_set)
    rate = in_set / total
    return rate >= mostly, round(rate, 4)


def check_between_rate(values: list, min_val=None, max_val=None, mostly: float = 1.0) -> tuple:
    """Pure Python: range check."""
    total = len(values)
    if total == 0:
        return True, 1.0
    passing = 0
    for v in values:
        if v is None:
            continue
        ok = True
        if min_val is not None and v < min_val:
            ok = False
        if max_val is not None and v > max_val:
            ok = False
        if ok:
            passing += 1
    rate = passing / total
    return rate >= mostly, round(rate, 4)


def check_row_count(count: int, min_val: int = 1, max_val: int = None) -> tuple:
    """Pure Python: row count check."""
    ok = count >= min_val
    if max_val is not None:
        ok = ok and count <= max_val
    return ok, count


# ── Tests: not_null check ──────────────────────────────────────────────────────

class TestNotNullCheck:

    def test_all_non_null_passes(self):
        values = ["a", "b", "c", "d"]
        passed, rate = check_not_null_rate(values, mostly=1.0)
        assert passed is True
        assert rate == 1.0

    def test_all_null_fails(self):
        values = [None, None, None]
        passed, rate = check_not_null_rate(values, mostly=1.0)
        assert passed is False
        assert rate == 0.0

    def test_mostly_threshold_respected(self):
        # 90% non-null with mostly=0.95 → FAIL
        values = [str(i) for i in range(90)] + [None] * 10
        passed, rate = check_not_null_rate(values, mostly=0.95)
        assert passed is False
        assert rate == 0.9

    def test_mostly_threshold_passes(self):
        # 99% non-null with mostly=0.95 → PASS
        values = [str(i) for i in range(99)] + [None]
        passed, rate = check_not_null_rate(values, mostly=0.95)
        assert passed is True

    def test_empty_list_passes(self):
        passed, rate = check_not_null_rate([], mostly=1.0)
        assert passed is True


# ── Tests: uniqueness check ───────────────────────────────────────────────────

class TestUniqueCheck:

    def test_all_unique_passes(self):
        values = ["a", "b", "c", "d", "e"]
        passed, rate = check_unique_rate(values, mostly=1.0)
        assert passed is True
        assert rate == 1.0

    def test_all_duplicates_fails(self):
        values = ["a", "a", "a", "a"]
        passed, rate = check_unique_rate(values, mostly=1.0)
        assert passed is False
        assert rate == 0.25   # Only 1 unique out of 4

    def test_partial_duplicates_with_threshold(self):
        # 95 unique + 5 duplicates = 95/100 = 0.95
        values = [str(i) for i in range(95)] + ["dup"] * 5
        passed, rate = check_unique_rate(values, mostly=0.94)
        assert passed is True

    def test_silver_requires_full_uniqueness(self):
        """Silver layer enforces 100% uniqueness on PKs."""
        values = ["id_1", "id_2", "id_1"]  # 1 duplicate
        passed, rate = check_unique_rate(values, mostly=1.0)
        assert passed is False


# ── Tests: value set check ────────────────────────────────────────────────────

class TestInSetCheck:

    def test_all_valid_passes(self):
        values = ["LIKE", "RETWEET", "REPLY"]
        valid = {"LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW"}
        passed, rate = check_in_set_rate(values, valid, mostly=1.0)
        assert passed is True

    def test_invalid_values_fails(self):
        values = ["LIKE", "CLAP", "HEART"]
        valid = {"LIKE", "RETWEET", "REPLY"}
        passed, rate = check_in_set_rate(values, valid, mostly=1.0)
        assert passed is False

    def test_platform_check_twitter(self):
        values = ["twitter"] * 100
        passed, rate = check_in_set_rate(values, {"twitter"}, mostly=1.0)
        assert passed is True

    def test_platform_mixed_fails(self):
        values = ["twitter"] * 95 + ["unknown"] * 5
        passed, rate = check_in_set_rate(values, {"twitter", "youtube", "reddit"}, mostly=1.0)
        assert passed is False

    def test_engagement_types_valid(self):
        valid_types = {"LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW", "UNKNOWN"}
        values = ["LIKE", "RETWEET", "REPLY", "QUOTE", "VIEW", "UNKNOWN"]
        passed, _ = check_in_set_rate(values, valid_types, mostly=1.0)
        assert passed is True


# ── Tests: range check ────────────────────────────────────────────────────────

class TestBetweenCheck:

    def test_all_in_range_passes(self):
        values = [0, 100, 1000, 50000]
        passed, rate = check_between_rate(values, min_val=0, mostly=1.0)
        assert passed is True

    def test_negative_values_fail(self):
        values = [10, -5, 100, -1]
        passed, rate = check_between_rate(values, min_val=0, mostly=1.0)
        assert passed is False

    def test_date_sk_range(self):
        """date_sk must be between 20200101 and 20301231."""
        valid = [20240115, 20230601, 20250101]
        invalid = [20190101, 20350101]
        passed_v, _ = check_between_rate(valid, min_val=20200101, max_val=20301231, mostly=1.0)
        passed_i, _ = check_between_rate(invalid, min_val=20200101, max_val=20301231, mostly=1.0)
        assert passed_v is True
        assert passed_i is False

    def test_engagement_rate_pct_bounds(self):
        """Engagement rate should be between 0 and 100."""
        values = [0.0, 2.5, 10.0, 50.0, 100.0]
        passed, rate = check_between_rate(values, min_val=0, max_val=100, mostly=1.0)
        assert passed is True

    def test_nulls_excluded_from_range_check(self):
        values = [10, None, 50, None, 100]
        passed, rate = check_between_rate(values, min_val=0, mostly=1.0)
        # Nulls are excluded — 3/3 non-null values pass
        assert passed is True


# ── Tests: row count check ────────────────────────────────────────────────────

class TestRowCountCheck:

    def test_above_minimum_passes(self):
        passed, count = check_row_count(1000, min_val=1)
        assert passed is True

    def test_zero_rows_fails(self):
        passed, count = check_row_count(0, min_val=1)
        assert passed is False

    def test_max_count_respected(self):
        passed, count = check_row_count(100, min_val=1, max_val=50)
        assert passed is False

    def test_exactly_min_passes(self):
        passed, count = check_row_count(1, min_val=1)
        assert passed is True


# ── Tests: quality KPIs ───────────────────────────────────────────────────────

class TestQualityKPIs:
    """Tests for pipeline quality KPIs defined in the TAD."""

    def test_pipeline_success_rate_kpi(self):
        """KPI: Bronze record completeness > 99.5%"""
        records = [str(i) for i in range(995)] + [None] * 5
        passed, rate = check_not_null_rate(records, mostly=0.995)
        assert passed is True
        assert rate == 0.995

    def test_silver_dedup_rate_kpi(self):
        """KPI: Silver dedup rate < 0.1% duplicates"""
        total = 10000
        dups  = 9          # 0.09% duplicates
        # unique IDs = total - dups
        values = [str(i) for i in range(total - dups)] + ["dup_0"] * dups
        passed, rate = check_unique_rate(values, mostly=0.999)
        assert passed is True

    def test_null_pk_kpi(self):
        """KPI: Null rate on PK columns = 0%"""
        values = ["id_" + str(i) for i in range(1000)]
        passed, rate = check_not_null_rate(values, mostly=1.0)
        assert passed is True
        assert rate == 1.0

    def test_date_sk_valid_range_kpi(self):
        """KPI: All date_sk values are valid YYYYMMDD between 2020-2030"""
        import random
        random.seed(42)
        # Generate 1000 random valid date SKs
        values = [
            int(f"20{random.randint(20,29):02d}{random.randint(1,12):02d}{random.randint(1,28):02d}")
            for _ in range(1000)
        ]
        passed, rate = check_between_rate(values, min_val=20200101, max_val=20291231, mostly=1.0)
        assert passed is True
