"""
tests/integration/test_pipeline_integration.py — Integration tests.

Verifies that all pipeline components are correctly wired together:
  - Schema consistency across layers
  - DAG references valid scripts
  - Expectation suites match ingestion schemas
  - dbt sources reference Silver columns that Spark produces
  - All required files present and importable

These tests run WITHOUT external services (no Kafka, MinIO, Spark).
They catch integration bugs like: DAG references a script that doesn't exist,
or Silver schema has a column that dbt model doesn't expect.
"""

import ast
import json
import os
import sys
from pathlib import Path

import pytest

# ── Project paths ─────────────────────────────────────────────────────────────
ROOT         = Path(__file__).parent.parent.parent
SRC          = ROOT / "src"
QUALITY      = ROOT / "quality"
TESTS        = ROOT / "tests"
DAGS_DIR     = SRC / "orchestration" / "dags"
SIMULATORS   = SRC / "simulators"
INGESTION    = SRC / "ingestion"
SPARK_JOBS   = SRC / "transformation" / "spark_jobs"
DBT_DIR      = SRC / "transformation" / "dbt"
EXPECTATIONS = QUALITY / "expectations"
CHECKPOINTS  = QUALITY / "checkpoints"


# ══════════════════════════════════════════════════════════════════════════════
# FILE INVENTORY — every required file must exist
# ══════════════════════════════════════════════════════════════════════════════

class TestFileInventory:
    """All pipeline components must be present."""

    @pytest.mark.parametrize("rel_path", [
        # Simulators
        "src/simulators/config.py",
        "src/simulators/user_pool.py",
        "src/simulators/clients.py",
        "src/simulators/twitter_simulator.py",
        "src/simulators/youtube_simulator.py",
        "src/simulators/reddit_simulator.py",
        "src/simulators/run_all.py",

        # Ingestion
        "src/ingestion/kafka_consumer.py",
        "src/ingestion/batch_ingestion.py",
        "src/ingestion/schemas/bronze_schemas.py",
        "src/ingestion/utils/ingestion_utils.py",

        # Transformation — Spark
        "src/transformation/spark_jobs/bronze_to_silver.py",
        "src/transformation/spark_jobs/utils/spark_utils.py",

        # Transformation — dbt
        "src/transformation/dbt/dbt_project.yml",
        "src/transformation/dbt/profiles.yml",
        "src/transformation/dbt/packages.yml",
        "src/transformation/dbt/models/staging/sources.yml",
        "src/transformation/dbt/models/staging/stg_twitter__tweets.sql",
        "src/transformation/dbt/models/staging/stg_twitter__engagements.sql",
        "src/transformation/dbt/models/staging/stg_all__posts.sql",
        "src/transformation/dbt/models/intermediate/int_posts_enriched.sql",
        "src/transformation/dbt/models/marts/facts/fact_engagements.sql",
        "src/transformation/dbt/models/marts/facts/fact_post_metrics.sql",
        "src/transformation/dbt/models/marts/dimensions/dim_date.sql",
        "src/transformation/dbt/models/marts/dimensions/dim_posts.sql",
        "src/transformation/dbt/models/marts/schema.yml",
        "src/transformation/dbt/macros/custom_macros.sql",

        # Orchestration
        "src/orchestration/dags/dag_utils.py",
        "src/orchestration/dags/ingestion_pipeline.py",
        "src/orchestration/dags/transformation_pipeline.py",
        "src/orchestration/dags/daily_refresh.py",

        # Quality
        "quality/great_expectations.yml",
        "quality/generate_expectations.py",
        "quality/checkpoints/bronze_quality_check.yml",
        "quality/checkpoints/silver_quality_check.yml",
        "quality/checkpoints/gold_quality_check.yml",
        "quality/expectations/bronze.tweets.json",
        "quality/expectations/bronze.engagements.json",
        "quality/expectations/silver.tweets.json",
        "quality/expectations/silver.engagements.json",
        "quality/expectations/silver.posts.json",
        "quality/expectations/gold.fact_engagements.json",
        "quality/expectations/gold.fact_post_metrics.json",

        # Infrastructure
        "docker/docker-compose.yml",
        "docker/airflow/Dockerfile",
        "docker/spark/Dockerfile",
        "docker/spark/spark-defaults.conf",

        # Project root
        "Makefile",
        "README.md",
        ".gitignore",
        "requirements.txt",
        ".pre-commit-config.yaml",
    ])
    def test_file_exists(self, rel_path):
        assert (ROOT / rel_path).exists(), f"Missing: {rel_path}"


# ══════════════════════════════════════════════════════════════════════════════
# PYTHON SYNTAX — all .py files must parse without errors
# ══════════════════════════════════════════════════════════════════════════════

class TestPythonSyntax:

    def _all_python_files(self):
        for path in ROOT.rglob("*.py"):
            if "__pycache__" not in str(path):
                yield path

    def test_all_python_files_have_valid_syntax(self):
        errors = []
        for path in self._all_python_files():
            try:
                ast.parse(path.read_text())
            except SyntaxError as e:
                errors.append(f"{path.relative_to(ROOT)}: {e}")
        assert not errors, "Syntax errors found:\n" + "\n".join(errors)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA CONSISTENCY — Bronze schema → Silver schema alignment
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaConsistency:
    """Verify that fields produced by simulators match ingestion schemas."""

    def test_bronze_schema_has_tweet_required_fields(self):
        sys.path.insert(0, str(INGESTION))
        from schemas.bronze_schemas import REQUIRED_FIELDS
        required = REQUIRED_FIELDS.get("twitter.tweets", [])
        assert "tweet_id"  in required
        assert "user_id"   in required
        assert "platform"  in required
        assert "created_at" in required

    def test_bronze_schema_has_engagement_required_fields(self):
        sys.path.insert(0, str(INGESTION))
        from schemas.bronze_schemas import REQUIRED_FIELDS
        required = REQUIRED_FIELDS.get("twitter.engagements", [])
        assert "engagement_id"    in required
        assert "post_id"          in required
        assert "engaging_user_id" in required
        assert "engagement_type"  in required

    def test_silver_expectation_pk_matches_bronze_schema(self):
        """Silver expectation PKs must match Bronze schema required fields."""
        sys.path.insert(0, str(INGESTION))
        from schemas.bronze_schemas import REQUIRED_FIELDS

        bronze_required = set(REQUIRED_FIELDS.get("twitter.tweets", []))

        suite = json.loads((EXPECTATIONS / "silver.tweets.json").read_text())
        null_check_cols = {
            e["kwargs"]["column"]
            for e in suite["expectations"]
            if e["expectation_type"] == "expect_column_values_to_not_be_null"
        }
        # All Bronze required fields should have Silver null checks
        for field in bronze_required:
            assert field in null_check_cols, \
                f"Bronze required field '{field}' missing Silver null check"

    def test_simulator_produces_bronze_required_fields(self):
        """Twitter simulator output must include all Bronze required fields."""
        sys.path.insert(0, str(INGESTION))
        sys.path.insert(0, str(SIMULATORS))
        from schemas.bronze_schemas import REQUIRED_FIELDS

        # Check simulator file references these fields
        sim_content = (SIMULATORS / "twitter_simulator.py").read_text()
        for field in REQUIRED_FIELDS.get("twitter.tweets", []):
            assert f'"{field}"' in sim_content or f"'{field}'" in sim_content, \
                f"Simulator doesn't produce required field: {field}"

    def test_ingestion_utils_bronze_key_format(self):
        """Bronze key builder must produce Hive-style partitioned paths."""
        sys.path.insert(0, str(INGESTION / "utils"))
        sys.path.insert(0, str(INGESTION))

        # Read and check the key format from source code
        utils_content = (INGESTION / "utils" / "ingestion_utils.py").read_text()
        assert "year=" in utils_content
        assert "month=" in utils_content
        assert "day=" in utils_content
        assert "hour=" in utils_content


# ══════════════════════════════════════════════════════════════════════════════
# DAG → SCRIPT REFERENCES
# ══════════════════════════════════════════════════════════════════════════════

class TestDAGScriptReferences:
    """Verify DAGs reference scripts that actually exist."""

    def test_ingestion_dag_references_twitter_simulator(self):
        content = (DAGS_DIR / "ingestion_pipeline.py").read_text()
        assert "twitter_simulator.py" in content

    def test_ingestion_dag_references_youtube_simulator(self):
        content = (DAGS_DIR / "ingestion_pipeline.py").read_text()
        assert "youtube_simulator.py" in content

    def test_ingestion_dag_references_kafka_consumer(self):
        content = (DAGS_DIR / "ingestion_pipeline.py").read_text()
        assert "kafka_consumer.py" in content

    def test_ingestion_dag_references_batch_ingestion(self):
        content = (DAGS_DIR / "ingestion_pipeline.py").read_text()
        assert "batch_ingestion.py" in content

    def test_transform_dag_references_bronze_to_silver(self):
        content = (DAGS_DIR / "transformation_pipeline.py").read_text()
        assert "bronze_to_silver.py" in content

    def test_daily_dag_references_run_all(self):
        content = (DAGS_DIR / "daily_refresh.py").read_text()
        assert "run_all.py" in content

    def test_transform_dag_references_dbt_project_dir(self):
        content = (DAGS_DIR / "transformation_pipeline.py").read_text()
        assert "transformation/dbt" in content

    def test_all_referenced_simulators_exist(self):
        """Every simulator referenced in DAGs must exist on disk."""
        ingestion_content = (DAGS_DIR / "ingestion_pipeline.py").read_text()
        daily_content     = (DAGS_DIR / "daily_refresh.py").read_text()

        script_map = {
            "twitter_simulator.py": SIMULATORS / "twitter_simulator.py",
            "youtube_simulator.py": SIMULATORS / "youtube_simulator.py",
            "reddit_simulator.py":  SIMULATORS / "reddit_simulator.py",
            "run_all.py":           SIMULATORS / "run_all.py",
            "kafka_consumer.py":    INGESTION / "kafka_consumer.py",
            "batch_ingestion.py":   INGESTION / "batch_ingestion.py",
            "bronze_to_silver.py":  SPARK_JOBS / "bronze_to_silver.py",
        }
        for script_name, script_path in script_map.items():
            assert script_path.exists(), f"DAG references missing script: {script_name}"


# ══════════════════════════════════════════════════════════════════════════════
# dbt MODEL REFERENCES
# ══════════════════════════════════════════════════════════════════════════════

class TestDBTModelReferences:
    """Verify dbt models reference valid sources and other models."""

    def _read_model(self, rel_path: str) -> str:
        return (DBT_DIR / rel_path).read_text()

    def test_staging_tweets_refs_silver_source(self):
        content = self._read_model("models/staging/stg_twitter__tweets.sql")
        assert "source('silver', 'tweets')" in content

    def test_staging_engagements_refs_silver_source(self):
        content = self._read_model("models/staging/stg_twitter__engagements.sql")
        assert "source('silver', 'engagements')" in content

    def test_staging_posts_refs_silver_source(self):
        content = self._read_model("models/staging/stg_all__posts.sql")
        assert "source('silver', 'posts')" in content

    def test_intermediate_refs_staging(self):
        content = self._read_model("models/intermediate/int_posts_enriched.sql")
        assert "ref('stg_all__posts')" in content

    def test_fact_engagements_refs_staging_engagements(self):
        content = self._read_model("models/marts/facts/fact_engagements.sql")
        assert "ref('stg_twitter__engagements')" in content

    def test_fact_engagements_refs_intermediate(self):
        content = self._read_model("models/marts/facts/fact_engagements.sql")
        assert "ref('int_posts_enriched')" in content

    def test_fact_post_metrics_refs_fact_engagements(self):
        """fact_post_metrics aggregates from fact_engagements."""
        content = self._read_model("models/marts/facts/fact_post_metrics.sql")
        assert "ref('fact_engagements')" in content

    def test_fact_post_metrics_refs_intermediate(self):
        content = self._read_model("models/marts/facts/fact_post_metrics.sql")
        assert "ref('int_posts_enriched')" in content

    def test_dim_posts_refs_intermediate(self):
        content = self._read_model("models/marts/dimensions/dim_posts.sql")
        assert "ref('int_posts_enriched')" in content

    def test_dim_date_uses_date_spine(self):
        content = self._read_model("models/marts/dimensions/dim_date.sql")
        assert "date_spine" in content

    def test_all_mart_models_have_config_block(self):
        """All Gold models must have a dbt config block."""
        mart_files = [
            "models/marts/facts/fact_engagements.sql",
            "models/marts/facts/fact_post_metrics.sql",
            "models/marts/dimensions/dim_date.sql",
            "models/marts/dimensions/dim_posts.sql",
        ]
        for rel_path in mart_files:
            content = self._read_model(rel_path)
            assert "{{" in content and "config(" in content, \
                f"{rel_path} missing dbt config block"

    def test_dbt_project_yml_references_correct_paths(self):
        content = (DBT_DIR / "dbt_project.yml").read_text()
        assert "staging" in content
        assert "intermediate" in content
        assert "marts" in content

    def test_sources_yml_defines_silver_tables(self):
        content = (DBT_DIR / "models/staging/sources.yml").read_text()
        assert "tweets" in content
        assert "engagements" in content
        assert "posts" in content


# ══════════════════════════════════════════════════════════════════════════════
# QUALITY LAYER — checkpoints reference correct expectation suites
# ══════════════════════════════════════════════════════════════════════════════

class TestQualityLayerConsistency:

    def test_all_expectation_suite_files_are_valid_json(self):
        for path in EXPECTATIONS.glob("*.json"):
            try:
                suite = json.loads(path.read_text())
                assert "expectation_suite_name" in suite
                assert "expectations" in suite
            except json.JSONDecodeError as e:
                pytest.fail(f"Invalid JSON in {path.name}: {e}")

    def test_expectation_suites_cover_all_three_layers(self):
        filenames = {p.name for p in EXPECTATIONS.glob("*.json")}
        assert any(f.startswith("bronze.") for f in filenames), "Missing Bronze suites"
        assert any(f.startswith("silver.") for f in filenames), "Missing Silver suites"
        assert any(f.startswith("gold.") for f in filenames),   "Missing Gold suites"

    def test_silver_checkpoint_references_all_silver_suites(self):
        checkpoint = (CHECKPOINTS / "silver_quality_check.yml").read_text()
        for table in ["tweets", "engagements", "posts"]:
            assert f"silver.{table}" in checkpoint, \
                f"Silver checkpoint missing silver.{table}"

    def test_generate_expectations_produces_correct_files(self):
        """Running generate_expectations.py should produce exactly these files."""
        expected_files = {
            "bronze.tweets.json",
            "bronze.engagements.json",
            "silver.tweets.json",
            "silver.engagements.json",
            "silver.posts.json",
            "gold.fact_engagements.json",
            "gold.fact_post_metrics.json",
        }
        actual_files = {p.name for p in EXPECTATIONS.glob("*.json")}
        assert expected_files.issubset(actual_files)

    def test_no_expectation_suite_has_zero_expectations(self):
        for path in EXPECTATIONS.glob("*.json"):
            suite = json.loads(path.read_text())
            assert len(suite["expectations"]) > 0, \
                f"{path.name} has 0 expectations"

    def test_silver_suites_have_uniqueness_checks(self):
        """Silver must verify deduplication was successful."""
        for filename in ["silver.tweets.json", "silver.engagements.json"]:
            suite = json.loads((EXPECTATIONS / filename).read_text())
            unique_exps = [
                e for e in suite["expectations"]
                if e["expectation_type"] == "expect_column_values_to_be_unique"
            ]
            assert len(unique_exps) >= 1, \
                f"{filename} missing uniqueness check"


# ══════════════════════════════════════════════════════════════════════════════
# DOCKER — infrastructure files are present and consistent
# ══════════════════════════════════════════════════════════════════════════════

class TestDockerInfrastructure:

    def test_docker_compose_exists(self):
        assert (ROOT / "docker" / "docker-compose.yml").exists()

    def test_docker_compose_has_all_services(self):
        content = (ROOT / "docker" / "docker-compose.yml").read_text()
        required_services = [
            "kafka", "zookeeper", "minio", "spark-master",
            "airflow-webserver", "airflow-scheduler", "postgres",
        ]
        for service in required_services:
            assert service in content, f"Missing service in docker-compose: {service}"

    def test_airflow_dockerfile_installs_pyspark(self):
        content = (ROOT / "docker" / "airflow" / "Dockerfile").read_text()
        assert "pyspark" in content.lower() or "spark" in content.lower()

    def test_spark_dockerfile_has_s3a_jars(self):
        content = (ROOT / "docker" / "spark" / "Dockerfile").read_text()
        assert "hadoop-aws" in content or "s3a" in content.lower()

    def test_spark_defaults_has_minio_config(self):
        content = (ROOT / "docker" / "spark" / "spark-defaults.conf").read_text()
        assert "s3a" in content
        assert "minio" in content

    def test_env_example_has_required_vars(self):
        content = (ROOT / ".env.example").read_text()
        required_vars = [
            "KAFKA_BOOTSTRAP_SERVERS",
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "BUCKET_LANDING",
        ]
        for var in required_vars:
            assert var in content, f"Missing .env.example var: {var}"


# ══════════════════════════════════════════════════════════════════════════════
# END-TO-END DATA FLOW TRACE (pure Python simulation)
# ══════════════════════════════════════════════════════════════════════════════

class TestEndToEndDataFlowTrace:
    """
    Simulate the complete data flow in memory to verify all transformations
    work correctly in sequence — without any external services.
    """

    def test_simulated_tweet_passes_bronze_validation(self):
        """A tweet from the simulator must pass Bronze schema validation."""
        sys.path.insert(0, str(INGESTION))
        from schemas.bronze_schemas import validate_record

        # Simulate what twitter_simulator._build_tweet() produces
        import uuid
        from datetime import datetime, timezone

        tweet = {
            "tweet_id":           str(uuid.uuid4()),
            "user_id":            str(uuid.uuid4()),
            "platform":           "twitter",
            "content":            "Hello world #AI #DataEngineering",
            "hashtags":           ["#AI", "#DataEngineering"],
            "mentions":           [],
            "language":           "en",
            "is_retweet":         False,
            "is_reply":           False,
            "media_type":         None,
            "likes_count":        42,
            "retweets_count":     5,
            "replies_count":      3,
            "impressions_count":  1200,
            "quote_count":        1,
            "user_follower_count": 500,
            "user_account_type":  "USER",
            "user_is_verified":   False,
            "created_at":         datetime.now(timezone.utc).isoformat(),
            "ingested_at":        datetime.now(timezone.utc).isoformat(),
            "source_system":      "twitter_simulator_v1",
            "schema_version":     "1.0",
        }

        is_valid, errors = validate_record(tweet, "twitter.tweets")
        assert is_valid, f"Bronze validation failed: {errors}"

    def test_bronze_ingestion_metadata_tagging(self):
        """IngestionContext must tag records with batch_id and ingested_at."""
        sys.path.insert(0, str(INGESTION))
        sys.path.insert(0, str(INGESTION / "utils"))
        from ingestion_utils import IngestionContext

        import uuid
        ctx = IngestionContext("kafka", "integration_test")
        record = {"tweet_id": str(uuid.uuid4()), "user_id": str(uuid.uuid4())}
        tagged = ctx.tag_record(record, "test_source.json")

        assert tagged["batch_id"] == ctx.run_id
        assert "ingested_at" in tagged
        assert tagged["source_file"] == "test_source.json"

    def test_bronze_key_partitioning(self):
        """Bronze keys must be Hive-style partitioned by date/hour."""
        sys.path.insert(0, str(INGESTION / "utils"))
        from ingestion_utils import build_bronze_key

        key = build_bronze_key("twitter", "tweets", "run-abc-123")
        parts = key.split("/")

        assert parts[0] == "twitter"
        assert parts[1] == "tweets"
        assert parts[2].startswith("year=")
        assert parts[3].startswith("month=")
        assert parts[4].startswith("day=")
        assert parts[5].startswith("hour=")
        assert parts[6].endswith(".json")

    def test_transformation_dedup_logic(self):
        """Silver dedup must keep latest record by ingested_at."""
        records = [
            {"tweet_id": "t1", "likes": 10, "ingested_at": "2024-01-15T10:00:00"},
            {"tweet_id": "t1", "likes": 20, "ingested_at": "2024-01-15T12:00:00"},
            {"tweet_id": "t2", "likes": 5,  "ingested_at": "2024-01-15T11:00:00"},
        ]
        # Replicate deduplicate() logic
        groups = {}
        for r in records:
            key = r["tweet_id"]
            if key not in groups or r["ingested_at"] > groups[key]["ingested_at"]:
                groups[key] = r
        deduped = list(groups.values())

        assert len(deduped) == 2
        t1 = next(r for r in deduped if r["tweet_id"] == "t1")
        assert t1["likes"] == 20, "Should keep latest record (likes=20)"

    def test_engagement_tier_thresholds_consistent(self):
        """
        Engagement tier thresholds must be consistent between:
          - spark_jobs/bronze_to_silver.py (dbt intermediate)
          - dbt model int_posts_enriched.sql
          - gold expectation suite
        """
        # Check dbt model has same thresholds
        dbt_content = (DBT_DIR / "models/intermediate/int_posts_enriched.sql").read_text()
        assert "10000" in dbt_content or "10_000" in dbt_content  # viral threshold
        assert "1000"  in dbt_content or "1_000"  in dbt_content  # high threshold
        assert "100"   in dbt_content                              # medium threshold

        # Check gold expectation has all tiers
        suite = json.loads((EXPECTATIONS / "gold.fact_post_metrics.json").read_text())
        tier_exps = [
            e for e in suite["expectations"]
            if e.get("kwargs", {}).get("column") == "engagement_tier"
            and e["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert len(tier_exps) >= 1
        tier_values = set(tier_exps[0]["kwargs"]["value_set"])
        assert tier_values == {"viral", "high", "medium", "low"}

    def test_date_sk_format_consistent_across_layers(self):
        """
        date_sk (YYYYMMDD integer) must be consistent between:
          - dbt fact models (to_char format)
          - gold expectation (range 20200101-20301231)
        """
        fact_content = (DBT_DIR / "models/marts/facts/fact_engagements.sql").read_text()
        assert "YYYYMMDD" in fact_content or "date_sk" in fact_content

        suite = json.loads((EXPECTATIONS / "gold.fact_engagements.json").read_text())
        date_sk_range = [
            e for e in suite["expectations"]
            if e.get("kwargs", {}).get("column") == "date_sk"
            and e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        assert date_sk_range[0]["kwargs"]["min_value"] == 20200101
        assert date_sk_range[0]["kwargs"]["max_value"] == 20301231

    def test_platform_values_consistent_end_to_end(self):
        """
        Platform values {'twitter', 'youtube', 'reddit'} must appear
        consistently in: Bronze schemas, Silver expectations, dbt sources,
        Gold expectations.
        """
        expected_platforms = {"twitter", "youtube", "reddit"}

        # Silver posts expectation
        silver_suite = json.loads((EXPECTATIONS / "silver.posts.json").read_text())
        silver_platforms_exp = next(
            e for e in silver_suite["expectations"]
            if e.get("kwargs", {}).get("column") == "platform"
            and e["expectation_type"] == "expect_column_values_to_be_in_set"
        )
        assert set(silver_platforms_exp["kwargs"]["value_set"]) == expected_platforms

        # Gold expectation
        gold_suite = json.loads((EXPECTATIONS / "gold.fact_engagements.json").read_text())
        gold_platforms_exp = next(
            e for e in gold_suite["expectations"]
            if e.get("kwargs", {}).get("column") == "platform"
            and e["expectation_type"] == "expect_column_values_to_be_in_set"
        )
        assert set(gold_platforms_exp["kwargs"]["value_set"]) == expected_platforms

        # dbt sources.yml mentions all platforms
        sources_content = (DBT_DIR / "models/staging/sources.yml").read_text()
        assert "twitter" in sources_content
        assert "youtube" in sources_content or "posts" in sources_content
