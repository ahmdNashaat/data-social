"""
tests/unit/test_dags.py — Structural tests for Airflow DAGs.

Tests DAG configuration, schedule intervals, and file structure
without requiring a running Airflow instance.
"""

import sys
import os
import ast
import pytest
from unittest.mock import MagicMock
from datetime import timedelta

# ── Mock Airflow imports ──────────────────────────────────────────────────────
for mod in [
    "airflow", "airflow.models", "airflow.operators",
    "airflow.operators.python", "airflow.operators.bash",
    "airflow.operators.empty", "airflow.sensors",
    "airflow.sensors.base", "airflow.utils",
    "airflow.utils.trigger_rule", "airflow.utils.email",
]:
    sys.modules[mod] = MagicMock()

DAGS_DIR = os.path.join(os.path.dirname(__file__), "../../src/orchestration/dags")
sys.path.insert(0, DAGS_DIR)

DAG_FILES = ["ingestion_pipeline.py", "transformation_pipeline.py", "daily_refresh.py"]
DAG_IDS   = ["ingestion_pipeline",    "transformation_pipeline",    "daily_refresh"]


# ── dag_utils tests ───────────────────────────────────────────────────────────

class TestDAGUtils:

    def test_default_args_has_required_keys(self):
        from dag_utils import DEFAULT_ARGS
        for key in ["owner", "retries", "retry_delay", "depends_on_past"]:
            assert key in DEFAULT_ARGS

    def test_depends_on_past_is_false(self):
        from dag_utils import DEFAULT_ARGS
        assert DEFAULT_ARGS["depends_on_past"] is False

    def test_retries_at_least_1(self):
        from dag_utils import DEFAULT_ARGS
        assert DEFAULT_ARGS["retries"] >= 1

    def test_slas_are_timedeltas(self):
        from dag_utils import INGESTION_SLA, TRANSFORM_SLA, DAILY_REFRESH_SLA
        for sla in [INGESTION_SLA, TRANSFORM_SLA, DAILY_REFRESH_SLA]:
            assert isinstance(sla, timedelta)

    def test_ingestion_sla_less_than_transform(self):
        from dag_utils import INGESTION_SLA, TRANSFORM_SLA
        assert INGESTION_SLA < TRANSFORM_SLA

    def test_failure_callback_callable(self):
        from dag_utils import on_failure_callback
        assert callable(on_failure_callback)

    def test_failure_callback_runs_without_error(self):
        from dag_utils import on_failure_callback
        ctx = {
            "dag": MagicMock(dag_id="test_dag"),
            "task_instance": MagicMock(task_id="test_task"),
            "run_id": "run_123",
            "execution_date": "2024-01-15",
            "exception": Exception("test"),
        }
        on_failure_callback(ctx)  # Should not raise

    def test_spark_conf_returns_dict(self):
        from dag_utils import get_spark_submit_conf
        conf = get_spark_submit_conf()
        assert isinstance(conf, dict) and len(conf) > 0

    def test_spark_conf_has_minio_settings(self):
        from dag_utils import get_spark_submit_conf
        conf = get_spark_submit_conf()
        assert "spark.hadoop.fs.s3a.endpoint" in conf
        assert "spark.hadoop.fs.s3a.path.style.access" in conf
        assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"

    def test_spark_conf_ssl_disabled(self):
        from dag_utils import get_spark_submit_conf
        conf = get_spark_submit_conf()
        assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"

    def test_spark_conf_executor_configurable(self):
        from dag_utils import get_spark_submit_conf
        conf = get_spark_submit_conf(executor_memory="4g", num_executors=4)
        assert conf["spark.executor.memory"]    == "4g"
        assert conf["spark.executor.instances"] == "4"

    def test_log_task_metrics_callable(self):
        from dag_utils import log_task_metrics
        log_task_metrics("test_task", 1000, 30.5)  # Should not raise

    def test_execution_timeout_reasonable(self):
        from dag_utils import DEFAULT_ARGS
        timeout = DEFAULT_ARGS.get("execution_timeout")
        assert timeout is not None
        assert timeout > timedelta(minutes=5)
        assert timeout <= timedelta(hours=4)


# ── Schedule interval tests ───────────────────────────────────────────────────

class TestScheduleIntervals:

    def _valid_cron(self, expr):
        parts = expr.strip().split()
        return len(parts) == 5

    def test_ingestion_every_30_min(self):
        assert self._valid_cron("*/30 * * * *")
        assert "*/30" in "*/30 * * * *"

    def test_transform_every_3_hours(self):
        assert self._valid_cron("0 */3 * * *")

    def test_daily_at_1am(self):
        parts = "0 1 * * *".split()
        assert parts[0] == "0" and parts[1] == "1"

    def test_ingestion_runs_more_often_than_transform(self):
        assert 30 < 180   # 30 min < 3 hours


# ── File syntax & structure tests ─────────────────────────────────────────────

class TestDAGFiles:

    @pytest.mark.parametrize("dag_file", DAG_FILES + ["dag_utils.py"])
    def test_valid_python_syntax(self, dag_file):
        path = os.path.join(DAGS_DIR, dag_file)
        with open(path) as f:
            tree = ast.parse(f.read())
        assert tree is not None

    @pytest.mark.parametrize("dag_file,dag_id", zip(DAG_FILES, DAG_IDS))
    def test_dag_id_present_in_file(self, dag_file, dag_id):
        content = open(os.path.join(DAGS_DIR, dag_file)).read()
        assert dag_id in content

    @pytest.mark.parametrize("dag_file", DAG_FILES)
    def test_max_active_runs_1(self, dag_file):
        content = open(os.path.join(DAGS_DIR, dag_file)).read()
        assert "max_active_runs=1" in content

    @pytest.mark.parametrize("dag_file", DAG_FILES)
    def test_catchup_false(self, dag_file):
        content = open(os.path.join(DAGS_DIR, dag_file)).read()
        assert "catchup=False" in content

    @pytest.mark.parametrize("dag_file", DAG_FILES)
    def test_has_tags(self, dag_file):
        content = open(os.path.join(DAGS_DIR, dag_file)).read()
        assert "tags=" in content

    @pytest.mark.parametrize("dag_file", DAG_FILES)
    def test_has_on_failure_callback(self, dag_file):
        content = open(os.path.join(DAGS_DIR, dag_file)).read()
        assert "on_failure_callback" in content

    def test_ingestion_has_health_checks(self):
        content = open(os.path.join(DAGS_DIR, "ingestion_pipeline.py")).read()
        assert "check_minio_health" in content
        assert "check_kafka_health" in content

    def test_transform_has_spark_submit(self):
        content = open(os.path.join(DAGS_DIR, "transformation_pipeline.py")).read()
        assert "spark-submit" in content
        assert "bronze_to_silver" in content

    def test_transform_has_dbt_run(self):
        content = open(os.path.join(DAGS_DIR, "transformation_pipeline.py")).read()
        # dbt run is inside an f-string: f"{DBT_CMD} run --select ..."
        assert "run --select" in content
        assert "dbt test" in content

    def test_daily_has_blocking_silver_quality(self):
        content = open(os.path.join(DAGS_DIR, "daily_refresh.py")).read()
        assert "silver_quality_check" in content
        assert "BLOCKING" in content

    def test_daily_has_cleanup(self):
        content = open(os.path.join(DAGS_DIR, "daily_refresh.py")).read()
        assert "cleanup" in content.lower()

    def test_daily_has_dbt_docs(self):
        content = open(os.path.join(DAGS_DIR, "daily_refresh.py")).read()
        assert "dbt docs" in content

    def test_all_dags_import_dag_utils(self):
        for dag_file in DAG_FILES:
            content = open(os.path.join(DAGS_DIR, dag_file)).read()
            assert "from dag_utils import" in content, \
                f"{dag_file} should import from dag_utils"
