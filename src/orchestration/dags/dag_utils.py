"""
dags/dag_utils.py — Shared utilities for all Airflow DAGs.

Provides:
  - Default DAG arguments (retries, alerts, SLAs)
  - Alert callbacks (on_failure, on_sla_miss)
  - Pipeline health check helpers
  - Spark submit configuration
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict


# ── Default DAG arguments ─────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                      "data-engineering",
    "depends_on_past":            False,
    "start_date":                 datetime(2024, 1, 1, tzinfo=timezone.utc),
    "email_on_failure":           False,
    "email_on_retry":             False,
    "retries":                    2,
    "retry_delay":                timedelta(minutes=5),
    "retry_exponential_backoff":  True,
    "max_retry_delay":            timedelta(minutes=30),
    "execution_timeout":          timedelta(hours=2),
}

# SLAs — alert if DAG not complete within this window
INGESTION_SLA     = timedelta(hours=1)
TRANSFORM_SLA     = timedelta(hours=4)
DAILY_REFRESH_SLA = timedelta(hours=3)


# ── Alert callbacks ───────────────────────────────────────────────────────────

def on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Called when a task fails after all retries.
    Logs a structured alert — replace with Slack/PagerDuty in production.
    """
    dag_id    = context["dag"].dag_id
    task_id   = context["task_instance"].task_id
    run_id    = context["run_id"]
    exec_dt   = context["execution_date"]
    exception = context.get("exception", "Unknown error")

    print(
        f"[PIPELINE FAILURE]\n"
        f"  DAG:       {dag_id}\n"
        f"  Task:      {task_id}\n"
        f"  Run ID:    {run_id}\n"
        f"  Exec Date: {exec_dt}\n"
        f"  Error:     {exception}\n"
    )
    # Production: _send_slack_alert(...) or send_email(...)


def on_success_callback(context: Dict[str, Any]) -> None:
    """Called on DAG-level success."""
    print(
        f"[PIPELINE SUCCESS] "
        f"DAG={context['dag'].dag_id} "
        f"RunID={context['run_id']}"
    )


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Called when an SLA is missed."""
    print(
        f"[SLA MISS] DAG={dag.dag_id} | "
        f"Missed tasks: {[t.task_id for t in task_list]}"
    )


# ── Infrastructure health checks ──────────────────────────────────────────────

def check_minio_health() -> bool:
    """Verify MinIO is reachable before starting pipeline tasks."""
    import urllib.request
    try:
        urllib.request.urlopen("http://minio:9000/minio/health/live", timeout=10)
        print("[HEALTH] MinIO: OK")
        return True
    except Exception as e:
        raise ConnectionError(f"MinIO health check failed: {e}")


def check_kafka_health() -> bool:
    """Verify Kafka broker is reachable."""
    import socket
    try:
        sock = socket.create_connection(("kafka", 9092), timeout=10)
        sock.close()
        print("[HEALTH] Kafka: OK")
        return True
    except Exception as e:
        raise ConnectionError(f"Kafka health check failed: {e}")


# ── Spark submit configuration ────────────────────────────────────────────────

def get_spark_submit_conf(
    executor_memory: str = "2g",
    executor_cores: int = 2,
    num_executors: int = 2,
) -> Dict[str, str]:
    """Return consistent Spark cluster configuration for all jobs."""
    return {
        "spark.executor.memory":                      executor_memory,
        "spark.executor.cores":                       str(executor_cores),
        "spark.executor.instances":                   str(num_executors),
        "spark.hadoop.fs.s3a.endpoint":               "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key":             "minioadmin",
        "spark.hadoop.fs.s3a.secret.key":             "minioadmin",
        "spark.hadoop.fs.s3a.path.style.access":      "true",
        "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }


# ── Metrics logging ───────────────────────────────────────────────────────────

def log_task_metrics(
    task_id: str,
    records_processed: int,
    duration_seconds: float,
) -> None:
    """Emit structured metrics for observability."""
    rate = records_processed / max(duration_seconds, 1)
    print(
        f"[TASK METRICS] "
        f"task={task_id} | "
        f"records={records_processed:,} | "
        f"duration={duration_seconds:.1f}s | "
        f"rate={rate:.0f} rec/s"
    )
