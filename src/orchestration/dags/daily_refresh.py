"""
dags/daily_refresh.py — Daily Full Refresh DAG.

Schedule:   Daily at 01:00 UTC
Purpose:    Full pipeline run + comprehensive quality checks +
            data docs publish + old file cleanup
SLA:        3 hours

Task flow:
    health_checks (minio + kafka)
            │
    full_simulation           (large-scale data generation)
            │
    full_ingestion            (all sources → Bronze)
            │
    bronze_quality_check      (Great Expectations — non-blocking)
            │
    spark_bronze_to_silver    (full date partition refresh)
            │
    silver_quality_check      (BLOCKING — halt on failure)
            │
    dbt_seed                  (refresh static seed data)
    dbt_full_run              (all models)
    dbt_full_test             (all tests)
            │
    gold_quality_check        (alert only)
    publish_dbt_docs          (generate + save docs)
            │
    cleanup_old_files         (remove landing zone files > 7 days)
            │
    daily_refresh_complete
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_utils import (
    DEFAULT_ARGS, DAILY_REFRESH_SLA,
    on_failure_callback, on_success_callback, on_sla_miss_callback,
    check_minio_health, check_kafka_health,
)

DBT_DIR = "/opt/airflow/src/transformation/dbt"
DBT = f"cd {DBT_DIR} && dbt"
GE_DIR = "/opt/airflow/quality"

with DAG(
    dag_id="daily_refresh",
    description="Daily full pipeline run with quality checks and cleanup",
    default_args={
        **DEFAULT_ARGS,
        "on_failure_callback": on_failure_callback,
        "retries": 1,                          # Fewer retries for daily — alert fast
        "execution_timeout": timedelta(hours=3),
    },
    schedule_interval="0 1 * * *",             # 01:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["daily", "full-refresh", "quality"],
    on_success_callback=on_success_callback,
    sla_miss_callback=on_sla_miss_callback,
    doc_md="""
## Daily Refresh — 01:00 UTC

Full pipeline run with comprehensive data quality checks.

**Scope:**
- Large-scale simulation (50K+ events)
- Full Bronze → Silver → Gold refresh
- All Great Expectations checkpoints
- dbt docs generation
- Landing zone cleanup (files > 7 days)

**SLA:** 3 hours  |  **Retries:** 1×
""",
) as dag:

    # ── 1. Health checks ──────────────────────────────────────────────────────

    check_minio = PythonOperator(
        task_id="check_minio",
        python_callable=check_minio_health,
        sla=DAILY_REFRESH_SLA,
    )

    check_kafka = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka_health,
        sla=DAILY_REFRESH_SLA,
    )

    health_ok = EmptyOperator(
        task_id="health_checks_ok",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 2. Full-scale simulation ──────────────────────────────────────────────

    simulate_all = BashOperator(
        task_id="simulate_all_sources",
        bash_command=(
            "cd /opt/airflow/src/simulators && "
            "python run_all.py --scale medium"     # 50K tweets, 500 videos, 2K posts
        ),
        execution_timeout=timedelta(minutes=30),
        sla=DAILY_REFRESH_SLA,
        doc_md="Run all simulators at medium scale: ~50K tweets, 500 YT videos, 2K Reddit posts.",
    )

    # ── 3. Full ingestion ─────────────────────────────────────────────────────

    ingest_streaming = BashOperator(
        task_id="ingest_twitter_streaming",
        bash_command=(
            "cd /opt/airflow/src/ingestion && "
            "timeout 300 python kafka_consumer.py --flush-interval 60 || "
            "[ $? -eq 124 ]"                  # Timeout is expected/OK
        ),
        sla=DAILY_REFRESH_SLA,
        doc_md="Consume all buffered Kafka messages → Bronze (5 min window).",
    )

    ingest_batch_all = BashOperator(
        task_id="ingest_all_batch_sources",
        bash_command=(
            "cd /opt/airflow/src/ingestion && "
            "python batch_ingestion.py"         # All sources (no --source flag)
        ),
        sla=DAILY_REFRESH_SLA,
        doc_md="Ingest all landing zone files (YouTube + Reddit) → Bronze.",
    )

    ingestion_done = EmptyOperator(
        task_id="ingestion_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 4. Bronze quality check (non-blocking) ────────────────────────────────

    bronze_quality = BashOperator(
        task_id="bronze_quality_check",
        bash_command=(
            f"cd {GE_DIR} && "
            "great_expectations checkpoint run bronze_quality_check "
            "--result-format SUMMARY || "
            "echo '[WARN] Bronze quality issues found — see GE docs'"
        ),
        # Non-blocking: || echo means task always succeeds, but logs the warning
        sla=DAILY_REFRESH_SLA,
        doc_md=(
            "Great Expectations Bronze checkpoint. NON-BLOCKING — "
            "logs warnings but pipeline continues. Issues addressed at Silver."
        ),
    )

    # ── 5. PySpark: Bronze → Silver (full refresh) ────────────────────────────

    spark_transform = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--executor-memory 2g "
            "--executor-cores 2 "
            "--num-executors 2 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "--conf 'spark.hadoop.fs.s3a.connection.ssl.enabled=false' "
            "/opt/airflow/src/transformation/spark_jobs/bronze_to_silver.py "
            "--date {{ ds }}"
        ),
        execution_timeout=timedelta(hours=2),
        sla=DAILY_REFRESH_SLA,
        doc_md="Full Bronze → Silver transformation for today's date partition.",
    )

    # ── 6. Silver quality gate (BLOCKING) ─────────────────────────────────────

    silver_quality = BashOperator(
        task_id="silver_quality_check",
        bash_command=(
            f"cd {GE_DIR} && "
            "great_expectations checkpoint run silver_quality_check "
            "--result-format SUMMARY"
        ),
        # BLOCKING — no || fallback. If Silver quality fails, DAG fails.
        sla=DAILY_REFRESH_SLA,
        doc_md=(
            "Great Expectations Silver checkpoint. BLOCKING — "
            "halts pipeline if null PKs, duplicate rate > 0.1%, "
            "or count completeness < 80% of Bronze."
        ),
    )

    # ── 7. dbt full run ───────────────────────────────────────────────────────

    dbt_deps = BashOperator(
        task_id="dbt_install_packages",
        bash_command=f"{DBT} deps --profiles-dir .",
        sla=DAILY_REFRESH_SLA,
        doc_md="Install/update dbt packages (dbt_utils, dbt_expectations).",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed_static_data",
        bash_command=f"{DBT} seed --profiles-dir . --full-refresh",
        sla=DAILY_REFRESH_SLA,
        doc_md="Load static seed CSV data (platform reference tables, etc.).",
    )

    dbt_run_all = BashOperator(
        task_id="dbt_run_all_models",
        bash_command=f"{DBT} run --profiles-dir . --full-refresh",
        execution_timeout=timedelta(hours=1),
        sla=DAILY_REFRESH_SLA,
        doc_md="Full dbt run: staging → intermediate → facts + dimensions.",
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all_models",
        bash_command=f"{DBT} test --profiles-dir .",
        sla=DAILY_REFRESH_SLA,
        doc_md="Run all dbt schema + data tests across all Gold models.",
    )

    # ── 8. Gold quality check (alert only) ───────────────────────────────────

    gold_quality = BashOperator(
        task_id="gold_quality_check",
        bash_command=(
            f"cd {GE_DIR} && "
            "great_expectations checkpoint run gold_quality_check "
            "--result-format SUMMARY || "
            "echo '[WARN] Gold quality issues — see GE docs'"
        ),
        sla=DAILY_REFRESH_SLA,
        doc_md="Great Expectations Gold checkpoint. Alert-only — does not block pipeline.",
    )

    # ── 9. Publish dbt docs ───────────────────────────────────────────────────

    publish_docs = BashOperator(
        task_id="publish_dbt_docs",
        bash_command=(
            f"{DBT} docs generate --profiles-dir . && "
            # Copy docs to MinIO for persistence
            "mc alias set local http://minio:9000 minioadmin minioadmin && "
            f"mc cp --recursive {DBT_DIR}/target/catalog.json local/quality-reports/dbt-docs/ && "
            f"mc cp --recursive {DBT_DIR}/target/manifest.json local/quality-reports/dbt-docs/ && "
            "echo 'dbt docs published to MinIO'"
        ),
        sla=DAILY_REFRESH_SLA,
        doc_md="Generate dbt documentation and save to MinIO quality-reports bucket.",
    )

    # ── 10. Cleanup old landing zone files ───────────────────────────────────

    def _cleanup_old_files(**context):
        """
        Delete landing zone files older than 7 days.
        These have already been ingested to Bronze — safe to remove.
        """
        import boto3
        from botocore.client import Config
        from datetime import date, timedelta

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

        bucket = "landing-zone"
        cutoff_date = date.today() - timedelta(days=7)
        deleted = 0

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                # Check processed tag + age
                try:
                    tags = s3.get_object_tagging(Bucket=bucket, Key=obj["Key"])
                    tag_dict = {t["Key"]: t["Value"] for t in tags.get("TagSet", [])}
                    is_processed = tag_dict.get("processed") == "true"

                    obj_date = obj["LastModified"].date()
                    if is_processed and obj_date < cutoff_date:
                        s3.delete_object(Bucket=bucket, Key=obj["Key"])
                        deleted += 1
                except Exception:
                    pass  # Skip if can't check tags

        print(f"[CLEANUP] Deleted {deleted} old landing zone files (> 7 days old, processed)")
        return deleted

    cleanup = PythonOperator(
        task_id="cleanup_old_landing_files",
        python_callable=_cleanup_old_files,
        sla=DAILY_REFRESH_SLA,
        doc_md=(
            "Delete landing zone files older than 7 days that have been "
            "successfully ingested to Bronze. Prevents storage bloat."
        ),
    )

    # ── 11. Daily summary ─────────────────────────────────────────────────────

    def _daily_summary(**context):
        """Log a summary of the daily refresh run."""
        print(
            f"\n{'='*60}\n"
            f"[DAILY REFRESH SUMMARY]\n"
            f"  Date:       {context['ds']}\n"
            f"  Run ID:     {context['run_id']}\n"
            f"  Status:     SUCCESS\n"
            f"  Finished:   {context['ts']}\n"
            f"{'='*60}"
        )

    daily_summary = PythonOperator(
        task_id="log_daily_summary",
        python_callable=_daily_summary,
    )

    done = EmptyOperator(
        task_id="daily_refresh_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────

    # Health → Simulate
    [check_minio, check_kafka] >> health_ok >> simulate_all

    # Simulate → Ingest (parallel streams)
    simulate_all >> [ingest_streaming, ingest_batch_all]
    [ingest_streaming, ingest_batch_all] >> ingestion_done

    # Ingest → Bronze quality (non-blocking) → Spark
    ingestion_done >> bronze_quality >> spark_transform

    # Spark → Silver quality (BLOCKING)
    spark_transform >> silver_quality

    # Silver quality → dbt (sequential: deps → seed → run → test)
    silver_quality >> dbt_deps >> dbt_seed >> dbt_run_all >> dbt_test_all

    # dbt test → Gold quality + docs (parallel)
    dbt_test_all >> [gold_quality, publish_docs]

    # Gold quality + docs → cleanup → summary → done
    [gold_quality, publish_docs] >> cleanup >> daily_summary >> done
