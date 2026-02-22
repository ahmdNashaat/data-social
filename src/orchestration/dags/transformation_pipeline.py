"""
dags/transformation_pipeline.py — Transformation Pipeline DAG.

Schedule:   Every 3 hours
Purpose:    Bronze → Silver (PySpark) → Gold (dbt)
SLA:        4 hours

Task flow:
    check_bronze_has_data
            │
    bronze_to_silver_spark       (PySpark job via spark-submit)
            │
    ┌──── silver_quality_check   (Great Expectations)
    │             │
    │    ┌────────┴────────┐
    │    │  dbt_run_staging │    (dbt staging models)
    │    └────────┬─────────┘
    │             │
    │    ┌────────┴──────────────┐
    │    │  dbt_run_intermediate │
    │    └────────┬──────────────┘
    │             │
    │    ┌────────┴──────┐
    │    │  dbt_run_facts │
    │    └────────┬───────┘
    │    ┌────────┴──────────┐
    │    │  dbt_run_dimensions│
    │    └────────┬────────────┘
    │             │
    │    dbt_test_gold_layer
    └──────────── │
                  │
    transformation_pipeline_complete
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_utils import (
    DEFAULT_ARGS, TRANSFORM_SLA,
    on_failure_callback, on_success_callback, on_sla_miss_callback,
    check_minio_health, get_spark_submit_conf, log_task_metrics,
)

# DBT project path inside Airflow container
DBT_PROJECT_DIR = "/opt/airflow/src/transformation/dbt"
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"

with DAG(
    dag_id="transformation_pipeline",
    description="Transform Bronze → Silver (PySpark) → Gold (dbt)",
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure_callback},
    schedule_interval="0 */3 * * *",      # Every 3 hours
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "silver", "gold", "spark", "dbt"],
    on_success_callback=on_success_callback,
    sla_miss_callback=on_sla_miss_callback,
    doc_md="""
## Transformation Pipeline — Every 3 hours

Transforms raw Bronze data into analytics-ready Gold tables.

**Stages:**
1. **PySpark** — Bronze → Silver (deduplicate, clean, standardize)
2. **Great Expectations** — Silver quality gate
3. **dbt** — Silver → Gold (Star Schema: facts + dimensions)
4. **dbt test** — Gold layer schema + data tests

**SLA:** 4 hours  |  **Retries:** 2× with exponential backoff
""",
) as dag:

    # ── 1. Pre-check: Bronze has data ─────────────────────────────────────────

    def _check_bronze_data(**context):
        """Verify Bronze has unprocessed data before running Spark."""
        import boto3
        from botocore.client import Config
        from datetime import date

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )
        today = date.today()
        prefix = (
            f"twitter/tweets/year={today.year:04d}/"
            f"month={today.month:02d}/day={today.day:02d}/"
        )
        resp = s3.list_objects_v2(Bucket="bronze", Prefix=prefix)
        count = resp.get("KeyCount", 0)

        if count == 0:
            # Skip silently — no data yet, not an error
            print(f"[SKIP] No Bronze data at bronze/{prefix} — skipping transform.")
            return 0

        print(f"[PRE-CHECK] Found {count} Bronze file(s) — proceeding with transform.")
        return count

    check_bronze = PythonOperator(
        task_id="check_bronze_has_data",
        python_callable=_check_bronze_data,
        sla=TRANSFORM_SLA,
        doc_md="Verify Bronze layer has data for today before running Spark.",
    )

    # ── 2. PySpark: Bronze → Silver ───────────────────────────────────────────

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_spark",
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
        sla=TRANSFORM_SLA,
        execution_timeout=timedelta(hours=2),
        doc_md=(
            "PySpark job: Bronze → Silver. "
            "Deduplicates, cleans, normalizes timestamps, enforces schemas. "
            "Writes partitioned Parquet to MinIO silver bucket."
        ),
    )

    # ── 3. Silver quality gate ────────────────────────────────────────────────

    silver_quality = BashOperator(
        task_id="silver_quality_check",
        bash_command=(
            "cd /opt/airflow && "
            "great_expectations checkpoint run silver_quality_check "
            "--result-format SUMMARY"
        ),
        sla=TRANSFORM_SLA,
        doc_md=(
            "Great Expectations checkpoint: validates Silver layer data quality. "
            "BLOCKS pipeline if: null PKs found, duplicates > 0.1%, "
            "count dropped > 20% vs Bronze."
        ),
    )

    # ── 4. dbt: Silver → Gold ─────────────────────────────────────────────────

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir .",
        sla=TRANSFORM_SLA,
        doc_md="dbt: compile and run staging models (1:1 Silver source views).",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT_CMD} run --select intermediate --profiles-dir .",
        sla=TRANSFORM_SLA,
        doc_md="dbt: run intermediate models (enrichment, date parts, engagement metrics).",
    )

    dbt_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=f"{DBT_CMD} run --select marts.facts --profiles-dir .",
        sla=TRANSFORM_SLA,
        doc_md="dbt: build fact tables (fact_engagements, fact_post_metrics).",
    )

    dbt_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=f"{DBT_CMD} run --select marts.dimensions --profiles-dir .",
        sla=TRANSFORM_SLA,
        doc_md="dbt: build dimension tables (dim_date, dim_posts).",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_gold_layer",
        bash_command=f"{DBT_CMD} test --profiles-dir . --vars '{{\"test_env\": true}}'",
        sla=TRANSFORM_SLA,
        doc_md=(
            "dbt test: run all schema + data tests on Gold layer. "
            "Checks: not_null, unique, accepted_values, "
            "engagement_rate in [0,100], no null foreign keys."
        ),
    )

    # ── 5. Completion ─────────────────────────────────────────────────────────

    done = EmptyOperator(
        task_id="transformation_pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────

    check_bronze >> bronze_to_silver >> silver_quality
    silver_quality >> dbt_staging >> dbt_intermediate
    dbt_intermediate >> [dbt_facts, dbt_dimensions]
    [dbt_facts, dbt_dimensions] >> dbt_test >> done
