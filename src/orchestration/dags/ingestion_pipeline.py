"""
dags/ingestion_pipeline.py — Ingestion Pipeline DAG.

Schedule:   Every 30 minutes
Purpose:    Simulate data + ingest to Bronze layer
SLA:        1 hour

Task flow:
    check_minio ──┐
    check_kafka ──┴── health_checks_passed
                            │
        simulate_twitter ───┤
        simulate_youtube ───┤── simulation_complete
        simulate_reddit  ───┘
                            │
        consume_kafka ──────┤
        ingest_batch   ─────┴── ingestion_complete
                            │
                  validate_bronze
                            │
              ingestion_pipeline_complete
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_utils import (
    DEFAULT_ARGS, INGESTION_SLA,
    on_failure_callback, on_success_callback, on_sla_miss_callback,
    check_minio_health, check_kafka_health, log_task_metrics,
)

with DAG(
    dag_id="ingestion_pipeline",
    description="Simulate social media data and ingest to Bronze layer",
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure_callback},
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze"],
    on_success_callback=on_success_callback,
    sla_miss_callback=on_sla_miss_callback,
    doc_md="""
## Ingestion Pipeline — Every 30 minutes

Generates simulated social media events and ingests them to Bronze.

**Sources:**
- Twitter → Kafka → Bronze (streaming)
- YouTube → MinIO landing zone → Bronze (batch)
- Reddit  → MinIO landing zone → Bronze (batch)

**SLA:** 1 hour  |  **Retries:** 2× with exponential backoff
""",
) as dag:

    # ── Health checks ─────────────────────────────────────────────────────────

    check_minio = PythonOperator(
        task_id="check_minio_health",
        python_callable=check_minio_health,
        sla=INGESTION_SLA,
    )

    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
        sla=INGESTION_SLA,
    )

    health_ok = EmptyOperator(
        task_id="health_checks_passed",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Simulation ────────────────────────────────────────────────────────────

    simulate_twitter = BashOperator(
        task_id="simulate_twitter_events",
        bash_command=(
            "cd /opt/airflow/src/simulators && "
            "python twitter_simulator.py --events 5000 --rate 100"
        ),
        sla=INGESTION_SLA,
        doc_md="Send ~5K tweet events to Kafka `social.tweets` topic.",
    )

    simulate_youtube = BashOperator(
        task_id="simulate_youtube_batch",
        bash_command=(
            "cd /opt/airflow/src/simulators && "
            "python youtube_simulator.py --videos 50"
        ),
        sla=INGESTION_SLA,
        doc_md="Generate 50 YouTube videos + comments → MinIO landing zone.",
    )

    simulate_reddit = BashOperator(
        task_id="simulate_reddit_batch",
        bash_command=(
            "cd /opt/airflow/src/simulators && "
            "python reddit_simulator.py --posts 200"
        ),
        sla=INGESTION_SLA,
        doc_md="Generate 200 Reddit posts → MinIO landing zone.",
    )

    simulation_done = EmptyOperator(
        task_id="simulation_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Ingestion to Bronze ───────────────────────────────────────────────────

    consume_kafka = BashOperator(
        task_id="consume_kafka_to_bronze",
        bash_command=(
            "cd /opt/airflow/src/ingestion && "
            "timeout 120 python kafka_consumer.py --flush-interval 30 || "
            "[ $? -eq 124 ]"    # Exit code 124 = timeout = OK
        ),
        sla=INGESTION_SLA,
        doc_md="Consume Kafka topics for 2 min, flush to Bronze, commit offsets.",
    )

    ingest_batch = BashOperator(
        task_id="ingest_batch_to_bronze",
        bash_command=(
            "cd /opt/airflow/src/ingestion && "
            "python batch_ingestion.py --source youtube && "
            "python batch_ingestion.py --source reddit"
        ),
        sla=INGESTION_SLA,
        doc_md="Ingest YouTube and Reddit files from landing zone to Bronze.",
    )

    ingestion_done = EmptyOperator(
        task_id="ingestion_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Bronze validation ─────────────────────────────────────────────────────

    def _validate_bronze(**context):
        """Verify Bronze layer received new files for today."""
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
            raise ValueError(
                f"Bronze validation FAILED — no files at bronze/{prefix}. "
                "Check simulation and ingestion logs."
            )
        print(f"[BRONZE OK] {count} file(s) at bronze/{prefix}")
        return count

    validate_bronze = PythonOperator(
        task_id="validate_bronze_records",
        python_callable=_validate_bronze,
        sla=INGESTION_SLA,
        doc_md="Sanity check: fails if Bronze has no new files today.",
    )

    done = EmptyOperator(task_id="ingestion_pipeline_complete")

    # ── Dependencies ──────────────────────────────────────────────────────────

    [check_minio, check_kafka] >> health_ok
    health_ok >> [simulate_twitter, simulate_youtube, simulate_reddit]
    [simulate_twitter, simulate_youtube, simulate_reddit] >> simulation_done
    simulation_done >> [consume_kafka, ingest_batch]
    [consume_kafka, ingest_batch] >> ingestion_done
    ingestion_done >> validate_bronze >> done
