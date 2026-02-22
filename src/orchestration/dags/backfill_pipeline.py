"""
dags/backfill_pipeline.py — Manual Backfill Pipeline DAG

Schedule: None (manually triggered)
Purpose:  Re-processes data for a specific date range.
          Useful for: fixing bugs, schema changes, or initial data load.

Trigger via Airflow UI with config:
    {"start_date": "2024-01-01", "end_date": "2024-01-31"}

Or via CLI:
    airflow dags trigger backfill_pipeline \\
        --conf '{"start_date": "2024-01-01", "end_date": "2024-01-07"}'
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from dag_utils import (
    make_default_args,
    generate_run_id,
    SRC_BASE,
)


# ── Helper ─────────────────────────────────────────────────────────────────────

def validate_backfill_config(**context) -> dict:
    """
    Validate the backfill configuration passed via DAG trigger.
    Returns the validated config dict.
    """
    conf = context["dag_run"].conf or {}

    start_date = conf.get("start_date")
    end_date   = conf.get("end_date")

    if not start_date or not end_date:
        raise ValueError(
            "Backfill requires 'start_date' and 'end_date' in DAG run config. "
            "Example: {\"start_date\": \"2024-01-01\", \"end_date\": \"2024-01-07\"}"
        )

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end   = datetime.strptime(end_date,   "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format (use YYYY-MM-DD): {e}")

    if start > end:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")

    num_days = (end - start).days + 1
    if num_days > 90:
        raise ValueError(
            f"Backfill range is {num_days} days — max 90 days per run. "
            "Split into smaller ranges."
        )

    config = {
        "start_date":    start_date,
        "end_date":      end_date,
        "num_days":      num_days,
        "reprocess":     conf.get("reprocess", False),
    }

    context["task_instance"].xcom_push(key="backfill_config", value=config)
    print(f"[BACKFILL] Config validated: {config}")
    return config


def run_backfill_ingestion(**context) -> None:
    """Re-ingest landing zone files with --reprocess flag."""
    import subprocess
    config = context["task_instance"].xcom_pull(
        task_ids="validate_config", key="backfill_config"
    )
    reprocess_flag = "--reprocess" if config.get("reprocess") else ""

    result = subprocess.run(
        [
            "python3", f"{SRC_BASE}/ingestion/batch_ingestion.py",
            reprocess_flag,
        ],
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Backfill ingestion failed: {result.stderr}")
    print(result.stdout)


def run_backfill_spark(**context) -> None:
    """Run Spark transformation for each day in the backfill range."""
    import subprocess
    from datetime import date, timedelta

    config = context["task_instance"].xcom_pull(
        task_ids="validate_config", key="backfill_config"
    )
    start = datetime.strptime(config["start_date"], "%Y-%m-%d").date()
    end   = datetime.strptime(config["end_date"],   "%Y-%m-%d").date()

    current = start
    while current <= end:
        date_str = current.isoformat()
        print(f"[BACKFILL] Processing Spark for {date_str}...")

        result = subprocess.run(
            [
                "spark-submit",
                "--master", "spark://spark-master:7077",
                f"{SRC_BASE}/transformation/spark_jobs/bronze_to_silver.py",
                "--date", date_str,
            ],
            capture_output=True,
            text=True,
            timeout=1800,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Spark failed for {date_str}: {result.stderr}")
        print(f"[BACKFILL] Spark complete for {date_str}")
        current += timedelta(days=1)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="backfill_pipeline",
    description="Manual backfill pipeline — re-processes Bronze→Silver→Gold for a date range",
    schedule_interval=None,                  # Manual trigger only
    default_args=make_default_args(
        retries=1,
        retry_delay_min=10,
    ),
    catchup=False,
    max_active_runs=1,
    tags=["backfill", "manual", "admin"],
    doc_md="""
## Backfill Pipeline

Manually triggered DAG for re-processing historical data.

### Usage
Trigger via Airflow UI with JSON config:
```json
{
    "start_date": "2024-01-01",
    "end_date": "2024-01-07",
    "reprocess": true
}
```

### When to Use
- After fixing a bug in transformation logic
- After a schema change
- For initial historical data load
- After a pipeline outage

### Limits
- Max 90 days per run (split larger ranges)
""",
) as dag:

    # ── Validate config ────────────────────────────────────────────────────────
    validate_config = PythonOperator(
        task_id="validate_config",
        python_callable=validate_backfill_config,
        doc_md="Validates the backfill date range from DAG run config.",
    )

    # ── Generate run ID ────────────────────────────────────────────────────────
    gen_run_id = PythonOperator(
        task_id="generate_run_id",
        python_callable=generate_run_id,
    )

    # ── Re-ingest (optional — only if --reprocess) ────────────────────────────
    backfill_ingest = PythonOperator(
        task_id="backfill_ingestion",
        python_callable=run_backfill_ingestion,
        doc_md="Re-ingests landing zone files to Bronze (if reprocess=true).",
    )

    # ── Spark backfill (one run per day in range) ──────────────────────────────
    backfill_spark = PythonOperator(
        task_id="backfill_spark_bronze_to_silver",
        python_callable=run_backfill_spark,
        execution_timeout=timedelta(hours=6),
        doc_md="Runs PySpark bronze_to_silver for each day in the range.",
    )

    # ── dbt full refresh ───────────────────────────────────────────────────────
    backfill_dbt = BashOperator(
        task_id="backfill_dbt_full_refresh",
        bash_command=(
            f"cd {SRC_BASE}/transformation/dbt && "
            "dbt run --profiles-dir . --full-refresh && "
            "dbt test --profiles-dir ."
        ),
        execution_timeout=timedelta(hours=1),
        doc_md="Runs dbt with --full-refresh to rebuild all Gold tables.",
    )

    # ── Quality check ──────────────────────────────────────────────────────────
    quality_check = BashOperator(
        task_id="quality_check_all_layers",
        bash_command=(
            "cd /opt/airflow/quality && "
            "python3 -m great_expectations checkpoint run silver_quality_check && "
            "python3 -m great_expectations checkpoint run gold_quality_check"
        ),
        doc_md="Runs quality checks on both Silver and Gold after backfill.",
    )

    # ── Complete ───────────────────────────────────────────────────────────────
    backfill_complete = EmptyOperator(task_id="backfill_complete")

    # ── Dependencies ───────────────────────────────────────────────────────────
    (
        validate_config
        >> gen_run_id
        >> backfill_ingest
        >> backfill_spark
        >> backfill_dbt
        >> quality_check
        >> backfill_complete
    )
