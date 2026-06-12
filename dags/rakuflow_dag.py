"""
rakuflow_dag.py — Airflow DAG for the RakuFlow data engineering pipeline.

Orchestrates the full pipeline on a daily schedule:
    1. Start Kafka producer (ingests Olist CSV → Kafka topics)
    2. Run Spark consumer (Kafka → PostgreSQL staging)
    3. dbt run (staging → mart models)
    4. dbt test (data quality assertions)
    5. Great Expectations validation suite
    6. Streamlit cache refresh

Schedule: @daily (midnight UTC)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Default arguments ──────────────────────────────────────────────────────────
DEFAULT_ARGS: dict = {
    "owner": "rakuflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "data-eng@rakuflow.io")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── Path configuration ─────────────────────────────────────────────────────────
RAKUFLOW_HOME: str = os.getenv("RAKUFLOW_HOME", "/opt/rakuflow")
DBT_PROJECT_DIR: str = f"{RAKUFLOW_HOME}/dbt_project"
DBT_PROFILES_DIR: str = f"{RAKUFLOW_HOME}/dbt_project"
INGESTION_DIR: str = f"{RAKUFLOW_HOME}/ingestion"
PROCESSING_DIR: str = f"{RAKUFLOW_HOME}/processing"
QUALITY_DIR: str = f"{RAKUFLOW_HOME}/quality"
VENV_PYTHON: str = f"{RAKUFLOW_HOME}/.venv/bin/python"
STREAMLIT_HOST: str = os.getenv("STREAMLIT_HOST", "streamlit")
STREAMLIT_PORT: str = os.getenv("STREAMLIT_PORT", "8501")


# ── Python callables ───────────────────────────────────────────────────────────

def run_great_expectations() -> None:
    """
    Run the Great Expectations validation suite for RakuFlow.

    Executes the expectations.py script which validates fact_orders and
    dim_customers against pre-defined expectation suites.

    Raises:
        RuntimeError: If any expectation suite fails validation.
    """
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, f"{QUALITY_DIR}/expectations.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Great Expectations validation FAILED:\n{result.stderr}"
        )
    print(result.stdout)


def refresh_streamlit_cache() -> None:
    """
    Trigger a Streamlit cache refresh by sending a health-check request.

    In production, this would call a Streamlit admin endpoint or restart
    the service. Here we send a simple HTTP request to signal completion.
    """
    import http.client

    try:
        conn = http.client.HTTPConnection(STREAMLIT_HOST, int(STREAMLIT_PORT), timeout=10)
        conn.request("GET", "/_stcore/health")
        response = conn.getresponse()
        print(f"Streamlit health check: HTTP {response.status}")
    except Exception as exc:  # noqa: BLE001
        # Non-fatal — dashboard will load fresh data on next user request
        print(f"Dashboard cache refresh skipped: {exc}")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="rakuflow_daily_pipeline",
    description="RakuFlow end-to-end e-commerce analytics pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["rakuflow", "e-commerce", "data-engineering"],
) as dag:

    # ── Task 1: Start Kafka Producer ──────────────────────────────────────────
    start_kafka_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command=(
            f"cd {INGESTION_DIR} && "
            f"{VENV_PYTHON} producer.py"
        ),
        env={
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "DATA_DIR": f"{RAKUFLOW_HOME}/data/raw",
            "MESSAGE_DELAY_SEC": "0.01",  # Faster in DAG (not demo mode)
        },
        execution_timeout=timedelta(hours=2),
        doc_md=(
            "**start_kafka_producer**\n\n"
            "Reads Olist CSV files and publishes OrderEvent and PaymentEvent "
            "messages to Kafka topics `rakuflow-orders` and `rakuflow-payments`."
        ),
    )

    # ── Task 2: Run Spark Consumer ────────────────────────────────────────────
    run_spark_consumer = BashOperator(
        task_id="run_spark_consumer",
        bash_command=(
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            f"--jars /opt/jars/postgresql-42.7.3.jar "
            f"{PROCESSING_DIR}/spark_consumer.py"
        ),
        env={
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "rakuflow"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "rakuflow"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "rakuflow"),
            "DATA_DIR": f"{RAKUFLOW_HOME}/data/raw",
        },
        execution_timeout=timedelta(hours=1),
        doc_md=(
            "**run_spark_consumer**\n\n"
            "PySpark batch job: reads from Kafka, cleans and enriches data, "
            "then writes to `staging.raw_orders` and `staging.raw_payments` "
            "in PostgreSQL."
        ),
    )

    # ── Task 3: dbt run ───────────────────────────────────────────────────────
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --target dev"
        ),
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "rakuflow"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "rakuflow"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "rakuflow"),
        },
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "**run_dbt_models**\n\n"
            "Runs all dbt models: staging layer → mart layer. "
            "Creates `fact_orders`, `dim_customers`, `dim_sellers`, `agg_daily_gmv`."
        ),
    )

    # ── Task 4: dbt test ──────────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --target dev"
        ),
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "rakuflow"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "rakuflow"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "rakuflow"),
        },
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "**run_dbt_tests**\n\n"
            "Runs all dbt tests: not_null, unique, and accepted_values "
            "assertions across all models."
        ),
    )

    # ── Task 5: Great Expectations ────────────────────────────────────────────
    run_expectations = PythonOperator(
        task_id="run_expectations",
        python_callable=run_great_expectations,
        doc_md=(
            "**run_expectations**\n\n"
            "Runs Great Expectations validation suite against "
            "`fact_orders` and `dim_customers` tables."
        ),
    )

    # ── Task 6: Refresh dashboard ─────────────────────────────────────────────
    refresh_dashboard = PythonOperator(
        task_id="refresh_dashboard",
        python_callable=refresh_streamlit_cache,
        doc_md=(
            "**refresh_dashboard**\n\n"
            "Sends a health-check request to the Streamlit service to "
            "trigger cache invalidation for fresh dashboard data."
        ),
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    (
        start_kafka_producer
        >> run_spark_consumer
        >> run_dbt_models
        >> run_dbt_tests
        >> run_expectations
        >> refresh_dashboard
    )
