"""
expectations.py — Great Expectations validation suite for RakuFlow.

Validates fact_orders and dim_customers tables to ensure data quality
before the Streamlit dashboard reads production data.

Usage:
    python quality/expectations.py

Exit codes:
    0 — All expectations passed
    1 — One or more expectations failed
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext
from sqlalchemy import create_engine

# ── Configuration ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("rakuflow.expectations")

POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "rakuflow")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "rakuflow")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "rakuflow")
POSTGRES_URL: str = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

GE_ROOT_DIR: str = os.getenv("GE_ROOT_DIR", "./great_expectations")


# ── Expectation Suites ────────────────────────────────────────────────────────

def build_fact_orders_suite(validator: Any) -> None:
    """
    Define and save expectations for the fact_orders mart table.

    Args:
        validator: A Great Expectations Validator object.
    """
    # Primary key checks
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")
    validator.expect_column_values_to_not_be_null("customer_key")

    # Payment value checks
    validator.expect_column_values_to_not_be_null("payment_value")
    validator.expect_column_values_to_be_between(
        "payment_value", min_value=0, max_value=100_000
    )

    # Order status checks
    validator.expect_column_values_to_not_be_null("order_status")
    validator.expect_column_values_to_be_in_set(
        "order_status",
        [
            "delivered",
            "shipped",
            "processing",
            "approved",
            "invoiced",
            "unavailable",
            "canceled",
        ],
    )

    # Delivery days: positive or null
    validator.expect_column_values_to_be_between(
        "delivery_days", min_value=0, max_value=365, mostly=0.98
    )

    # Table-level row count sanity check
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=10_000_000)

    # Timestamp not in the future (mostly)
    validator.expect_column_values_to_not_be_null(
        "order_purchase_timestamp", mostly=0.99
    )

    validator.save_expectation_suite(discard_failed_expectations=False)


def build_dim_customers_suite(validator: Any) -> None:
    """
    Define and save expectations for the dim_customers mart table.

    Args:
        validator: A Great Expectations Validator object.
    """
    # Primary key checks
    validator.expect_column_values_to_not_be_null("customer_key")
    validator.expect_column_values_to_be_unique("customer_key")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_unique("customer_id")

    # State must be a valid 2-letter Brazilian state code
    valid_states = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
        "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
        "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
    ]
    validator.expect_column_values_to_be_in_set(
        "state", valid_states, mostly=0.95
    )

    # City should not be null
    validator.expect_column_values_to_not_be_null("city", mostly=0.99)

    validator.save_expectation_suite(discard_failed_expectations=False)


# ── Runner ────────────────────────────────────────────────────────────────────

def run_validation(
    context: Any,
    suite_name: str,
    datasource_name: str,
    table: str,
) -> bool:
    """
    Run a Great Expectations validation suite against a PostgreSQL table.

    Args:
        context:         GE DataContext.
        suite_name:      Name of the expectation suite.
        datasource_name: Name of the GE datasource.
        table:           Fully qualified PostgreSQL table name.

    Returns:
        True if all expectations pass, False otherwise.
    """
    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=table,
        runtime_parameters={"query": f"SELECT * FROM {table}"},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    results = validator.validate()
    success = results["success"]
    logger.info(
        "Suite '%s' → %s (%d/%d expectations passed)",
        suite_name,
        "PASSED ✓" if success else "FAILED ✗",
        results["statistics"]["successful_expectations"],
        results["statistics"]["evaluated_expectations"],
    )
    return bool(success)


def main() -> None:
    """
    Entry point: set up GE context, build suites, and run validations.

    Exits with code 1 if any validation fails.
    """
    logger.info("Initializing Great Expectations context from: %s", GE_ROOT_DIR)

    # Use ephemeral in-memory context if no file context exists
    context = gx.get_context(mode="ephemeral")

    # Add PostgreSQL datasource
    datasource_config = {
        "name": "rakuflow_postgres",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": POSTGRES_URL,
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            }
        },
    }
    context.add_datasource(**datasource_config)

    # Build and validate fact_orders suite
    fact_orders_suite_name = "rakuflow.fact_orders"
    context.add_or_update_expectation_suite(fact_orders_suite_name)
    fact_validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="rakuflow_postgres",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="marts.fact_orders",
            runtime_parameters={"query": "SELECT * FROM marts.fact_orders LIMIT 10000"},
            batch_identifiers={"default_identifier_name": "build"},
        ),
        expectation_suite_name=fact_orders_suite_name,
    )
    build_fact_orders_suite(fact_validator)

    # Build and validate dim_customers suite
    dim_customers_suite_name = "rakuflow.dim_customers"
    context.add_or_update_expectation_suite(dim_customers_suite_name)
    dim_validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="rakuflow_postgres",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="marts.dim_customers",
            runtime_parameters={"query": "SELECT * FROM marts.dim_customers"},
            batch_identifiers={"default_identifier_name": "build"},
        ),
        expectation_suite_name=dim_customers_suite_name,
    )
    build_dim_customers_suite(dim_validator)

    # Run validations
    all_passed = True
    for suite, table in [
        (fact_orders_suite_name, "marts.fact_orders"),
        (dim_customers_suite_name, "marts.dim_customers"),
    ]:
        passed = run_validation(context, suite, "rakuflow_postgres", table)
        if not passed:
            all_passed = False

    if not all_passed:
        logger.error("One or more Great Expectations suites FAILED. Pipeline halted.")
        sys.exit(1)

    logger.info("All Great Expectations suites passed. Pipeline continuing.")


if __name__ == "__main__":
    main()
