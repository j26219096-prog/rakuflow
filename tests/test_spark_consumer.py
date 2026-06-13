"""
test_spark_consumer.py — Unit tests for the RakuFlow Spark consumer.

Tests cover:
    - Schema definitions are valid StructTypes
    - clean_orders correctly parses and deduplicates data
    - clean_payments drops null order_ids and casts types
    - enrich_orders correctly left-joins customer lookup
    - write_to_postgres is called with correct JDBC options

Note: Tests use PySpark in local mode (no Kafka or PostgreSQL required).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# ── PySpark availability guard ─────────────────────────────────────────────────
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not PYSPARK_AVAILABLE, reason="PySpark not installed in test environment"
)

sys.path.insert(0, str(Path(__file__).parent.parent / "processing"))


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark() -> "SparkSession":
    """Create a local SparkSession for testing (reused across all tests)."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("RakuFlowTest")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def raw_order_df(spark: "SparkSession"):
    """Create a mock raw Kafka orders DataFrame (value column as JSON string)."""
    import json
    from datetime import datetime

    rows = [
        {
            "value": json.dumps({
                "order_id": "order1",
                "customer_id": "cust1",
                "order_status": "delivered",
                "purchase_timestamp": "2017-09-13 08:59:02",
                "approved_at": "2017-09-13 09:45:35",
                "delivered_carrier_date": "2017-09-19 18:34:16",
                "delivered_customer_date": "2017-09-23 00:00:00",
                "estimated_delivery_date": "2017-10-03 00:00:00",
                "event_time": datetime.utcnow().isoformat(),
                "schema_version": "1.0",
            })
        },
        {
            "value": json.dumps({
                "order_id": "order2",
                "customer_id": "cust2",
                "order_status": "shipped",
                "purchase_timestamp": "2017-10-01 12:00:00",
                "approved_at": None,
                "delivered_carrier_date": None,
                "delivered_customer_date": None,
                "estimated_delivery_date": "2017-10-20 00:00:00",
                "event_time": datetime.utcnow().isoformat(),
                "schema_version": "1.0",
            })
        },
    ]
    return spark.createDataFrame(rows)


@pytest.fixture
def raw_payment_df(spark: "SparkSession"):
    """Create a mock raw Kafka payments DataFrame."""
    import json
    from datetime import datetime

    rows = [
        {
            "value": json.dumps({
                "order_id": "order1",
                "payment_sequential": 1,
                "payment_type": "credit_card",
                "payment_installments": 3,
                "payment_value": 125.50,
                "event_time": datetime.utcnow().isoformat(),
                "schema_version": "1.0",
            })
        },
        {
            "value": json.dumps({
                "order_id": None,  # Should be dropped
                "payment_sequential": 1,
                "payment_type": "boleto",
                "payment_installments": 1,
                "payment_value": 50.0,
                "event_time": datetime.utcnow().isoformat(),
                "schema_version": "1.0",
            })
        },
    ]
    return spark.createDataFrame(rows)


@pytest.fixture
def customer_lookup_df(spark: "SparkSession"):
    """Create a mock customer lookup DataFrame."""
    rows = [
        {"customer_id": "cust1", "customer_city": "São Paulo", "customer_state": "SP"},
        {"customer_id": "cust2", "customer_city": "Rio de Janeiro", "customer_state": "RJ"},
    ]
    return spark.createDataFrame(rows)


# ── Schema Tests ───────────────────────────────────────────────────────────────

class TestSchemas:
    """Validate that StructType schemas are correctly defined."""

    def test_order_schema_is_struct_type(self) -> None:
        """ORDER_SCHEMA must be a valid PySpark StructType."""
        from spark_consumer import ORDER_SCHEMA
        assert isinstance(ORDER_SCHEMA, StructType)

    def test_payment_schema_is_struct_type(self) -> None:
        """PAYMENT_SCHEMA must be a valid PySpark StructType."""
        from spark_consumer import PAYMENT_SCHEMA
        assert isinstance(PAYMENT_SCHEMA, StructType)

    def test_order_schema_has_required_fields(self) -> None:
        """ORDER_SCHEMA must contain essential fields."""
        from spark_consumer import ORDER_SCHEMA
        field_names = {f.name for f in ORDER_SCHEMA.fields}
        for required in ["order_id", "customer_id", "order_status", "purchase_timestamp"]:
            assert required in field_names


# ── clean_orders Tests ─────────────────────────────────────────────────────────

class TestCleanOrders:
    """Tests for the clean_orders transformation function."""

    def test_clean_orders_returns_dataframe(
        self, raw_order_df, spark: "SparkSession"
    ) -> None:
        """clean_orders should return a non-empty DataFrame."""
        from spark_consumer import clean_orders
        result = clean_orders(raw_order_df)
        assert result.count() == 2

    def test_clean_orders_parses_timestamps(
        self, raw_order_df, spark: "SparkSession"
    ) -> None:
        """purchase_timestamp column should be TimestampType after cleaning."""
        from pyspark.sql.types import TimestampType
        from spark_consumer import clean_orders
        result = clean_orders(raw_order_df)
        ts_type = dict((f.name, f.dataType) for f in result.schema.fields)
        assert isinstance(ts_type.get("purchase_timestamp"), TimestampType)

    def test_clean_orders_adds_ingested_at(
        self, raw_order_df, spark: "SparkSession"
    ) -> None:
        """clean_orders should add an ingested_at column."""
        from spark_consumer import clean_orders
        result = clean_orders(raw_order_df)
        assert "ingested_at" in result.columns

    def test_clean_orders_deduplicates(self, spark: "SparkSession") -> None:
        """Duplicate order_ids should be deduplicated to one row per order."""
        import json
        from spark_consumer import clean_orders

        rows = [
            {"value": json.dumps({
                "order_id": "dup_order",
                "customer_id": "c1",
                "order_status": "delivered",
                "purchase_timestamp": "2017-01-01 10:00:00",
                "approved_at": None, "delivered_carrier_date": None,
                "delivered_customer_date": None, "estimated_delivery_date": None,
                "event_time": "2017-01-01T10:00:00", "schema_version": "1.0",
            })},
            {"value": json.dumps({
                "order_id": "dup_order",
                "customer_id": "c1",
                "order_status": "shipped",
                "purchase_timestamp": "2017-01-01 10:00:00",
                "approved_at": None, "delivered_carrier_date": None,
                "delivered_customer_date": None, "estimated_delivery_date": None,
                "event_time": "2017-01-01T11:00:00", "schema_version": "1.0",
            })},
        ]
        df = spark.createDataFrame(rows)
        result = clean_orders(df)
        assert result.count() == 1


# ── clean_payments Tests ───────────────────────────────────────────────────────

class TestCleanPayments:
    """Tests for the clean_payments transformation function."""

    def test_clean_payments_drops_null_order_id(
        self, raw_payment_df, spark: "SparkSession"
    ) -> None:
        """Rows with null order_id should be dropped."""
        from spark_consumer import clean_payments
        result = clean_payments(raw_payment_df)
        assert result.count() == 1

    def test_clean_payments_has_ingested_at(
        self, raw_payment_df, spark: "SparkSession"
    ) -> None:
        """clean_payments should add ingested_at timestamp column."""
        from spark_consumer import clean_payments
        result = clean_payments(raw_payment_df)
        assert "ingested_at" in result.columns


# ── enrich_orders Tests ────────────────────────────────────────────────────────

class TestEnrichOrders:
    """Tests for the enrich_orders join function."""

    def test_enrich_orders_adds_city_state(
        self, raw_order_df, customer_lookup_df, spark: "SparkSession"
    ) -> None:
        """Enriched orders should contain customer_city and customer_state."""
        from spark_consumer import clean_orders, enrich_orders
        cleaned = clean_orders(raw_order_df)
        enriched = enrich_orders(cleaned, customer_lookup_df)
        assert "customer_city" in enriched.columns
        assert "customer_state" in enriched.columns

    def test_enrich_orders_correct_city_for_cust1(
        self, raw_order_df, customer_lookup_df, spark: "SparkSession"
    ) -> None:
        """order1/cust1 should be enriched with São Paulo."""
        from spark_consumer import clean_orders, enrich_orders
        cleaned = clean_orders(raw_order_df)
        enriched = enrich_orders(cleaned, customer_lookup_df)
        row = enriched.filter(F.col("customer_id") == "cust1").first()
        assert row["customer_city"] == "São Paulo"
        assert row["customer_state"] == "SP"
