"""
simple_consumer.py — Lightweight Python consumer for local development.

Reads from Kafka topics and writes to PostgreSQL staging tables using
psycopg2 (no Spark/Java required). Performs the same cleaning, enrichment,
and deduplication as spark_consumer.py.

Use this for local dev/demo. In production the Airflow DAG runs spark_consumer.py.

Usage:
    python simple_consumer.py
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("rakuflow.simple_consumer")

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "rakuflow")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "rakuflow")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "changeme_strong_password")
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "./data/raw"))
ORDERS_TOPIC: str = "rakuflow-orders"
PAYMENTS_TOPIC: str = "rakuflow-payments"


def get_pg_conn() -> psycopg2.extensions.connection:
    """Return a PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def load_customer_lookup() -> dict[str, dict]:
    """
    Load customer city/state from CSV into a dict keyed by customer_id.

    Returns:
        Dict mapping customer_id -> {customer_city, customer_state}
    """
    lookup: dict[str, dict] = {}
    csv_path = DATA_DIR / "olist_customers_dataset.csv"
    if not csv_path.exists():
        logger.warning("Customer CSV not found: %s", csv_path)
        return lookup
    with csv_path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lookup[row["customer_id"]] = {
                "customer_city": row.get("customer_city", "").strip().title(),
                "customer_state": row.get("customer_state", "").strip().upper(),
            }
    logger.info("Loaded %d customer lookup records", len(lookup))
    return lookup


def consume_topic(topic: str, timeout_ms: int = 5000) -> list[dict]:
    """
    Consume all available messages from a Kafka topic (batch mode).

    Args:
        topic:      Kafka topic name.
        timeout_ms: How long to wait for new messages before stopping.

    Returns:
        List of parsed message dicts.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
        group_id=f"rakuflow-simple-consumer-{topic}",
    )
    messages = []
    for msg in consumer:
        messages.append(msg.value)
    consumer.close()
    logger.info("Consumed %d messages from topic: %s", len(messages), topic)
    return messages


def parse_ts(val: str | None) -> datetime | None:
    """Parse a timestamp string to a datetime, returning None on failure."""
    if not val:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except (ValueError, TypeError):
            continue
    return None


def load_orders(conn: psycopg2.extensions.connection, customer_lookup: dict) -> int:
    """
    Consume orders from Kafka, enrich with customer data, and upsert to staging.

    Args:
        conn:            Active PostgreSQL connection.
        customer_lookup: Dict of customer_id -> {city, state}.

    Returns:
        Number of rows upserted.
    """
    messages = consume_topic(ORDERS_TOPIC)
    if not messages:
        logger.warning("No order messages found in Kafka.")
        return 0

    # Deduplicate: keep latest event_time per order_id
    deduped: dict[str, dict] = {}
    for msg in messages:
        oid = msg.get("order_id")
        if not oid:
            continue
        existing = deduped.get(oid)
        if not existing or msg.get("event_time", "") > existing.get("event_time", ""):
            deduped[oid] = msg

    rows = []
    for msg in deduped.values():
        customer_id = msg.get("customer_id", "")
        cust = customer_lookup.get(customer_id, {})
        rows.append((
            msg.get("order_id"),
            customer_id,
            msg.get("order_status"),
            parse_ts(msg.get("purchase_timestamp")),
            parse_ts(msg.get("approved_at")),
            parse_ts(msg.get("delivered_carrier_date")),
            parse_ts(msg.get("delivered_customer_date")),
            parse_ts(msg.get("estimated_delivery_date")),
            cust.get("customer_city"),
            cust.get("customer_state"),
        ))

    sql = """
        INSERT INTO staging.raw_orders (
            order_id, customer_id, order_status,
            purchase_timestamp, approved_at, delivered_carrier_date,
            delivered_customer_date, estimated_delivery_date,
            customer_city, customer_state
        ) VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            order_status             = EXCLUDED.order_status,
            purchase_timestamp       = EXCLUDED.purchase_timestamp,
            delivered_customer_date  = EXCLUDED.delivered_customer_date,
            customer_city            = EXCLUDED.customer_city,
            customer_state           = EXCLUDED.customer_state;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
    conn.commit()
    logger.info("Upserted %d orders into staging.raw_orders", len(rows))
    return len(rows)


def load_payments(conn: psycopg2.extensions.connection) -> int:
    """
    Consume payments from Kafka and upsert to staging.raw_payments.

    Args:
        conn: Active PostgreSQL connection.

    Returns:
        Number of rows upserted.
    """
    messages = consume_topic(PAYMENTS_TOPIC)
    if not messages:
        logger.warning("No payment messages found in Kafka.")
        return 0

    rows = []
    for msg in messages:
        if not msg.get("order_id"):
            continue
        rows.append((
            msg.get("order_id"),
            int(msg.get("payment_sequential", 1)),
            msg.get("payment_type"),
            int(msg.get("payment_installments", 1)),
            float(msg.get("payment_value", 0.0)),
        ))

    sql = """
        INSERT INTO staging.raw_payments (
            order_id, payment_sequential, payment_type,
            payment_installments, payment_value
        ) VALUES %s
        ON CONFLICT (order_id, payment_sequential) DO UPDATE SET
            payment_value = EXCLUDED.payment_value,
            payment_type  = EXCLUDED.payment_type;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
    conn.commit()
    logger.info("Upserted %d payments into staging.raw_payments", len(rows))
    return len(rows)


def load_sellers(conn: psycopg2.extensions.connection) -> int:
    """
    Load seller data from CSV directly into staging.raw_sellers.

    Args:
        conn: Active PostgreSQL connection.

    Returns:
        Number of rows upserted.
    """
    csv_path = DATA_DIR / "olist_sellers_dataset.csv"
    if not csv_path.exists():
        logger.warning("Sellers CSV not found: %s", csv_path)
        return 0

    rows = []
    with csv_path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append((
                row.get("seller_id"),
                row.get("seller_zip_code_prefix"),
                row.get("seller_city", "").strip().title(),
                row.get("seller_state", "").strip().upper(),
            ))

    sql = """
        INSERT INTO staging.raw_sellers (
            seller_id, seller_zip_code_prefix, seller_city, seller_state
        ) VALUES %s
        ON CONFLICT (seller_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
    conn.commit()
    logger.info("Upserted %d sellers into staging.raw_sellers", len(rows))
    return len(rows)


def load_order_items(conn: psycopg2.extensions.connection) -> int:
    """
    Load order items from CSV directly into staging.raw_order_items.

    Args:
        conn: Active PostgreSQL connection.

    Returns:
        Number of rows upserted.
    """
    csv_path = DATA_DIR / "olist_order_items_dataset.csv"
    if not csv_path.exists():
        logger.warning("Order items CSV not found: %s", csv_path)
        return 0

    rows = []
    with csv_path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append((
                row.get("order_id"),
                int(row.get("order_item_id", 1)),
                row.get("product_id"),
                row.get("seller_id"),
                parse_ts(row.get("shipping_limit_date")),
                float(row.get("price", 0.0)),
                float(row.get("freight_value", 0.0)),
            ))

    sql = """
        INSERT INTO staging.raw_order_items (
            order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value
        ) VALUES %s
        ON CONFLICT (order_id, order_item_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
    conn.commit()
    logger.info("Upserted %d order items into staging.raw_order_items", len(rows))
    return len(rows)


def main() -> None:
    """Run the full simple consumer pipeline."""
    logger.info("=== RakuFlow Simple Consumer starting ===")
    logger.info("PostgreSQL: %s:%d/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
    logger.info("Kafka: %s", KAFKA_BOOTSTRAP)

    customer_lookup = load_customer_lookup()

    conn = get_pg_conn()
    try:
        n_orders = load_orders(conn, customer_lookup)
        n_payments = load_payments(conn)
        n_sellers = load_sellers(conn)
        n_items = load_order_items(conn)
        logger.info("=== Pipeline complete ===")
        logger.info("  Orders loaded   : %d", n_orders)
        logger.info("  Payments loaded : %d", n_payments)
        logger.info("  Sellers loaded  : %d", n_sellers)
        logger.info("  Order items     : %d", n_items)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
