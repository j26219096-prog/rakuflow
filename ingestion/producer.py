"""
producer.py — Kafka producer for RakuFlow pipeline.

Reads Olist CSV datasets row-by-row and publishes events to Kafka topics
with a configurable delay to simulate real-time streaming.

Topics:
    - rakuflow-orders   : OrderEvent messages from olist_orders_dataset.csv
    - rakuflow-payments : PaymentEvent messages from olist_order_payments_dataset.csv
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from pathlib import Path
from typing import Generator

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

from schemas import OrderEvent, PaymentEvent

# ── Configuration ──────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("rakuflow.producer")

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "../data/raw"))
ORDERS_CSV: Path = DATA_DIR / "olist_orders_dataset.csv"
PAYMENTS_CSV: Path = DATA_DIR / "olist_order_payments_dataset.csv"
ORDERS_TOPIC: str = "rakuflow-orders"
PAYMENTS_TOPIC: str = "rakuflow-payments"
MESSAGE_DELAY_SEC: float = float(os.getenv("MESSAGE_DELAY_SEC", "0.1"))


# ── Helpers ────────────────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    """
    Build and return a configured KafkaProducer instance.

    Returns:
        A KafkaProducer connected to the bootstrap servers defined in .env.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        linger_ms=10,
        batch_size=16384,
    )


def csv_row_generator(filepath: Path) -> Generator[dict, None, None]:
    """
    Lazily yield rows from a CSV file as dictionaries.

    Args:
        filepath: Path to the CSV file.

    Yields:
        One dict per row.
    """
    if not filepath.exists():
        logger.error("CSV file not found: %s", filepath)
        return
    with filepath.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield dict(row)


def on_send_success(record_metadata: object) -> None:
    """Log successful Kafka message delivery."""
    logger.debug(
        "Message delivered → topic=%s partition=%s offset=%s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc: Exception) -> None:
    """Log Kafka delivery failure."""
    logger.error("Failed to deliver message: %s", exc)


# ── Producers ─────────────────────────────────────────────────────────────────

def produce_orders(producer: KafkaProducer, delay: float = MESSAGE_DELAY_SEC) -> None:
    """
    Read olist_orders_dataset.csv row-by-row and publish OrderEvents to Kafka.

    Args:
        producer: An active KafkaProducer instance.
        delay:    Seconds to sleep between messages (simulates real-time stream).
    """
    logger.info("Starting order producer → topic: %s", ORDERS_TOPIC)
    count = 0
    for row in csv_row_generator(ORDERS_CSV):
        try:
            event = OrderEvent.from_csv_row(row)
            future = producer.send(ORDERS_TOPIC, value=event.to_json())
            future.add_callback(on_send_success).add_errback(on_send_error)
            count += 1
            if count % 100 == 0:
                logger.info("Orders published: %d", count)
            time.sleep(delay)
        except (KafkaError, ValueError) as exc:
            logger.warning("Skipping order row due to error: %s", exc)

    producer.flush()
    logger.info("Order producer finished. Total messages sent: %d", count)


def produce_payments(producer: KafkaProducer, delay: float = MESSAGE_DELAY_SEC) -> None:
    """
    Read olist_order_payments_dataset.csv row-by-row and publish PaymentEvents to Kafka.

    Args:
        producer: An active KafkaProducer instance.
        delay:    Seconds to sleep between messages (simulates real-time stream).
    """
    logger.info("Starting payment producer → topic: %s", PAYMENTS_TOPIC)
    count = 0
    for row in csv_row_generator(PAYMENTS_CSV):
        try:
            event = PaymentEvent.from_csv_row(row)
            future = producer.send(PAYMENTS_TOPIC, value=event.to_json())
            future.add_callback(on_send_success).add_errback(on_send_error)
            count += 1
            if count % 100 == 0:
                logger.info("Payments published: %d", count)
            time.sleep(delay)
        except (KafkaError, ValueError) as exc:
            logger.warning("Skipping payment row due to error: %s", exc)

    producer.flush()
    logger.info("Payment producer finished. Total messages sent: %d", count)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    """Run both order and payment Kafka producers sequentially."""
    logger.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
    producer = build_producer()
    try:
        produce_orders(producer)
        produce_payments(producer)
    finally:
        producer.close()
        logger.info("Kafka producer connection closed.")


if __name__ == "__main__":
    main()
