"""
spark_consumer.py — PySpark streaming consumer for RakuFlow pipeline.

Reads order and payment events from Kafka topics, cleans and enriches the data,
deduplicates on order_id, and writes results to PostgreSQL staging tables.

Output tables:
    staging.raw_orders    — cleaned order records
    staging.raw_payments  — cleaned payment records
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Configuration ──────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("rakuflow.spark_consumer")

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC: str = "rakuflow-orders"
PAYMENTS_TOPIC: str = "rakuflow-payments"

POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "rakuflow")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "rakuflow")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "rakuflow")
POSTGRES_JDBC_URL: str = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

DATA_DIR: Path = Path(os.getenv("DATA_DIR", "../data/raw"))
CUSTOMERS_CSV: Path = DATA_DIR / "olist_customers_dataset.csv"
SPARK_KAFKA_JAR: str = os.getenv(
    "SPARK_KAFKA_JAR",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
)
SPARK_POSTGRES_JAR: str = os.getenv(
    "SPARK_POSTGRES_JAR",
    "/opt/jars/postgresql-42.7.3.jar",
)

# ── Schemas ───────────────────────────────────────────────────────────────────

ORDER_SCHEMA: StructType = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("purchase_timestamp", StringType(), True),
        StructField("approved_at", StringType(), True),
        StructField("delivered_carrier_date", StringType(), True),
        StructField("delivered_customer_date", StringType(), True),
        StructField("estimated_delivery_date", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("schema_version", StringType(), True),
    ]
)

PAYMENT_SCHEMA: StructType = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
        StructField("event_time", StringType(), True),
        StructField("schema_version", StringType(), True),
    ]
)

CUSTOMER_SCHEMA: StructType = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ]
)


# ── Spark Session ─────────────────────────────────────────────────────────────

def build_spark_session() -> SparkSession:
    """
    Build and return a configured SparkSession.

    Returns:
        A SparkSession with Kafka and PostgreSQL JDBC support.
    """
    return (
        SparkSession.builder.appName("RakuFlow-Consumer")
        .config("spark.jars.packages", SPARK_KAFKA_JAR)
        .config("spark.jars", SPARK_POSTGRES_JAR)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ── Data Loading ──────────────────────────────────────────────────────────────

def read_kafka_batch(spark: SparkSession, topic: str) -> DataFrame:
    """
    Read all available messages from a Kafka topic in batch mode.

    Args:
        spark: Active SparkSession.
        topic: Kafka topic name to consume from.

    Returns:
        Raw DataFrame with Kafka envelope columns (key, value, topic, etc.)
    """
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def load_customer_lookup(spark: SparkSession) -> DataFrame:
    """
    Load customer dimension data from CSV for enrichment joins.

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame with customer_id, city, and state columns.
    """
    return (
        spark.read.option("header", "true")
        .schema(CUSTOMER_SCHEMA)
        .csv(str(CUSTOMERS_CSV))
        .select("customer_id", "customer_city", "customer_state")
        .dropDuplicates(["customer_id"])
    )


# ── Cleaning & Enrichment ─────────────────────────────────────────────────────

def clean_orders(raw_df: DataFrame) -> DataFrame:
    """
    Parse, clean, and type-cast raw order Kafka messages.

    Steps:
        1. Parse JSON value into structured columns.
        2. Drop rows with null order_id or customer_id.
        3. Cast timestamp strings to TimestampType.
        4. Deduplicate on order_id (keep first occurrence).

    Args:
        raw_df: Raw Kafka DataFrame.

    Returns:
        Cleaned and typed orders DataFrame.
    """
    parsed = (
        raw_df.select(
            F.from_json(F.col("value").cast("string"), ORDER_SCHEMA).alias("data")
        )
        .select("data.*")
        .dropna(subset=["order_id", "customer_id"])
        .withColumn(
            "purchase_timestamp",
            F.to_timestamp("purchase_timestamp", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("approved_at", F.to_timestamp("approved_at", "yyyy-MM-dd HH:mm:ss"))
        .withColumn(
            "delivered_carrier_date",
            F.to_timestamp("delivered_carrier_date", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "delivered_customer_date",
            F.to_timestamp("delivered_customer_date", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "estimated_delivery_date",
            F.to_timestamp("estimated_delivery_date", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Deduplicate: keep the row with the latest event_time per order_id
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("order_id")
        .orderBy(F.col("event_time").desc())
    )
    deduplicated = (
        parsed.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "schema_version", "event_time")
    )
    return deduplicated


def enrich_orders(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    Enrich orders with customer city and state from the customer lookup table.

    Args:
        orders_df:    Cleaned orders DataFrame.
        customers_df: Customer lookup DataFrame.

    Returns:
        Orders DataFrame with customer_city and customer_state columns appended.
    """
    return orders_df.join(customers_df, on="customer_id", how="left")


def clean_payments(raw_df: DataFrame) -> DataFrame:
    """
    Parse, clean, and type-cast raw payment Kafka messages.

    Args:
        raw_df: Raw Kafka DataFrame for the payments topic.

    Returns:
        Cleaned and typed payments DataFrame.
    """
    return (
        raw_df.select(
            F.from_json(F.col("value").cast("string"), PAYMENT_SCHEMA).alias("data")
        )
        .select("data.*")
        .dropna(subset=["order_id"])
        .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("schema_version", "event_time")
    )


# ── Writing ───────────────────────────────────────────────────────────────────

def write_to_postgres(df: DataFrame, table: str, mode: str = "append") -> None:
    """
    Write a DataFrame to a PostgreSQL table via JDBC.

    Args:
        df:    DataFrame to write.
        table: Target table name (schema.tablename format).
        mode:  Spark write mode — 'append' or 'overwrite'.
    """
    count = df.count()
    logger.info("Writing %d records to PostgreSQL table: %s", count, table)
    (
        df.write.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 1000)
        .mode(mode)
        .save()
    )
    logger.info("Successfully wrote %d records to %s", count, table)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Entry point for the RakuFlow Spark consumer job.

    Orchestrates reading from Kafka, cleaning, enrichment, deduplication,
    and writing results to PostgreSQL staging tables.
    """
    logger.info("Initializing SparkSession for RakuFlow consumer")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ── Orders pipeline ──
        logger.info("Reading orders from Kafka topic: %s", ORDERS_TOPIC)
        raw_orders = read_kafka_batch(spark, ORDERS_TOPIC)
        customers_lookup = load_customer_lookup(spark)
        cleaned_orders = clean_orders(raw_orders)
        enriched_orders = enrich_orders(cleaned_orders, customers_lookup)
        write_to_postgres(enriched_orders, "staging.raw_orders")

        # ── Payments pipeline ──
        logger.info("Reading payments from Kafka topic: %s", PAYMENTS_TOPIC)
        raw_payments = read_kafka_batch(spark, PAYMENTS_TOPIC)
        cleaned_payments = clean_payments(raw_payments)
        write_to_postgres(cleaned_payments, "staging.raw_payments")

        logger.info("RakuFlow Spark consumer job completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
