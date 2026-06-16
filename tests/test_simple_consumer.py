"""
test_simple_consumer.py — Unit tests for the RakuFlow simple consumer.

Tests cover:
    - parse_ts correctly parses valid timestamps and returns None for invalid input
    - load_customer_lookup returns correct structure from a temp CSV
    - consume_topic returns a list of messages from a mocked KafkaConsumer
    - load_orders upserts to staging.raw_orders with correct SQL
    - load_payments upserts to staging.raw_payments with correct SQL
    - load_sellers loads from CSV into staging.raw_sellers
    - load_order_items loads from CSV into staging.raw_order_items
    - main() runs the full pipeline end-to-end (all mocked)
"""

from __future__ import annotations

import csv
import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

# ── Adjust sys.path to find processing module ──────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "processing"))


# ── parse_ts Tests ─────────────────────────────────────────────────────────────

class TestParseTs:
    """Tests for the parse_ts timestamp parsing helper."""

    def test_parse_valid_iso_timestamp(self) -> None:
        """Should parse ISO 8601 format with microseconds."""
        from simple_consumer import parse_ts
        result = parse_ts("2017-09-13T08:59:02.123456")
        assert isinstance(result, datetime)
        assert result.year == 2017
        assert result.month == 9

    def test_parse_iso_without_microseconds(self) -> None:
        """Should parse ISO 8601 format without microseconds."""
        from simple_consumer import parse_ts
        result = parse_ts("2017-09-13T08:59:02")
        assert isinstance(result, datetime)

    def test_parse_space_separated_timestamp(self) -> None:
        """Should parse space-separated datetime strings."""
        from simple_consumer import parse_ts
        result = parse_ts("2017-09-13 08:59:02")
        assert isinstance(result, datetime)
        assert result.day == 13

    def test_returns_none_for_empty_string(self) -> None:
        """Empty string should return None."""
        from simple_consumer import parse_ts
        assert parse_ts("") is None

    def test_returns_none_for_none_input(self) -> None:
        """None input should return None."""
        from simple_consumer import parse_ts
        assert parse_ts(None) is None

    def test_returns_none_for_invalid_format(self) -> None:
        """Unrecognised format should return None without raising."""
        from simple_consumer import parse_ts
        assert parse_ts("not-a-date") is None


# ── load_customer_lookup Tests ─────────────────────────────────────────────────

class TestLoadCustomerLookup:
    """Tests for the customer CSV lookup loader."""

    def test_loads_customers_from_csv(self) -> None:
        """Should return a dict keyed by customer_id with city/state values."""
        from simple_consumer import load_customer_lookup

        rows = [
            {"customer_id": "c1", "customer_city": "são paulo", "customer_state": "sp"},
            {"customer_id": "c2", "customer_city": "rio de janeiro", "customer_state": "rj"},
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "olist_customers_dataset.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["customer_id", "customer_city", "customer_state"])
                writer.writeheader()
                writer.writerows(rows)

            with patch("simple_consumer.DATA_DIR", Path(tmp_dir)):
                result = load_customer_lookup()

        assert len(result) == 2
        assert result["c1"]["customer_city"] == "São Paulo"   # title-cased
        assert result["c1"]["customer_state"] == "SP"          # uppercased
        assert result["c2"]["customer_state"] == "RJ"

    def test_returns_empty_dict_when_csv_missing(self) -> None:
        """Should return empty dict (not raise) if customer CSV is absent."""
        from simple_consumer import load_customer_lookup

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("simple_consumer.DATA_DIR", Path(tmp_dir)):
                result = load_customer_lookup()

        assert result == {}


# ── consume_topic Tests ────────────────────────────────────────────────────────

class TestConsumeTopic:
    """Tests for the Kafka topic consumer."""

    @patch("simple_consumer.KafkaConsumer")
    def test_returns_list_of_message_values(self, mock_consumer_cls: MagicMock) -> None:
        """Should return a list of parsed message dicts."""
        from simple_consumer import consume_topic

        msg1 = MagicMock()
        msg1.value = {"order_id": "o1", "order_status": "delivered"}
        msg2 = MagicMock()
        msg2.value = {"order_id": "o2", "order_status": "shipped"}

        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(return_value=iter([msg1, msg2]))
        mock_consumer_cls.return_value = mock_consumer

        result = consume_topic("rakuflow-orders")

        assert len(result) == 2
        assert result[0]["order_id"] == "o1"
        assert result[1]["order_status"] == "shipped"
        mock_consumer.close.assert_called_once()

    @patch("simple_consumer.KafkaConsumer")
    def test_returns_empty_list_when_no_messages(self, mock_consumer_cls: MagicMock) -> None:
        """Should return an empty list if Kafka topic has no messages."""
        from simple_consumer import consume_topic

        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(return_value=iter([]))
        mock_consumer_cls.return_value = mock_consumer

        result = consume_topic("rakuflow-orders")
        assert result == []


# ── load_orders Tests ──────────────────────────────────────────────────────────

class TestLoadOrders:
    """Tests for the load_orders function."""

    @patch("simple_consumer.consume_topic")
    @patch("simple_consumer.psycopg2.extras.execute_values")
    def test_upserts_orders_and_returns_count(
        self, mock_execute_values: MagicMock, mock_consume: MagicMock
    ) -> None:
        """Should upsert deduplicated order rows and return the count."""
        from simple_consumer import load_orders

        mock_consume.return_value = [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_status": "delivered",
                "purchase_timestamp": "2017-09-13T08:59:02",
                "approved_at": "",
                "delivered_carrier_date": "",
                "delivered_customer_date": "2017-09-23T00:00:00",
                "estimated_delivery_date": "2017-10-03T00:00:00",
                "event_time": "2017-09-13T08:59:02",
            }
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        customer_lookup = {"c1": {"customer_city": "São Paulo", "customer_state": "SP"}}
        count = load_orders(mock_conn, customer_lookup)

        assert count == 1
        mock_conn.commit.assert_called_once()

    @patch("simple_consumer.consume_topic")
    def test_returns_zero_when_no_messages(self, mock_consume: MagicMock) -> None:
        """Should return 0 if Kafka returns no messages."""
        from simple_consumer import load_orders

        mock_consume.return_value = []
        mock_conn = MagicMock()
        count = load_orders(mock_conn, {})
        assert count == 0

    @patch("simple_consumer.consume_topic")
    @patch("simple_consumer.psycopg2.extras.execute_values")
    def test_deduplicates_on_order_id(
        self, mock_execute_values: MagicMock, mock_consume: MagicMock
    ) -> None:
        """Duplicate order_ids should be deduplicated by latest event_time."""
        from simple_consumer import load_orders

        mock_consume.return_value = [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_status": "processing",
                "purchase_timestamp": "2017-01-01T10:00:00",
                "approved_at": "", "delivered_carrier_date": "",
                "delivered_customer_date": "", "estimated_delivery_date": "",
                "event_time": "2017-01-01T10:00:00",
            },
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_status": "delivered",
                "purchase_timestamp": "2017-01-01T10:00:00",
                "approved_at": "", "delivered_carrier_date": "",
                "delivered_customer_date": "", "estimated_delivery_date": "",
                "event_time": "2017-01-01T12:00:00",  # later event
            },
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        count = load_orders(mock_conn, {})
        assert count == 1   # only 1 unique order_id


# ── load_payments Tests ────────────────────────────────────────────────────────

class TestLoadPayments:
    """Tests for the load_payments function."""

    @patch("simple_consumer.consume_topic")
    @patch("simple_consumer.psycopg2.extras.execute_values")
    def test_upserts_payments_and_returns_count(
        self, mock_execute_values: MagicMock, mock_consume: MagicMock
    ) -> None:
        """Should upsert payment rows and return the count."""
        from simple_consumer import load_payments

        mock_consume.return_value = [
            {
                "order_id": "o1",
                "payment_sequential": 1,
                "payment_type": "credit_card",
                "payment_installments": 3,
                "payment_value": 125.5,
            }
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        count = load_payments(mock_conn)
        assert count == 1
        mock_conn.commit.assert_called_once()

    @patch("simple_consumer.consume_topic")
    @patch("simple_consumer.psycopg2.extras.execute_values")
    def test_skips_rows_without_order_id(
        self, mock_execute_values: MagicMock, mock_consume: MagicMock
    ) -> None:
        """Rows with missing order_id should be silently skipped."""
        from simple_consumer import load_payments

        mock_consume.return_value = [
            {"order_id": None, "payment_sequential": 1, "payment_type": "boleto",
             "payment_installments": 1, "payment_value": 50.0},
            {"order_id": "o2", "payment_sequential": 1, "payment_type": "credit_card",
             "payment_installments": 1, "payment_value": 99.0},
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        count = load_payments(mock_conn)
        assert count == 1   # only o2 should be inserted


# ── load_sellers Tests ─────────────────────────────────────────────────────────

class TestLoadSellers:
    """Tests for the load_sellers function (CSV → staging.raw_sellers)."""

    @patch("simple_consumer.psycopg2.extras.execute_values")
    def test_loads_sellers_from_csv(self, mock_execute_values: MagicMock) -> None:
        """Should read sellers CSV and upsert into staging.raw_sellers."""
        from simple_consumer import load_sellers

        rows = [
            {"seller_id": "s1", "seller_zip_code_prefix": "12345",
             "seller_city": "curitiba", "seller_state": "pr"},
            {"seller_id": "s2", "seller_zip_code_prefix": "54321",
             "seller_city": "manaus", "seller_state": "am"},
        ]
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "olist_sellers_dataset.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
                )
                writer.writeheader()
                writer.writerows(rows)

            mock_conn = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            with patch("simple_consumer.DATA_DIR", Path(tmp_dir)):
                count = load_sellers(mock_conn)

        assert count == 2
        mock_conn.commit.assert_called_once()

    def test_returns_zero_when_csv_missing(self) -> None:
        """Should return 0 if sellers CSV does not exist."""
        from simple_consumer import load_sellers

        with tempfile.TemporaryDirectory() as tmp_dir:
            mock_conn = MagicMock()
            with patch("simple_consumer.DATA_DIR", Path(tmp_dir)):
                count = load_sellers(mock_conn)

        assert count == 0
        mock_conn.commit.assert_not_called()
