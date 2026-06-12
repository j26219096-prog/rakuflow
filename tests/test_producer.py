"""
test_producer.py — Unit tests for the RakuFlow Kafka producer.

Tests cover:
    - OrderEvent and PaymentEvent schema creation from CSV rows
    - JSON serialization
    - CSV row generator with valid and missing files
    - Producer integration (mocked Kafka)
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Adjust sys.path to find ingestion module ───────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))

from schemas import OrderEvent, PaymentEvent


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_order_row() -> dict:
    """Return a sample row matching olist_orders_dataset.csv schema."""
    return {
        "order_id": "abc123",
        "customer_id": "cust456",
        "order_status": "delivered",
        "order_purchase_timestamp": "2017-09-13 08:59:02",
        "order_approved_at": "2017-09-13 09:45:35",
        "order_delivered_carrier_date": "2017-09-19 18:34:16",
        "order_delivered_customer_date": "2017-09-23 00:00:00",
        "order_estimated_delivery_date": "2017-10-03 00:00:00",
    }


@pytest.fixture
def sample_payment_row() -> dict:
    """Return a sample row matching olist_order_payments_dataset.csv schema."""
    return {
        "order_id": "abc123",
        "payment_sequential": "1",
        "payment_type": "credit_card",
        "payment_installments": "3",
        "payment_value": "125.50",
    }


# ── OrderEvent Tests ───────────────────────────────────────────────────────────

class TestOrderEvent:
    """Tests for the OrderEvent dataclass."""

    def test_from_csv_row_creates_event(self, sample_order_row: dict) -> None:
        """OrderEvent.from_csv_row should populate all fields correctly."""
        event = OrderEvent.from_csv_row(sample_order_row)
        assert event.order_id == "abc123"
        assert event.customer_id == "cust456"
        assert event.order_status == "delivered"
        assert event.purchase_timestamp == "2017-09-13 08:59:02"
        assert event.approved_at == "2017-09-13 09:45:35"
        assert event.schema_version == "1.0"

    def test_from_csv_row_handles_empty_optional_fields(self) -> None:
        """Optional timestamp fields should be None when empty string."""
        row = {
            "order_id": "xyz",
            "customer_id": "c1",
            "order_status": "processing",
            "order_purchase_timestamp": "2017-01-01 10:00:00",
            "order_approved_at": "",
            "order_delivered_carrier_date": "",
            "order_delivered_customer_date": "",
            "order_estimated_delivery_date": "",
        }
        event = OrderEvent.from_csv_row(row)
        assert event.approved_at is None
        assert event.delivered_customer_date is None

    def test_to_json_is_valid_json(self, sample_order_row: dict) -> None:
        """to_json() should return a valid JSON string."""
        event = OrderEvent.from_csv_row(sample_order_row)
        json_str = event.to_json()
        parsed = json.loads(json_str)
        assert parsed["order_id"] == "abc123"
        assert parsed["schema_version"] == "1.0"
        assert "event_time" in parsed

    def test_to_json_contains_required_fields(self, sample_order_row: dict) -> None:
        """JSON output must contain all required pipeline fields."""
        event = OrderEvent.from_csv_row(sample_order_row)
        parsed = json.loads(event.to_json())
        required_fields = [
            "order_id", "customer_id", "order_status",
            "purchase_timestamp", "event_time", "schema_version",
        ]
        for field in required_fields:
            assert field in parsed, f"Missing field: {field}"


# ── PaymentEvent Tests ─────────────────────────────────────────────────────────

class TestPaymentEvent:
    """Tests for the PaymentEvent dataclass."""

    def test_from_csv_row_creates_event(self, sample_payment_row: dict) -> None:
        """PaymentEvent.from_csv_row should correctly cast numeric types."""
        event = PaymentEvent.from_csv_row(sample_payment_row)
        assert event.order_id == "abc123"
        assert event.payment_type == "credit_card"
        assert event.payment_value == 125.50
        assert event.payment_installments == 3
        assert event.payment_sequential == 1

    def test_to_json_is_valid_json(self, sample_payment_row: dict) -> None:
        """PaymentEvent.to_json() should produce parseable JSON."""
        event = PaymentEvent.from_csv_row(sample_payment_row)
        parsed = json.loads(event.to_json())
        assert parsed["order_id"] == "abc123"
        assert parsed["payment_value"] == 125.50

    def test_payment_value_is_float(self, sample_payment_row: dict) -> None:
        """payment_value must be cast to float, not string."""
        event = PaymentEvent.from_csv_row(sample_payment_row)
        assert isinstance(event.payment_value, float)


# ── CSV Generator Tests ────────────────────────────────────────────────────────

class TestCsvRowGenerator:
    """Tests for the csv_row_generator helper."""

    def test_yields_rows_from_valid_csv(self) -> None:
        """Generator should yield one dict per CSV row."""
        from producer import csv_row_generator

        content = "order_id,customer_id,order_status\nabc,cust1,delivered\nxyz,cust2,shipped\n"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            tmp_path = Path(f.name)

        rows = list(csv_row_generator(tmp_path))
        assert len(rows) == 2
        assert rows[0]["order_id"] == "abc"
        assert rows[1]["order_status"] == "shipped"
        tmp_path.unlink()

    def test_yields_nothing_for_missing_file(self) -> None:
        """Generator should not raise but silently yield nothing if file missing."""
        from producer import csv_row_generator

        rows = list(csv_row_generator(Path("/nonexistent/file.csv")))
        assert rows == []


# ── Producer Integration Tests (mocked Kafka) ─────────────────────────────────

class TestProduceOrders:
    """Integration tests for produce_orders with mocked Kafka."""

    @patch("producer.KafkaProducer")
    def test_produce_orders_calls_send(self, mock_producer_cls: MagicMock) -> None:
        """produce_orders should call producer.send for each CSV row."""
        from producer import produce_orders

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.add_callback.return_value = mock_future
        mock_future.add_errback.return_value = mock_future
        mock_producer.send.return_value = mock_future

        content = (
            "order_id,customer_id,order_status,order_purchase_timestamp,"
            "order_approved_at,order_delivered_carrier_date,"
            "order_delivered_customer_date,order_estimated_delivery_date\n"
            "id1,c1,delivered,2017-01-01 10:00:00,,,,\n"
            "id2,c2,shipped,2017-01-02 11:00:00,,,,\n"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            tmp_path = Path(f.name)

        with patch("producer.ORDERS_CSV", tmp_path):
            produce_orders(mock_producer, delay=0)

        assert mock_producer.send.call_count == 2
        mock_producer.flush.assert_called_once()
        tmp_path.unlink()
