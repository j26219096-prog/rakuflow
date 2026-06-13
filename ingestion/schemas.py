"""
schemas.py — Avro-like Python dataclasses for RakuFlow Kafka message schemas.

Defines strongly-typed schemas for order and payment events published to Kafka.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class OrderEvent:
    """Schema for an order event published to the 'rakuflow-orders' Kafka topic."""

    order_id: str
    customer_id: str
    order_status: str
    purchase_timestamp: str
    approved_at: Optional[str]
    delivered_carrier_date: Optional[str]
    delivered_customer_date: Optional[str]
    estimated_delivery_date: Optional[str]
    event_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    schema_version: str = "1.0"

    def to_json(self) -> str:
        """Serialize the event to a JSON string for Kafka publishing."""
        return json.dumps(asdict(self), default=str)

    @classmethod
    def from_csv_row(cls, row: dict) -> "OrderEvent":
        """
        Create an OrderEvent from a CSV row dictionary.

        Args:
            row: Dictionary representing one row from olist_orders_dataset.csv

        Returns:
            An OrderEvent instance populated with the row data.
        """
        return cls(
            order_id=row.get("order_id", ""),
            customer_id=row.get("customer_id", ""),
            order_status=row.get("order_status", ""),
            purchase_timestamp=row.get("order_purchase_timestamp", ""),
            approved_at=row.get("order_approved_at") or None,
            delivered_carrier_date=row.get("order_delivered_carrier_date") or None,
            delivered_customer_date=row.get("order_delivered_customer_date") or None,
            estimated_delivery_date=row.get("order_estimated_delivery_date") or None,
        )


@dataclass
class PaymentEvent:
    """Schema for a payment event published to the 'rakuflow-payments' Kafka topic."""

    order_id: str
    payment_sequential: int
    payment_type: str
    payment_installments: int
    payment_value: float
    event_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    schema_version: str = "1.0"

    def to_json(self) -> str:
        """Serialize the event to a JSON string for Kafka publishing."""
        return json.dumps(asdict(self), default=str)

    @classmethod
    def from_csv_row(cls, row: dict) -> "PaymentEvent":
        """
        Create a PaymentEvent from a CSV row dictionary.

        Args:
            row: Dictionary representing one row from olist_order_payments_dataset.csv

        Returns:
            A PaymentEvent instance populated with the row data.
        """
        return cls(
            order_id=row.get("order_id", ""),
            payment_sequential=int(row.get("payment_sequential", 1)),
            payment_type=row.get("payment_type", "unknown"),
            payment_installments=int(row.get("payment_installments", 1)),
            payment_value=float(row.get("payment_value", 0.0)),
        )


@dataclass
class CustomerLookup:
    """Schema for customer dimension data used for enrichment."""

    customer_id: str
    customer_unique_id: str
    customer_zip_code_prefix: str
    customer_city: str
    customer_state: str

    @classmethod
    def from_csv_row(cls, row: dict) -> "CustomerLookup":
        """
        Create a CustomerLookup from a CSV row dictionary.

        Args:
            row: Dictionary representing one row from olist_customers_dataset.csv

        Returns:
            A CustomerLookup instance populated with the row data.
        """
        return cls(
            customer_id=row.get("customer_id", ""),
            customer_unique_id=row.get("customer_unique_id", ""),
            customer_zip_code_prefix=row.get("customer_zip_code_prefix", ""),
            customer_city=row.get("customer_city", ""),
            customer_state=row.get("customer_state", ""),
        )
