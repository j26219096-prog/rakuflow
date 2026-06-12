"""
generate_sample_data.py — Generate sample Olist-format CSV files for local testing.

Run this script if you don't have access to the Kaggle dataset.
Generates ~500 realistic-looking synthetic orders, payments, customers,
sellers, and order items.

Usage:
    python scripts/generate_sample_data.py
"""

from __future__ import annotations

import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_DIR: Path = Path(os.getenv("DATA_DIR", "./data/raw"))
RANDOM_SEED: int = 42
NUM_ORDERS: int = 500
NUM_CUSTOMERS: int = 400
NUM_SELLERS: int = 50

random.seed(RANDOM_SEED)

BRAZIL_STATES: list[str] = [
    "SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE",
    "ES", "PA", "MT", "MS", "DF", "RN", "AM", "MA", "PB", "AL",
]

CITIES: dict[str, list[str]] = {
    "SP": ["São Paulo", "Campinas", "Santos", "Ribeirão Preto"],
    "RJ": ["Rio de Janeiro", "Niterói", "Duque de Caxias"],
    "MG": ["Belo Horizonte", "Uberlândia", "Contagem"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas"],
    "PR": ["Curitiba", "Londrina", "Maringá"],
    "SC": ["Florianópolis", "Joinville", "Blumenau"],
    "BA": ["Salvador", "Feira de Santana", "Vitória da Conquista"],
    "GO": ["Goiânia", "Aparecida de Goiânia", "Anápolis"],
    "PE": ["Recife", "Caruaru", "Petrolina"],
    "CE": ["Fortaleza", "Caucaia", "Juazeiro do Norte"],
    "ES": ["Vitória", "Vila Velha", "Serra"],
    "PA": ["Belém", "Ananindeua", "Santarém"],
    "MT": ["Cuiabá", "Várzea Grande", "Rondonópolis"],
    "MS": ["Campo Grande", "Dourados", "Três Lagoas"],
    "DF": ["Brasília", "Taguatinga", "Ceilândia"],
    "RN": ["Natal", "Mossoró", "Caicó"],
    "AM": ["Manaus", "Parintins", "Itacoatiara"],
    "MA": ["São Luís", "Imperatriz", "Caxias"],
    "PB": ["João Pessoa", "Campina Grande", "Santa Rita"],
    "AL": ["Maceió", "Arapiraca", "Rio Largo"],
}

ORDER_STATUSES: list[str] = [
    "delivered", "delivered", "delivered", "delivered",  # 60% delivered
    "shipped", "shipped",                                  # 20% shipped
    "processing", "approved", "invoiced", "canceled",
]

PAYMENT_TYPES: list[str] = [
    "credit_card", "credit_card", "credit_card",  # 50% credit card
    "boleto", "boleto",                             # 30% boleto
    "voucher", "debit_card",                        # 20% other
]


def random_date(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end."""
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_customers(n: int) -> list[dict]:
    """Generate n synthetic customer records."""
    customers = []
    for _ in range(n):
        state = random.choice(BRAZIL_STATES)
        city = random.choice(CITIES.get(state, ["Unknown City"]))
        customers.append({
            "customer_id": str(uuid.uuid4()),
            "customer_unique_id": str(uuid.uuid4()),
            "customer_zip_code_prefix": str(random.randint(10000, 99999)),
            "customer_city": city.lower(),
            "customer_state": state,
        })
    return customers


def generate_sellers(n: int) -> list[dict]:
    """Generate n synthetic seller records."""
    sellers = []
    for _ in range(n):
        state = random.choice(BRAZIL_STATES)
        city = random.choice(CITIES.get(state, ["Unknown City"]))
        sellers.append({
            "seller_id": str(uuid.uuid4()),
            "seller_zip_code_prefix": str(random.randint(10000, 99999)),
            "seller_city": city.lower(),
            "seller_state": state,
        })
    return sellers


def generate_orders(customers: list[dict], n: int) -> list[dict]:
    """Generate n synthetic order records linked to existing customers."""
    orders = []
    start = datetime(2016, 9, 1)
    end = datetime(2018, 10, 1)

    for _ in range(n):
        customer = random.choice(customers)
        status = random.choice(ORDER_STATUSES)
        purchase_ts = random_date(start, end)
        approved_ts = purchase_ts + timedelta(hours=random.randint(1, 48))
        carrier_ts = approved_ts + timedelta(days=random.randint(1, 5))
        delivery_ts = carrier_ts + timedelta(days=random.randint(1, 15))
        estimated_ts = purchase_ts + timedelta(days=random.randint(10, 40))

        orders.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "order_status": status,
            "order_purchase_timestamp": purchase_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "order_approved_at": (
                approved_ts.strftime("%Y-%m-%d %H:%M:%S")
                if status not in ["canceled"]
                else ""
            ),
            "order_delivered_carrier_date": (
                carrier_ts.strftime("%Y-%m-%d %H:%M:%S")
                if status in ["delivered", "shipped"]
                else ""
            ),
            "order_delivered_customer_date": (
                delivery_ts.strftime("%Y-%m-%d %H:%M:%S")
                if status == "delivered"
                else ""
            ),
            "order_estimated_delivery_date": estimated_ts.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return orders


def generate_payments(orders: list[dict]) -> list[dict]:
    """Generate payment records for each order."""
    payments = []
    for order in orders:
        num_payments = random.randint(1, 2)
        for seq in range(1, num_payments + 1):
            payments.append({
                "order_id": order["order_id"],
                "payment_sequential": seq,
                "payment_type": random.choice(PAYMENT_TYPES),
                "payment_installments": random.choice([1, 1, 1, 2, 3, 6, 12]),
                "payment_value": round(random.uniform(20.0, 1500.0), 2),
            })
    return payments


def generate_order_items(orders: list[dict], sellers: list[dict]) -> list[dict]:
    """Generate order item records linking orders to sellers."""
    items = []
    for order in orders:
        seller = random.choice(sellers)
        purchase_ts = datetime.strptime(
            order["order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S"
        )
        items.append({
            "order_id": order["order_id"],
            "order_item_id": 1,
            "product_id": str(uuid.uuid4()),
            "seller_id": seller["seller_id"],
            "shipping_limit_date": (
                purchase_ts + timedelta(days=random.randint(2, 7))
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "price": round(random.uniform(15.0, 1200.0), 2),
            "freight_value": round(random.uniform(5.0, 80.0), 2),
        })
    return items


def write_csv(filepath: Path, rows: list[dict]) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [OK] Written {len(rows):,} rows -> {filepath}")


def main() -> None:
    """Generate all sample datasets."""
    print(f"[*] Generating sample Olist-format data in: {OUTPUT_DIR.resolve()}")
    print(f"   Orders: {NUM_ORDERS} | Customers: {NUM_CUSTOMERS} | Sellers: {NUM_SELLERS}")

    customers = generate_customers(NUM_CUSTOMERS)
    sellers = generate_sellers(NUM_SELLERS)
    orders = generate_orders(customers, NUM_ORDERS)
    payments = generate_payments(orders)
    order_items = generate_order_items(orders, sellers)

    write_csv(OUTPUT_DIR / "olist_customers_dataset.csv", customers)
    write_csv(OUTPUT_DIR / "olist_sellers_dataset.csv", sellers)
    write_csv(OUTPUT_DIR / "olist_orders_dataset.csv", orders)
    write_csv(OUTPUT_DIR / "olist_order_payments_dataset.csv", payments)
    write_csv(OUTPUT_DIR / "olist_order_items_dataset.csv", order_items)

    print("\n[OK] Sample data generation complete!")
    print("   You can now run: make run")


if __name__ == "__main__":
    main()
