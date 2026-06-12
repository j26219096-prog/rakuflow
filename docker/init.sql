-- init.sql — PostgreSQL initialization script for RakuFlow
-- Creates schemas and staging tables for the data pipeline.
-- Executed automatically when the postgres container first starts.

-- ── Schemas ───────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- ── Staging Tables ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.raw_orders (
    order_id                    VARCHAR(255)    NOT NULL,
    customer_id                 VARCHAR(255)    NOT NULL,
    order_status                VARCHAR(50),
    purchase_timestamp          TIMESTAMP,
    approved_at                 TIMESTAMP,
    delivered_carrier_date      TIMESTAMP,
    delivered_customer_date     TIMESTAMP,
    estimated_delivery_date     TIMESTAMP,
    customer_city               VARCHAR(255),
    customer_state              CHAR(2),
    ingested_at                 TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS staging.raw_payments (
    order_id                    VARCHAR(255)    NOT NULL,
    payment_sequential          INT             NOT NULL DEFAULT 1,
    payment_type                VARCHAR(50),
    payment_installments        INT,
    payment_value               NUMERIC(12, 2),
    ingested_at                 TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS staging.raw_sellers (
    seller_id                   VARCHAR(255)    NOT NULL,
    seller_zip_code_prefix      VARCHAR(10),
    seller_city                 VARCHAR(255),
    seller_state                CHAR(2),
    PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS staging.raw_order_items (
    order_id                    VARCHAR(255)    NOT NULL,
    order_item_id               INT             NOT NULL,
    product_id                  VARCHAR(255),
    seller_id                   VARCHAR(255),
    shipping_limit_date         TIMESTAMP,
    price                       NUMERIC(12, 2),
    freight_value               NUMERIC(12, 2),
    PRIMARY KEY (order_id, order_item_id)
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer   ON staging.raw_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_status     ON staging.raw_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_raw_orders_purchase   ON staging.raw_orders(purchase_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_payments_order    ON staging.raw_payments(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_items_seller      ON staging.raw_order_items(seller_id);

-- ── Mart Tables (pre-created for dbt) ─────────────────────────────────────────
-- dbt will create/replace these, but grant permissions upfront

GRANT ALL PRIVILEGES ON SCHEMA staging TO rakuflow;
GRANT ALL PRIVILEGES ON SCHEMA marts   TO rakuflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO rakuflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts   TO rakuflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO rakuflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts   GRANT ALL ON TABLES TO rakuflow;
