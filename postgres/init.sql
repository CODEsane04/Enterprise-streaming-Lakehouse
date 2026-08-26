-- ============================================================
-- Olist E-Commerce Schema
-- Engineer 1: PostgreSQL Schema + CDC Publication
-- ============================================================
-- This script runs automatically when the postgres container
-- first starts (mounted at /docker-entrypoint-initdb.d/).
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- 1. CUSTOMERS
-- Who placed the orders. customer_unique_id is the true entity;
-- customer_id is order-scoped (one unique ID per order).
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id             VARCHAR(50)  PRIMARY KEY,
    customer_unique_id      VARCHAR(50)  NOT NULL,
    customer_zip_code_prefix VARCHAR(10) NOT NULL,
    customer_city           VARCHAR(100) NOT NULL,
    customer_state          CHAR(2)      NOT NULL,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- 2. GEOLOCATION
-- Zip code → latitude/longitude mapping.
-- Contains real-world nulls and duplicate zip codes (intentional).
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_id          SERIAL       PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(10) NOT NULL,
    geolocation_lat         DOUBLE PRECISION,
    geolocation_lng         DOUBLE PRECISION,
    geolocation_city        VARCHAR(100),
    geolocation_state       CHAR(2)
);

CREATE INDEX IF NOT EXISTS idx_geolocation_zip
    ON geolocation(geolocation_zip_code_prefix);

-- ──────────────────────────────────────────────────────────────
-- 3. SELLERS
-- Seller profiles — linked to order_items via seller_id.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sellers (
    seller_id               VARCHAR(50)  PRIMARY KEY,
    seller_zip_code_prefix  VARCHAR(10)  NOT NULL,
    seller_city             VARCHAR(100) NOT NULL,
    seller_state            CHAR(2)      NOT NULL,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- 4. PRODUCTS
-- Product catalog. Olist data contains Portuguese category names
-- that need translation. Weight and dimensions included for
-- freight calculation logic.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id                  VARCHAR(50)  PRIMARY KEY,
    product_category_name       VARCHAR(100),   -- Portuguese; may be NULL
    product_name_length         INTEGER,
    product_description_length  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            INTEGER,
    product_length_cm           INTEGER,
    product_height_cm           INTEGER,
    product_width_cm            INTEGER,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- 5. PRODUCT CATEGORY NAME TRANSLATION
-- Portuguese → English lookup table.
-- Engineer 4's AI agent must join through this for English queries.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name           VARCHAR(100) PRIMARY KEY,  -- Portuguese
    product_category_name_english   VARCHAR(100) NOT NULL      -- English
);

-- ──────────────────────────────────────────────────────────────
-- 6. ORDERS
-- The central fact table. order_status tracks the full lifecycle:
--   processing → approved → shipped → delivered (or canceled)
-- The data mutator will INSERT with 'processing' then UPDATE
-- through states — this is what drives Debezium CDC events.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id                        VARCHAR(50)  PRIMARY KEY,
    customer_id                     VARCHAR(50)  NOT NULL REFERENCES customers(customer_id),
    order_status                    VARCHAR(20)  NOT NULL
                                        CHECK (order_status IN (
                                            'created','approved','processing',
                                            'shipped','delivered','canceled',
                                            'unavailable','invoiced'
                                        )),
    order_purchase_timestamp        TIMESTAMPTZ  NOT NULL,
    order_approved_at               TIMESTAMPTZ,
    order_delivered_carrier_date    TIMESTAMPTZ,
    order_delivered_customer_date   TIMESTAMPTZ,   -- NULL for canceled orders (real Olist anomaly)
    order_estimated_delivery_date   TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_purchase_ts
    ON orders(order_purchase_timestamp);

-- ──────────────────────────────────────────────────────────────
-- 7. ORDER ITEMS
-- Line items within each order. An order can have multiple items
-- from different sellers. Composite primary key.
-- price and freight_value can be NULL/negative in raw Olist data
-- — Engineer 2's Flink DLQ will catch these anomalies.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
    order_id            VARCHAR(50)  NOT NULL REFERENCES orders(order_id),
    order_item_id       INTEGER      NOT NULL,       -- sequence number within the order
    product_id          VARCHAR(50)  NOT NULL REFERENCES products(product_id),
    seller_id           VARCHAR(50)  NOT NULL REFERENCES sellers(seller_id),
    shipping_limit_date TIMESTAMPTZ,
    price               NUMERIC(10,2),               -- intentionally nullable for DLQ testing
    freight_value       NUMERIC(10,2),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_order_items_product
    ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_seller
    ON order_items(seller_id);

-- ──────────────────────────────────────────────────────────────
-- 8. ORDER PAYMENTS
-- How the order was paid. One order can have multiple payment
-- installments. Composite primary key.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_payments (
    order_id            VARCHAR(50)  NOT NULL REFERENCES orders(order_id),
    payment_sequential  INTEGER      NOT NULL,       -- installment number
    payment_type        VARCHAR(30)  NOT NULL,       -- credit_card, boleto, voucher, debit_card
    payment_installments INTEGER     NOT NULL DEFAULT 1,
    payment_value       NUMERIC(10,2) NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE INDEX IF NOT EXISTS idx_order_payments_type
    ON order_payments(payment_type);

-- ──────────────────────────────────────────────────────────────
-- 9. ORDER REVIEWS
-- Customer satisfaction (1–5 stars). Contains real-world messy
-- data: Portuguese text, empty comments, late submissions.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id               VARCHAR(50)  PRIMARY KEY,
    order_id                VARCHAR(50)  NOT NULL REFERENCES orders(order_id),
    review_score            SMALLINT     NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title    VARCHAR(255),            -- often NULL
    review_comment_message  TEXT,                   -- Portuguese text; often NULL
    review_creation_date    TIMESTAMPTZ,
    review_answer_timestamp TIMESTAMPTZ,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reviews_order_id
    ON order_reviews(order_id);
CREATE INDEX IF NOT EXISTS idx_reviews_score
    ON order_reviews(review_score);

-- ============================================================
-- CDC PUBLICATION FOR DEBEZIUM
-- ============================================================
-- This tells PostgreSQL to expose ALL table changes
-- (INSERT, UPDATE, DELETE) through logical decoding.
-- Debezium Server will subscribe to this publication via
-- the replication slot it creates automatically.
-- ============================================================
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- ============================================================
-- VERIFY
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '✅ Olist schema created successfully (9 tables + publication)';
END $$;
