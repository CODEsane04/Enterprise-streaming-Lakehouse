-- ============================================================
-- 02-sources.sql: Debezium CDC Kafka Source Tables
-- Engineer 2: Enterprise Agentic Streaming Lakehouse
-- ============================================================
-- Consumes CDC events emitted by Debezium Server into Redpanda.
-- Uses 'debezium-json' to automatically unpack the CDC envelope
-- (op = 'c'/'u'/'d') into Flink changelog streams.
-- ============================================================

-- ── 1. Orders CDC Source Table ───────────────────────────────
CREATE TABLE IF NOT EXISTS cdc_orders (
  order_id                      STRING,
  customer_id                   STRING,
  order_status                  STRING,
  order_purchase_timestamp      TIMESTAMP_LTZ(6),
  order_approved_at             TIMESTAMP_LTZ(6),
  order_delivered_carrier_date  TIMESTAMP_LTZ(6),
  order_delivered_customer_date TIMESTAMP_LTZ(6),
  order_estimated_delivery_date TIMESTAMP_LTZ(6),
  created_at                    TIMESTAMP_LTZ(6),
  updated_at                    TIMESTAMP_LTZ(6),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ecommerce.public.orders',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-cdc-orders',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json',
  'value.debezium-json.schema-include' = 'false',
  'value.debezium-json.ignore-parse-errors' = 'true'
);

-- ── 2. Order Items CDC Source Table (DLQ Target) ─────────────
CREATE TABLE IF NOT EXISTS cdc_order_items (
  order_id            STRING,
  order_item_id       INT,
  product_id          STRING,
  seller_id           STRING,
  shipping_limit_date TIMESTAMP_LTZ(6),
  price               DECIMAL(10, 2),
  freight_value       DECIMAL(10, 2),
  created_at          TIMESTAMP_LTZ(6),
  updated_at          TIMESTAMP_LTZ(6),
  PRIMARY KEY (order_id, order_item_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ecommerce.public.order_items',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-cdc-order-items',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json',
  'value.debezium-json.schema-include' = 'false',
  'value.debezium-json.ignore-parse-errors' = 'true'
);

-- ── 3. Order Payments CDC Source Table ───────────────────────
CREATE TABLE IF NOT EXISTS cdc_order_payments (
  order_id             STRING,
  payment_sequential   INT,
  payment_type         STRING,
  payment_installments INT,
  payment_value        DECIMAL(10, 2),
  created_at           TIMESTAMP_LTZ(6),
  PRIMARY KEY (order_id, payment_sequential) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ecommerce.public.order_payments',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-cdc-order-payments',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json',
  'value.debezium-json.schema-include' = 'false',
  'value.debezium-json.ignore-parse-errors' = 'true'
);
