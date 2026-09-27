-- ============================================================
-- 03-sinks.sql: Iceberg Lakehouse Sinks & Redpanda DLQ Sink
-- Engineer 2: Enterprise Agentic Streaming Lakehouse
-- ============================================================

-- ── 1. Dead Letter Queue (DLQ) Kafka Sink ───────────────────
-- Emits corrupted or anomalous order_items records to Redpanda.
-- Uses 'debezium-json' to properly serialize changelog records
-- and appends the 'error_reason' diagnostic attribute.
CREATE TABLE IF NOT EXISTS dlq_order_items (
  order_id            STRING,
  order_item_id       INT,
  product_id          STRING,
  seller_id           STRING,
  shipping_limit_date TIMESTAMP_LTZ(6),
  price               DECIMAL(10, 2),
  freight_value       DECIMAL(10, 2),
  error_reason        STRING,
  created_at          TIMESTAMP_LTZ(6),
  updated_at          TIMESTAMP_LTZ(6),
  PRIMARY KEY (order_id, order_item_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ecommerce.dlq.order_items',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'value.format' = 'debezium-json'
);

-- ── 2. Iceberg Sink: Orders ─────────────────────────────────
-- Upsert mode enabled (format v2) to track PostgreSQL updates.
CREATE TABLE IF NOT EXISTS iceberg_catalog.ecommerce.orders (
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
  'format-version' = '2',
  'write.upsert.enabled' = 'true',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '134217728'
);

-- ── 3. Iceberg Sink: Order Items (Clean Records Only) ───────
CREATE TABLE IF NOT EXISTS iceberg_catalog.ecommerce.order_items (
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
  'format-version' = '2',
  'write.upsert.enabled' = 'true',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '134217728'
);

-- ── 4. Iceberg Sink: Order Payments ─────────────────────────
CREATE TABLE IF NOT EXISTS iceberg_catalog.ecommerce.order_payments (
  order_id             STRING,
  payment_sequential   INT,
  payment_type         STRING,
  payment_installments INT,
  payment_value        DECIMAL(10, 2),
  created_at           TIMESTAMP_LTZ(6),
  PRIMARY KEY (order_id, payment_sequential) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'true',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '134217728'
);
