-- ============================================================
-- run_all.sql: Complete Flink Pipeline Execution Script
-- Engineer 2: Enterprise Agentic Streaming Lakehouse
-- ============================================================
-- Executes the complete pipeline end-to-end:
-- 1. Checkpoint & Iceberg Hadoop Catalog Setup
-- 2. CDC Kafka Source Table Definitions
-- 3. DLQ & Iceberg Sink Table Definitions
-- 4. Unified Statement Set (Continuous Ingestion & Routing)
-- ============================================================

-- ── 1. Checkpointing & State Configuration ──────────────────
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.min-pause' = '30s';
SET 'execution.checkpointing.timeout' = '120s';
SET 'state.backend.type' = 'hashmap';
SET 'state.checkpoints.dir' = 's3a://lakehouse/checkpoints/flink';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '3';
SET 'restart-strategy.fixed-delay.delay' = '10s';

-- ── 2. Iceberg Hadoop Catalog ───────────────────────────────
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 's3a://lakehouse/warehouse',
  'property-version' = '1'
);

CREATE DATABASE IF NOT EXISTS iceberg_catalog.ecommerce;

-- ── 3. Source Tables (Debezium CDC over Kafka) ───────────────
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

-- ── 4. Sink Tables (DLQ Kafka + Iceberg Lakehouse) ───────────
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

-- ── 5. Unified Streaming Job Submission ─────────────────────
EXECUTE STATEMENT SET
BEGIN

  -- 1. DLQ Routing for Anomalous Order Items
  INSERT INTO dlq_order_items
  SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    CASE
      WHEN price < 0 THEN 'NEGATIVE_PRICE'
      WHEN price IS NULL THEN 'NULL_PRICE'
      WHEN freight_value < 0 THEN 'NEGATIVE_FREIGHT'
      ELSE 'UNKNOWN_ANOMALY'
    END AS error_reason,
    created_at,
    updated_at
  FROM cdc_order_items
  WHERE price < 0 OR price IS NULL OR freight_value < 0;

  -- 2. Clean Order Items to Iceberg
  INSERT INTO iceberg_catalog.ecommerce.order_items
  SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    created_at,
    updated_at
  FROM cdc_order_items
  WHERE price >= 0 AND (freight_value >= 0 OR freight_value IS NULL);

  -- 3. Orders to Iceberg
  INSERT INTO iceberg_catalog.ecommerce.orders
  SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    created_at,
    updated_at
  FROM cdc_orders;

  -- 4. Order Payments to Iceberg
  INSERT INTO iceberg_catalog.ecommerce.order_payments
  SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    created_at
  FROM cdc_order_payments;

END;
