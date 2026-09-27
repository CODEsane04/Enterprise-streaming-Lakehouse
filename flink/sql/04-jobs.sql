-- ============================================================
-- 04-jobs.sql: Continuous Streaming Ingestion & Routing Jobs
-- Engineer 2: Enterprise Agentic Streaming Lakehouse
-- ============================================================
-- Using EXECUTE STATEMENT SET allows Flink to execute all sinks
-- concurrently in a single streaming DAG, sharing the Kafka consumer
-- instances across both the clean Iceberg sink and the DLQ sink.
-- ============================================================

EXECUTE STATEMENT SET
BEGIN

  -- ── Job 1: Route Bad Order Items to Dead Letter Queue (DLQ) ──
  -- Catches anomalies: negative price (mutator), NULL price, negative freight
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

  -- ── Job 2: Upsert Clean Order Items to Iceberg Lakehouse ─────
  -- Strict data quality gate: filters out any bad records
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

  -- ── Job 3: Upsert Orders to Iceberg Lakehouse ─────────────────
  -- Tracks processing -> approved -> shipped -> delivered lifecycle
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

  -- ── Job 4: Upsert Order Payments to Iceberg Lakehouse ────────
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
