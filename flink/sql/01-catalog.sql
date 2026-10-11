-- ============================================================
-- 01-catalog.sql: Apache Iceberg Hadoop Catalog & Checkpointing
-- Engineer 2: Enterprise Agentic Streaming Lakehouse
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

-- ── 2. Create Iceberg Hadoop Catalog on MinIO ───────────────
-- Using Hadoop catalog type for Engineer 2 testing and verification.
-- Tables and Parquet data are stored under s3a://lakehouse/warehouse/.
-- Cleanly migrates to Apache Polaris REST catalog in Phase 3.
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'io-impl' = 'org.apache.iceberg.hadoop.HadoopFileIO',
  'warehouse' = 'polaris',
  'uri' = 'http://polaris:8181/api/catalog',
  'credential' = 'a03cca35bfce7cf8:b6d7a396db32af73b5c2c9d41e8fa7b1',
  'property-version' = '1',
  'client.region' = 'us-east-1',
  's3.region' = 'us-east-1',
  'hadoop.fs.s3a.endpoint' = 'http://minio:9000',
  'hadoop.fs.s3a.path.style.access' = 'true',
  'hadoop.fs.s3a.access.key' = 'minioadmin',
  'hadoop.fs.s3a.secret.key' = 'minioadmin123'
);

-- ── 3. Create E-Commerce Database in Catalog ────────────────
CREATE DATABASE IF NOT EXISTS iceberg_catalog.ecommerce;
