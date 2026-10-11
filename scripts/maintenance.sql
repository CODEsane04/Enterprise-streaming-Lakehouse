-- ============================================================
-- Engineer 3: Iceberg Table Maintenance in Trino
-- ============================================================

-- 1. Compact small files in the orders table
ALTER TABLE iceberg.ecommerce.orders EXECUTE optimize;

-- 2. Compact small files in the order_items table
ALTER TABLE iceberg.ecommerce.order_items EXECUTE optimize;

-- 3. Expire snapshots older than 7 days
ALTER TABLE iceberg.ecommerce.orders EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.ecommerce.order_items EXECUTE expire_snapshots(retention_threshold => '7d');

-- 4. Remove orphan files (files not tracked by Iceberg metadata)
ALTER TABLE iceberg.ecommerce.orders EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE iceberg.ecommerce.order_items EXECUTE remove_orphan_files(retention_threshold => '7d');
