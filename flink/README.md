# Apache Flink 1.20 Streaming Engine (Engineer 2)

This directory contains the Apache Flink stream processing infrastructure for the **Enterprise Agentic Streaming Lakehouse**.

---

## 1. Architecture & Components

```
                          ┌──────────────────────────────┐
                          │   Redpanda CDC Topics        │
                          │   (ecommerce.public.*)       │
                          └──────────────┬───────────────┘
                                         │
                        [Debezium JSON Deserialization]
                                         │
                          ┌──────────────▼───────────────┐
                          │     Apache Flink 1.20        │
                          │   (Session Cluster on JVM)   │
                          └──────┬───────────────┬───────┘
                                 │               │
                 [price < 0 OR   │               │ [price >= 0 AND
                  price IS NULL  │               │  freight_value >= 0]
                  OR freight < 0]│               │
                                 │               │
                  ┌──────────────▼──────┐ ┌──────▼───────────────┐
                  │ Redpanda DLQ Topic  │ │ Apache Iceberg       │
                  │ (ecommerce.dlq.     │ │ (MinIO S3A Storage)  │
                  │  order_items)       │ │ (Parquet + V2 Upsert)│
                  └─────────────────────┘ └──────────────────────┘
```

### Components
1. **Dockerfile**: Builds a custom Flink 1.20 image with pinned JARs:
   - `flink-sql-connector-kafka-3.4.0-1.20.jar`
   - `iceberg-flink-runtime-1.20-1.7.1.jar`
   - `flink-shaded-hadoop-2-uber-2.8.3-10.0.jar`
   - `aws-java-sdk-bundle-1.12.648.jar`
2. **MinIO Init Container**: Ensures the `lakehouse` bucket exists prior to Flink JobManager initialization.
3. **Flink Cluster**:
   - `flink-jobmanager`: Port `8081` (Web Dashboard & Job Coordinator)
   - `flink-taskmanager`: Worker processing 2 slots, checkpointing to `s3a://lakehouse/checkpoints/flink`
   - `flink-sql-client`: Interactive CLI client with `./flink/sql` mounted at `/opt/flink/sql`

---

## 2. SQL Scripts Organization (`flink/sql/`)

| Script | Purpose |
|:---|:---|
| `01-catalog.sql` | Configures EXACTLY_ONCE checkpoints (60s interval) and creates the Iceberg Hadoop Catalog `iceberg_catalog` backed by `s3a://lakehouse/warehouse`. |
| `02-sources.sql` | Declares CDC source tables (`cdc_orders`, `cdc_order_items`, `cdc_order_payments`) reading Redpanda topics with `value.format = 'debezium-json'`. |
| `03-sinks.sql` | Declares the Dead Letter Queue Kafka sink (`dlq_order_items`) and Iceberg tables with format v2 upsert enabled. |
| `04-jobs.sql` | Submits the continuous streaming pipeline using `EXECUTE STATEMENT SET` with data quality filters. |
| `run_all.sql` | Single consolidated script executing the entire pipeline (01 through 04) in one shot. |

---

## 3. Data Quality Gate & DLQ Strategy

The stream branches based on strict validation rules for `order_items`:
- **Anomalous Records**:
  - Condition: `price < 0 OR price IS NULL OR freight_value < 0`
  - Action: Routed to `ecommerce.dlq.order_items` in Redpanda with an enriched `error_reason` tag (`NEGATIVE_PRICE`, `NULL_PRICE`, `NEGATIVE_FREIGHT`).
- **Clean Records**:
  - Condition: `price >= 0 AND (freight_value >= 0 OR freight_value IS NULL)`
  - Action: Upserted into `iceberg_catalog.ecommerce.order_items` on MinIO.

---

## 4. Execution Walkthrough (When Ready to Run)

### Step 1: Build & Start Services
```bash
docker compose up -d --build minio-init flink-jobmanager flink-taskmanager flink-sql-client
```

### Step 2: Verify Flink Cluster Health
- Open Flink Web UI: [http://localhost:8081](http://localhost:8081)
- Verify 1 TaskManager and 2 Task Slots are registered.

### Step 3: Run the Streaming Pipeline
Execute all SQL scripts in one shot via the SQL Client container:
```bash
docker exec -i flink-sql-client bin/sql-client.sh -f /opt/flink/sql/run_all.sql
```
Or interactively step-by-step:
```bash
docker exec -it flink-sql-client bin/sql-client.sh
```
Inside the interactive SQL Client:
```sql
-- Run script 1:
!run /opt/flink/sql/01-catalog.sql
-- Run script 2:
!run /opt/flink/sql/02-sources.sql
-- Run script 3:
!run /opt/flink/sql/03-sinks.sql
-- Run script 4:
!run /opt/flink/sql/04-jobs.sql
```

### Step 4: Verification Queries
Within the SQL Client:
```sql
-- Verify Iceberg tables have data:
SELECT count(*) FROM iceberg_catalog.ecommerce.orders;
SELECT count(*) FROM iceberg_catalog.ecommerce.order_items;

-- Verify NO anomalies exist in clean lakehouse:
SELECT * FROM iceberg_catalog.ecommerce.order_items WHERE price < 0; -- Returns 0 rows
```

In Redpanda Console ([http://localhost:8088](http://localhost:8088)):
- Check topic `ecommerce.dlq.order_items` for negative-price anomalies and `error_reason = 'NEGATIVE_PRICE'`.
