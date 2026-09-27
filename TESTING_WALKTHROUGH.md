# Testing & Verification Walkthrough (Engineer 1 — Windows 11)

This guide is designed for **Engineer 1** to test and verify Engineer 2's Apache Flink CDC Ingestion, Data Quality Gate (DLQ), and Apache Iceberg Lakehouse setup on your Windows machine (`D:\DE project\`).

---

## Prerequisites Check

Before starting:
1. **Docker Desktop is running** on Windows with WSL 2.
2. Ensure Docker Desktop has at least **6 GB of RAM allocated** (Settings → Resources → Advanced / WSL 2).
3. Open a **PowerShell** terminal at your project directory:
   ```powershell
   cd "D:\DE project"
   ```

---

## Step 1: Switch to the Engineer 2 Branch

Fetch and checkout the feature branch:

```powershell
git fetch origin
git checkout feat/engineer-2-flink-iceberg
git pull origin feat/engineer-2-flink-iceberg
```

Verify you have the new files:
```powershell
Get-ChildItem flink
# Should list Dockerfile, README.md, and sql/ folder
```

---

## Step 2: Ensure Environment File (`.env`)

Check if your `.env` file exists in the root folder. If not, copy from `.env.example`:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Ensure `.env` contains:
```ini
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=olist_ecommerce
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
REDPANDA_BROKER_PORT=9092
REDPANDA_SCHEMA_REGISTRY_PORT=8081
REDPANDA_ADMIN_PORT=9644
REDPANDA_CONSOLE_PORT=8080
```

---

## Step 3: Build & Start the Flink Lakehouse Services

Build the custom Flink image (downloads Kafka, Iceberg, and S3A JARs) and start the new containers:

```powershell
docker compose up -d --build minio-init flink-jobmanager flink-taskmanager flink-sql-client
```

Wait ~30 seconds, then check that all containers are healthy and running:

```powershell
docker compose ps
```

You should see 8 active containers and 1 completed container (`minio-init` exits 0 after creating the bucket):
- `postgres` (healthy)
- `redpanda` (healthy)
- `redpanda-console` (running)
- `minio` (healthy)
- `minio-init` (Exited 0 — bucket initialized)
- `debezium` (running)
- `flink-jobmanager` (running)
- `flink-taskmanager` (running)
- `flink-sql-client` (running)

---

## Step 4: Verify Web Dashboards

Open the following in your web browser:
1. **Flink Web Dashboard**: [http://localhost:8081](http://localhost:8081)
   - Verify: Under **Task Managers**, you see **1 TaskManager** with **2 Task Slots**.
2. **Redpanda Console**: [http://localhost:8088](http://localhost:8088)
   - Verify: 7 CDC topics exist (`ecommerce.public.*`).
3. **MinIO Console**: [http://localhost:9001](http://localhost:9001)
   - Login: `minioadmin` / `minioadmin123`
   - Verify: Bucket `lakehouse` exists.

---

## Step 5: Submit the Flink Streaming Pipeline

You can submit all SQL definitions and streaming jobs in **one shot**:

```powershell
docker exec -i flink-sql-client bin/sql-client.sh -f /opt/flink/sql/run_all.sql
```

*(Optional alternative for live presentation: run interactively)*
```powershell
docker exec -it flink-sql-client bin/sql-client.sh
```
Inside the interactive SQL Client:
```sql
!run /opt/flink/sql/01-catalog.sql
!run /opt/flink/sql/02-sources.sql
!run /opt/flink/sql/03-sinks.sql
!run /opt/flink/sql/04-jobs.sql
```

### What Just Happened?
1. Flink configured `EXACTLY_ONCE` checkpoints to `s3a://lakehouse/checkpoints/flink`.
2. Created `iceberg_catalog` backed by MinIO S3 storage (`s3a://lakehouse/warehouse`).
3. Registered CDC source tables reading Redpanda topics with `TIMESTAMP_LTZ(6)`.
4. Submitted an `EXECUTE STATEMENT SET` job combining 4 continuous streaming sinks:
   - Sinks bad records (`price < 0`, `NULL price`, `negative freight`) to Dead Letter Queue topic.
   - Sinks valid records (`price >= 0`) into Iceberg table `order_items`.
   - Sinks orders and payments into Iceberg tables `orders` and `order_payments`.

Check the **Flink Web Dashboard** at [http://localhost:8081](http://localhost:8081) — you will now see **1 Running Job** in the dashboard with an active execution graph!

---

## Step 6: Run the Data Mutator (Generate Live Events)

In a separate PowerShell terminal, run your data mutator to simulate live e-commerce orders and inject anomalies:

```powershell
py scripts\data_mutator.py
```

Let it run through 20–30 orders (including at least 2 anomaly injections where `price = -999.99`).

---

## Step 7: Verification & Proof of Work

### 1. Verify Dead Letter Queue in Redpanda
Open **Redpanda Console** at [http://localhost:8088/topics](http://localhost:8088/topics).
- You will see a new topic: `ecommerce.dlq.order_items`.
- Click into the topic and inspect the messages.
- **Expected**: Every message has `price = -999.99` and an enriched field `error_reason = "NEGATIVE_PRICE"`.
- Clean orders (valid prices) are **not** in this topic.

Or verify via CLI:
```powershell
docker exec redpanda rpk topic consume ecommerce.dlq.order_items --num 2
```

### 2. Verify Clean Lakehouse Data in Iceberg (MinIO)
Open **MinIO Console** at [http://localhost:9001](http://localhost:9001).
- Navigate into `lakehouse/warehouse/ecommerce/`.
- Inspect folders:
  - `orders/` → `data/` (Parquet files) and `metadata/` (`.metadata.json`, `.avro`)
  - `order_items/` → `data/` and `metadata/`
  - `order_payments/` → `data/` and `metadata/`
- Navigate to `lakehouse/checkpoints/flink/` → verify active checkpoint metadata.

### 3. Query Iceberg via Flink SQL Client
Enter the SQL client:

```powershell
docker exec -it flink-sql-client bin/sql-client.sh
```

Run these verification queries:

```sql
-- 1. Check orders ingested into Iceberg:
SELECT count(*) FROM iceberg_catalog.ecommerce.orders;

-- 2. Check order items ingested into Iceberg:
SELECT count(*) FROM iceberg_catalog.ecommerce.order_items;

-- 3. PROVE DATA QUALITY GATE (Must return 0 rows):
SELECT * FROM iceberg_catalog.ecommerce.order_items WHERE price < 0;

-- 4. Check order status distribution:
SELECT order_status, count(*) FROM iceberg_catalog.ecommerce.orders GROUP BY order_status;
```

---

## Step 8: Fault-Tolerance Demonstration (Crash & Recovery)

To prove **Exactly-Once Fault Tolerance**:
1. Check current row count in SQL client:
   ```sql
   SELECT count(*) FROM iceberg_catalog.ecommerce.orders;
   ```
2. While `data_mutator.py` is running, simulate a hard worker crash:
   ```powershell
   docker kill flink-taskmanager
   ```
3. Docker will automatically restart `flink-taskmanager` within 10 seconds.
4. Watch the Flink Dashboard at `:8081` — the job will recover automatically from the last MinIO S3A checkpoint.
5. Re-run row count and duplicate check:
   ```sql
   -- Verify row count continued increasing:
   SELECT count(*) FROM iceberg_catalog.ecommerce.orders;

   -- PROVE NO DUPLICATES:
   SELECT order_id, count(*) FROM iceberg_catalog.ecommerce.orders GROUP BY order_id HAVING count(*) > 1;
   -- Must return 0 rows (Upsert mode ensures idempotency)
   ```

---

## Troubleshooting on Windows

- **Port 8081 already in use**:
  If another service is using port 8081 on your machine, edit `docker-compose.yml` to change `8081:8081` under `flink-jobmanager` to `8082:8081`.
- **WSL 2 Memory Pressure**:
  If containers exit unexpectedly with code 137 (OOM), create or edit `C:\Users\<YourUser>\.wslconfig` with:
  ```ini
  [wsl2]
  memory=6GB
  ```
  Then run `wsl --shutdown` in PowerShell and restart Docker Desktop.
- **Resetting state if needed**:
  ```powershell
  docker compose down -v
  docker compose up -d
  ```
