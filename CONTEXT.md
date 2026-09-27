# PROJECT CONTEXT — Engineer 1 & Engineer 2 Complete
# Enterprise Agentic Streaming Lakehouse
# Paste this into a new chat to restore full context.
# Last updated: 2026-09-27
# Branch: feat/engineer-2-flink-iceberg
# ================================================================

## PROJECT OVERVIEW

This is a 4-engineer undergraduate capstone project called the
"Enterprise Agentic Streaming Lakehouse". It is a real-time data
pipeline that:
1. Captures live database changes from PostgreSQL via CDC
2. Streams them through Redpanda (Kafka-compatible broker)
3. Processes them with Apache Flink (stream processor, data quality, DLQ)
4. Stores them in Apache Iceberg tables on MinIO (object storage)
5. Queries them with Trino (SQL engine)
6. Exposes them to an LLM via FastMCP (AI agent layer)

The dataset is the **Olist Brazilian E-Commerce** dataset (~100k orders,
8 relational tables, ~120MB of CSV files).

**Engineer split:**
- Engineer 1 (DONE): PostgreSQL + Debezium CDC + Redpanda + Data Mutator
- Engineer 2 (DONE): Apache Flink 1.20 + Data Quality (DLQ) + Iceberg Sink on MinIO + Checkpointing
- Engineer 3 (NEXT): MinIO + Apache Polaris (REST Catalog) + Trino Query Engine
- Engineer 4: FastMCP server + sqlglot AST security + LLM benchmarking

---

## MACHINE / ENVIRONMENT

- Primary Development & Validation Host: Windows 11 / PowerShell (`D:\DE project\`)
- Secondary Development Host: macOS (Apple Silicon arm64)
- Shell: PowerShell (`py` command on Windows, `python3` on Mac)
- Python: 3.12+ (pandas, sqlalchemy, psycopg2-binary, kagglehub)
- Docker Desktop: v28+ (WSL 2 on Windows, VirtioFS on Mac; requires ≥6 GB RAM allocated)
- Docker Compose: v2.20+

---

## CURRENT STATE — CONTAINER TOPOLOGY (9 SERVICES)

Run `docker compose ps` to verify all services.

| Container | Image | Ports | Role / Status |
|---|---|---|---|
| postgres | postgres:16 | 5433→5432 | Primary DB (logical decoding, dbz_publication) |
| redpanda | redpandadata/redpanda:latest | 19092, 18081, 18082, 9644 | Event broker (7 CDC topics + 1 DLQ topic) |
| redpanda-console | redpandadata/console:latest | 8088→8080 | Web UI for topics, consumers, messages |
| minio | minio/minio:latest | 9000, 9001 | S3 storage (Iceberg warehouse + checkpoints) |
| minio-init | minio/mc:latest | (none, runs once) | Auto-provisions 'lakehouse' bucket on boot |
| debezium | quay.io/debezium/server:2.7 | (none, internal) | Tails Postgres WAL -> Redpanda topics |
| flink-jobmanager | custom-flink:1.20 | 8081→8081 | Flink coordinator & Web Dashboard |
| flink-taskmanager | custom-flink:1.20 | (none, internal) | Flink streaming worker (2 slots) |
| flink-sql-client | custom-flink:1.20 | (none, interactive) | Interactive CLI for SQL job submission |

**NOTE:** Port 5432 was already in use on the host machine, so PostgreSQL
is mapped to host port **5433**. Port 8080 was also in use, so Redpanda
Console is on **8088**. Flink Dashboard is on **8081**.

**Web UIs:**
- Redpanda Console: http://localhost:8088
- MinIO Console: http://localhost:9001 (login: minioadmin / minioadmin123)
- Flink Dashboard: http://localhost:8081

---

## DATABASE

- Host (from host machine): localhost:5433
- Host (from inside Docker network): postgres:5432
- User: postgres
- Password: postgres
- Database: olist_ecommerce
- Connection string (Python): `postgresql+psycopg2://postgres:postgres@localhost:5433/olist_ecommerce`

### WAL Settings (verified working)
```
wal_level = logical              -- enables CDC
max_replication_slots = 4
max_wal_senders = 4
max_slot_wal_keep_size = 1GB     -- prevents disk explosion if Debezium goes down
```

### Replication Publication (verified working)
```sql
SELECT pubname, puballtables FROM pg_publication;
-- Returns: dbz_publication | t
-- This allows Debezium to subscribe to ALL table changes
```

---

## DATABASE SCHEMA (9 tables in olist_ecommerce database)

All tables are in the `public` schema.

### 1. customers
```sql
customer_id             VARCHAR(50) PRIMARY KEY
customer_unique_id      VARCHAR(50) NOT NULL
customer_zip_code_prefix VARCHAR(10) NOT NULL
customer_city           VARCHAR(100) NOT NULL
customer_state          CHAR(2) NOT NULL
created_at              TIMESTAMPTZ DEFAULT NOW()
updated_at              TIMESTAMPTZ DEFAULT NOW()
```

### 2. geolocation
```sql
geolocation_id              SERIAL PRIMARY KEY
geolocation_zip_code_prefix VARCHAR(10)
geolocation_lat             DOUBLE PRECISION
geolocation_lng             DOUBLE PRECISION
geolocation_city            VARCHAR(100)
geolocation_state           CHAR(2)
-- Index on zip_code_prefix
```

### 3. sellers
```sql
seller_id               VARCHAR(50) PRIMARY KEY
seller_zip_code_prefix  VARCHAR(10) NOT NULL
seller_city             VARCHAR(100) NOT NULL
seller_state            CHAR(2) NOT NULL
created_at              TIMESTAMPTZ DEFAULT NOW()
updated_at              TIMESTAMPTZ DEFAULT NOW()
```

### 4. products
```sql
product_id                  VARCHAR(50) PRIMARY KEY
product_category_name       VARCHAR(100)   -- Portuguese, may be NULL
product_name_length         INTEGER
product_description_length  INTEGER
product_photos_qty          INTEGER
product_weight_g            INTEGER
product_length_cm           INTEGER
product_height_cm           INTEGER
product_width_cm            INTEGER
created_at                  TIMESTAMPTZ DEFAULT NOW()
updated_at                  TIMESTAMPTZ DEFAULT NOW()
```

### 5. product_category_name_translation
```sql
product_category_name           VARCHAR(100) PRIMARY KEY   -- Portuguese
product_category_name_english   VARCHAR(100) NOT NULL      -- English
```

### 6. orders  ← CENTRAL FACT TABLE
```sql
order_id                        VARCHAR(50) PRIMARY KEY
customer_id                     VARCHAR(50) NOT NULL REFERENCES customers
order_status                    VARCHAR(20) NOT NULL
    -- CHECK: created|approved|processing|shipped|delivered|canceled|unavailable|invoiced
order_purchase_timestamp        TIMESTAMPTZ NOT NULL
order_approved_at               TIMESTAMPTZ
order_delivered_carrier_date    TIMESTAMPTZ
order_delivered_customer_date   TIMESTAMPTZ   -- NULL for canceled orders (real anomaly)
order_estimated_delivery_date   TIMESTAMPTZ
created_at                      TIMESTAMPTZ DEFAULT NOW()
updated_at                      TIMESTAMPTZ DEFAULT NOW()
-- Indexes on: customer_id, order_status, order_purchase_timestamp
```

### 7. order_items
```sql
order_id            VARCHAR(50) NOT NULL REFERENCES orders
order_item_id       INTEGER NOT NULL          -- sequence number within order
product_id          VARCHAR(50) NOT NULL REFERENCES products
seller_id           VARCHAR(50) NOT NULL REFERENCES sellers
shipping_limit_date TIMESTAMPTZ
price               NUMERIC(10,2)             -- intentionally nullable for DLQ testing
freight_value       NUMERIC(10,2)
created_at          TIMESTAMPTZ DEFAULT NOW()
updated_at          TIMESTAMPTZ DEFAULT NOW()
PRIMARY KEY (order_id, order_item_id)
-- Indexes on: product_id, seller_id
```

### 8. order_payments
```sql
order_id            VARCHAR(50) NOT NULL REFERENCES orders
payment_sequential  INTEGER NOT NULL          -- installment number
payment_type        VARCHAR(30) NOT NULL      -- credit_card|boleto|voucher|debit_card
payment_installments INTEGER DEFAULT 1
payment_value       NUMERIC(10,2) NOT NULL
created_at          TIMESTAMPTZ DEFAULT NOW()
PRIMARY KEY (order_id, payment_sequential)
-- Index on: payment_type
```

### 9. order_reviews
```sql
review_id               VARCHAR(50) PRIMARY KEY
order_id                VARCHAR(50) NOT NULL REFERENCES orders
review_score            SMALLINT NOT NULL CHECK (1-5)
review_comment_title    VARCHAR(255)          -- often NULL
review_comment_message  TEXT                 -- Portuguese, often NULL
review_creation_date    TIMESTAMPTZ
review_answer_timestamp TIMESTAMPTZ
created_at              TIMESTAMPTZ DEFAULT NOW()
-- Indexes on: order_id, review_score
```

---

## REDPANDA CDC TOPICS (7 topics, all verified working)

Topic naming format: `ecommerce.public.<table_name>`

| Topic | Content |
|---|---|
| ecommerce.public.orders | Order lifecycle events |
| ecommerce.public.order_items | Line item events |
| ecommerce.public.order_payments | Payment events |
| ecommerce.public.customers | Customer insert events |
| ecommerce.public.products | Product insert events |
| ecommerce.public.sellers | Seller insert events |
| ecommerce.public.product_category_name_translation | Translation inserts |

### CDC JSON Message Format (Debezium)
Every message has this structure:
```json
{
  "key": "{\"order_id\":\"abc123\"}",
  "value": {
    "before": null,          // null for INSERT; old row for UPDATE/DELETE
    "after": {               // new row state
      "order_id": "abc123",
      "order_status": "processing",
      ...
    },
    "op": "c",               // "c"=INSERT, "u"=UPDATE, "d"=DELETE
    "source": {
      "connector": "postgresql",
      "table": "orders",
      "lsn": 26844864,       // WAL position
      "txId": 764
    }
  }
}
```

**Key field `"op"`:**
- `"c"` = create (INSERT)
- `"u"` = update (UPDATE)
- `"d"` = delete (DELETE)
- `"r"` = read (snapshot)

---

## DATA MUTATOR

**File:** `D:\DE project\scripts\data_mutator.py`

Replays Olist CSV data as live database mutations to drive the CDC pipeline.

**Config at top of file:**
```python
DB_URL     = "postgresql+psycopg2://postgres:postgres@localhost:5433/olist_ecommerce"
DATA_DIR   = r"d:\DE project\data\olist"
DELAY      = 2.0    # seconds between order status transitions
MAX_ORDERS = 200    # how many orders to simulate
```

**How it works:**
1. Loads 9 CSV files into Pandas DataFrames
2. Seeds ALL customers, products, sellers, translations into DB first (one-time)
3. Then loops over orders chronologically:
   - INSERT order with status=`processing`
   - INSERT order_items (line items for that order)
   - INSERT order_payments
   - Every 10th order: injects a NEGATIVE PRICE record (anomaly for Flink DLQ)
   - Wait DELAY → UPDATE to `approved`
   - Wait DELAY → UPDATE to `shipped`
   - Wait DELAY → UPDATE to `delivered`

**Anomaly injection (every 10th order):**
Inserts an order_items record with `price = -999.99`. This is intentional —
Engineer 2's Flink SQL will catch this and route it to the Dead Letter Queue.

**Run command:**
```powershell
py scripts\data_mutator.py
```

**Status:** Successfully ran 200 orders including anomaly injections. ✅

---

## DEBEZIUM SERVER CONFIG

**File:** `D:\DE project\debezium\application.properties`

```properties
# Sink: Redpanda (Kafka API)
debezium.sink.type=kafka
debezium.sink.kafka.producer.bootstrap.servers=redpanda:9092

# Source: PostgreSQL
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.topic.prefix=ecommerce
debezium.source.database.hostname=postgres
debezium.source.database.port=5432
debezium.source.database.user=postgres
debezium.source.database.password=postgres
debezium.source.database.dbname=olist_ecommerce
debezium.source.plugin.name=pgoutput
debezium.source.slot.name=debezium_slot
debezium.source.publication.name=dbz_publication
debezium.source.publication.autocreate.mode=disabled

# Format: pure JSON, no schema envelope
debezium.format.key=json
debezium.format.value=json
debezium.format.key.schemas.enable=false
debezium.format.value.schemas.enable=false
```

**Replication slot name:** `debezium_slot` (auto-created by Debezium on first start)
**WAL offset tracking:** stored in `/debezium/conf/data/offsets.dat` inside container

---

## FILE STRUCTURE

```
D:\DE project\ (or git repository root)
├── .env                            ← Passwords/config (DO NOT commit to git)
├── .env.example                    ← Reference template for required variables
├── .gitattributes                  ← Enforces Unix LF line endings across OS
├── .gitignore                      ← Excludes .env, large datasets, and artifacts
├── docker-compose.yml              ← All 9 Docker services (Infra + Flink Lakehouse)
├── postgres/
│   └── init.sql                    ← 9-table schema + dbz_publication
├── debezium/
│   └── application.properties      ← Debezium CDC config
├── flink/
│   ├── Dockerfile                  ← Custom Flink 1.20 image with pinned JARs
│   ├── README.md                   ← Flink architecture & operational guide
│   └── sql/
│       ├── 01-catalog.sql          ← Iceberg Hadoop catalog & checkpointing config
│       ├── 02-sources.sql          ← Debezium CDC Kafka source tables (orders, items, pmts)
│       ├── 03-sinks.sql            ← Redpanda DLQ sink + Iceberg Lakehouse sinks
│       ├── 04-jobs.sql             ← Continuous ingestion jobs (Statement Set)
│       └── run_all.sql             ← One-shot pipeline submission script
├── scripts/
│   ├── download_olist.py           ← Downloads CSVs from Kaggle (cross-platform)
│   └── data_mutator.py             ← Time-dilated simulation engine (cross-platform)
└── data/
    └── olist/                      ← 9 CSV files (NOT in git, 120MB)
        ├── olist_orders_dataset.csv
        ├── olist_order_items_dataset.csv
        ├── olist_order_payments_dataset.csv
        ├── olist_order_reviews_dataset.csv
        ├── olist_customers_dataset.csv
        ├── olist_sellers_dataset.csv
        ├── olist_products_dataset.csv
        ├── olist_geolocation_dataset.csv
        └── product_category_name_translation.csv
```

---

## .env FILE CONTENTS (for reference — never commit to git)

```
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

## FLINK & LAKEHOUSE ARCHITECTURE (ENGINEER 2)

### Pinned Compatibility Matrix
- Flink Base: `flink:1.20-java17`
- Kafka Connector: `flink-sql-connector-kafka:3.4.0-1.20`
- Iceberg Runtime: `iceberg-flink-runtime-1.20:1.7.1`
- Hadoop S3A Filesystem: `flink-shaded-hadoop-2-uber:2.8.3-10.0`
- AWS SDK Bundle: `aws-java-sdk-bundle:1.12.648`

### Data Quality & Dead Letter Queue (DLQ) Strategy
- **Source Topic**: `ecommerce.public.order_items`
- **Anomaly Detection**: `price < 0 OR price IS NULL OR freight_value < 0`
- **DLQ Sink**: `ecommerce.dlq.order_items` (Kafka format: `debezium-json`) enriched with `error_reason`:
  - `NEGATIVE_PRICE` (captures mutator anomaly injected every 10th order)
  - `NULL_PRICE` (captures missing prices)
  - `NEGATIVE_FREIGHT` (captures freight anomalies)
- **Clean Lakehouse Sink**: `iceberg_catalog.ecommerce.order_items`
  - Filter: `WHERE price >= 0 AND (freight_value >= 0 OR freight_value IS NULL)`

### Iceberg Lakehouse Catalog
- **Catalog Type**: Hadoop Catalog (`iceberg_catalog`) backed by `s3a://lakehouse/warehouse`
- **Table Format**: Version 2 (`format-version = '2'`) with row-level upserts (`write.upsert.enabled = 'true'`)
- **Compression**: `zstd` Parquet files with target size 128 MB

### Fault Tolerance & Checkpointing
- **Mode**: `EXACTLY_ONCE`
- **Interval**: 60 seconds (min pause: 30s, timeout: 120s)
- **Storage**: `s3a://lakehouse/checkpoints/flink`
- **Restart Strategy**: Fixed delay (3 attempts, 10s delay)

---

## VERIFICATION COMMANDS

```powershell
# ── 1. Check all 9 containers ─────────────────────────────────
docker compose ps

# ── 2. Check Flink cluster health ─────────────────────────────
# Open Web UI at http://localhost:8081 (1 TaskManager, 2 slots)
curl -s http://localhost:8081/overview

# ── 3. Run streaming jobs in one shot ─────────────────────────
docker exec -i flink-sql-client bin/sql-client.sh -f /opt/flink/sql/run_all.sql

# ── 4. Query clean Iceberg tables via Flink SQL ───────────────
docker exec -it flink-sql-client bin/sql-client.sh
# Inside SQL Client:
SELECT count(*) FROM iceberg_catalog.ecommerce.orders;
SELECT count(*) FROM iceberg_catalog.ecommerce.order_items;
SELECT * FROM iceberg_catalog.ecommerce.order_items WHERE price < 0; -- MUST return 0 rows

# ── 5. Verify Dead Letter Queue in Redpanda ───────────────────
# Check messages in topic 'ecommerce.dlq.order_items'
docker exec redpanda rpk topic consume ecommerce.dlq.order_items --num 5

# ── 6. Verify Parquet data files on MinIO ─────────────────────
# MinIO Console at http://localhost:9001 (minioadmin / minioadmin123)
# Inspect bucket: lakehouse/warehouse/ecommerce/
```

---

## ENGINEER 1 DEFINITION OF DONE ✅ (ACHIEVED)

- 7 Redpanda topics with real CDC events confirmed ✅
- WAL level = logical confirmed ✅
- dbz_publication = all tables confirmed ✅
- 200 orders simulated through full lifecycle ✅
- Anomalies injected (negative prices) every 10th order ✅
- Memory limits set on all containers ✅

---

## ENGINEER 2 DEFINITION OF DONE ✅ (IMPLEMENTED)

- Custom Flink 1.20 Docker image built with Kafka, Iceberg, and S3A JARs ✅
- MinIO init container auto-provisions `lakehouse` bucket ✅
- Flink Session Cluster (JobManager + TaskManager + SQL Client) added to Docker Compose ✅
- Flink SQL sources consume CDC changelogs with `TIMESTAMP_LTZ(6)` ✅
- Data quality gate branches clean records to Iceberg and invalid records to DLQ topic ✅
- DLQ messages enriched with `error_reason` diagnostics ✅
- Clean records upserted into Iceberg v2 tables on MinIO as compressed Parquet files ✅
- Exactly-once checkpointing configured to S3A storage ✅
- Cross-platform Unix LF line endings enforced via `.gitattributes` ✅

---

## WHAT ENGINEER 3 NEEDS TO DO NEXT

Engineer 3 picks up from the Iceberg tables on MinIO:
1. **Apache Polaris REST Catalog**:
   - Deploy Apache Polaris server in `docker-compose.yml`
   - Point Polaris at `s3://lakehouse/warehouse`
   - Migrate/register Iceberg tables from Hadoop Catalog to Polaris REST Catalog
2. **Trino Query Engine**:
   - Deploy Trino container connected to Polaris REST Catalog
   - Configure Iceberg connector in Trino (`iceberg.properties`)
   - Verify fast, federated analytical SQL queries across all e-commerce tables
3. **Table Maintenance**:
   - Schedule Iceberg table compaction and orphan file cleanup jobs

---

## KNOWN ISSUES / GOTCHAS

1. **Port conflicts on host:** PostgreSQL is on 5433 (not 5432), Redpanda Console
   is on 8088 (not 8080), Flink Web UI is on 8081.
2. **Docker Desktop RAM allocation:** Requires ≥ 6 GB memory allocated to Docker VM (WSL 2 on Windows).
3. **init.sql only runs once:** If you need to change the schema, run
   `docker compose down -v` first (wipes volumes) then `docker compose up -d`.
4. **Data mutator must be re-run** after `docker compose down -v` since it wipes
   the database. Always seed first, then simulate.
5. **Windows terminal emoji issue:** PowerShell on Windows uses cp1252 encoding
   by default. Avoid emoji in Python print() or use `$env:PYTHONIOENCODING="utf-8"`.

