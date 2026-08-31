# PROJECT CONTEXT — Engineer 1 Complete
# Enterprise Agentic Streaming Lakehouse
# Paste this into a new chat to restore full context.
# Last updated: 2026-08-26
# ================================================================

## PROJECT OVERVIEW

This is a 4-engineer undergraduate capstone project called the
"Enterprise Agentic Streaming Lakehouse". It is a real-time data
pipeline that:
1. Captures live database changes from PostgreSQL via CDC
2. Streams them through Redpanda (Kafka-compatible broker)
3. Processes them with Apache Flink (stream processor)
4. Stores them in Apache Iceberg tables on MinIO (object storage)
5. Queries them with Trino (SQL engine)
6. Exposes them to an LLM via FastMCP (AI agent layer)

The dataset is the **Olist Brazilian E-Commerce** dataset (~100k orders,
8 relational tables, ~120MB of CSV files).

**Engineer split:**
- Engineer 1 (DONE): PostgreSQL + Debezium CDC + Redpanda + Data Mutator
- Engineer 2 (NEXT): Apache Flink + Data Quality + DLQ + Iceberg sink
- Engineer 3: MinIO + Apache Polaris (catalog) + Trino
- Engineer 4: FastMCP server + sqlglot AST security + LLM benchmarking

---

## MACHINE / ENVIRONMENT

- OS: Windows 11
- Shell: PowerShell (`py` command for Python, not `python`)
- Python: 3.12 at `C:\Users\Lenovo\AppData\Local\Programs\Python\Python312\`
- Docker Desktop: v28.5.1 installed and RUNNING
- Docker Compose: v2.40.0
- Project root: `D:\DE project\`
- Python packages installed: pandas, sqlalchemy, psycopg2-binary, kagglehub, pymupdf

---

## CURRENT STATE — ALL CONTAINERS RUNNING

Run `docker compose ps` from `D:\DE project\` to verify.

| Container | Image | Ports | Status |
|---|---|---|---|
| postgres | postgres:16 | 5433→5432 | healthy |
| redpanda | redpandadata/redpanda:latest | 19092, 18081, 18082, 9644 | healthy |
| redpanda-console | redpandadata/console:latest | 8088→8080 | running |
| minio | minio/minio:latest | 9000, 9001 | healthy |
| debezium | quay.io/debezium/server:2.7 | (no ports, internal only) | running |

**NOTE:** Port 5432 was already in use on the host machine, so PostgreSQL
is mapped to host port **5433**. Port 8080 was also in use, so Redpanda
Console is on **8088**.

**Web UIs:**
- Redpanda Console: http://localhost:8088
- MinIO Console: http://localhost:9001 (login: minioadmin / minioadmin123)

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
D:\DE project\
├── docker-compose.yml              ← All 5 Docker services
├── .env                            ← Passwords/config (DO NOT commit to git)
├── .gitignore                      ← Excludes .env and data/
├── postgres/
│   └── init.sql                    ← 9-table schema + dbz_publication
├── debezium/
│   └── application.properties      ← Debezium CDC config
├── scripts/
│   ├── download_olist.py           ← Downloads CSVs from Kaggle
│   └── data_mutator.py             ← Time-dilated simulation engine
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

## VERIFICATION COMMANDS

```powershell
# Check all containers are healthy
docker compose ps

# Check WAL is logical
docker exec -it postgres psql -U postgres -d olist_ecommerce -c "SHOW wal_level;"

# Check publication exists
docker exec -it postgres psql -U postgres -d olist_ecommerce -c "SELECT pubname, puballtables FROM pg_publication;"

# List all Redpanda topics
docker exec redpanda rpk topic list

# Read last 5 messages from orders topic
docker exec redpanda rpk topic consume ecommerce.public.orders --num 5 --offset start

# Check Debezium logs
docker logs debezium --tail 50
```

---

## ENGINEER 1 DEFINITION OF DONE ✅ (ACHIEVED)

From the architecture document:
> "Continuous, real-time CDC events are visible in the Redpanda Console
> mirroring the exact state of the PostgreSQL schema, operating stably
> under strict Docker memory limits."

**Verified:**
- 7 Redpanda topics with real CDC events confirmed ✅
- WAL level = logical confirmed ✅
- dbz_publication = all tables confirmed ✅
- 200 orders simulated through full lifecycle ✅
- Anomalies injected (negative prices) every 10th order ✅
- Memory limits set on all containers ✅

---

## WHAT ENGINEER 2 NEEDS TO DO NEXT

Engineer 2 picks up from the Redpanda topics.

**Their tasks:**
1. Deploy Apache Flink MiniCluster (add to docker-compose.yml)
2. Write Flink SQL DDL to consume `ecommerce.public.orders` topic
3. Define data quality rules (filter records where price < 0)
4. Route bad records to a DLQ topic (e.g., `ecommerce.dlq.order_items`)
5. Sink clean records to Apache Iceberg via REST catalog (Polaris)
6. Configure exactly-once semantics via Flink checkpointing
7. Prove recovery: kill Flink task manager, verify it resumes without data loss

**Key integration point for Engineer 2:**
- Flink must parse the Debezium JSON format (the `"op"` field for upserts)
- The Iceberg sink must use equality fields = `order_id` for UPSERT mode
- Flink SQL DDL must match the exact column names in the CDC JSON `"after"` payload

**Flink will connect to:**
- Redpanda: `redpanda:9092` (inside Docker network)
- MinIO (S3): `http://minio:9000` (inside Docker network)
- Polaris catalog: `http://polaris:8181` (Engineer 3 sets this up)

---

## KNOWN ISSUES / GOTCHAS

1. **Port conflicts on host:** PostgreSQL is on 5433 (not 5432), Redpanda Console
   is on 8088 (not 8080) because those ports were already in use on this machine.

2. **init.sql only runs once:** If you need to change the schema, you must run
   `docker compose down -v` first (wipes volumes) then `docker compose up -d`
   to force re-initialization.

3. **Data mutator must be re-run** after `docker compose down -v` since it wipes
   the database. Always seed first, then simulate.

4. **SQLAlchemy 2.0+** requires `text()` wrapper for all raw SQL strings.
   Never use `conn.execute("SELECT ...")` — always use `conn.execute(text("SELECT ..."))`.

5. **Windows terminal emoji issue:** PowerShell on Windows uses cp1252 encoding
   by default. Avoid emoji (✅, ❌) in Python print() statements or use
   `PYTHONIOENCODING=utf-8` environment variable.
