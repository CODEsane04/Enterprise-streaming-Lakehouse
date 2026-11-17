# Enterprise Agentic Streaming Lakehouse

A production single-tier Kappa lakehouse architecture featuring real-time PostgreSQL Change Data Capture (CDC), shift-left Apache Flink stream quality validation, Apache Iceberg v2 storage on MinIO S3, Apache Polaris REST catalog synchronization, Trino distributed SQL, and deterministic `sqlglot` AST-governed multi-agent querying.

---

## Architecture

```
PostgreSQL 16 (Logical WAL)
       │
       ▼
Debezium Server 2.7 (Quarkus CDC)
       │
       ▼
Redpanda C++ Broker (Topic: ecommerce.orders)
       │
       ▼
Apache Flink 1.20 (Inline Quality Gate)
       ├── Valid Stream   ──► Apache Iceberg v2 (Parquet MoR on MinIO S3)
       └── Anomaly Divert ──► Redpanda DLQ (ecommerce.dlq.order_items)
                                     │
Apache Polaris REST Catalog ◄────────┴────────► Trino 442 Query Engine
                                                      ▲
                                                      │ (Sanitized SQL)
                                            FastMCP Security Firewall
                                            (sqlglot AST Parser)
                                                      ▲
                                                      │ (NL2SQL Intent)
                                            LangGraph Multi-Agent UI
```

### Core Highlights
* **Zero JVM Message Broker Overhead:** Sub-millisecond messaging via C++ Redpanda Raft broker.
* **Shift-Left Quality Quarantine:** In-stream anomaly interception routing corrupted records (`price < 0`) to a Dead Letter Queue before reaching storage.
* **ACID Streaming Lakehouse:** Apache Iceberg v2 Merge-on-Read (MoR) with positional deletes, minimizing write amplification ($1.18\times$).
* **Open Catalog Concurrency:** Centralized REST catalog metadata and snapshot arbitration via Apache Polaris.
* **Deterministic AI Security:** Model Context Protocol (MCP) gateway enforcing recursive AST query validation and `LIMIT 1000` clamping.

---

## Quickstart

### Prerequisites
* Docker Engine 24+ & Docker Compose v2
* Python 3.11+
* At least 10 GB free host RAM

### 1. Environment Setup
```bash
git clone https://github.com/CODEsane04/Enterprise-streaming-Lakehouse.git
cd Enterprise-streaming-Lakehouse

cp .env.example .env
```

### 2. Launch Infrastructure
```bash
docker-compose up -d
```
Verify all 13 containers report healthy:
```bash
docker-compose ps
```

### 3. Bootstrap Catalog & Sync Credentials
```bash
# Sync dynamic Polaris OAuth2 credentials
python update_creds.py

# Initialize catalog namespace
python scripts/bootstrap_polaris.py
```

### 4. Start Streaming Quality Pipeline
```bash
docker exec -i flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/run_all.sql
```

### 5. Replay Live Mutations & Query
```bash
# In terminal 1: Stream e-commerce orders with simulated anomalies
python scripts/data_mutator.py

# In terminal 2: Launch multi-agent analytical UI
streamlit run frontend/app.py
```

---

## Service Directory

| Service | Container | Port | Interface |
| :--- | :--- | :--- | :--- |
| **PostgreSQL 16** | `postgres` | `5434` | Relational source (`wal_level=logical`) |
| **Redpanda Console** | `redpanda-console` | `8088` | Stream management: http://localhost:8088 |
| **Flink Dashboard** | `flink-jobmanager` | `8081` | Stream monitoring: http://localhost:8081 |
| **MinIO Console** | `minio` | `9001` | Object storage: http://localhost:9001 |
| **Apache Polaris** | `polaris` | `8181` | Iceberg REST catalog API |
| **Trino Coordinator** | `trino` | `8089` | Distributed SQL console: http://localhost:8089 |
| **FastAPI Backend** | `backend-api` | `8002` | API documentation: http://localhost:8002/docs |
| **Streamlit UI** | `frontend-ui` | `8501` | Multi-agent interface: http://localhost:8501 |

---

## Benchmark & Empirical Summary

* **Write Amplification (WAF):** Iceberg v2 MoR achieved $1.18\times$ WAF vs. $4.82\times$ for Copy-on-Write (CoW), reducing ingestion latency by $79.1\%$ ($142\text{ms}$ vs. $680\text{ms}$).
* **Compression & Scan Speedup:** Parquet with Zstandard achieved an $87.1\%$ storage reduction ($18.4\text{MB}$ vs. $142.8\text{MB}$ JSON) and a $20.8\times$ Trino scan speedup ($68\text{ms}$ vs. $1,420\text{ms}$).
* **Inline Quality Interception:** Flink DLQ intercepted $100\%$ ($20/20$) of injected negative price anomalies, preventing a $-\$19,999.80$ deficit in analytical Gross Merchandise Value.
* **Checkpoint Tuning Knee:** Sensitivity sweeps identify $\tau_{\text{ckpt}} = 60\text{s}$ as the optimal knee point, reducing S3 file generation by $83.3\%$ while keeping commit latency below $410\text{ms}$.
* **AST Security Gate:** Deterministic parser blocked $100\%$ ($9/9$) of adversarial injection attempts (destructive DDL, nested CTE mutations, namespace escapes) under $2\text{ms}$.
* **Cost Avoidance:** Local containerized topology avoids ₹8.49 lakh / year ($10,111 USD) relative to equivalent AWS managed infrastructure.

---

## Automated Tests

Run the full verification suite across security guardrails, MCP server wrappers, and multi-agent workflows:

```bash
pytest tests/ -v
```

---

## Repository Structure

```
Enterprise-streaming-Lakehouse/
├── docker-compose.yml         # 13-container production orchestration
├── requirements.txt           # Python dependencies
├── core-site.xml              # Hadoop S3A MinIO connector configuration
├── mcp_config.json            # Model Context Protocol client specification
├── update_creds.py            # Automated OAuth2 credential synchronizer
├── Group1_Project_Tracker.xlsx# Agile delivery ledger & Little's Law telemetry
├── README.md                  # System overview & quickstart
│
├── benchmarks/                # 20-scenario benchmark suite & evaluation results
├── data/olist/                # Brazilian E-Commerce dataset for streaming replay
├── debezium/                  # Debezium Quarkus CDC configuration
├── flink/                     # Flink 1.20 SQL quality pipelines & Dockerfile
├── frontend/                  # Streamlit Multi-Agent UI
├── postgres/                  # PostgreSQL transactional schema initialization
├── scripts/                   # Data mutator, Polaris bootstrap, and maintenance SQL
├── src/                       # Application source (security, mcp, agent, api)
├── tests/                     # Automated pytest test suites
└── trino/                     # Trino Iceberg catalog & coordinator configs
```

---

## Team & Credits

Developed as the course project for **Data Engineering (CSL4030)**.

* **Advaith Moholkar** (B23ES1005) - *Project Lead, Redpanda Streaming & Flink Quality Gate*
* **Debjit Ghorai** (B23ES1013) - *FastMCP Server, sqlglot AST Firewall & LangGraph Multi-Agent*
* **Aditya Jain** (B23ES1004) - *PostgreSQL 16 Operational WAL & Debezium CDC Pipeline*
* **Adithya Subhash** (B23ES1003) - *MinIO S3 Storage, Iceberg v2 MoR & Polaris REST Catalog*

Course Instructor: **Dr. Romi Banerjee** | *Department of Computer Science and Engineering, IIT Jodhpur*
