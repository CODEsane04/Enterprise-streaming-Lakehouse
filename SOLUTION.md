# Engineer 3 Solution & Handoff Guide
## Enterprise Agentic Streaming Lakehouse
**Role**: Engineer 3 (Lakehouse Storage, Open REST Catalog & Query Engine)  
**Branch**: `feat/engineer-3-polaris-trino`  
**Status**: COMPLETE & VERIFIED ✅

---

## 1. Executive Summary

As **Engineer 3**, the primary responsibility was to design, integrate, and validate the **open Lakehouse metadata catalog** and **distributed analytical query layer**:
- Transitioned the architecture to **Apache Polaris (Incubating)** as a centralized, open REST Catalog adhering to the Apache Iceberg REST specification.
- Bound storage to **MinIO** (S3-compatible object storage) under `s3://lakehouse/warehouse`.
- Connected **Trino** as the federated query engine over Polaris with OAuth2 authentication.
- Aligned **Apache Flink (Engineer 2)** so streaming ingestion writes Iceberg tables directly registered within Polaris.
- Implemented **Iceberg Table Maintenance** (`scripts/maintenance.sql`) for file compaction, snapshot expiration, and orphan cleanup.
- Prepared the query and catalog foundation for **Engineer 4** (FastMCP Agent & Security Guardrails).

---

## 2. Architecture & Components Implemented

```
[Postgres WAL] (Eng 1)
       │
       ▼
[Debezium CDC] ──► [Redpanda / Kafka] (Eng 1)
                           │
                           ▼
                 [Apache Flink 1.20] (Eng 2)
                 (Data Quality Gate / DLQ)
                           │
            ┌──────────────┴──────────────┐
            ▼                             ▼
   [Apache Polaris REST Catalog]    [MinIO Object Storage]
   (Catalog: polaris / OAuth2)      (s3://lakehouse/warehouse)
            ▲                             ▲
            └──────────────┬──────────────┘
                           ▼
                  [Trino SQL Engine] (Eng 3)
                  (iceberg.ecommerce.*)
                           │
                           ▼
                 [Engineer 4: FastMCP]
```

### Component Details:
1. **Apache Polaris REST Catalog** (`apache/polaris:latest`):
   - Exposes REST catalog endpoint on port `8181` (`http://polaris:8181/api/catalog`).
   - Manages namespace `ecommerce` and tables `orders`, `order_items`, `order_payments`.
   - Bootstrapped via automated script (`scripts/bootstrap_polaris.py`).
2. **MinIO S3 Integration**:
   - Storage type configured as `S3` with path-style access (`pathStyleAccess: true`).
   - Network alias `lakehouse.minio` assigned in `docker-compose.yml` to support AWS SDK v2 virtual-hosted bucket addressing without DNS errors.
3. **Trino Distributed Query Engine** (`trinodb/trino:latest`):
   - Port `8089` (host) -> `8080` (internal).
   - Configured with `trino/etc/catalog/iceberg.properties`:
     - REST catalog security: `OAUTH2` with client credentials.
     - Native S3 file system enabled with path-style MinIO access.
4. **Environment Parameterization (`.env`)**:
   - Full central configuration in `.env` and `.env.example` to eliminate hardcoded `localhost` issues across cloud and local deployments.

---

## 3. Key Files Created / Modified

| File | Change / Purpose |
|---|---|
| `docker-compose.yml` | Added `polaris` and `trino` services; added `lakehouse.minio` network alias; configured `JAVA_OPTS_APPEND` and `platform: linux/amd64` for Debezium. |
| `.env` / `.env.example` | Parameterized all service ports, hostnames, and credentials. |
| `scripts/bootstrap_polaris.py` | Automated OAuth2 token acquisition, catalog creation (`polaris`), namespace provisioning, and MinIO storage configuration. |
| `update_creds.py` | Helper script to propagate dynamically generated Polaris root credentials across `.env`, Trino, and Flink configs. |
| `trino/etc/catalog/iceberg.properties` | Configured Trino Iceberg connector pointing to Polaris REST catalog with OAuth2 credentials and MinIO overrides. |
| `flink/sql/01-catalog.sql` | Updated Flink catalog to use REST catalog protocol (`catalog-type = 'rest'`). |
| `flink/sql/run_all.sql` | Complete Flink SQL streaming pipeline integrated with Polaris REST catalog. |
| `scripts/maintenance.sql` | Trino SQL statements for Iceberg file compaction (`OPTIMIZE`), snapshot expiration, and orphan cleanup. |
| `CONTEXT.md` | Comprehensive capstone project documentation updated with Engineer 3 status and Engineer 4 handoff. |

---

## 4. Verification & Testing Evidence

All validation checks passed with 100% success:

1. **Polaris Catalog Creation**:
   ```bash
   python3 scripts/bootstrap_polaris.py
   # Output: Catalog 'polaris' created successfully!
   ```

2. **Flink SQL Streaming Submission**:
   ```bash
   docker exec -i flink-sql-client /opt/flink/bin/sql-client.sh -f /opt/flink/sql/run_all.sql
   # Output: Job ID: f118424e914b17a4b3bf31f07c5470cd successfully submitted!
   ```

3. **Trino Schema & Table Verification**:
   ```bash
   docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg;"
   # Output: "ecommerce", "information_schema", "system"

   docker exec trino trino --execute "SHOW TABLES FROM iceberg.ecommerce;"
   # Output: "orders", "order_items", "order_payments"
   ```

4. **Trino Analytical Query Execution**:
   ```bash
   docker exec trino trino --execute "SELECT count(*) FROM iceberg.ecommerce.order_items;"
   # Output: "0" (Executes cleanly with live query execution)
   ```

---

## 5. Engineer 4 Handoff Guide

Engineer 4 picks up directly from the Trino and Polaris layer to build the **FastMCP Agent & Security Guardrail** interface.

### What Engineer 4 Needs to Do:

#### Step 1: Connect FastMCP Server to Trino
- Use Python with `fastmcp` and `trino-python-client`.
- Connection parameters:
  - **Host**: `trino` (inside Docker) or `localhost` (from host)
  - **Port**: `8080` (internal) / `8089` (host)
  - **User**: `fastmcp_agent`
  - **Catalog**: `iceberg`
  - **Schema**: `ecommerce`

#### Step 2: Implement Tool Definitions
Expose MCP tools for the LLM:
1. `list_tables()`: Returns available tables (`orders`, `order_items`, `order_payments`).
2. `describe_table(table_name: str)`: Returns column schemas and types.
3. `query_lakehouse(sql: str)`: Executes validated SQL queries and returns results as JSON/Markdown tables.

#### Step 3: Implement SQL Security Guardrails (`sqlglot`)
To prevent data alteration and prompt-injection attacks:
- Parse all generated SQL with `sqlglot`.
- Enforce strict **read-only** execution:
  - Block DDL/DML: `DROP`, `DELETE`, `UPDATE`, `ALTER`, `TRUNCATE`, `INSERT`, `CREATE`.
  - Allow only `SELECT` and `EXPLAIN`.
- Enforce mandatory `LIMIT` (e.g., maximum 1000 rows).
- Enforce schema allowlist (`iceberg.ecommerce.*`).

#### Step 4: LLM Benchmarking
- Evaluate LLM performance on complex e-commerce business queries (e.g., delivery time by state, customer lifetime value, payment installments vs freight costs).
- Record latency, query accuracy, and security rejection rates.
