# PROJECT CONTEXT — Engineer 4 In Progress (Engineers 1, 2, 3 Complete)
# Enterprise Agentic Streaming Lakehouse
# Paste this into a new chat to restore full context.
# Last updated: 2026-10-22
# Branch: feat/engineer-4-fastmcp-security
# ================================================================

## PROJECT OVERVIEW

This is a 4-engineer undergraduate capstone project called the
"Enterprise Agentic Streaming Lakehouse". It is a real-time data
pipeline that:
1. Captures live database changes from PostgreSQL via CDC
2. Streams them through Redpanda (Kafka-compatible broker)
3. Processes them with Apache Flink (stream processor, data quality, DLQ)
4. Stores them in Apache Iceberg tables on MinIO (S3 object storage)
5. Catalogs and governs them with Apache Polaris (open REST Catalog)
6. Queries them with Trino (distributed SQL engine)
7. Exposes them to an LLM via FastMCP (AI agent layer)

The dataset is the **Olist Brazilian E-Commerce** dataset (~100k orders,
8 relational tables, ~120MB of CSV files).

**Engineer split:**
- Engineer 1 (DONE): PostgreSQL + Debezium CDC + Redpanda + Data Mutator
- Engineer 2 (DONE): Apache Flink 1.20 + Data Quality (DLQ) + Iceberg Sink on MinIO + Checkpointing
- Engineer 3 (DONE): MinIO S3 Object Storage + Apache Polaris (REST Catalog) + Trino Query Engine + Table Maintenance
- Engineer 4 (IN PROGRESS): FastMCP Server + sqlglot AST Security / Guardrails + LLM Benchmarking & Evaluation

---

## MACHINE / ENVIRONMENT

- Primary Development & Validation Host: Windows 11 / PowerShell (`D:\DE project\`)
- Secondary Development Host: macOS (Apple Silicon arm64)
- Shell: PowerShell (`py` command on Windows, `python3` on Mac)
- Python: 3.12+ (pandas, sqlalchemy, psycopg2-binary, requests, pyyaml)
- Docker Desktop / Colima: Requires >= 6 GB RAM allocated
- Docker Compose: v2.20+
- Environment Config: Unified `.env` file (parameterized for local and cloud deployment)

---

## CURRENT STATE — CONTAINER TOPOLOGY (11 SERVICES)

Run `docker compose ps` to verify all services.

| Container | Image | Ports | Role / Status |
|---|---|---|---|
| postgres | postgres:16 | 5433→5432 | Primary DB (logical decoding, dbz_publication) |
| redpanda | redpandadata/redpanda:latest | 19092, 18081, 18082, 9644 | Event broker (7 CDC topics + 1 DLQ topic) |
| redpanda-console | redpandadata/console:latest | 8088→8080 | Web UI for topics, consumers, messages |
| minio | elestio/minio:latest | 9000, 9001 | S3 storage (Iceberg warehouse + checkpoints) |
| minio-init | elestio/minio:latest | (none, runs once) | Auto-provisions 'lakehouse' bucket on boot |
| debezium | quay.io/debezium/server:2.7 | (none, internal) | Tails Postgres WAL -> Redpanda topics (linux/amd64) |
| flink-jobmanager | custom-flink:1.20 | 8081→8081 | Flink coordinator & Web Dashboard |
| flink-taskmanager | custom-flink:1.20 | (none, internal) | Flink streaming worker (2 slots) |
| flink-sql-client | custom-flink:1.20 | (none, interactive) | Interactive CLI for SQL job submission |
| polaris | apache/polaris:latest | 8181→8181 | Apache Polaris Iceberg REST Catalog (OAuth2) |
| trino | trinodb/trino:latest | 8089→8080 | Trino distributed SQL query engine |

**Port Mappings on Host:**
- PostgreSQL: `localhost:5433` (internal 5432)
- Redpanda Kafka: `localhost:19092` (internal 9092)
- Redpanda Schema Registry: `localhost:18081` (internal 8081)
- Redpanda Console: `http://localhost:8088` (internal 8080)
- MinIO S3 API: `http://localhost:9000`
- MinIO Web Console: `http://localhost:9001` (login: minioadmin / minioadmin123)
- Flink Web Dashboard: `http://localhost:8081`
- Polaris Catalog API: `http://localhost:8181/api/catalog`
- Trino Web UI & Coordinator: `http://localhost:8089` (internal 8080)

---

## DATABASE SCHEMA (PostgreSQL `public` schema)

1. `customers` (customer_id PK)
2. `geolocation` (zip_code_prefix)
3. `order_items` (order_id, order_item_id PK, price, freight_value)
4. `order_payments` (order_id, payment_sequential PK, payment_value)
5. `order_reviews` (review_id PK, order_id)
6. `orders` (order_id PK, customer_id, order_status, timestamps)
7. `product_category_name_translation`
8. `products` (product_id PK)
9. `sellers` (seller_id PK)

WAL Level: `logical`, Publication: `dbz_publication` (covers all tables).

---

## STREAM PROCESSING & CATALOG ARCHITECTURE

### Apache Polaris REST Catalog (Engineer 3)
- **Role**: Open, multi-engine Iceberg metadata catalog serving Flink and Trino.
- **REST URI**: `http://polaris:8181/api/catalog` (internal) / `http://localhost:8181/api/catalog` (host)
- **Catalog Name**: `polaris`
- **Namespace**: `ecommerce`
- **Default Base Location**: `s3://lakehouse/warehouse`
- **Authentication**: OAuth2 Client Credentials (`PRINCIPAL_ROLE:ALL`)
- **Storage Type**: S3 (`AwsStorageConfigurationInfo` with `pathStyleAccess: true`, endpoint: `http://minio:9000`)
- **Network Alias**: `minio` has Docker network alias `lakehouse.minio` to satisfy AWS SDK v2 virtual-host style DNS resolution without errors.
- **Bootstrap Automation**: `python3 scripts/bootstrap_polaris.py`

### Trino Query Engine (Engineer 3)
- **Role**: High-performance federated analytical engine querying Iceberg tables managed by Polaris.
- **Catalog Name in Trino**: `iceberg`
- **Catalog Config**: `trino/etc/catalog/iceberg.properties`
  - `connector.name=iceberg`
  - `iceberg.catalog.type=rest`
  - `iceberg.rest-catalog.uri=http://polaris:8181/api/catalog`
  - `iceberg.rest-catalog.security=OAUTH2`
  - `fs.native-s3.enabled=true`
  - `s3.endpoint=http://minio:9000`
  - `s3.path-style-access=true`
- **Maintenance Operations**: `scripts/maintenance.sql` (file compaction, snapshot expiration, orphan file removal).

### Apache Flink Lakehouse Pipeline (Engineer 2)
- **Catalog**: `iceberg_catalog` pointing to Polaris REST catalog (`catalog-type = rest`, uri: `http://polaris:8181/api/catalog`).
- **Data Quality & DLQ**: Clean records upserted to `iceberg_catalog.ecommerce.*`; anomalous price/freight records routed to `ecommerce.dlq.order_items`.
- **Checkpointing**: Exactly-once, 60s interval stored at `s3a://lakehouse/checkpoints/flink`.

---

## VERIFICATION COMMANDS

```bash
# ── 1. Check all 11 containers ─────────────────────────────────
docker compose ps

# ── 2. Run Polaris Bootstrap (if setting up fresh) ────────────
python3 scripts/bootstrap_polaris.py

# ── 3. Submit Flink Streaming Job ─────────────────────────────
docker exec -i flink-sql-client /opt/flink/bin/sql-client.sh -f /opt/flink/sql/run_all.sql

# ── 4. Verify Iceberg Schemas and Tables in Trino ─────────────
docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg;"
docker exec trino trino --execute "SHOW TABLES FROM iceberg.ecommerce;"

# ── 5. Run Analytical Queries in Trino ────────────────────────
docker exec trino trino --execute "SELECT count(*) FROM iceberg.ecommerce.orders;"
docker exec trino trino --execute "SELECT count(*) FROM iceberg.ecommerce.order_items;"
docker exec trino trino --execute "SELECT * FROM iceberg.ecommerce.order_items LIMIT 5;"

# ── 6. Run Iceberg Table Maintenance in Trino ─────────────────
docker exec -i trino trino < scripts/maintenance.sql
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

## ENGINEER 2 DEFINITION OF DONE ✅ (ACHIEVED)

- Custom Flink 1.20 Docker image built with Kafka, Iceberg, and S3A JARs ✅
- MinIO init container auto-provisions `lakehouse` bucket ✅
- Flink Session Cluster (JobManager + TaskManager + SQL Client) added to Docker Compose ✅
- Flink SQL sources consume CDC changelogs with `TIMESTAMP_LTZ(6)` ✅
- Data quality gate branches clean records to Iceberg and invalid records to DLQ topic ✅
- Clean records upserted into Iceberg v2 tables on MinIO as compressed Parquet files ✅
- Exactly-once checkpointing configured to S3A storage ✅

---

## ENGINEER 3 DEFINITION OF DONE ✅ (ACHIEVED)

- Apache Polaris container deployed and integrated into `docker-compose.yml` ✅
- S3 / MinIO storage configuration wired with path-style access and `lakehouse.minio` network alias ✅
- Automated Polaris bootstrap script (`scripts/bootstrap_polaris.py`) created and validated ✅
- Dynamic credential propagation script (`update_creds.py`) and unified `.env` parameterized ✅
- Flink SQL pipeline updated to create and write Iceberg tables directly via Polaris REST Catalog ✅
- Trino container configured with Iceberg REST catalog connector (`trino/etc/catalog/iceberg.properties`) ✅
- Trino end-to-end queries verified: `SHOW SCHEMAS`, `SHOW TABLES`, analytical SQL queries ✅
- Iceberg table maintenance SQL script created (`scripts/maintenance.sql`) for compaction, snapshot expiration, and orphan cleanup ✅
- Cross-platform support verified for both Windows (PowerShell/WSL2) and macOS (Apple Silicon arm64) ✅

---

## WHAT ENGINEER 4 NEEDS TO DO NEXT

Engineer 4 picks up from the Trino query engine and Apache Polaris catalog:

1. **FastMCP Server Implementation**:
   - Build a Python FastMCP service that exposes tools for querying the Lakehouse.
   - Connect FastMCP directly to Trino using `trino-python-client` (`trino://trino@trino:8080/iceberg/ecommerce`).
   - Expose tools such as:
     - `execute_query(sql: str)`: Execute analytical SQL queries.
     - `get_table_schema(table_name: str)`: Inspect table columns and types.
     - `list_tables()`: Return available e-commerce tables.

2. **SQL AST Validation & Security Guardrails (sqlglot)**:
   - Implement read-only AST parsing before queries reach Trino:
     - Reject `DROP`, `DELETE`, `UPDATE`, `ALTER`, `TRUNCATE`, `INSERT`.
     - Allow only `SELECT` and `EXPLAIN` statements.
     - Enforce `LIMIT` clauses to prevent runaway memory usage.
     - Validate table and schema identifiers against an allowlist (`iceberg.ecommerce.*`).

3. **LLM Benchmarking & Evaluation**:
   - Formulate benchmark questions spanning descriptive, diagnostic, and analytical e-commerce metrics (e.g., revenue trends, delivery performance, freight anomaly rates).
   - Test LLM accuracy in generating correct Trino SQL across single-turn and multi-turn workflows.
   - Benchmark latency, accuracy, and adherence to security constraints.

---

## ENGINEER 4 PROGRESS & STATUS (IN PROGRESS)

- **Branch**: `feat/engineer-4-fastmcp-security`
- **Master Plan**: Defined in `eng_4.md` (5 phases).
- **Phase 1: Environment & Trino Connectivity (COMPLETE ✅)**:
  - `requirements.txt` created with pinned versions (`fastmcp`, `mcp`, `trino`, `sqlglot`, `pydantic`, `pytest`, `python-dotenv`) ✅
  - Python 3.12 virtual environment (`.venv`) initialized and all dependencies installed and validated ✅
  - Trino connectivity validation script implemented (`scripts/test_trino_connection.py`) and verified end-to-end against live Trino v484 coordinator on port 8089 ✅
  - Iceberg catalog (`iceberg`), namespace (`ecommerce`), and tables (`orders`, `order_items`, `order_payments`) successfully discovered and queried via Python ✅
- **Phase 2: Deterministic SQL Security Guardrails (COMPLETE ✅)**:
  - `src/security/exceptions.py` implemented with custom exception hierarchy (`SQLSecurityError`, `SQLSyntaxError`, `ForbiddenOperationError`, etc.) ✅
  - `src/security/validator.py` created with `SQLSecurityValidator` and `ValidationResult` Pydantic models ✅
  - Implemented AST parsing using `sqlglot.parse_one(sql, read="trino")` with multi-statement injection protection and table identifier extraction ✅
  - Enforced strict read-only execution policy via root node inspection and recursive tree walking (`ast.walk()`), rejecting `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, and `CREATE` mutations ✅
  - Implemented resource governance and row limiting via AST manipulation (`enforce_resource_governance`), injecting default limits (`LIMIT 100`) and clamping runaway queries (`MAX_LIMIT 1000`) to protect memory ✅
  - Authored `llm_orchestration_notes.md` detailing analytical push-down aggregations vs. raw batched extractions and agent pre-planning directives ✅
  - Implemented namespace and table allowlisting (`check_table_allowlists`), restricting access strictly to `iceberg.ecommerce.*` and the 9 Olist tables, with dynamic CTE alias awareness and blocking unauthorized catalogs/tables (`system`, `postgres`, etc.) ✅
  - Built comprehensive automated security test suite (`tests/test_security.py`) covering 43 unit tests across happy paths, injection attacks, mutations, allowlists, and edge cases (100% pass rate) ✅
- **Phase 3: FastMCP Server Implementation (COMPLETE ✅)**:
  - Created `src/mcp` package with `config.py` (Pydantic settings) and `trino_client.py` (database client layer) with dual Host/Docker awareness ✅
  - Initialized `Lakehouse-Analytics-Server` FastMCP instance in `src/mcp/server.py` wired to `SQLSecurityValidator` and `TrinoClient` ✅
  - Implemented Tool 1: `list_tables()` discovering live Iceberg tables from Trino/Polaris and enriching with semantic metadata descriptions for LLM reasoning ✅
  - Implemented Tool 2: `get_table_schema(table_name)` delivering dynamic Schema RAG context with exact physical Trino types, primary keys, relational join hints, and column descriptions ✅
  - Implemented Tool 3: `execute_analytical_query(sql_query)` integrating the end-to-end pipeline: AST validation, mutation interception, LIMIT injection, and Trino execution with structured tabular JSON outputs ✅
  - Built MCP client test harness (`scripts/test_mcp_client.py`) and pytest suite (`tests/test_mcp.py`) verifying true stdio JSON-RPC handshake, tool discovery, and remote tool execution (100% pass rate) ✅
- **Phase 4: LLM Benchmarking & Evaluation Framework (COMPLETE ✅)**:
  - Created dedicated `benchmarks/` package with `benchmark_suite.json` containing 20 curated test scenarios across Descriptive Analytics (5), Diagnostic & Complex Joins (6), and Adversarial Security Attacks (9) ✅
  - Implemented Pydantic-validated suite loader in `benchmarks/loader.py` with criteria filtering and statistics ✅
  - Verified benchmark suite integrity and golden SQL metadata via verification runner (100% pass) ✅
  - Implemented core LangGraph agent (`src/agent/`) with Dual-Perimeter Security (Node 0 Gate Guardrail + Perimeter 2 AST Firewall), Node 1 SQL Generator (`gemma-4-31b-it`), Node 2B Primary Intent Judge (`gemma-4-31b-it`), Node 2B Sub-Role Runtime Error Diagnosis, Trino MCP Execution, and Stage 2 Business Synthesis (`gemma-4-26b-a4b-it`) ✅
  - Standardized all 4 reasoning/gate nodes on structured JSON schema outputs (`method="json_schema"`), eliminating string parsing and `.strip()` errors ✅
  - Built comprehensive automated evaluation harness (`scripts/evaluate_llm.py`) and instrumented real-time test runner (`tests/test_agent_workflow.py`) with 100% verified end-to-end execution against live Trino/Iceberg infrastructure ✅
- **Phase 5: Containerization & Final Handover (COMPLETE ✅)**:
  - Created production `Dockerfile` with multi-stage build, healthchecks, and Uvicorn runtime on port 8000 ✅
  - Implemented production FastAPI application (`src/api/app.py`) with CORS middleware, Pydantic contracts, Swagger docs, endpoints (`/health`, `/api/v1/query`, `/api/v1/tables`, `/api/v1/schema/{table}`), and Server-Sent Events (SSE) streaming endpoint (`/api/v1/query/stream`) with background threaded queue yielding `: keep-alive\n\n` comments every 2 seconds, completely eliminating client HTTP socket read timeouts during heavy analytical reasoning ✅
  - Built comprehensive API test suite (`tests/test_api.py`) with 7/7 tests passing (100% pass rate) ✅
  - Built modern Streamlit frontend (`frontend/app.py`) with dark grey theme, terminal green styling, schema explorer, real-time Server-Sent Events (SSE) agent telemetry visualizer, typewriter narrative streaming, Node 1 SQL Generator Cognitive Reasoning & Thinking Process display, 5-minute analytical timeout resilience, and CSV export, verified via `tests/test_frontend.py` (5/5 tests passing) ✅
  - Containerized frontend via `frontend/Dockerfile` and integrated Service #13 (`lakehouse-frontend` on port 8501) into `docker-compose.yml` ✅
  - Registered unified AI service (`lakehouse-ai-service`) as Service #12 in `docker-compose.yml`, completing the full 13-container lakehouse architecture ✅
  - Configured resilient model defaults (`gemma-4-26b-a4b-it`) across generator, judge, and synthesizer nodes to guarantee fast, deterministic latency (<10s) ✅
  - Documented Model Context Protocol configuration in `mcp_config.json` supporting both local host Python execution and Docker container execution ✅



---

## KNOWN ISSUES / GOTCHAS

1. **Host Port Mappings**:
   - PostgreSQL is on host port `5434` (internal 5432).
   - Redpanda Console is on host port `8088` (internal 8080).
   - Flink Dashboard is on host port `8081`.
   - Trino is on host port `8089` (internal 8080).
   - Polaris is on host port `8181`.
2. **Polaris Credentials**:
   - Polaris generates dynamic root principal credentials on fresh bootstrap if not set statically. `update_creds.py` automatically syncs credentials across `.env`, `trino/etc/catalog/iceberg.properties`, `flink/sql/01-catalog.sql`, and `flink/sql/run_all.sql`.
3. **MinIO S3 Path Style**:
   - MinIO requires path-style access (`s3.path-style-access=true`) and bucket virtual-host alias `lakehouse.minio` defined in Docker Compose to prevent `UnknownHostException` from AWS SDK v2.
4. **Debezium Platform on Apple Silicon**:
   - `quay.io/debezium/server:2.7` requires `platform: linux/amd64` in `docker-compose.yml` to prevent `exec format error` on Apple Silicon Macs.
