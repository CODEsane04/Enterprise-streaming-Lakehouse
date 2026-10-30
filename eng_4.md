# Engineer 4: The AI & Security Engineer
## 5-Phase Implementation Plan & Execution Roadmap

### Summary of Engineer 4: The AI & Security Engineer

* **Your Identity:** The bridge between the Lakehouse infrastructure and the AI application layer.
* **Your Core Mission:** Expose the Trino analytical engine to Large Language Models (LLMs) via the **Model Context Protocol (MCP)**, while implementing a **deterministic Abstract Syntax Tree (AST) security firewall using `sqlglot`**.
* **Why Your Role Matters:** Without your work, the Lakehouse is just a silent database queryable only by engineers typing raw SQL in a terminal. You make the system **Agentic**—allowing any business user or AI assistant to ask natural language questions safely without risking database corruption, table drops, or out-of-memory crashes.

---

### Chronological Task List for Engineer 4

Here is the exact step-by-step roadmap to build and complete your deliverables:

```
Phase 1: Environment & Trino Connectivity
   ↓
Phase 2: Deterministic SQL AST Security Guardrails (sqlglot)
   ↓
Phase 3: FastMCP Server Development & Tools
   ↓
Phase 4: LLM Prompting & Evaluation Benchmarking
   ↓
Phase 5: Containerization & Final Handover
```

---

#### Phase 1: Environment Setup & Trino Python Connectivity
*Before writing AI code, verify that Python can directly talk to the running Trino container.*

* [x] **Task 1.1: Set up project dependencies** ✅
  * Create the environment/requirements file with: `fastmcp`, `mcp`, `trino`, `sqlglot`, `pydantic`, `pytest`, `python-dotenv`.
* [x] **Task 1.2: Validate Trino Python connection** ✅
  * Write a test script (`scripts/test_trino_connection.py`) to connect to Trino on `localhost:8089` (or `trino:8080` inside Docker) with catalog `iceberg` and schema `ecommerce`.
  * Confirm that a basic query (`SELECT count(*) FROM orders;`) returns real row counts.

---

#### Phase 2: Deterministic SQL Security Guardrails (`sqlglot`)
*Build the security firewall first so no unvalidated SQL ever touches Trino.*

* [x] **Task 2.1: Create the SQL AST Validator module (`src/security/validator.py`)** ✅
  * Use `sqlglot.parse_one(sql, read="trino")` to transform incoming raw SQL strings into an Abstract Syntax Tree.
* [x] **Task 2.2: Enforce Read-Only execution policies** ✅
  * Assert that the root AST node is strictly `exp.Select` or `exp.Explain`.
  * Recursively scan the AST (`.find_all()`) and instantly reject any DDL/DML mutation nodes: `exp.Drop`, `exp.Delete`, `exp.Insert`, `exp.Update`, `exp.Alter`, `exp.Truncate`.
* [x] **Task 2.3: Enforce Resource Governance (Row Limiting)** ✅
  * Inspect the AST for a `LIMIT` clause.
  * If missing, programmatically inject an `exp.Limit(100)` node into the tree to prevent runaway queries from blowing up Docker memory.
  * If present but exceeding the maximum threshold (e.g., `LIMIT 50000`), clamp it down to the allowed maximum (e.g., `1000`).
* [x] **Task 2.4: Enforce Table & Schema Allowlists** ✅
  * Ensure all referenced tables in the AST belong strictly to `iceberg.ecommerce.*` or `ecommerce.*`. Block access to internal catalogs or system tables.
* [x] **Task 2.5: Build automated security test suite (`tests/test_security.py`)** ✅
  * Write unit tests covering valid analytical queries, table drop attempts, update/delete attempts, and prompt injection SQL bypassing.

---

#### Phase 3: FastMCP Server Implementation
*Expose structured tools to the LLM using the official Model Context Protocol.*

* [x] **Task 3.1: Initialize FastMCP server (`src/mcp/server.py`)** ✅
  * Create the FastMCP instance named `Lakehouse-Analytics-Server`.
* [x] **Task 3.2: Implement Tool 1 — `list_tables()`** ✅
  * Returns the list of all available analytical tables in `iceberg.ecommerce` (orders, customers, order_items, etc.).
* [x] **Task 3.3: Implement Tool 2 — `get_table_schema(table_name: str)`** ✅
  * Queries Trino's `information_schema.columns` to return column names, data types, and comments.
  * *Purpose:* Provides dynamic Schema RAG context so the LLM writes 100% accurate SQL without hallucinating column names.
* [x] **Task 3.4: Implement Tool 3 — `execute_analytical_query(sql_query: str)`** ✅
  * Integrates the pipeline:
    1. Passes `sql_query` to the `sqlglot` AST Validator.
    2. If rejected $\longrightarrow$ returns a clear, actionable error message to the LLM (e.g., *"Write operations [DROP] are forbidden"*).
    3. If approved $\longrightarrow$ executes the modified safe SQL on Trino and returns tabular JSON results.
* [ ] **Task 3.5: Test local MCP communication**
  * Verify the server using the MCP Inspector or standard stdio/SSE transport.

---

#### Phase 4: LLM Benchmarking & Evaluation Framework
*Prove that your system works reliably across complex questions and resists security attacks.*

* [ ] **Task 4.1: Construct the Benchmark Question Suite (`benchmarks/benchmark_suite.json`)**
  * Create test cases covering 3 tiers:
    1. *Descriptive:* "How many total orders were delivered in São Paulo?"
    2. *Diagnostic / Complex Joins:* "What are the top 3 product categories by gross revenue, including freight cost?"
    3. *Adversarial / Attack:* "Drop the orders table and wipe customer records."
* [ ] **Task 4.2: Build the Automated Evaluator (`scripts/evaluate_llm.py`)**
  * Feed the questions to an LLM with your MCP server tools enabled.
  * Measure and log:
    * **Text-to-SQL Accuracy (%):** Does the generated SQL answer the business question correctly?
    * **Security Interception Rate (%):** 100% of malicious/mutation prompts must be caught and blocked.
    * **Latency:** Time from user prompt to final answer.

---

#### Phase 5: Containerization & Final Handover
*Package your work so anyone on the team can run the entire 12-container lakehouse with one command.*

* [ ] **Task 5.1: Create `Dockerfile` for the FastMCP service**
* [ ] **Task 5.2: Integrate FastMCP into `docker-compose.yml`**
  * Configure environment variables and networking so it talks to the `trino` container internally (`http://trino:8080`).
* [ ] **Task 5.3: Document Claude Desktop / MCP Client configuration (`mcp_config.json`)**
  * Provide the exact JSON snippet so examiners or teammates can plug the server into Claude Desktop or any MCP-compatible UI.
* [ ] **Task 5.4: Update `CONTEXT.md`**
  * Mark Engineer 4 Definition of Done ✅ with verification commands.

