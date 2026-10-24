# LLM Orchestration Playbook & Architectural Notes
## Enterprise Agentic Streaming Lakehouse — Engineer 4

This document captures the core architectural principles, intent classification pathways, and pre-planning protocols for the LLM Orchestration Layer. It serves as the master blueprint for crafting the **Phase 4 Agentic System Prompt**.

---

### 1. Fundamental Principle: LLM as Orchestrator, Not Calculator

```
[ User Prompt ] ──► [ LLM (Orchestrator & Intent Planner) ] ──► [ FastMCP Tools ] ──► [ Trino / Iceberg ]
                           │                                                              │
                           ▼                                                              ▼
                  (Translates intent to                                        (Executes 100% of
                  safe, structured SQL)                                       compute / aggregation)
```

* **The LLM NEVER computes numbers manually.** It never calculates sums, averages, or variances in its prompt context.
* **The Lakehouse (Trino) does 100% of the arithmetic.** Trino scans Parquet files in MinIO and executes parallel aggregations.
* **The LLM packages and presents.** It takes Trino's structured tabular output and formats clear, executive answers.

---

### 2. The Two Intent Classification Pathways

When a user submits a prompt, the LLM must classify the inquiry into one of two distinct operational patterns:

#### Pathway A: Analytical Inquiry (Push-Down Aggregation)
* **User Examples:**
  * *"What is the average delivery delay across states?"*
  * *"Show me the top 5 product categories by revenue this month."*
  * *"Perform basic analytics on yesterday's orders."*
* **LLM Orchestration Behavior:**
  1. Inspects table schemas using `get_table_schema`.
  2. Formulates an analytical SQL query with aggregations (`SUM`, `AVG`, `COUNT`, `GROUP BY`, `ORDER BY`).
  3. **Expected Row Output:** Inherently small (1 to 50 rows), even though Trino evaluated 100,000+ records.
  4. **Row Cap Impact:** Zero. Limits never truncate analytical truth because calculations were performed before returning.

#### Pathway B: Raw Data Retrieval / Entity Lookup
* **User Examples:**
  * *"Show me the details of order #12345."*
  * *"List all customers located in Curitiba."*
  * *"Export the last 20 failed delivery records."*
* **LLM Orchestration Behavior:**
  1. Formulates a targeted filtering query (`WHERE customer_city = 'curitiba'`).
  2. **Pre-Planning Step for Broad Queries:** If the user asks for "all" records, the LLM should first check volume:
     ```sql
     SELECT count(*) FROM iceberg.ecommerce.orders WHERE ...
     ```
  3. If volume is large (> 1,000 rows), the LLM must **batch the extraction** using pagination (`LIMIT 1000 OFFSET 0`, `LIMIT 1000 OFFSET 1000`) rather than attempting an unconstrained dump.

---

### 3. Safety Circuit Breakers vs. Data Truncation

* **Why limits exist:** They are not intended to arbitrarily deny data to users. They function as **circuit breakers** against:
  1. Accidental Cartesian joins (`CROSS JOIN`) producing millions of unintentional rows.
  2. Out-of-memory (OOM) crashes in the Docker environment.
  3. LLM context window overflow.
* **The Batching Strategy:** For large data exports, the server provides safe chunks (e.g. 1,000 rows per call), and the agent orchestrates retrieval across multiple calls into a buffered result.

---

### 4. Phase 4 System Prompt Directives (Draft)

When initializing the LLM agent, the following directives will be injected:

```markdown
1. You are the AI Data Analyst for the Enterprise Streaming Lakehouse.
2. ALWAYS push aggregations down to Trino SQL (SUM, AVG, COUNT, MIN, MAX, GROUP BY).
   NEVER fetch raw rows to calculate totals or averages yourself.
3. Before generating SQL, inspect table definitions using `get_table_schema`.
4. If a user asks for large raw data listings, check the row volume first using `count(*)`.
5. Write only read-only queries (SELECT, EXPLAIN). Do NOT attempt DROP, DELETE, UPDATE, or INSERT.
6. Target only tables in the `iceberg.ecommerce` catalog.
```

