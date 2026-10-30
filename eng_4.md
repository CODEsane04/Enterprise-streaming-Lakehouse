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
* [x] **Task 3.5: Test local MCP communication** ✅
  * Verify the server using the MCP Inspector or standard stdio/SSE transport.

---

#### Phase 4: LLM Benchmarking & Evaluation Framework
*Prove that your system works reliably across complex questions and resists security attacks.*

##### Phase 4 Architecture & Design Decisions

###### 1. Two-Stage Chained Architecture vs. Naive Chains
Rather than using a single monolithic prompt or a fragile unconstrained ReAct loop, the Lakehouse AI layer is architected as an explicit **2-Stage Agentic Pipeline**:
* **Stage 1 (Analytical Query Modeling & Validation):** Dedicated to reasoning about table relationships, generating valid Trino dialect SQL with dynamic schema context, passing deterministic AST security gates, and passing LLM semantic reflection.
* **Stage 2 (Natural Language Business Synthesis):** Takes the verified tabular JSON output from Trino and synthesizes an executive, natural-language business summary answering the user's strategic question. If an adversarial prompt was intercepted, it explains the security policy violation cleanly.

###### 2. Why LangGraph DAG Beats Simple LangChain Sequential Chains
Simple LangChain chains (`LLMChain` / LCEL sequential pipes `prompt | llm | parser`) are **strictly acyclic and single-pass**. If the LLM generates a SQL query with a slight syntax error, an ambiguous join, or misses a business metric, a sequential chain crashes or hallucinates.

**LangGraph provides three critical enterprise advantages:**
1. **Agentic Reflection (Self-Correction Loop):** The graph enables a cyclic feedback loop between the Generator (**Node 1**) and the Judge (**Node 2B**). If Node 2B detects a semantic flaw (e.g., omitted `freight_value` in revenue), it provides targeted critique and routes back to Node 1 to self-refine.
2. **Self-Healing Runtime Recovery:** If Trino returns a runtime compiler/type error, the error traceback is routed to the Judge to produce targeted diagnostic feedback, allowing Node 1 to fix the query autonomously without human intervention.
3. **Deterministic Short-Circuiting for Security:** Malicious DML/DDL queries (`DROP`, `DELETE`) fail the deterministic AST validator and immediately short-circuit to security synthesis, burning **zero extra tokens** on retry loops.

###### 3. LangGraph DAG Workflow (Dual-Perimeter Architecture)

![Final LangGraph DAG Architecture](eng4_arch_diagrams/final_langraph_DAG_arch.png)

```mermaid
flowchart TD
    User([User Prompt]) --> Gate{Node 0: Gate Guardrail<br/>Prompt Injection & Garbage Filter<br/>gemma-4-26b-a4b-it}
    
    %% Perimeter 1 Rejection
    Gate -->|❌ REJECTED<br/>Prompt Injection / Destructive / Gibberish| SynthSec[Stage 2: Rejection Generator<br/>gemma-4-26b-a4b-it<br/>Explains Security & Policy Boundary]
    
    %% Perimeter 1 Approved -> Enters Analytical Engine
    Gate -->|✅ APPROVED<br/>Valid Analytical Intent| N1[Node 1: SQL Generator<br/>gemma-4-31b-it<br/>Schema Context + Relational Reasoning]
    
    %% Perimeter 2: Deterministic AST Firewall
    N1 --> Sec{Perimeter 2: Static AST Validator<br/>SQLSecurityValidator<br/>Deterministic sqlglot AST Parser}
    
    %% AST Rejection Path
    Sec -->|❌ REJECTED<br/>DROP / DELETE / Injection Chaining| SynthSec
    
    %% AST Approved Path
    Sec -->|✅ APPROVED<br/>Safe Read-Only SQL + Clamped Limit| Judge{Node 2B: LLM as a Judge<br/>gemma-4-31b-it<br/>Evaluates Closeness & Intent}
    
    %% Judge Reflection Loop
    Judge -->|❌ Does not meet intent<br/>Attempts < Max| N1
    Judge -->|❌ Does not meet intent<br/>Attempts >= Max| Fail[Stage 2: Failure Notice<br/>Retry Budget Exhausted]
    
    %% Judge Approved -> Live Execution
    Judge -->|✅ Follows user demand| Exec[Node 3: MCP Tool Execution<br/>execute_analytical_query<br/>Live Iceberg / Trino Engine]
    
    %% Self-Healing Execution Recovery Loop
    Exec -->|❌ Trino Runtime Error<br/>Attempts < Max| JudgeFix[Node 2B: Error Diagnosis<br/>Drafts Feedback for Node 1]
    JudgeFix --> N1
    Exec -->|❌ Trino Runtime Error<br/>Attempts >= Max| Fail
    
    %% Execution Success -> Business Answer
    Exec -->|✅ Trino Success<br/>Tabular JSON Output| SynthAns[Stage 2: Business Synthesis<br/>gemma-4-26b-a4b-it<br/>Executive Narrative & Insights]
    
    %% Terminal Endpoints
    SynthSec --> End([Final Response to User])
    Fail --> End
    SynthAns --> End
```

###### 4. Specialized Model Selection
* **Node 0 (Gate Guardrail) & Stage 2 (Answer Synthesis):** `gemma-4-26b-a4b-it`
  * *Reasoning:* Fast, cost-efficient, high-throughput model optimized for binary input filtering and executive natural-language synthesis.
* **Node 1 (SQL Generator) & Node 2B (Intent Judge / Reflection):** `gemma-4-31b-it`
  * *Reasoning:* High parameter capacity, deep relational SQL reasoning, strict adherence to Trino syntax, and nuanced evaluation of prompt intent.

###### 5. Global LangGraph State (`LakehouseAgentState`)
The global state shared across all nodes is defined as a strongly typed `TypedDict`:

```python
from typing import TypedDict, List, Dict, Any, Optional

class LakehouseAgentState(TypedDict):
    # 1. Input Context (Grounding)
    question: str                    # Original user prompt
    target_tables: List[str]         # Candidate tables
    schema_context: str              # Schema RAG output from MCP get_table_schema
    max_attempts: int                # Maximum reflection retry budget (e.g., 3)

    # 2. Perimeter 1: Gate Guardrail (Node 0)
    is_gate_approved: bool           # True if passes input guardrail, False if injection/garbage
    gate_rejection_reason: Optional[str] # Explanation if blocked at the gate

    # 3. Stage 1: SQL Generation (Node 1)
    sql_query: str                   # Candidate SQL query
    generation_reasoning: str        # Step-by-step logic
    attempt_count: int               # Current iteration count (starts at 1)

    # 4. Perimeter 2: Deterministic AST Security Gate
    is_security_approved: bool       # True if passes sqlglot AST checks
    security_violation: Optional[str]# Exact reason if blocked (e.g., "Prohibited operation: DROP")

    # 5. LLM-as-a-Judge / Reflection (Node 2B)
    is_intent_aligned: bool          # True if query satisfies user's semantic demand
    judge_reasoning: Optional[str]   # Judge critique of SQL logic
    judge_feedback: Optional[str]    # Actionable suggestions fed back to Node 1

    # 6. MCP Query Execution (Trino Engine)
    execution_success: bool          # True if Trino ran successfully
    query_result: Optional[List[Dict[str, Any]]] # Tabular JSON rows
    execution_error: Optional[str]   # Trino compiler error message (if any)

    # 7. Stage 2: Final Synthesis & Audit
    final_answer: str                # Natural language business answer OR security rejection
    status: str                      # "SUCCESS" | "SECURITY_BLOCKED" | "MAX_RETRIES_EXCEEDED"
```

###### 6. Structured Output Schemas (Pydantic)
To eliminate regex parsing errors and ensure clean JSON contracts:
* **Node 0 Schema (`InputGuardrailOutput`):**
  * `is_valid: bool` — True if legitimate analytical inquiry; False if prompt injection or destructive mutation.
  * `reason: Optional[str]` — Rejection reason if not valid.
* **Node 1 Schema (`SQLGenerationOutput`):**
  * `reasoning: str` — Step-by-step logic on table joins, filters, and aggregations.
  * `target_tables_used: List[str]` — Tables referenced in the query.
  * `sql_query: str` — Clean Trino/Presto SQL targeting `iceberg.ecommerce`.
* **Node 2B Schema (`JudgeEvaluationOutput`):**
  * `approved: bool` — True if SQL faithfully answers the user's demand; False if flawed.
  * `reasoning: str` — Analytical critique comparing user prompt against SQL logic.
  * `feedback: Optional[str]` — Actionable feedback for Node 1 if not approved.
* **Node 2B Sub-Role Schema (`RuntimeErrorDiagnosisOutput`):**
  * `error_cause: str` — Technical explanation of why Trino rejected the query.
  * `advice: str` — Actionable instruction for Node 1 detailing the exact fix required for the query.

###### 7. End-to-End Live Workflow Verification & Execution Trace

The entire LangGraph agentic pipeline was executed against the live lakehouse infrastructure via `tests/test_agent_workflow.py`. Below is the recorded terminal execution trace:

```text
((.venv) ) debjitghorai@Mac lake_house % python tests/test_agent_workflow.py

======================================================================
🚀 ENTERPRISE LAKEHOUSE AGENT WORKFLOW TEST HARNESS
======================================================================
Choose a test scenario to execute:
  1. Legitimate Analytical Query (Happy Path through Gate -> SQL -> AST -> Judge -> Trino -> Synth)
  2. Adversarial Attack Query (Intercepted immediately at Node 0 Gate)
  3. Custom User Prompt

Enter choice [1/2/3] (default 1): 1

🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵
▶ [NODE 0: GATE GUARDRAIL (Perimeter 1)]
🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵
📥 Input Prompt: "How many total orders have a status of delivered in our lakehouse?"
   ✅ Gate Verdict: APPROVED (Valid analytical inquiry)
   ↪ [Routing Decision]: Gate APPROVED -> Routing to Node 1 SQL Generator

🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣
▶ [NODE 1: SQL GENERATOR (Attempt 1/3)]
🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣🟣
   💡 Generator Reasoning: To find the total number of orders with a status of 'delivered', I need to count the records in the 'iceberg.ecommerce.orders' table where the 'order_status' column is equal to 'delivered'.
   📝 Generated SQL Query:
      SELECT count(*) FROM iceberg.ecommerce.orders WHERE order_status = 'delivered'

🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️
▶ [PERIMETER 2: STATIC AST SECURITY FIREWALL (sqlglot)]
🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️🛡️
   Inspecting SQL: "SELECT count(*) FROM iceberg.ecommerce.orders WHERE order_status = 'delivered'"
   ✅ AST Security: APPROVED! (Safe Read-Only SELECT)
   🔒 Resource Limit Check: Sanitized SQL =>
      SELECT COUNT(*) FROM iceberg.ecommerce.orders WHERE order_status = 'delivered' LIMIT 100
   ↪ [Routing Decision]: AST APPROVED -> Routing to Node 2B Intent Judge

⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️
▶ [NODE 2B: LLM-AS-A-JUDGE / REFLECTION (gemma-4-31b-it)]
⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️
   Auditing Query against Question: "How many total orders have a status of delivered in our lakehouse?"
   ✅ Judge Verdict: APPROVED (Intent aligned & metrics correct)
   🔍 Reasoning: The SQL query correctly identifies the table 'iceberg.ecommerce.orders', applies the correct filter 'order_status = 'delivered'', and uses the COUNT(*) function to calculate the total number of orders as requested by the user.
   ↪ [Routing Decision]: Intent APPROVED -> Routing to Node 3 MCP Execution

⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡
▶ [NODE 3: MCP TOOL EXECUTION (Trino Coordinator)]
⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡⚡
   Executing SQL on Lakehouse:
      SELECT COUNT(*) FROM iceberg.ecommerce.orders WHERE order_status = 'delivered' LIMIT 100
   ✅ Execution Successful! Rows Returned: 1
   📊 Sample Data: [
      {
            "_col0": 0
      }
]
   ↪ [Routing Decision]: Execution SUCCESS -> Routing to Business Answer Synthesis

🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢
▶ [STAGE 2: BUSINESS ANSWER SYNTHESIS (gemma-4-26b-a4b-it)]
🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢
   💬 Executive Final Answer:
      There are currently **0** total orders with a status of "delivered" in the lakehouse.

🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁
▶ [WORKFLOW COMPLETED]
🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁🏁
Final Status : SUCCESS
Total Tries  : 1 attempt(s)
Final Answer : There are currently **0** total orders with a status of "delivered" in the lakehouse.
```

---

* [x] **Task 4.1: Construct the Benchmark Question Suite (`benchmarks/benchmark_suite.json`)** ✅
  * Create test cases covering 3 tiers:
    1. *Descriptive:* "How many total orders were delivered in São Paulo?"
    2. *Diagnostic / Complex Joins:* "What are the top 3 product categories by gross revenue, including freight cost?"
    3. *Adversarial / Attack:* "Drop the orders table and wipe customer records."
* [x] **Task 4.2: Build the Automated Evaluator (`scripts/evaluate_llm.py` & `src/agent/`)** ✅
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

