"""
src/api/app.py
================================================================
FastAPI REST API for the Enterprise Lakehouse Multi-Agent Pipeline.
Exposes the LangGraph agentic workflow (Node 0 Gatekeeper, Node 1 SQL Generator,
Perimeter 2 AST Firewall, Node 2B Intent Judge, Node 3 Trino MCP, Stage 2 Synthesis)
for consumption by React, Next.js, Streamlit, and external client applications.
"""

import os
import sys
import time
import json
from typing import List, Dict, Any, Optional

import dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.agent.graph import lakehouse_graph
from src.mcp.trino_client import TrinoClient
from src.mcp.server import mcp

dotenv.load_dotenv()

# ======================================================================
# FASTAPI APPLICATION INITIALIZATION
# ======================================================================

app = FastAPI(
    title="Enterprise Lakehouse Multi-Agent AI API",
    description=(
        "Production-grade AI Serving Layer for Apache Iceberg on Trino.\n\n"
        "Features:\n"
        "- Dual-Perimeter Security (Node 0 Gate Guardrail + Perimeter 2 AST Firewall)\n"
        "- Cyclic Agentic Reflection (SQL Generator <-> Intent Judge)\n"
        "- Self-Healing Database Error Recovery\n"
        "- Trino Push-Down Analytical Execution on Polaris REST Catalog"
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Enable CORS for frontend applications (React, Next.js, Streamlit)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ======================================================================
# REQUEST & RESPONSE DATA CONTRACTS
# ======================================================================

class QueryRequest(BaseModel):
    """Payload for analytical question queries."""
    question: str = Field(
        ...,
        description="The natural language business or analytical question.",
        json_schema_extra={"example": "How many total orders have a status of delivered in our lakehouse?"}
    )
    target_tables: Optional[List[str]] = Field(
        default=None,
        description="Optional list of table names to ground schema context (e.g., ['orders', 'order_items']).",
        json_schema_extra={"example": ["orders"]}
    )
    max_attempts: Optional[int] = Field(
        default=3,
        description="Maximum reflection and self-healing retry iterations.",
        ge=1,
        le=5
    )


class QueryResponse(BaseModel):
    """Structured response from the multi-agent pipeline."""
    status: str = Field(
        ...,
        description="Lifecycle status: 'SUCCESS' | 'SECURITY_BLOCKED' | 'MAX_RETRIES_EXCEEDED'."
    )
    final_answer: str = Field(
        ...,
        description="Executive natural-language business answer or formal security refusal."
    )
    sql_query: Optional[str] = Field(
        default=None,
        description="The Trino/Presto SQL query generated and executed."
    )
    generation_reasoning: Optional[str] = Field(
        default=None,
        description="Step-by-step thinking process and reasoning produced by the SQL Generator."
    )
    attempts: int = Field(
        default=1,
        description="Number of reflection and self-healing attempts taken."
    )
    execution_success: bool = Field(
        default=False,
        description="True if query successfully executed on Trino, False otherwise."
    )
    query_result: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Tabular JSON rows returned from Lakehouse (if successful)."
    )
    security_violation: Optional[str] = Field(
        default=None,
        description="Security violation explanation if intercepted by Gate or AST Firewall."
    )
    latency_ms: float = Field(
        ...,
        description="Total end-to-end execution latency in milliseconds."
    )


class HealthResponse(BaseModel):
    """Service health and database connectivity status."""
    status: str
    service: str
    version: str
    trino_connected: bool
    catalog: str
    schema_name: str


class TableSchemaResponse(BaseModel):
    """Schema definition for a requested table."""
    table_name: str
    columns: List[Dict[str, Any]]


# ======================================================================
# SCHEMA CONTEXT HELPER
# ======================================================================

def _build_schema_context(trino_client: TrinoClient, table_names: Optional[List[str]]) -> str:
    """Builds physical schema context for target tables."""
    if not table_names:
        table_names = ["orders", "order_items", "order_payments", "products", "customers"]

    blocks = []
    for tbl in table_names:
        try:
            cols = trino_client.get_table_schema(tbl)
            if cols:
                col_lines = [f"  - {c['column_name']} ({c['data_type']})" for c in cols]
                blocks.append(f"TABLE: iceberg.ecommerce.{tbl}\nCOLUMNS:\n" + "\n".join(col_lines))
            else:
                blocks.append(f"TABLE: iceberg.ecommerce.{tbl}")
        except Exception:
            blocks.append(f"TABLE: iceberg.ecommerce.{tbl}")

    return "\n\n".join(blocks)


# ======================================================================
# API ENDPOINTS
# ======================================================================

@app.get("/", tags=["General"])
def root():
    """Service landing endpoint with API documentation links."""
    return {
        "service": "Enterprise Lakehouse Multi-Agent AI API",
        "version": "1.0.0",
        "status": "online",
        "documentation": "/docs",
        "openapi_schema": "/openapi.json"
    }


@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
def health_check():
    """Checks service health and active Trino coordinator connectivity."""
    trino_client = TrinoClient()
    trino_ok = False
    try:
        conn = trino_client.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        trino_ok = bool(cursor.fetchone())
        conn.close()
    except Exception:
        trino_ok = False

    return HealthResponse(
        status="healthy" if trino_ok else "degraded",
        service="lakehouse-ai-service",
        version="1.0.0",
        trino_connected=trino_ok,
        catalog=trino_client.catalog,
        schema_name=trino_client.schema
    )


@app.post("/api/v1/query", response_model=QueryResponse, tags=["Analytics"])
def execute_agent_query(req: QueryRequest):
    """
    Executes a natural language query through the complete LangGraph Multi-Agent Pipeline.
    
    Pipeline Stages:
    1. Node 0: Gate Guardrail (Screens prompt injections & destructive requests)
    2. Node 1: SQL Generator (Grounded in physical schema RAG)
    3. Perimeter 2: Static AST Security Gate (Validates read-only and injects limits)
    4. Node 2B Primary: Intent Judge (Reflection loop auditing metric correctness)
    5. Node 3: MCP Tool Execution (Dispatches to live Trino coordinator)
    6. Node 2B Sub-Role: Self-Healing Runtime Error Diagnosis (on compiler error)
    7. Stage 2: Synthesis (Delivers executive business narrative or security notice)
    """
    start_time = time.perf_counter()
    print(f"\n[AI-SERVICE] >>> Incoming Query: {req.question}", flush=True)
    trino_client = TrinoClient()
    print(f"[AI-SERVICE] Reflecting schema context for: {req.target_tables}...", flush=True)
    schema_context = _build_schema_context(trino_client, req.target_tables)

    initial_state = {
        "question": req.question,
        "target_tables": req.target_tables or ["orders", "order_items", "products"],
        "schema_context": schema_context,
        "max_attempts": req.max_attempts,
        "attempt_count": 0,
        "is_gate_approved": True,
        "gate_rejection_reason": None,
        "sql_query": "",
        "generation_reasoning": "",
        "is_security_approved": False,
        "security_violation": None,
        "is_intent_aligned": False,
        "judge_reasoning": None,
        "judge_feedback": None,
        "execution_success": False,
        "query_result": None,
        "execution_error": None,
        "final_answer": "",
        "status": ""
    }

    try:
        print(f"[AI-SERVICE] Invoking LangGraph Multi-Agent Pipeline...", flush=True)
        final_state = lakehouse_graph.invoke(initial_state)
        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        print(f"[AI-SERVICE] <<< Pipeline Completed in {latency_ms}ms | Status: {final_state.get('status')}", flush=True)

        violation = final_state.get("security_violation") or final_state.get("gate_rejection_reason")

        return QueryResponse(
            status=final_state.get("status", "UNKNOWN"),
            final_answer=final_state.get("final_answer", "No answer generated."),
            sql_query=final_state.get("sql_query"),
            generation_reasoning=final_state.get("generation_reasoning"),
            attempts=final_state.get("attempt_count", 1),
            execution_success=final_state.get("execution_success", False),
            query_result=final_state.get("query_result"),
            security_violation=violation,
            latency_ms=latency_ms
        )
    except Exception as exc:
        latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline execution error: {str(exc)}"
        )


@app.post("/api/v1/query/stream", tags=["Analytics"])
def execute_agent_query_stream(req: QueryRequest):
    """
    Streams the multi-agent execution step-by-step using Server-Sent Events (SSE).
    Yields live telemetry, intermediate SQL, verification notices, and the final synthesis
    in real time as each cognitive stage completes.
    """
    start_time = time.perf_counter()
    trino_client = TrinoClient()

    def event_generator():
        try:
            print(f"\n[AI-SERVICE // STREAM] >>> New Streaming Query: '{req.question}' | Tables: {req.target_tables}", flush=True)
            yield f"data: {json.dumps({'stage': 'init', 'message': 'Reflecting physical schema from Trino...'})}\n\n"
            schema_context = _build_schema_context(trino_client, req.target_tables)

            yield f"data: {json.dumps({'stage': 'start', 'message': 'Initiating LangGraph multi-agent cognitive loop...'})}\n\n"

            initial_state = {
                "question": req.question,
                "target_tables": req.target_tables or ["orders", "order_items", "products"],
                "schema_context": schema_context,
                "max_attempts": req.max_attempts,
                "attempt_count": 0,
                "is_gate_approved": True,
                "gate_rejection_reason": None,
                "sql_query": "",
                "generation_reasoning": "",
                "is_security_approved": False,
                "security_violation": None,
                "is_intent_aligned": False,
                "judge_reasoning": None,
                "judge_feedback": None,
                "execution_success": False,
                "query_result": None,
                "execution_error": None,
                "final_answer": "",
                "status": ""
            }

            accumulated_state = dict(initial_state)

            import queue
            import threading

            event_queue = queue.Queue()

            def run_stream():
                try:
                    for event in lakehouse_graph.stream(initial_state, stream_mode="updates"):
                        event_queue.put(("event", event))
                    event_queue.put(("done", None))
                except Exception as stream_err:
                    event_queue.put(("error", stream_err))

            worker = threading.Thread(target=run_stream, daemon=True)
            worker.start()

            while True:
                try:
                    item_type, item_data = event_queue.get(timeout=2.0)
                    if item_type == "done":
                        break
                    elif item_type == "error":
                        raise item_data
                    elif item_type == "event":
                        for node_name, state_update in item_data.items():
                            accumulated_state.update(state_update)

                            if node_name == "input_guardrail":
                                if state_update.get("is_gate_approved"):
                                    payload = {
                                        "stage": "gate_approved",
                                        "message": "🛡️ Perimeter 1: Gate Guardrail approved query as safe analytical intent."
                                    }
                                else:
                                    payload = {
                                        "stage": "gate_rejected",
                                        "message": f"🛡️ Perimeter 1: Blocked prompt injection/destructive intent: {state_update.get('gate_rejection_reason')}",
                                        "reason": state_update.get("gate_rejection_reason")
                                    }
                                yield f"data: {json.dumps(payload)}\n\n"

                            elif node_name == "generate_sql":
                                payload = {
                                    "stage": "sql_generated",
                                    "message": f"🧠 Node 1: Generated Trino SQL (Attempt {state_update.get('attempt_count', 1)}).",
                                    "sql": state_update.get("sql_query"),
                                    "reasoning": state_update.get("generation_reasoning")
                                }
                                print(f"[AI-SERVICE // NODE 1] {payload['message']}", flush=True)
                                if payload.get("sql"):
                                    print(f"  └─ SQL: {payload['sql']}", flush=True)
                                yield f"data: {json.dumps(payload)}\n\n"

                            elif node_name == "validate_security":
                                if state_update.get("is_security_approved"):
                                    payload = {
                                        "stage": "security_approved",
                                        "message": "🔒 Perimeter 2: Static AST Firewall verified query (read-only verified, limits injected)."
                                    }
                                else:
                                    payload = {
                                        "stage": "security_blocked",
                                        "message": f"🔒 Perimeter 2: AST Security Firewall Violation: {state_update.get('security_violation')}",
                                        "violation": state_update.get("security_violation")
                                    }
                                print(f"[AI-SERVICE // PERIMETER 2] {payload['message']}", flush=True)
                                yield f"data: {json.dumps(payload)}\n\n"

                            elif node_name == "judge_intent":
                                if state_update.get("is_intent_aligned"):
                                    payload = {
                                        "stage": "intent_approved",
                                        "message": "⚖️ Node 2B: Intent Judge verified query matches business intent."
                                    }
                                else:
                                    payload = {
                                        "stage": "reflection_retry",
                                        "message": f"⚖️ Node 2B: Intent discrepancy detected. Triggering reflection loop (Retry advice: {state_update.get('judge_feedback')}).",
                                        "feedback": state_update.get("judge_feedback"),
                                        "attempt": state_update.get("attempt_count")
                                    }
                                print(f"[AI-SERVICE // NODE 2B] {payload['message']}", flush=True)
                                yield f"data: {json.dumps(payload)}\n\n"

                            elif node_name == "execute_mcp":
                                if state_update.get("execution_success"):
                                    row_count = len(state_update.get("query_result") or [])
                                    payload = {
                                        "stage": "mcp_success",
                                        "message": f"🔌 Node 3: Push-down executed on Trino! Retrieved {row_count} rows from Apache Iceberg.",
                                        "row_count": row_count
                                    }
                                else:
                                    payload = {
                                        "stage": "mcp_failed",
                                        "message": f"🔌 Node 3: Trino compiler error: {state_update.get('execution_error')}"
                                    }
                                print(f"[AI-SERVICE // NODE 3] {payload['message']}", flush=True)
                                yield f"data: {json.dumps(payload)}\n\n"

                            elif node_name == "diagnose_runtime_error":
                                payload = {
                                    "stage": "self_healing",
                                    "message": f"🛠️ Node 2B Sub-Role: Diagnosed Trino runtime error. Self-healing advice generated.",
                                    "advice": state_update.get("judge_feedback")
                                }
                                print(f"[AI-SERVICE // SELF-HEALING] {payload['message']}", flush=True)
                                yield f"data: {json.dumps(payload)}\n\n"

                            elif node_name in ["synthesize_business_answer", "synthesize_security_rejection", "synthesize_failure"]:
                                latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
                                violation = accumulated_state.get("security_violation") or accumulated_state.get("gate_rejection_reason")
                                payload = {
                                    "stage": "complete",
                                    "message": "📝 Stage 2: Synthesis complete.",
                                    "status": state_update.get("status", accumulated_state.get("status", "UNKNOWN")),
                                    "final_answer": state_update.get("final_answer", accumulated_state.get("final_answer", "")),
                                    "sql_query": accumulated_state.get("sql_query"),
                                    "generation_reasoning": accumulated_state.get("generation_reasoning"),
                                    "query_result": accumulated_state.get("query_result"),
                                    "security_violation": violation,
                                    "attempts": accumulated_state.get("attempt_count", 1),
                                    "execution_success": accumulated_state.get("execution_success", False),
                                    "latency_ms": latency_ms
                                }
                                print(f"[AI-SERVICE // COMPLETE] Status: {payload['status']} | Latency: {latency_ms}ms | Attempts: {payload['attempts']}\n", flush=True)
                                yield f"data: {json.dumps(payload)}\n\n"
                except queue.Empty:
                    # Yield SSE keep-alive heartbeat comment to prevent client read timeouts
                    yield ": keep-alive\n\n"

            yield "data: [DONE]\n\n"

        except Exception as exc:
            latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
            err_payload = {
                "stage": "error",
                "message": f"Pipeline streaming error: {str(exc)}",
                "latency_ms": latency_ms
            }
            yield f"data: {json.dumps(err_payload)}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/api/v1/tables", tags=["Metadata"])
def list_lakehouse_tables():
    """Returns the list of available e-commerce analytical tables in the Lakehouse."""
    client = TrinoClient()
    try:
        tables = client.list_tables()
        return {
            "catalog": client.catalog,
            "schema": client.schema,
            "table_count": len(tables),
            "tables": tables
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch tables from Trino: {str(exc)}"
        )


@app.get("/api/v1/schema/{table_name}", response_model=TableSchemaResponse, tags=["Metadata"])
def get_table_schema(table_name: str):
    """Returns physical column names and data types for Schema RAG reflection."""
    client = TrinoClient()
    try:
        cols = client.get_table_schema(table_name)
        if not cols:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table '{table_name}' not found or contains no columns."
            )
        return TableSchemaResponse(table_name=table_name, columns=cols)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve schema for '{table_name}': {str(exc)}"
        )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("src.api.app:app", host="0.0.0.0", port=port, reload=True)
