"""
tests/test_agent.py
================================================================
Unit tests for the LangGraph Agentic Pipeline (Task 4.2).
Validates state transitions, Node 0 Gate Guardrail, AST security gate routing, and schema contracts.
"""

from unittest.mock import MagicMock, patch
from src.agent.state import LakehouseAgentState
from src.agent.schemas import (
    InputGuardrailOutput,
    SQLGenerationOutput,
    JudgeEvaluationOutput,
    RuntimeErrorDiagnosisOutput,
)
from src.agent.nodes import node_validate_security, node_synthesize_failure_answer
from src.agent.graph import (
    create_lakehouse_graph,
    _route_after_gate,
    _route_after_security,
    _route_after_judge,
    _route_after_execution
)


def test_state_schema_instantiation():
    """Validates that LakehouseAgentState can be constructed with all required fields."""
    state: LakehouseAgentState = {
        "question": "How many orders exist?",
        "target_tables": ["orders"],
        "schema_context": "TABLE: iceberg.ecommerce.orders",
        "max_attempts": 3,
        "is_gate_approved": True,
        "gate_rejection_reason": None,
        "sql_query": "SELECT count(*) FROM iceberg.ecommerce.orders",
        "generation_reasoning": "Simple aggregation",
        "attempt_count": 1,
        "is_security_approved": True,
        "security_violation": None,
        "is_intent_aligned": True,
        "judge_reasoning": "Valid query",
        "judge_feedback": None,
        "execution_success": True,
        "query_result": [{"count": 42}],
        "execution_error": None,
        "final_answer": "There are 42 orders.",
        "status": "SUCCESS"
    }
    assert state["question"] == "How many orders exist?"
    assert state["is_gate_approved"] is True
    assert state["status"] == "SUCCESS"


def test_structured_output_models():
    """Validates Pydantic schema validation for Node 0, Node 1, and Node 2B."""
    gate_out = InputGuardrailOutput(
        is_valid=True,
        reason=None
    )
    assert gate_out.is_valid is True

    gate_blocked = InputGuardrailOutput(
        is_valid=False,
        reason="Detected prompt injection attack"
    )
    assert gate_blocked.is_valid is False
    assert "injection" in gate_blocked.reason

    gen_out = SQLGenerationOutput(
        reasoning="Count delivered orders",
        target_tables_used=["orders"],
        sql_query="SELECT count(*) FROM iceberg.ecommerce.orders WHERE order_status = 'delivered'"
    )
    assert "delivered" in gen_out.sql_query
    assert gen_out.target_tables_used == ["orders"]

    judge_out = JudgeEvaluationOutput(
        approved=True,
        reasoning="Query correctly filters delivered orders."
    )
    assert judge_out.approved is True
    assert judge_out.feedback is None

    diag_out = RuntimeErrorDiagnosisOutput(
        error_cause="Column 'status' does not exist in iceberg.ecommerce.orders",
        advice="Use column 'order_status' instead of 'status'"
    )
    assert diag_out.advice == "Use column 'order_status' instead of 'status'"
    assert "status" in diag_out.error_cause


def test_conditional_routing_after_gate():
    """Tests edge routing logic after Node 0 Gate Guardrail."""
    approved_state = {"is_gate_approved": True}
    blocked_state = {"is_gate_approved": False}

    assert _route_after_gate(approved_state) == "generate_sql"
    assert _route_after_gate(blocked_state) == "synthesize_security_rejection"


def test_ast_security_gate_allows_safe_select():
    """Validates that node_validate_security approves read-only SELECT and injects limit."""
    state = {
        "sql_query": "SELECT * FROM iceberg.ecommerce.orders",
        "is_security_approved": False,
        "security_violation": None
    }
    updated = node_validate_security(state)
    assert updated["is_security_approved"] is True
    assert updated["security_violation"] is None
    assert "LIMIT" in updated["sql_query"]


def test_ast_security_gate_blocks_drop_table():
    """Validates that node_validate_security blocks DROP TABLE attacks."""
    state = {
        "sql_query": "DROP TABLE iceberg.ecommerce.orders",
        "is_security_approved": False,
        "security_violation": None
    }
    updated = node_validate_security(state)
    assert updated["is_security_approved"] is False
    assert "DROP" in updated["security_violation"]


def test_conditional_routing_after_security():
    """Tests edge routing logic after AST security validation."""
    approved_state = {"is_security_approved": True}
    blocked_state = {"is_security_approved": False}

    assert _route_after_security(approved_state) == "judge_intent"
    assert _route_after_security(blocked_state) == "synthesize_security_rejection"


def test_conditional_routing_after_judge():
    """Tests edge routing logic for judge approval and retry budgeting."""
    approved_state = {"is_intent_aligned": True}
    retry_allowed_state = {"is_intent_aligned": False, "attempt_count": 1, "max_attempts": 3}
    budget_exhausted_state = {"is_intent_aligned": False, "attempt_count": 3, "max_attempts": 3}

    assert _route_after_judge(approved_state) == "execute_mcp"
    assert _route_after_judge(retry_allowed_state) == "generate_sql"
    assert _route_after_judge(budget_exhausted_state) == "synthesize_failure"


def test_conditional_routing_after_execution():
    """Tests edge routing logic for Trino execution recovery."""
    success_state = {"execution_success": True}
    fail_retry_state = {"execution_success": False, "attempt_count": 1, "max_attempts": 3}
    fail_exhausted_state = {"execution_success": False, "attempt_count": 3, "max_attempts": 3}

    assert _route_after_execution(success_state) == "synthesize_business_answer"
    assert _route_after_execution(fail_retry_state) == "diagnose_runtime_error"
    assert _route_after_execution(fail_exhausted_state) == "synthesize_failure"


def test_graph_compilation():
    """Verifies that the LangGraph StateGraph compiles cleanly without errors."""
    graph = create_lakehouse_graph()
    assert graph is not None
