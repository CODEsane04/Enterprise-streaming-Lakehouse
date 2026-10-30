"""
LangGraph Assembly for the Enterprise Lakehouse AI Agent.
Constructs and compiles the cyclic StateGraph with Perimeter 1 (Gate Guardrail),
Perimeter 2 (Deterministic AST Security Firewall), reflection loops, Trino MCP execution,
and dual-stage synthesis.
"""

from langgraph.graph import StateGraph, START, END

from src.agent.state import LakehouseAgentState
from src.agent.nodes import (
    node_input_guardrail,
    node_generate_sql,
    node_validate_security,
    node_judge_intent,
    node_execute_mcp,
    node_diagnose_runtime_error,
    node_synthesize_business_answer,
    node_synthesize_security_rejection,
    node_synthesize_failure_answer,
)


def _route_after_gate(state: LakehouseAgentState) -> str:
    """Routes to security rejection synthesis if blocked at the gate, else forward to SQL generator."""
    if not state.get("is_gate_approved", True):
        return "synthesize_security_rejection"
    return "generate_sql"


def _route_after_security(state: LakehouseAgentState) -> str:
    """Routes to security rejection synthesis if blocked by AST, else forward to intent judge."""
    if not state.get("is_security_approved", False):
        return "synthesize_security_rejection"
    return "judge_intent"


def _route_after_judge(state: LakehouseAgentState) -> str:
    """Routes to retry generation if intent failed and attempts remain, else to execution or failure."""
    if not state.get("is_intent_aligned", False):
        current_attempts = state.get("attempt_count", 1)
        max_attempts = state.get("max_attempts", 3)
        if current_attempts < max_attempts:
            return "generate_sql"
        return "synthesize_failure"
    return "execute_mcp"


def _route_after_execution(state: LakehouseAgentState) -> str:
    """Routes to error diagnosis if Trino failed, else to business synthesis."""
    if not state.get("execution_success", False):
        current_attempts = state.get("attempt_count", 1)
        max_attempts = state.get("max_attempts", 3)
        if current_attempts < max_attempts:
            return "diagnose_runtime_error"
        return "synthesize_failure"
    return "synthesize_business_answer"


def create_lakehouse_graph():
    """Builds and compiles the cyclic LangGraph agent."""
    builder = StateGraph(LakehouseAgentState)

    # 1. Add all graph nodes
    builder.add_node("input_guardrail", node_input_guardrail)
    builder.add_node("generate_sql", node_generate_sql)
    builder.add_node("validate_security", node_validate_security)
    builder.add_node("judge_intent", node_judge_intent)
    builder.add_node("execute_mcp", node_execute_mcp)
    builder.add_node("diagnose_runtime_error", node_diagnose_runtime_error)
    builder.add_node("synthesize_business_answer", node_synthesize_business_answer)
    builder.add_node("synthesize_security_rejection", node_synthesize_security_rejection)
    builder.add_node("synthesize_failure", node_synthesize_failure_answer)

    # 2. Add entry edge to Node 0 (Gate Guardrail)
    builder.add_edge(START, "input_guardrail")

    # 3. Conditional routing after Node 0 Gate Guardrail
    builder.add_conditional_edges(
        "input_guardrail",
        _route_after_gate,
        {
            "synthesize_security_rejection": "synthesize_security_rejection",
            "generate_sql": "generate_sql"
        }
    )

    # 4. Generator transitions directly to AST Security Gate
    builder.add_edge("generate_sql", "validate_security")

    # 5. Conditional routing after Perimeter 2 AST Security Gate
    builder.add_conditional_edges(
        "validate_security",
        _route_after_security,
        {
            "synthesize_security_rejection": "synthesize_security_rejection",
            "judge_intent": "judge_intent"
        }
    )

    # 6. Conditional routing after Intent Judge (Reflection loop)
    builder.add_conditional_edges(
        "judge_intent",
        _route_after_judge,
        {
            "generate_sql": "generate_sql",
            "execute_mcp": "execute_mcp",
            "synthesize_failure": "synthesize_failure"
        }
    )

    # 7. Conditional routing after MCP Execution (Self-healing loop)
    builder.add_conditional_edges(
        "execute_mcp",
        _route_after_execution,
        {
            "diagnose_runtime_error": "diagnose_runtime_error",
            "synthesize_business_answer": "synthesize_business_answer",
            "synthesize_failure": "synthesize_failure"
        }
    )

    # 8. Diagnosis loops back to generator
    builder.add_edge("diagnose_runtime_error", "generate_sql")

    # 9. Terminal synthesis edges
    builder.add_edge("synthesize_security_rejection", END)
    builder.add_edge("synthesize_business_answer", END)
    builder.add_edge("synthesize_failure", END)

    return builder.compile()


# Pre-compiled graph instance
lakehouse_graph = create_lakehouse_graph()
