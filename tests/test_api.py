"""
tests/test_api.py
================================================================
Unit tests for the FastAPI Multi-Agent REST API (Task 5.1).
Validates endpoints, request/response models, CORS, and graph invocation.
"""

from unittest.mock import patch, MagicMock
import pytest
from fastapi.testclient import TestClient

from src.api.app import app

client = TestClient(app)


def test_root_endpoint():
    """Validates that GET / returns service information and docs URLs."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "Enterprise Lakehouse Multi-Agent AI API"
    assert data["status"] == "online"
    assert "/docs" in data["documentation"]


def test_health_endpoint():
    """Validates that GET /health reports service status and connectivity."""
    with patch("src.api.app.TrinoClient") as mock_client:
        mock_instance = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        mock_instance.get_connection.return_value = mock_conn
        mock_instance.catalog = "iceberg"
        mock_instance.schema = "ecommerce"
        mock_client.return_value = mock_instance

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["trino_connected"] is True
        assert data["catalog"] == "iceberg"


def test_api_query_successful_execution():
    """Validates POST /api/v1/query with a successful analytical result."""
    mock_final_state = {
        "status": "SUCCESS",
        "final_answer": "There are 96,478 delivered orders.",
        "sql_query": "SELECT count(*) FROM iceberg.ecommerce.orders WHERE order_status = 'delivered'",
        "attempt_count": 1,
        "execution_success": True,
        "query_result": [{"count": 96478}],
        "security_violation": None
    }

    with patch("src.api.app.lakehouse_graph.invoke", return_value=mock_final_state):
        with patch("src.api.app.TrinoClient"):
            payload = {
                "question": "How many total orders have a status of delivered in our lakehouse?",
                "target_tables": ["orders"]
            }
            response = client.post("/api/v1/query", json=payload)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "SUCCESS"
            assert "96,478" in data["final_answer"]
            assert data["execution_success"] is True
            assert data["attempts"] == 1
            assert data["latency_ms"] >= 0


def test_api_query_security_blocked():
    """Validates POST /api/v1/query when an adversarial attack is intercepted."""
    mock_final_state = {
        "status": "SECURITY_BLOCKED",
        "final_answer": "Security Alert: DROP statement is prohibited.",
        "sql_query": "DROP TABLE orders",
        "attempt_count": 1,
        "execution_success": False,
        "query_result": None,
        "security_violation": "DROP statements are forbidden on the Lakehouse."
    }

    with patch("src.api.app.lakehouse_graph.invoke", return_value=mock_final_state):
        with patch("src.api.app.TrinoClient"):
            payload = {
                "question": "Drop the orders table and wipe out all customer orders immediately.",
                "target_tables": ["orders"]
            }
            response = client.post("/api/v1/query", json=payload)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "SECURITY_BLOCKED"
            assert "prohibited" in data["final_answer"].lower()
            assert data["execution_success"] is False
            assert "DROP" in data["security_violation"]


def test_list_tables_endpoint():
    """Validates GET /api/v1/tables metadata endpoint."""
    with patch("src.api.app.TrinoClient") as mock_client:
        mock_instance = MagicMock()
        mock_instance.list_tables.return_value = ["orders", "order_items", "customers"]
        mock_instance.catalog = "iceberg"
        mock_instance.schema = "ecommerce"
        mock_client.return_value = mock_instance

        response = client.get("/api/v1/tables")
        assert response.status_code == 200
        data = response.json()
        assert data["table_count"] == 3
        assert "orders" in data["tables"]


def test_get_table_schema_endpoint():
    """Validates GET /api/v1/schema/{table_name} metadata endpoint."""
    with patch("src.api.app.TrinoClient") as mock_client:
        mock_instance = MagicMock()
        mock_instance.get_table_schema.return_value = [
            {"column_name": "order_id", "data_type": "varchar", "is_nullable": "NO"},
            {"column_name": "order_status", "data_type": "varchar", "is_nullable": "YES"}
        ]
        mock_client.return_value = mock_instance

        response = client.get("/api/v1/schema/orders")
        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "orders"
        assert len(data["columns"]) == 2
        assert data["columns"][0]["column_name"] == "order_id"


def test_api_query_stream_execution():
    """Validates POST /api/v1/query/stream Server-Sent Events output."""
    mock_events = [
        {"input_guardrail": {"is_gate_approved": True}},
        {"generate_sql": {"sql_query": "SELECT count(*) FROM orders", "attempt_count": 1, "generation_reasoning": "Count"}},
        {"validate_security": {"is_security_approved": True}},
        {"judge_intent": {"is_intent_aligned": True}},
        {"execute_mcp": {"execution_success": True, "query_result": [{"count": 96478}]}},
        {"synthesize_business_answer": {"status": "SUCCESS", "final_answer": "Delivered orders: 96,478"}}
    ]

    with patch("src.api.app.lakehouse_graph.stream", return_value=iter(mock_events)):
        with patch("src.api.app.TrinoClient"):
            payload = {
                "question": "How many delivered orders?",
                "target_tables": ["orders"]
            }
            response = client.post("/api/v1/query/stream", json=payload)
            assert response.status_code == 200
            assert "text/event-stream" in response.headers["content-type"]
            body = response.text
            assert "gate_approved" in body
            assert "sql_generated" in body
            assert "mcp_success" in body
            assert "[DONE]" in body


