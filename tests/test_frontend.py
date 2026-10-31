"""
tests/test_frontend.py
================================================================
Unit tests for Streamlit Frontend helpers and API connection contracts.
"""

import pytest
from unittest.mock import patch, MagicMock
from frontend.app import (
    fetch_health_status,
    fetch_tables,
    fetch_table_schema,
    execute_multiagent_query,
    stream_multiagent_query
)


def test_fetch_health_status_success():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "status": "healthy",
        "service": "lakehouse-ai-service",
        "version": "1.0.0",
        "trino_connected": True,
        "catalog": "iceberg",
        "schema_name": "ecommerce"
    }
    with patch("requests.get", return_value=mock_resp):
        res = fetch_health_status()
        assert res["status"] == "healthy"
        assert res["trino_connected"] is True


def test_fetch_health_status_failure():
    with patch("requests.get", side_effect=Exception("Connection refused")):
        res = fetch_health_status()
        assert res["status"] == "unreachable"
        assert res["trino_connected"] is False


def test_fetch_tables_success():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "catalog": "iceberg",
        "schema": "ecommerce",
        "table_count": 2,
        "tables": ["orders", "products"]
    }
    with patch("requests.get", return_value=mock_resp):
        res = fetch_tables()
        assert res == ["orders", "products"]


def test_execute_multiagent_query_success():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "status": "SUCCESS",
        "final_answer": "There are 96,478 delivered orders.",
        "sql_query": "SELECT count(*) FROM iceberg.ecommerce.orders WHERE order_status = 'delivered'",
        "attempts": 1,
        "execution_success": True,
        "query_result": [{"_col0": 96478}],
        "security_violation": None,
        "latency_ms": 1250.0
    }
    with patch("requests.post", return_value=mock_resp):
        res = execute_multiagent_query(
            question="How many delivered orders?",
            target_tables=["orders"],
            max_attempts=3
        )
        assert res["status"] == "SUCCESS"
        assert res["execution_success"] is True
        assert res["query_result"] == [{"_col0": 96478}]


def test_stream_multiagent_query_events():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_lines = [
        b'data: {"stage": "init", "message": "Reflecting..."}\n\n',
        b'data: {"stage": "gate_approved", "message": "Gate approved"}\n\n',
        b'data: {"stage": "sql_generated", "message": "Generated", "sql": "SELECT 1"}\n\n',
        b'data: {"stage": "complete", "message": "Done", "status": "SUCCESS", "final_answer": "Complete answer"}\n\n',
        b'data: [DONE]\n\n'
    ]
    mock_resp.iter_lines.return_value = mock_lines

    with patch("requests.post", return_value=mock_resp):
        events = list(stream_multiagent_query(
            question="How many orders?",
            target_tables=["orders"],
            max_attempts=3
        ))
        assert len(events) == 4
        assert events[0]["stage"] == "init"
        assert events[1]["stage"] == "gate_approved"
        assert events[2]["stage"] == "sql_generated"
        assert events[2]["sql"] == "SELECT 1"
        assert events[3]["stage"] == "complete"
        assert events[3]["status"] == "SUCCESS"


