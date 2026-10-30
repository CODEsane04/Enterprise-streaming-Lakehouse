"""
tests/test_mcp.py
================================================================
Enterprise Agentic Streaming Lakehouse - Engineer 4
Task 3.5: FastMCP Tool and Protocol Unit Tests
================================================================
"""

import pytest
from src.mcp.server import mcp, list_tables, get_table_schema, execute_analytical_query


class TestMCPTools:
    def test_mcp_server_identity(self):
        assert mcp.name == "Lakehouse-Analytics-Server"

    def test_list_tools_registered(self):
        import asyncio
        tools = asyncio.run(mcp.list_tools())
        tool_names = [t.name for t in tools]
        assert "list_tables" in tool_names
        assert "get_table_schema" in tool_names
        assert "execute_analytical_query" in tool_names

    def test_get_table_schema_valid_orders(self):
        result = get_table_schema("orders")
        assert result["success"] is True
        assert result["table_name"] == "orders"
        assert result["column_count"] > 0
        assert "order_id" in result["primary_keys"]

    def test_get_table_schema_unauthorized_blocked(self):
        result = get_table_schema("unauthorized_passwords")
        assert result["success"] is False
        assert "unauthorized" in result["error"].lower()

    def test_execute_analytical_query_drop_blocked(self):
        result = execute_analytical_query("DROP TABLE orders")
        assert result["success"] is False
        assert result["error_type"] == "SecurityGuardrailViolation"

    def test_execute_analytical_query_limit_injected(self):
        result = execute_analytical_query("SELECT * FROM orders")
        # Query will be sanitized and have LIMIT 100
        assert result["applied_limit"] == 100
        assert "LIMIT 100" in result["executed_query"]

