"""
src/mcp/server.py
================================================================
Enterprise Agentic Streaming Lakehouse - FastMCP Server
Task 3.1: Initialize FastMCP Server

Initializes the Model Context Protocol (MCP) server instance,
attaching Trino client connectivity and the deterministic sqlglot
AST security guardrail firewall.
================================================================
"""

import sys
from fastmcp import FastMCP

from .config import settings
from .trino_client import TrinoClient
from src.security.validator import SQLSecurityValidator

# Initialize the official FastMCP Server Instance
mcp = FastMCP(
    name=settings.server_name,
)

# Shared security validator and database client instances
security_validator = SQLSecurityValidator(
    default_limit=settings.default_limit,
    max_limit=settings.max_limit,
)

trino_client = TrinoClient(
    host=settings.trino_host,
    port=settings.trino_port,
    user=settings.trino_user,
    catalog=settings.trino_catalog,
    schema=settings.trino_schema,
)


def get_server_info():
    """Returns server identity, status, and connection parameters."""
    return {
        "server_name": settings.server_name,
        "version": settings.server_version,
        "trino_target": f"http://{settings.trino_host}:{settings.trino_port}",
        "catalog": settings.trino_catalog,
        "schema": settings.trino_schema,
        "security_guardrails_active": True,
        "default_limit": settings.default_limit,
        "max_limit": settings.max_limit,
    }


if __name__ == "__main__":
    print(f"🚀 Starting {settings.server_name} via standard stdio transport...")
    mcp.run()

