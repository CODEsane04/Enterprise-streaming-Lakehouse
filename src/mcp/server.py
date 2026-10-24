"""
src/mcp/server.py
================================================================
Enterprise Agentic Streaming Lakehouse - FastMCP Server
Task 3.1 & 3.2: FastMCP Server & Tool 1 (list_tables)

Exposes the Lakehouse analytical engine to Large Language Models (LLMs)
via the Model Context Protocol (MCP), protected by deterministic AST
security guardrails.
================================================================
"""

import sys
from typing import Dict, Any, List
from fastmcp import FastMCP

from .config import settings
from .trino_client import TrinoClient
from src.security.validator import SQLSecurityValidator

# Semantic catalog descriptions for the Olist E-commerce Lakehouse
TABLE_DESCRIPTIONS: Dict[str, str] = {
    "orders": "Core order tracking: customer_id, order_status, and purchase/delivery timestamps.",
    "order_items": "Item-level details: price, freight_value, product_id, and seller_id.",
    "order_payments": "Payment transactions: payment_type, installments, and payment_value.",
    "order_reviews": "Customer reviews, ratings (1-5 stars), and review comments.",
    "customers": "Customer demographics: customer_id, city, and state.",
    "products": "Product specifications: category_name, weight, and dimensions.",
    "sellers": "Seller demographics: seller_id, city, and state.",
    "geolocation": "Zip code prefixes, latitude, longitude, city, and state.",
    "product_category_name_translation": "Translations between Portuguese and English category names.",
}

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


@mcp.tool()
def list_tables() -> Dict[str, Any]:
    """
    List all available analytical Iceberg tables in the Lakehouse (catalog: iceberg, schema: ecommerce).

    Use this tool to discover what tables exist before querying schemas or writing analytical SQL.
    Returns table names, schema metadata, and semantic descriptions.
    """
    try:
        tables = trino_client.list_tables()
        descriptions = {
            t: TABLE_DESCRIPTIONS.get(t.lower(), "Lakehouse analytical table")
            for t in tables
        }
        return {
            "success": True,
            "catalog": settings.trino_catalog,
            "schema": settings.trino_schema,
            "table_count": len(tables),
            "tables": tables,
            "descriptions": descriptions,
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to list tables from Trino: {str(e)}",
            "catalog": settings.trino_catalog,
            "schema": settings.trino_schema,
            "tables": [],
        }


def get_server_info() -> Dict[str, Any]:
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
