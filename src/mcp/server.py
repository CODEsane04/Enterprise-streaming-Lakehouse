"""
src/mcp/server.py
================================================================
Enterprise Agentic Streaming Lakehouse - FastMCP Server
Task 3.1, 3.2 & 3.3: FastMCP Server, list_tables & get_table_schema

Exposes the Lakehouse analytical engine to Large Language Models (LLMs)
via the Model Context Protocol (MCP), protected by deterministic AST
security guardrails and enhanced with dynamic Schema RAG context.
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

# Rich Schema RAG metadata: primary keys, relationships, and column descriptions
TABLE_SCHEMA_METADATA: Dict[str, Dict[str, Any]] = {
    "orders": {
        "primary_keys": ["order_id"],
        "relationships": [
            "orders.customer_id joins with customers.customer_id",
            "orders.order_id joins with order_items.order_id",
            "orders.order_id joins with order_payments.order_id",
            "orders.order_id joins with order_reviews.order_id",
        ],
        "column_descriptions": {
            "order_id": "Unique identifier for the order (Primary Key)",
            "customer_id": "Foreign key token referencing customers.customer_id",
            "order_status": "Status of the order (delivered, shipped, invoiced, created, canceled, etc.)",
            "order_purchase_timestamp": "Timestamp when customer placed the order",
            "order_approved_at": "Timestamp when payment was approved",
            "order_delivered_carrier_date": "Timestamp when order was handed to delivery carrier",
            "order_delivered_customer_date": "Actual delivery timestamp to customer",
            "order_estimated_delivery_date": "Estimated delivery date promised to customer",
            "created_at": "Record creation timestamp in Lakehouse",
            "updated_at": "Record update timestamp in Lakehouse",
        },
    },
    "order_items": {
        "primary_keys": ["order_id", "order_item_id"],
        "relationships": [
            "order_items.order_id joins with orders.order_id",
            "order_items.product_id joins with products.product_id",
            "order_items.seller_id joins with sellers.seller_id",
        ],
        "column_descriptions": {
            "order_id": "Foreign key identifying the order",
            "order_item_id": "Sequential number identifying item in order (composite PK)",
            "product_id": "Foreign key to products.product_id",
            "seller_id": "Foreign key to sellers.seller_id",
            "shipping_limit_date": "Seller shipping limit date",
            "price": "Item selling price in Brazilian Reais (BRL)",
            "freight_value": "Shipping / freight value in BRL",
            "created_at": "Record creation timestamp",
            "updated_at": "Record update timestamp",
        },
    },
    "order_payments": {
        "primary_keys": ["order_id", "payment_sequential"],
        "relationships": [
            "order_payments.order_id joins with orders.order_id",
        ],
        "column_descriptions": {
            "order_id": "Foreign key to orders.order_id",
            "payment_sequential": "Sequence number for split payments (composite PK)",
            "payment_type": "Payment method (credit_card, boleto, voucher, debit_card)",
            "payment_installments": "Number of payment installments chosen",
            "payment_value": "Total transaction amount paid in BRL",
            "created_at": "Record creation timestamp",
        },
    },
    "customers": {
        "primary_keys": ["customer_id"],
        "relationships": [
            "customers.customer_id joins with orders.customer_id",
        ],
        "column_descriptions": {
            "customer_id": "Unique customer order token (Primary Key)",
            "customer_unique_id": "Unique identifier of the physical person",
            "customer_zip_code_prefix": "First 5 digits of customer zip code",
            "customer_city": "City where customer is located",
            "customer_state": "State code (e.g., SP, RJ, MG)",
        },
    },
    "products": {
        "primary_keys": ["product_id"],
        "relationships": [
            "products.product_id joins with order_items.product_id",
            "products.product_category_name joins with product_category_name_translation.product_category_name",
        ],
        "column_descriptions": {
            "product_id": "Unique product identifier (Primary Key)",
            "product_category_name": "Category name in Portuguese",
            "product_name_lenght": "Length of product name",
            "product_description_lenght": "Length of product description",
            "product_photos_qty": "Number of product photos published",
            "product_weight_g": "Product weight in grams",
            "product_length_cm": "Product length in centimeters",
            "product_height_cm": "Product height in centimeters",
            "product_width_cm": "Product width in centimeters",
        },
    },
    "sellers": {
        "primary_keys": ["seller_id"],
        "relationships": [
            "sellers.seller_id joins with order_items.seller_id",
        ],
        "column_descriptions": {
            "seller_id": "Unique seller identifier (Primary Key)",
            "seller_zip_code_prefix": "First 5 digits of seller zip code",
            "seller_city": "City where seller is located",
            "seller_state": "State code of seller (e.g., SP, PR)",
        },
    },
    "order_reviews": {
        "primary_keys": ["review_id"],
        "relationships": [
            "order_reviews.order_id joins with orders.order_id",
        ],
        "column_descriptions": {
            "review_id": "Unique review identifier (Primary Key)",
            "order_id": "Foreign key to orders.order_id",
            "review_score": "Satisfaction score from 1 to 5 stars",
            "review_comment_title": "Title of the review comment",
            "review_comment_message": "Text of the review comment",
            "review_creation_date": "Timestamp when survey was sent",
            "review_answer_timestamp": "Timestamp when review was submitted",
        },
    },
    "geolocation": {
        "primary_keys": ["geolocation_zip_code_prefix"],
        "relationships": [],
        "column_descriptions": {
            "geolocation_zip_code_prefix": "First 5 digits of zip code",
            "geolocation_lat": "Latitude coordinate",
            "geolocation_lng": "Longitude coordinate",
            "geolocation_city": "City name",
            "geolocation_state": "State code",
        },
    },
    "product_category_name_translation": {
        "primary_keys": ["product_category_name"],
        "relationships": [
            "product_category_name_translation.product_category_name joins with products.product_category_name",
        ],
        "column_descriptions": {
            "product_category_name": "Category name in Portuguese",
            "product_category_name_english": "Category name in English",
        },
    },
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


@mcp.tool()
def get_table_schema(table_name: str) -> Dict[str, Any]:
    """
    Get detailed schema information for a specific table in the Lakehouse (catalog: iceberg, schema: ecommerce).

    Returns column names, data types, nullability, primary key indicators, and relational join hints.
    Use this tool before generating SQL queries to ensure exact column names and types are used (Schema RAG).

    Args:
        table_name: Name of the table (e.g., 'orders', 'order_items', 'order_payments').
                    Can be unqualified ('orders') or fully qualified ('iceberg.ecommerce.orders').
    """
    clean_name = table_name.strip().strip("'\"`").lower()
    base_table = clean_name.split(".")[-1]

    # Security Check: Enforce table allowlist before accessing database
    if base_table not in security_validator.allowed_tables:
        return {
            "success": False,
            "error": f"Unknown or unauthorized table '{table_name}'.",
            "details": f"Table '{table_name}' is not part of the e-commerce Lakehouse. Allowed tables: {', '.join(sorted(security_validator.allowed_tables))}",
            "allowed_tables": sorted(list(security_validator.allowed_tables)),
        }

    meta = TABLE_SCHEMA_METADATA.get(base_table, {})
    primary_keys = set(meta.get("primary_keys", []))
    col_descriptions = meta.get("column_descriptions", {})

    try:
        raw_columns = trino_client.get_table_schema(base_table)
        enriched_columns = []

        if raw_columns:
            for col in raw_columns:
                cname = col["column_name"].lower()
                enriched_columns.append({
                    "column_name": col["column_name"],
                    "data_type": col["data_type"],
                    "is_nullable": col["is_nullable"],
                    "is_primary_key": cname in primary_keys,
                    "description": col_descriptions.get(cname, "Analytical field"),
                })
        else:
            # Fallback to static schema metadata if table exists in catalog definition but Trino reflection is pending
            for cname, cdesc in col_descriptions.items():
                enriched_columns.append({
                    "column_name": cname,
                    "data_type": "STRING",
                    "is_nullable": "YES",
                    "is_primary_key": cname in primary_keys,
                    "description": cdesc,
                })

        return {
            "success": True,
            "table_name": base_table,
            "catalog": settings.trino_catalog,
            "schema": settings.trino_schema,
            "full_name": f"{settings.trino_catalog}.{settings.trino_schema}.{base_table}",
            "column_count": len(enriched_columns),
            "columns": enriched_columns,
            "primary_keys": list(primary_keys),
            "relationships": meta.get("relationships", []),
            "sample_query": f"SELECT * FROM {settings.trino_catalog}.{settings.trino_schema}.{base_table} LIMIT 5;",
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to retrieve schema for '{table_name}': {str(e)}",
            "table_name": base_table,
            "catalog": settings.trino_catalog,
            "schema": settings.trino_schema,
            "columns": [],
        }


@mcp.tool()
def execute_analytical_query(sql_query: str) -> Dict[str, Any]:
    """
    Safely execute a read-only analytical SQL query against the Lakehouse (catalog: iceberg, schema: ecommerce).

    The query is deterministically parsed into an Abstract Syntax Tree (AST) using sqlglot and validated
    against security guardrails before execution:
    - Only read-only queries (SELECT, EXPLAIN) are permitted.
    - Write operations (DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE, CREATE) are strictly blocked.
    - Semicolon-separated multi-statement injections are intercepted.
    - Queries without a LIMIT clause automatically receive a default limit (100) to protect memory.
    - Excessive LIMIT clauses (> 1000) are clamped down to 1000.
    - Only tables within the 'iceberg.ecommerce' namespace are accessible.

    Args:
        sql_query: The SQL query to validate and execute.
    """
    # Step 1: Deterministic SQL AST Security & Policy Validation
    validation = security_validator.validate(sql_query)

    if not validation.is_valid:
        return {
            "success": False,
            "error_type": "SecurityGuardrailViolation",
            "error": validation.error_message,
            "actionable_feedback": "Your query was blocked by Lakehouse deterministic AST security policies. Only read-only SELECT queries on allowed tables are permitted.",
            "raw_query": sql_query,
            "executed_query": None,
            "data": [],
        }

    # Step 2: Execute validated and sanitized query on Trino engine
    db_result = trino_client.execute_query(validation.sanitized_sql)

    if not db_result["success"]:
        return {
            "success": False,
            "error_type": "TrinoExecutionError",
            "error": db_result.get("error"),
            "raw_query": sql_query,
            "executed_query": validation.sanitized_sql,
            "tables_referenced": validation.tables_referenced,
            "applied_limit": validation.applied_limit,
            "execution_time_ms": db_result.get("execution_time_ms", 0),
            "data": [],
        }

    return {
        "success": True,
        "raw_query": sql_query,
        "executed_query": validation.sanitized_sql,
        "tables_referenced": validation.tables_referenced,
        "applied_limit": validation.applied_limit,
        "row_count": db_result["row_count"],
        "columns": db_result["columns"],
        "data": db_result["data"],
        "execution_time_ms": db_result["execution_time_ms"],
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
