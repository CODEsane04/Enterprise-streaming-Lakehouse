"""
tests/test_security.py
================================================================
Enterprise Agentic Streaming Lakehouse - Engineer 4
Task 2.5: Automated Security Test Suite for SQL AST Guardrails

Covers:
- Valid analytical queries (aggregations, joins, CTEs, window functions)
- DDL/DML mutation interception (DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE)
- Multi-statement injection attacks
- Resource governance & LIMIT injection/clamping
- Table & schema namespace allowlisting
- Malformed syntax & edge-case handling
================================================================
"""

import pytest
from src.security.validator import SQLSecurityValidator
from src.security.exceptions import (
    SQLSecurityError,
    SQLSyntaxError,
    ForbiddenOperationError,
    InvalidNamespaceError,
)


@pytest.fixture
def validator():
    """Provides a fresh instance of the SQLSecurityValidator."""
    return SQLSecurityValidator(default_limit=100, max_limit=1000)


# ====================================================================
# 1. VALID ANALYTICAL QUERIES (HAPPY PATH)
# ====================================================================

class TestValidAnalyticalQueries:
    def test_simple_select_all(self, validator):
        result = validator.validate("SELECT * FROM iceberg.ecommerce.orders")
        assert result.is_valid is True
        assert "orders" in result.tables_referenced[0]
        assert result.applied_limit == 100

    def test_select_specific_columns(self, validator):
        sql = "SELECT order_id, order_status FROM ecommerce.orders WHERE order_status = 'delivered'"
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit == 100

    def test_aggregations_and_group_by(self, validator):
        sql = """
        SELECT customer_state, count(*) as order_count
        FROM customers
        GROUP BY customer_state
        ORDER BY order_count DESC
        """
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit == 100

    def test_multi_table_join(self, validator):
        sql = """
        SELECT o.order_id, c.customer_city, oi.price, oi.freight_value
        FROM iceberg.ecommerce.orders o
        JOIN iceberg.ecommerce.customers c ON o.customer_id = c.customer_id
        JOIN iceberg.ecommerce.order_items oi ON o.order_id = oi.order_id
        WHERE oi.price > 100.0
        """
        result = validator.validate(sql)
        assert result.is_valid is True
        assert len(result.tables_referenced) == 3

    def test_common_table_expressions_cte(self, validator):
        sql = """
        WITH high_value_orders AS (
            SELECT order_id, sum(price) as total_val
            FROM order_items
            GROUP BY order_id
            HAVING sum(price) > 500
        )
        SELECT h.order_id, h.total_val, o.order_status
        FROM high_value_orders h
        JOIN orders o ON h.order_id = o.order_id
        """
        result = validator.validate(sql)
        assert result.is_valid is True
        assert "order_items" in result.sanitized_sql.lower()
        assert "orders" in result.sanitized_sql.lower()

    def test_window_function_query(self, validator):
        sql = """
        SELECT order_id, price,
               rank() OVER (PARTITION BY seller_id ORDER BY price DESC) as rank
        FROM order_items
        """
        result = validator.validate(sql)
        assert result.is_valid is True

    def test_explain_statement_allowed(self, validator):
        sql = "EXPLAIN SELECT * FROM orders"
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit is None


# ====================================================================
# 2. PROHIBITED MUTATIONS & DDL/DML ATTACKS
# ====================================================================

class TestProhibitedMutations:
    @pytest.mark.parametrize("query,mutation_type", [
        ("DROP TABLE iceberg.ecommerce.orders", "DROP"),
        ("DROP TABLE IF EXISTS orders", "DROP"),
        ("DELETE FROM iceberg.ecommerce.orders WHERE order_id = '123'", "DELETE"),
        ("DELETE FROM orders", "DELETE"),
        ("UPDATE iceberg.ecommerce.order_items SET price = 0", "UPDATE"),
        ("UPDATE orders SET order_status = 'cancelled' WHERE order_id = '1'", "UPDATE"),
        ("INSERT INTO iceberg.ecommerce.orders (order_id) VALUES ('fake')", "INSERT"),
        ("ALTER TABLE orders ADD COLUMN test_col INT", "ALTER"),
        ("ALTER TABLE orders DROP COLUMN customer_id", "ALTER"),
        ("TRUNCATE TABLE orders", "TRUNCATE"),
        ("CREATE TABLE test_table (id INT)", "CREATE"),
    ])
    def test_direct_mutations_blocked(self, validator, query, mutation_type):
        result = validator.validate(query)
        assert result.is_valid is False
        assert "prohibited" in result.error_message.lower() or "forbidden" in result.error_message.lower()

    def test_nested_mutation_inside_subquery(self, validator):
        # Attacker attempts to hide a mutation inside a subquery
        sql = "SELECT * FROM orders WHERE order_id IN (DELETE FROM order_items WHERE price < 0 RETURNING order_id)"
        result = validator.validate(sql)
        assert result.is_valid is False

    def test_administrative_commands_blocked(self, validator):
        admin_queries = [
            "KILL QUERY '20261024_123456_00001_abcde'",
            "SET SESSION query_max_run_time = '10s'",
        ]
        for query in admin_queries:
            result = validator.validate(query)
            assert result.is_valid is False


# ====================================================================
# 3. MULTI-STATEMENT & SQL INJECTION ATTACKS
# ====================================================================

class TestInjectionAttacks:
    def test_semicolon_chained_injection(self, validator):
        sql = "SELECT * FROM orders; DROP TABLE order_items;"
        result = validator.validate(sql)
        assert result.is_valid is False
        assert "multiple sql statements" in result.error_message.lower()

    def test_triple_statement_injection(self, validator):
        sql = "SELECT 1; SELECT 2; DROP TABLE orders;"
        result = validator.validate(sql)
        assert result.is_valid is False
        assert "multiple sql statements" in result.error_message.lower()


# ====================================================================
# 4. RESOURCE GOVERNANCE & ROW LIMITING
# ====================================================================

class TestResourceGovernance:
    def test_injects_default_limit_when_missing(self, validator):
        sql = "SELECT * FROM orders"
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit == 100
        assert "LIMIT 100" in result.sanitized_sql

    def test_clamps_excessive_limit(self, validator):
        sql = "SELECT * FROM orders LIMIT 50000"
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit == 1000
        assert "LIMIT 1000" in result.sanitized_sql

    def test_preserves_safe_limit_within_bounds(self, validator):
        sql = "SELECT * FROM orders LIMIT 42"
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit == 42
        assert "LIMIT 42" in result.sanitized_sql

    def test_resets_negative_or_zero_limit(self, validator):
        sql = "SELECT * FROM orders LIMIT 0"
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit == 100

    def test_preserves_offset_pagination(self, validator):
        sql = "SELECT * FROM orders LIMIT 500 OFFSET 1000"
        result = validator.validate(sql)
        assert result.is_valid is True
        assert result.applied_limit == 500
        assert "OFFSET 1000" in result.sanitized_sql


# ====================================================================
# 5. NAMESPACE & TABLE ALLOWLISTS
# ====================================================================

class TestNamespaceAllowlists:
    @pytest.mark.parametrize("table", [
        "orders", "order_items", "order_payments", "order_reviews",
        "customers", "geolocation", "products", "sellers",
        "product_category_name_translation"
    ])
    def test_all_official_tables_permitted(self, validator, table):
        result = validator.validate(f"SELECT * FROM iceberg.ecommerce.{table}")
        assert result.is_valid is True

    def test_system_catalog_blocked(self, validator):
        sql = "SELECT * FROM system.runtime.nodes"
        result = validator.validate(sql)
        assert result.is_valid is False
        assert "unauthorized catalog" in result.error_message.lower()

    def test_external_postgres_catalog_blocked(self, validator):
        sql = "SELECT * FROM postgres.public.users"
        result = validator.validate(sql)
        assert result.is_valid is False
        assert "unauthorized catalog" in result.error_message.lower()

    def test_unauthorized_schema_blocked(self, validator):
        sql = "SELECT * FROM iceberg.secret_finance.salaries"
        result = validator.validate(sql)
        assert result.is_valid is False
        assert "unauthorized schema" in result.error_message.lower()

    def test_hallucinated_table_blocked(self, validator):
        sql = "SELECT * FROM iceberg.ecommerce.employee_credentials"
        result = validator.validate(sql)
        assert result.is_valid is False
        assert "unknown or unauthorized table" in result.error_message.lower()


# ====================================================================
# 6. MALFORMED SQL & SYNTAX ERRORS
# ====================================================================

class TestMalformedQueries:
    def test_empty_string(self, validator):
        result = validator.validate("")
        assert result.is_valid is False
        assert "empty or blank" in result.error_message.lower()

    def test_whitespace_only(self, validator):
        result = validator.validate("   \n\t  ")
        assert result.is_valid is False
        assert "empty or blank" in result.error_message.lower()

    def test_broken_syntax(self, validator):
        result = validator.validate("SELECT FROM WHERE 1 = 1")
        assert result.is_valid is False
        assert "syntax error" in result.error_message.lower() or "failed to parse" in result.error_message.lower()

