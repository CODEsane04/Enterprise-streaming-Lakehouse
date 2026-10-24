"""
src/security/validator.py
================================================================
Enterprise Agentic Streaming Lakehouse - Engineer 4
Task 2.1: SQL AST Validator Module using sqlglot

Parses incoming raw SQL strings into Abstract Syntax Trees (AST)
using the Trino dialect, inspects structural intent, and provides
the foundation for deterministic security guardrails.
================================================================
"""

from typing import Optional, List
from pydantic import BaseModel, Field
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .exceptions import SQLSyntaxError, ForbiddenOperationError


class ValidationResult(BaseModel):
    """Encapsulates the outcome of SQL AST parsing and security evaluation."""

    is_valid: bool = Field(description="True if the SQL passed all security checks, False otherwise")
    sanitized_sql: Optional[str] = Field(default=None, description="Transpiled and safe SQL string ready for execution")
    error_message: Optional[str] = Field(default=None, description="Descriptive error feedback for the LLM / caller")
    tables_referenced: List[str] = Field(default_factory=list, description="List of table names identified in the AST")
    applied_limit: Optional[int] = Field(default=None, description="Row limit enforced on the AST")

    model_config = {"arbitrary_types_allowed": True}


class SQLSecurityValidator:
    """
    Deterministic SQL AST security firewall for Trino Lakehouse.
    
    Transforms raw SQL queries into sqlglot Abstract Syntax Trees (AST)
    under the Trino dialect to inspect query intent and guarantee safety.
    """

    DEFAULT_DIALECT = "trino"

    def __init__(self, dialect: str = DEFAULT_DIALECT):
        self.dialect = dialect

    def parse(self, sql: str) -> exp.Expression:
        """
        Parses a raw SQL string into a single sqlglot AST expression.

        Args:
            sql: Raw SQL query string.

        Returns:
            sqlglot.Expression: Root node of the AST.

        Raises:
            SQLSyntaxError: If the SQL is empty, malformed, or contains multiple statements.
        """
        if not sql or not sql.strip():
            raise SQLSyntaxError("SQL query is empty or blank.")

        clean_sql = sql.strip().rstrip(";")

        # Detect multiple statement injection (e.g., 'SELECT 1; DROP TABLE orders')
        try:
            parsed_statements = sqlglot.parse(clean_sql, read=self.dialect)
            # Filter out empty statements
            statements = [s for s in parsed_statements if s is not None]
            if len(statements) > 1:
                raise ForbiddenOperationError(
                    "Multiple SQL statements detected.",
                    details=f"Query contains {len(statements)} statements. Exactly one statement is permitted."
                )
        except ParseError as e:
            raise SQLSyntaxError(f"Failed to parse SQL: {e.errors[0]['description'] if e.errors else str(e)}")

        # Parse into a single AST root expression
        try:
            ast = sqlglot.parse_one(clean_sql, read=self.dialect)
            if ast is None:
                raise SQLSyntaxError("SQL parsing produced an empty AST.")
            return ast
        except ParseError as e:
            err_msg = e.errors[0]["description"] if e.errors else str(e)
            raise SQLSyntaxError(f"SQL Syntax Error: {err_msg}")

    def extract_tables(self, ast: exp.Expression) -> List[str]:
        """
        Extracts all table identifiers referenced in the AST.

        Args:
            ast: The sqlglot Expression root.

        Returns:
            List[str]: Table names extracted from the query.
        """
        tables = set()
        for table_node in ast.find_all(exp.Table):
            # Capture full qualification (catalog.schema.table or table)
            full_name = table_node.sql(dialect=self.dialect)
            if full_name:
                tables.add(full_name.lower())
        return sorted(list(tables))

    def validate(self, sql: str) -> ValidationResult:
        """
        Validates raw SQL string and produces a ValidationResult.
        
        Args:
            sql: Raw SQL string to validate.

        Returns:
            ValidationResult with validation status and extracted metadata.
        """
        try:
            ast = self.parse(sql)
            tables = self.extract_tables(ast)
            sanitized = ast.sql(dialect=self.dialect)
            return ValidationResult(
                is_valid=True,
                sanitized_sql=sanitized,
                tables_referenced=tables,
            )
        except (SQLSyntaxError, ForbiddenOperationError) as e:
            return ValidationResult(
                is_valid=False,
                error_message=str(e),
                tables_referenced=[],
            )
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                error_message=f"Unexpected validation error: {str(e)}",
                tables_referenced=[],
            )

