"""
src/security/validator.py
================================================================
Enterprise Agentic Streaming Lakehouse - Engineer 4
Task 2.1 & 2.2: SQL AST Validator with Deterministic Read-Only Guardrails

Parses incoming raw SQL strings into Abstract Syntax Trees (AST)
using the Trino dialect, inspects structural intent, and enforces
strict read-only execution policies (blocking DDL/DML mutations).
================================================================
"""

from typing import Optional, List, Tuple, Type
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

    # AST node types that are strictly forbidden anywhere in the query
    FORBIDDEN_MUTATION_NODES: Tuple[Type[exp.Expression], ...] = (
        exp.Drop,
        exp.Delete,
        exp.Insert,
        exp.Update,
        exp.Alter,
        exp.TruncateTable,
        exp.Create,
    )

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
            SQLSyntaxError: If the SQL is empty, malformed, or contains syntax errors.
            ForbiddenOperationError: If multiple semicolon-separated statements are detected.
        """
        if not sql or not sql.strip():
            raise SQLSyntaxError("SQL query is empty or blank.")

        clean_sql = sql.strip().rstrip(";")

        # Detect multiple statement injection (e.g., 'SELECT 1; DROP TABLE orders')
        try:
            parsed_statements = sqlglot.parse(clean_sql, read=self.dialect)
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

    def check_read_only(self, ast: exp.Expression) -> None:
        """
        Enforces strict read-only execution policies on the AST.

        Validates that:
        1. The root expression is a query (SELECT, UNION, etc.) or an EXPLAIN command.
        2. No DDL/DML mutation nodes (DROP, DELETE, UPDATE, INSERT, ALTER, etc.)
           exist anywhere in the AST hierarchy.

        Args:
            ast: The root sqlglot Expression node.

        Raises:
            ForbiddenOperationError: If any mutation or disallowed statement is found.
        """
        # Step 1: Validate root node intent
        is_query = isinstance(ast, (exp.Query, exp.Select))
        is_explain = (
            isinstance(ast, exp.Command)
            and str(ast.this).strip().upper().startswith("EXPLAIN")
        )

        if not (is_query or is_explain):
            root_op = ast.key.upper() if hasattr(ast, "key") else type(ast).__name__.upper()
            raise ForbiddenOperationError(
                f"Prohibited operation: '{root_op}' statements are forbidden.",
                details="Only read-only analytical queries (SELECT, EXPLAIN) are permitted on the Lakehouse."
            )

        # Step 2: Recursively scan the entire AST for mutation nodes
        for node in ast.walk():
            if isinstance(node, self.FORBIDDEN_MUTATION_NODES):
                op_name = node.key.upper() if hasattr(node, "key") else type(node).__name__.upper()
                raise ForbiddenOperationError(
                    f"Prohibited mutation detected: '{op_name}'.",
                    details=f"Write operations ({op_name}) are strictly forbidden on the Lakehouse."
                )

            # Block any raw commands that are not EXPLAIN
            if isinstance(node, exp.Command) and not str(node.this).strip().upper().startswith("EXPLAIN"):
                raise ForbiddenOperationError(
                    "Prohibited administrative command detected.",
                    details=f"Command '{node.this}' is not permitted."
                )

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
            full_name = table_node.sql(dialect=self.dialect)
            if full_name:
                tables.add(full_name.lower())
        return sorted(list(tables))

    def validate(self, sql: str) -> ValidationResult:
        """
        Validates raw SQL string against all security guardrails.
        
        Args:
            sql: Raw SQL string to validate.

        Returns:
            ValidationResult with validation status and extracted metadata.
        """
        try:
            ast = self.parse(sql)
            self.check_read_only(ast)
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
