"""
src/security/exceptions.py
================================================================
Enterprise Agentic Streaming Lakehouse - Security Exceptions
Custom exceptions for SQL AST parsing and policy violations.
================================================================
"""


class SQLSecurityError(Exception):
    """Base exception for all SQL security and validation failures."""

    def __init__(self, message: str, details: str = ""):
        super().__init__(message)
        self.message = message
        self.details = details

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} ({self.details})"
        return self.message


class SQLSyntaxError(SQLSecurityError):
    """Raised when an incoming SQL string cannot be parsed into a valid AST."""
    pass


class ForbiddenOperationError(SQLSecurityError):
    """Raised when a query attempts DDL/DML mutations or prohibited statements."""
    pass


class ResourceLimitError(SQLSecurityError):
    """Raised when a query violates row limiting or resource governance policies."""
    pass


class InvalidNamespaceError(SQLSecurityError):
    """Raised when a query references tables outside the allowlisted schemas."""
    pass

