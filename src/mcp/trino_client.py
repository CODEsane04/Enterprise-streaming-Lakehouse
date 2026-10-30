"""
src/mcp/trino_client.py
================================================================
Enterprise Agentic Streaming Lakehouse - Trino Client Layer
Handles low-level Trino connections, schema reflection, and queries.
================================================================
"""

import time
from typing import Dict, Any, List, Optional
import trino
from trino.dbapi import connect
from trino.exceptions import TrinoConnectionError, TrinoUserError

from .config import settings


class TrinoClient:
    """Manages connections and executions against the Trino query engine."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        self.host = host or settings.trino_host
        self.port = port or settings.trino_port
        self.user = user or settings.trino_user
        self.catalog = catalog or settings.trino_catalog
        self.schema = schema or settings.trino_schema

    def get_connection(self):
        """Creates and returns an active connection to Trino."""
        return connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme="http",
            request_timeout=30.0,
        )

    def execute_query(self, sql: str) -> Dict[str, Any]:
        """
        Executes a validated SQL statement on Trino and returns tabular data.
        
        Args:
            sql: The sanitized and validated SQL query.

        Returns:
            Dict containing column metadata, row records, row_count, and execution_time_ms.
        """
        start_time = time.time()
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            
            # Fetch column descriptors
            columns = [col[0] for col in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            elapsed_ms = round((time.time() - start_time) * 1000, 2)

            # Format as list of dictionaries
            records = [dict(zip(columns, row)) for row in rows]

            return {
                "success": True,
                "columns": columns,
                "row_count": len(records),
                "execution_time_ms": elapsed_ms,
                "data": records,
            }
        except (TrinoConnectionError, TrinoUserError) as e:
            return {
                "success": False,
                "error": str(e),
                "execution_time_ms": round((time.time() - start_time) * 1000, 2),
            }
        finally:
            conn.close()

    def list_tables(self) -> List[str]:
        """Returns the list of tables available in the current catalog and schema."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES FROM {self.catalog}.{self.schema}")
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Returns column definitions and types for a specified table."""
        conn = self.get_connection()
        clean_table = table_name.strip().strip("'\"`").lower().split(".")[-1]
        try:
            cursor = conn.cursor()
            query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_catalog = '{self.catalog}'
              AND table_schema = '{self.schema}'
              AND table_name = '{clean_table}'
            ORDER BY ordinal_position
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                # Fallback to DESCRIBE if information_schema returned empty
                try:
                    cursor.execute(f"DESCRIBE {self.catalog}.{self.schema}.{clean_table}")
                    desc_rows = cursor.fetchall()
                    return [
                        {"column_name": row[0], "data_type": row[1], "is_nullable": "YES"}
                        for row in desc_rows
                    ]
                except Exception:
                    return []
            return [
                {"column_name": row[0], "data_type": row[1], "is_nullable": row[2]}
                for row in rows
            ]
        finally:
            conn.close()

