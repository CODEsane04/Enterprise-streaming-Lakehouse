#!/usr/bin/env python3
"""
scripts/test_trino_connection.py
================================================================
Enterprise Agentic Streaming Lakehouse - Engineer 4
Task 1.2: Validate Trino Python Connectivity

Connects to the Trino distributed query engine via the official
trino-python-client, checks catalog/schema availability, and executes
a test analytical query over Apache Iceberg tables.
================================================================
"""

import os
import sys
import argparse
from dotenv import load_dotenv

# Load unified environment variables
load_dotenv()

try:
    import trino
    from trino.dbapi import connect
    from trino.exceptions import TrinoConnectionError, TrinoUserError
except ImportError as e:
    print(f"❌ Missing required library: {e}")
    print("Please activate your virtual environment: source .venv/bin/activate")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate Trino Python Connectivity for Lakehouse"
    )
    # Default to host external port 8089 if run locally, or 8080 if inside docker
    default_host = os.getenv("TRINO_HOST_IP", "localhost")
    default_port = int(os.getenv("TRINO_EXTERNAL_PORT", "8089"))
    default_catalog = os.getenv("TRINO_CATALOG", "iceberg")
    default_schema = os.getenv("TRINO_SCHEMA", "ecommerce")
    default_user = os.getenv("TRINO_USER", "trino")

    parser.add_argument("--host", default=default_host, help=f"Trino host (default: {default_host})")
    parser.add_argument("--port", type=int, default=default_port, help=f"Trino port (default: {default_port})")
    parser.add_argument("--catalog", default=default_catalog, help=f"Iceberg catalog (default: {default_catalog})")
    parser.add_argument("--schema", default=default_schema, help=f"Namespace/schema (default: {default_schema})")
    parser.add_argument("--user", default=default_user, help=f"Trino user (default: {default_user})")
    return parser.parse_args()


def test_trino_connection(host: str, port: int, catalog: str, schema: str, user: str) -> bool:
    print("=" * 64)
    print("🚀 Lakehouse Trino Connectivity Test (Engineer 4, Task 1.2)")
    print("=" * 64)
    print(f"Target Endpoint : http://{host}:{port}")
    print(f"Catalog / Schema: {catalog}.{schema}")
    print(f"User            : {user}")
    print("-" * 64)

    try:
        conn = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            http_scheme="http",
            request_timeout=10.0,
        )
        cursor = conn.cursor()

        # Step 1: Engine Version Check
        print("🔍 [1/5] Verifying Trino Engine Version...")
        cursor.execute("SELECT version()")
        trino_version = cursor.fetchone()[0]
        print(f"   ✅ Connected to Trino v{trino_version}")

        # Step 2: Catalogs Discovery
        print("🔍 [2/5] Checking Available Catalogs...")
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
        print(f"   Catalogs found: {', '.join(catalogs)}")
        if catalog not in catalogs:
            print(f"   ❌ Catalog '{catalog}' not found in Trino! Expected Iceberg REST catalog.")
            return False
        print(f"   ✅ Iceberg catalog '{catalog}' is active.")

        # Step 3: Schemas in Catalog
        print(f"🔍 [3/5] Verifying Schemas in '{catalog}'...")
        cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = [row[0] for row in cursor.fetchall()]
        print(f"   Schemas found: {', '.join(schemas)}")
        if schema not in schemas:
            print(f"   ⚠️ Schema '{schema}' not yet registered in Polaris/Iceberg.")
            print(f"   (Run 'python3 scripts/bootstrap_polaris.py' or start Flink pipeline).")
            return False
        print(f"   ✅ Schema '{schema}' confirmed.")

        # Step 4: Tables Discovery
        print(f"🔍 [4/5] Discovering Tables in '{catalog}.{schema}'...")
        cursor.execute(f"SHOW TABLES FROM {catalog}.{schema}")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"   Tables available ({len(tables)}): {', '.join(tables)}")

        # Step 5: Test Analytical Query (SELECT count(*) FROM orders)
        target_table = "orders" if "orders" in tables else (tables[0] if tables else None)
        if target_table:
            print(f"🔍 [5/5] Executing Test Query on '{catalog}.{schema}.{target_table}'...")
            query = f"SELECT count(*) FROM {catalog}.{schema}.{target_table}"
            cursor.execute(query)
            row_count = cursor.fetchone()[0]
            print(f"   SQL  : {query}")
            print(f"   ✅ Result: {row_count} records returned successfully.")
        else:
            print("   ⚠️ No tables found in schema yet. Waiting for Flink streaming ingestion.")

        print("=" * 64)
        print("🎉 SUCCESS: Trino Python connectivity and Iceberg queries verified!")
        print("=" * 64)
        return True

    except TrinoConnectionError as e:
        print(f"\n❌ Connection Error: Could not reach Trino at http://{host}:{port}")
        print(f"   Details: {e}")
        print("\n💡 Troubleshooting Tips:")
        print("   1. Verify Docker containers are running: 'docker compose ps'")
        print("   2. If starting from scratch: 'docker compose up -d'")
        print("   3. On host machines, Trino coordinator is mapped to port 8089.")
        print("   4. Inside Docker network, Trino is reached at trino:8080.")
        return False
    except TrinoUserError as e:
        print(f"\n❌ Trino SQL Execution Error: {e.message}")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected Error: {type(e).__name__} - {e}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    args = parse_args()
    success = test_trino_connection(
        host=args.host,
        port=args.port,
        catalog=args.catalog,
        schema=args.schema,
        user=args.user,
    )
    sys.exit(0 if success else 1)

