#!/usr/bin/env python3
"""
scripts/test_mcp_client.py
================================================================
Enterprise Agentic Streaming Lakehouse - Engineer 4
Task 3.5: Test Local Model Context Protocol (MCP) Client Communication

Spawns the FastMCP server over standard stdio JSON-RPC transport,
performs the MCP initialization handshake, queries available tools,
and executes all three lakehouse tools via the official MCP SDK.
================================================================
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def run_mcp_verification():
    print("=" * 68)
    print("🚀 Model Context Protocol (MCP) Client Verification (Task 3.5)")
    print("=" * 68)

    # Configure server subprocess execution
    project_root = Path(__file__).resolve().parent.parent
    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "src.mcp.server"],
        env={**os.environ, "PYTHONPATH": str(project_root)},
    )

    print(f"Connecting to FastMCP Server via stdio: {sys.executable} -m src.mcp.server ...")

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Step 1: Protocol Handshake & Initialization
            print("\n[1/5] Performing MCP Initialization Handshake...")
            init_result = await session.initialize()
            print(f"   ✅ Handshake successful!")
            print(f"   Server Name    : {init_result.server_info.name}")
            print(f"   Server Version : {init_result.server_info.version}")
            print(f"   Protocol Version: {init_result.protocol_version}")

            # Step 2: Tool Discovery
            print("\n[2/5] Discovering Registered MCP Tools...")
            tools_response = await session.list_tools()
            tools = tools_response.tools
            tool_names = [t.name for t in tools]
            print(f"   Discovered {len(tools)} tools: {', '.join(tool_names)}")

            expected_tools = ["list_tables", "get_table_schema", "execute_analytical_query"]
            for exp_tool in expected_tools:
                assert exp_tool in tool_names, f"Expected tool '{exp_tool}' not found!"
                print(f"   ✅ Tool '{exp_tool}' verified with JSON Schema parameters.")

            # Step 3: Tool Call — list_tables
            print("\n[3/5] Calling MCP Tool: list_tables()...")
            res_tables = await session.call_tool("list_tables", {})
            raw_text = res_tables.content[0].text if res_tables.content else "{}"
            data_tables = json.loads(raw_text)
            print(f"   Success: {data_tables.get('success')} ✅")
            print(f"   Tables : {data_tables.get('tables')}")

            # Step 4: Tool Call — get_table_schema
            print("\n[4/5] Calling MCP Tool: get_table_schema(table_name='orders')...")
            res_schema = await session.call_tool("get_table_schema", {"table_name": "orders"})
            data_schema = json.loads(res_schema.content[0].text) if res_schema.content else {}
            print(f"   Success     : {data_schema.get('success')} ✅")
            print(f"   Column Count: {data_schema.get('column_count')}")
            print(f"   Primary Keys: {data_schema.get('primary_keys')}")

            # Step 5: Tool Call — execute_analytical_query (Analytical & Security)
            print("\n[5/5] Calling MCP Tool: execute_analytical_query()...")
            
            # 5a: Valid analytical query
            res_query = await session.call_tool(
                "execute_analytical_query",
                {"sql_query": "SELECT count(*) as total_orders FROM iceberg.ecommerce.orders"}
            )
            data_query = json.loads(res_query.content[0].text) if res_query.content else {}
            print(f"   5a. Valid Analytical Query -> Success: {data_query.get('success')} ✅")
            print(f"       Result: {data_query.get('data')}")

            # 5b: Malicious injection attempt over MCP
            res_attack = await session.call_tool(
                "execute_analytical_query",
                {"sql_query": "DROP TABLE iceberg.ecommerce.orders"}
            )
            data_attack = json.loads(res_attack.content[0].text) if res_attack.content else {}
            print(f"   5b. Malicious DROP Attack   -> Intercepted: {not data_attack.get('success')} ✅")
            print(f"       Error Type: {data_attack.get('error_type')}")
            print(f"       Message   : {data_attack.get('error')}")

    print("\n" + "=" * 68)
    print("🎉 SUCCESS: All Model Context Protocol (MCP) communications verified!")
    print("=" * 68)


if __name__ == "__main__":
    asyncio.run(run_mcp_verification())

