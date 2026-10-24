"""
src/mcp/config.py
================================================================
Enterprise Agentic Streaming Lakehouse - FastMCP Configuration
Loads environment variables and server settings with dual-mode
awareness (Local Host vs Docker Container).
================================================================
"""

import os
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

# Detect whether we are executing inside Docker or on the local host
IS_DOCKER = os.path.exists("/.dockerenv") or os.getenv("DOCKER_CONTAINER", "").lower() == "true"


class MCPSettings(BaseSettings):
    """Configuration settings for the FastMCP Lakehouse Server."""

    server_name: str = Field(default="Lakehouse-Analytics-Server", description="FastMCP Server Name")
    server_version: str = Field(default="1.0.0", description="Server Version")

    # Trino Connection Settings with dynamic Host/Docker resolution
    trino_host: str = Field(
        default_factory=lambda: (
            os.getenv("TRINO_HOST_IP")
            or ("trino" if IS_DOCKER else "localhost")
        )
    )
    trino_port: int = Field(
        default_factory=lambda: int(
            os.getenv("TRINO_PORT", "8080") if IS_DOCKER else os.getenv("TRINO_EXTERNAL_PORT", "8089")
        )
    )
    trino_user: str = Field(default_factory=lambda: os.getenv("TRINO_USER", "trino"))
    trino_catalog: str = Field(default_factory=lambda: os.getenv("TRINO_CATALOG", "iceberg"))
    trino_schema: str = Field(default_factory=lambda: os.getenv("TRINO_SCHEMA", "ecommerce"))

    # Security Settings
    default_limit: int = Field(default=100, description="Default row limit injected on unconstrained queries")
    max_limit: int = Field(default=1000, description="Maximum row limit clamped on large queries")

    model_config = {"env_prefix": "LAKEHOUSE_", "extra": "ignore"}


settings = MCPSettings()
