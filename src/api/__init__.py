"""
Lakehouse Multi-Agent AI API Package.
Exposes the LangGraph multi-agent pipeline and Trino tools via FastAPI.
"""

from src.api.app import app

__all__ = ["app"]

