# ================================================================
# Enterprise Agentic Streaming Lakehouse - Service #12
# Lakehouse AI & Security Service (FastAPI + FastMCP + LangGraph)
# ================================================================

FROM python:3.12-slim AS base

# System dependencies for compiling and healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set environment paths and disable buffer
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PORT=8000 \
    DOCKER_CONTAINER=true

WORKDIR /app

# Copy dependency requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and benchmarks
COPY src/ /app/src/
COPY benchmarks/ /app/benchmarks/

# Expose HTTP API & MCP SSE serving port
EXPOSE 8000

# Container healthcheck testing the live health endpoint
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Launch ASGI server hosting the multi-agent API with auto-reload
CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

