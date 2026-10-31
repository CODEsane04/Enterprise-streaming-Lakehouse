# ================================================================
# Enterprise Agentic Streaming Lakehouse - Service #13
# Lakehouse Streamlit Frontend Dashboard
# ================================================================

FROM python:3.12-slim

# System dependencies for curl healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8501

WORKDIR /app

# Install lightweight frontend dependencies
RUN pip install --no-cache-dir \
    streamlit>=1.35.0 \
    pandas>=2.2.0 \
    requests>=2.31.0

# Copy configuration and frontend application
COPY .streamlit/ /app/.streamlit/
COPY frontend/ /app/frontend/

# Expose Streamlit web port
EXPOSE 8501

# Streamlit native healthcheck
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Launch Streamlit server bound to 0.0.0.0
CMD ["streamlit", "run", "frontend/app.py", "--server.port=8501", "--server.address=0.0.0.0"]

