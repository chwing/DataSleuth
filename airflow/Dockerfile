FROM apache/airflow:2.8.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Install DuckDB and related packages
# Using versions compatible with Airflow 2.8.1 constraints
RUN pip install --no-cache-dir \
    duckdb==0.8.1 \
    requests

# Set working directory
WORKDIR /opt/airflow