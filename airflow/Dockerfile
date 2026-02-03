FROM apache/airflow:2.10.4-python3.10

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

# Install DuckDB and other packages
RUN pip install --no-cache-dir \
    duckdb==0.9.2 \
    requests \
    pyarrow

# Set working directory
WORKDIR /opt/airflow
