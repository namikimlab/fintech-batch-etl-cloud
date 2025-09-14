FROM apache/airflow:2.10.2-python3.12

# --- OS deps (Spark JRE + psycopg2 build tools) ---
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      gcc \
      build-essential \
      libpq-dev \
      pkg-config \
      procps \
    && rm -rf /var/lib/apt/lists/*

# --- Back to airflow user ---
USER airflow

# --- Airflow constraints: match Airflow 2.10.2 + Python 3.12 ---
ARG PYVER=3.12
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-${PYVER}.txt"

# --- Python deps install with constraints (권장 방식) ---
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt -c "${CONSTRAINTS_URL}"
