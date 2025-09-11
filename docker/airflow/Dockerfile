FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      procps \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# (Optional) bake Python deps here instead of using _PIP_ADDITIONAL_REQUIREMENTS
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

