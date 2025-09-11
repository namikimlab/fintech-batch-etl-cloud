# Minimal image with Python 3.11, Java (for PySpark), and deps
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# Install Java (17) for Spark and basic tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    faker \
    pandas==2.2.2 \
    pyarrow==16.1.0

# Spark env (local mode is default)
ENV PYSPARK_PYTHON=python
ENV PYTHONUNBUFFERED=1

WORKDIR /app