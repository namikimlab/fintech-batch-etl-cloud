# Minimal image with Python 3.11, Java (for PySpark), and deps
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install Java for Spark and basic tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    faker \
    pandas \
    pyarrow

# Spark env (local mode is default)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PYSPARK_PYTHON=python
ENV PYTHONUNBUFFERED=1

WORKDIR /app