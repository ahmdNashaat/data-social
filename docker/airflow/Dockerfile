# ══════════════════════════════════════════════════════════════════════════════
# Airflow Custom Image
# Base: apache/airflow:2.8.1-python3.11
# Adds: PySpark, dbt, Great Expectations, boto3, kafka-python
# ══════════════════════════════════════════════════════════════════════════════
FROM apache/airflow:2.8.1-python3.11

USER root

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark binaries for spark-submit from Airflow
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3
RUN curl -fsSL \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    apache-airflow-providers-amazon==8.14.0 \
    apache-airflow-providers-redis==3.6.1 \
    pyspark==3.5.0 \
    dbt-core==1.7.4 \
    dbt-duckdb==1.7.1 \
    great-expectations==0.18.8 \
    kafka-python==2.0.2 \
    boto3==1.34.0 \
    minio==7.2.3 \
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    duckdb==0.9.2 \
    faker==22.2.0 \
    python-dotenv==1.0.0 \
    loguru==0.7.2 \
    tenacity==8.2.3
