# ══════════════════════════════════════════════════════════════════════════════
# Spark Custom Image
# Base: spark:3.5.0
# Adds: S3A connector (MinIO), Kafka connector, Delta Lake, Python packages
# ══════════════════════════════════════════════════════════════════════════════
FROM spark:3.5.0

USER root

# Install Python packages for PySpark jobs
RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    duckdb==0.9.2 \
    great-expectations==0.18.8 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.0 \
    loguru==0.7.2 \
    tenacity==8.2.3 \
    pydantic==2.5.3

# Download required JARs for S3A (MinIO) and Kafka
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3.3.4
ARG SCALA_VERSION=2.12

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar" \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    -o /opt/spark/jars/spark-sql-kafka-0-10.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar" \
    -o /opt/spark/jars/kafka-clients-3.5.1.jar

# Spark config for S3A / MinIO
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER 1001
