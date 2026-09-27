# ============================================================
# Enterprise Agentic Streaming Lakehouse - Apache Flink 1.20
# Engineer 2: Stream Processing, Data Quality & Iceberg Sink
# ============================================================
FROM flink:1.20-java17

USER root

# Install tools for downloading connector dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pinned connector and filesystem runtime dependencies:
# 1. flink-sql-connector-kafka (3.4.0-1.20): Kafka/Redpanda consumer & producer
# 2. iceberg-flink-runtime-1.20 (1.7.1): Apache Iceberg engine & table format
# 3. flink-shaded-hadoop-2-uber (2.8.3-10.0): Hadoop S3A filesystem provider
# 4. aws-java-sdk-bundle (1.12.648): AWS S3 SDK for MinIO S3 authentication
ENV KAFKA_JAR_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
ENV ICEBERG_JAR_URL=https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.1/iceberg-flink-runtime-1.20-1.7.1.jar
ENV HADOOP_JAR_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
ENV HADOOP_AWS_JAR_URL=https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.8.3/hadoop-aws-2.8.3.jar
ENV AWS_SDK_JAR_URL=https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar
ENV GUAVA_JAR_URL=https://repo1.maven.org/maven2/com/google/guava/guava/30.0-jre/guava-30.0-jre.jar

RUN wget -nv -P /opt/flink/lib/ "${KAFKA_JAR_URL}" && \
    wget -nv -P /opt/flink/lib/ "${ICEBERG_JAR_URL}" && \
    wget -nv -P /opt/flink/lib/ "${HADOOP_JAR_URL}" && \
    wget -nv -P /opt/flink/lib/ "${HADOOP_AWS_JAR_URL}" && \
    wget -nv -P /opt/flink/lib/ "${GUAVA_JAR_URL}" && \
    wget -nv -P /opt/flink/lib/ "${AWS_SDK_JAR_URL}" && \
    chown -R flink:flink /opt/flink/lib/

# Set working directory and revert to non-root flink user
WORKDIR /opt/flink
USER flink
