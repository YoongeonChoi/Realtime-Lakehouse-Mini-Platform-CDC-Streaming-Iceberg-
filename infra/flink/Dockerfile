FROM flink:1.20.1-scala_2.12-java17

ARG FLINK_KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG FLINK_VERSION=1.20.1
ARG ICEBERG_VERSION=1.7.1
ARG HADOOP_VERSION=3.3.6

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar \
      -o /opt/flink/lib/flink-sql-connector-kafka.jar \
    && curl -fsSL \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar \
      -o /opt/flink/lib/flink-sql-avro-confluent-registry.jar \
    && curl -fsSL \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
      -o /opt/flink/lib/iceberg-flink-runtime.jar \
    && curl -fsSL \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      -o /opt/flink/lib/iceberg-aws-bundle.jar \
    && curl -fsSL \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar \
      -o /opt/flink/lib/hadoop-client-api.jar \
    && curl -fsSL \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar \
      -o /opt/flink/lib/hadoop-client-runtime.jar \
    && mkdir -p /opt/flink/checkpoints /opt/flink/savepoints /opt/flink/state

COPY conf/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
