FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
boto3

# Download PostgreSQL JDBC driver
RUN wget -P /opt/spark/jars/ \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 /opt/spark/jars/*.jar

RUN mkdir -p /tmp/spark-checkpoints && \
    chmod 777 /tmp/spark-checkpoints

WORKDIR /app
COPY spark_streaming.py .
COPY spark_batch.py .
COPY entrypoint.sh .

RUN chmod +x /app/spark_streaming.py /app/spark_batch.py /app/entrypoint.sh

USER spark

CMD ["/app/entrypoint.sh"]
