FROM apache/airflow:2.11.0-python3.10

# Environment variables
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.651
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER root

# Install Java 17 and utilities
RUN apt-get update && \
    apt-get install -y \
    openjdk-17-jre-headless \
    wget \
    procps \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Apache Spark
RUN echo "Downloading Spark ${SPARK_VERSION}..." && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    echo "Extracting Spark..." && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    echo "Setting permissions..." && \
    chown -R airflow:root ${SPARK_HOME} && \
    echo "Spark installation complete!"

# Download S3A JARs
RUN echo "Downloading S3A JARs..." && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    -P ${SPARK_HOME}/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -P ${SPARK_HOME}/jars/ && \
    echo "S3A JARs downloaded!"

# Verify installations
RUN echo "Java version:" && java -version && \
    echo "Spark location:" && ls -la ${SPARK_HOME}/bin/spark-submit

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon \
    apache-airflow-providers-snowflake

