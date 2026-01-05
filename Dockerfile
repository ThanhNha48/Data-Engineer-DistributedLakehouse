FROM apache/airflow:2.10.0-python3.10

USER root

# ===============================
# System dependencies
# ===============================
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jre-headless \
        curl \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ===============================
# Java
# ===============================
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ===============================
# Spark
# ===============================
# ===============================
# Spark
# ===============================
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -fL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz && \
    mkdir -p ${SPARK_HOME} && \
    tar -xzf /tmp/spark.tgz --strip-components=1 -C ${SPARK_HOME} && \
    rm /tmp/spark.tgz

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# ===============================
# Spark JARs (Iceberg + Nessie + S3)
# ===============================
RUN mkdir -p $SPARK_HOME/jars && \
    cd $SPARK_HOME/jars && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.766/aws-java-sdk-bundle-1.12.766.jar


# ===============================
# Python
# ===============================
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
