FROM apache/airflow:2.6.3

# =========================
# ROOT
# =========================
USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}

# =========================
# Spark
# =========================
ENV SPARK_VERSION=3.4.3

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

# =========================
# AIRFLOW USER
# =========================
USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.1.0
