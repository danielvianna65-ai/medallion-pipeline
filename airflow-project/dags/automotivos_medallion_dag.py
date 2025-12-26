from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# =========================
# Default args
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# =========================
# DAG definition
# =========================
with DAG(
    dag_id="automotivos_raw_silver_gold",
    default_args=default_args,
    description="Pipeline Medallion RAW → SILVER → GOLD (Spark + HDFS)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # execução sob demanda
    catchup=False,
    tags=["spark", "hdfs", "medallion", "portfolio"],
) as dag:

    # =========================
    # RAW → ingestão
    # =========================
    raw_ingest = SparkSubmitOperator(
        task_id="raw_ingest_hdfs",
        application="/opt/airflow/spark-jobs/raw_ingest_hdfs.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
        },
    )

    # =========================
    # SILVER → limpeza e tipagem
    # =========================
    silver_transform = SparkSubmitOperator(
        task_id="silver_transform_automotivos",
        application="/opt/airflow/spark-jobs/silver_transform_automotivos.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        },
    )

    # =========================
    # GOLD → agregações
    # =========================
    gold_aggregate = SparkSubmitOperator(
        task_id="gold_aggregate_automotivos",
        application="/opt/airflow/spark-jobs/gold_aggregate_automotivos.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        },
    )

    # =========================
    # Orquestração
    # =========================
    raw_ingest >> silver_transform >> gold_aggregate
