from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# =========================
# Default Arguments
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

# =========================
# Definição da DAG
# =========================
with DAG(
    dag_id="automotivos_raw_silver_gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "hdfs", "medallion"],
) as dag:

    # =========================
    # Task: SparkSubmitOperator
    # =========================
    spark_raw_silver_gold = SparkSubmitOperator(
        task_id="spark_raw_silver_gold",
        application=(
            "/opt/airflow/spark-jobs/"
            "spark_raw_silver_gold_com_hdfs.py"
        ),
        conn_id="spark_default",
        verbose=True,
    )

    # =========================
    # Definição do fluxo
    # =========================
    # DAG com apenas uma task
    spark_raw_silver_gold
