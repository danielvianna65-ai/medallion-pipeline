from pyspark.sql import SparkSession

# =========================
# Paths HDFS
# =========================
RAW_INPUT = "hdfs://namenode:9000/data/input/precos_semestrais_automotivos_2025_01.csv"
RAW_HDFS = "hdfs://namenode:9000/user/airflow/raw/orders/automotivos"

# =========================
# Spark Session
# =========================
spark = (
    SparkSession.builder
    .appName("RAW-Ingest-Automotivos")
    .getOrCreate()
)

# =========================
# Ingestão RAW
# =========================
df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(RAW_INPUT)
)

# =========================
# Escrita no HDFS
# =========================
(
    df_raw
    .write
    .mode("overwrite")
    .csv(RAW_HDFS, header=True)
)

spark.stop()
