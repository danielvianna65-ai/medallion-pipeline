from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, year, month

RAW_HDFS = "hdfs://namenode:9000/user/airflow/raw/orders/automotivos"
SILVER_HDFS = "hdfs://namenode:9000/user/airflow/silver/automotivos"

spark = SparkSession.builder.appName("SILVER-Automotivos").getOrCreate()

df_raw = spark.read.option("header", "true").csv(RAW_HDFS)

df_silver = (
    df_raw
    .withColumnRenamed("Valor de Venda", "valor_venda")
    .withColumnRenamed("Valor de Compra", "valor_compra")
    .withColumnRenamed("Data da Coleta", "data_coleta")
    .withColumnRenamed("Regiao - Sigla", "regiao_sigla")
    .withColumn("valor_venda", regexp_replace(col("valor_venda"), ",", ".").cast("double"))
    .withColumn("valor_compra", regexp_replace(col("valor_compra"), ",", ".").cast("double"))
    .withColumn("data_coleta", to_date(col("data_coleta"), "dd/MM/yyyy"))
    .withColumn("year", year(col("data_coleta")))
    .withColumn("month", month(col("data_coleta")))
    .dropDuplicates()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(SILVER_HDFS)
)

spark.stop()
