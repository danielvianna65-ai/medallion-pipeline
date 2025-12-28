from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, year, month
from pyspark.sql.types import DecimalType

# =========================================================
# Paths HDFS
# =========================================================
RAW_HDFS = "hdfs://namenode:9000/user/airflow/raw/orders/automotivos"
SILVER_HDFS = "hdfs://namenode:9000/user/airflow/silver/automotivos"

# =========================================================
# Spark Session
# =========================================================
spark = (
    SparkSession.builder
    .appName("SILVER-Automotivos")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Leitura RAW
# =========================================================
df_raw = (
    spark.read
    .option("header", "true")
    .csv(RAW_HDFS)
)

# =========================================================
# Limpeza, tipagem e normalização
# =========================================================
df_silver = (
    df_raw
    # Remove registros sem data válida
    .filter(col("Data da Coleta").isNotNull())

    # Normalização de nomes
    .withColumnRenamed("Valor de Venda", "valor_venda")
    .withColumnRenamed("Data da Coleta", "data_coleta")
    .withColumnRenamed("Regiao - Sigla", "regiao")
    .withColumnRenamed("Estado - Sigla", "estado")
    .withColumnRenamed("Municipio", "municipio")
    .withColumnRenamed("Produto", "produto")
    .withColumnRenamed("Revenda", "revenda")
    .withColumnRenamed("Bandeira", "bandeira")

    # Tipagem
    .withColumn(
        "valor_venda",
        regexp_replace(col("valor_venda"), ",", ".")
        .cast(DecimalType(10, 2))
    )
    .withColumn("data_coleta", to_date(col("data_coleta"), "dd/MM/yyyy"))

    # Partições temporais
    .withColumn("year", year(col("data_coleta")))
    .withColumn("month", month(col("data_coleta")))

    # Remove duplicados
    .dropDuplicates()
)

# =========================================================
# Escrita SILVER
# =========================================================
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(SILVER_HDFS)
)

spark.stop()
