from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_replace,
    to_date,
    avg,
    when,
    year,
    month
)
from pyspark.sql.types import DecimalType

# =========================
# HDFS Paths (Medallion)
# =========================
RAW_PATH = "hdfs://namenode:9000/user/airflow/raw/orders/precos_semestrais_automotivos_2025_01.csv"
SILVER_PATH = "hdfs://namenode:9000/user/airflow/silver/automotivos"
GOLD_PATH = "hdfs://namenode:9000/user/airflow/gold/automotivos"

# =========================
# Spark Session
# =========================
spark = (
    SparkSession.builder
    .appName("Automotivos-Medallion-Pipeline")
    .getOrCreate()
)

# =========================
# BRONZE - RAW ingestion
# =========================
df_bronze = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv(RAW_PATH)
)

print("Schema BRONZE:")
df_bronze.printSchema()

# =========================
# SILVER - Data cleaning
# =========================
df_silver = (
    df_bronze
    .withColumnRenamed("Valor de Venda", "valor_venda")
    .withColumnRenamed("Valor de Compra", "valor_compra")
    .withColumnRenamed("Data da Coleta", "data_coleta")
    .withColumnRenamed("Regiao - Sigla", "regiao_sigla")
)

# Correção de decimal brasileiro + tipagem
df_silver = (
    df_silver
    .withColumn(
        "valor_venda",
        regexp_replace(col("valor_venda"), ",", ".").cast("double")
    )
    .withColumn(
        "valor_compra",
        regexp_replace(col("valor_compra"), ",", ".").cast("double")
    )
    .withColumn(
        "data_coleta",
        to_date(col("data_coleta"), "dd/MM/yyyy")
    )
    .withColumn("year", year(col("data_coleta")))
    .withColumn("month", month(col("data_coleta")))
    .dropDuplicates()
)

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(SILVER_PATH)
)
# =========================
# GOLD - Analytical layer
# =========================
df_gold = (
    df_silver
    .filter(col("regiao_sigla").isNotNull())
    .groupBy("Produto", "regiao_sigla", "year", "month")
    .agg(
        avg(col("valor_venda"))
        .cast(DecimalType(10, 2))
        .alias("preco_medio_venda")
    )
)
df_gold = (
    df_gold
    .withColumn(
        "ordem_regiao",
        when(col("regiao_sigla") == "N", 1)
        .when(col("regiao_sigla") == "NE", 2)
        .when(col("regiao_sigla") == "CO", 3)
        .when(col("regiao_sigla") == "SE", 4)
        .when(col("regiao_sigla") == "S", 5)
        .otherwise(99)
    )
    .orderBy("ordem_regiao", "Produto")
    .drop("ordem_regiao")
)

(
    df_gold
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(GOLD_PATH)
)

