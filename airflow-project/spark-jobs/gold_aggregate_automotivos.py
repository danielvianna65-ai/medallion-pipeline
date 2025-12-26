from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.sql.types import DecimalType

SILVER_HDFS = "hdfs://namenode:9000/user/airflow/silver/automotivos"
GOLD_HDFS = "hdfs://namenode:9000/user/airflow/gold/automotivos"

spark = SparkSession.builder.appName("GOLD-Automotivos").getOrCreate()

df_silver = spark.read.parquet(SILVER_HDFS)

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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_gold
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(GOLD_HDFS)
)

spark.stop()
