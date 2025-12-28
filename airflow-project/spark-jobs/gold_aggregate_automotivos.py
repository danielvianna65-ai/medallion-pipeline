from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    avg,
    count,
    concat_ws,
    monotonically_increasing_id,
    expr,
)

# =========================================================
# Paths HDFS (PADRÃO POR DOMÍNIO)
# =========================================================
SILVER_PATH = "hdfs://namenode:9000/user/airflow/silver/automotivos"
GOLD_BASE_PATH = "hdfs://namenode:9000/user/airflow/gold/automotivos"

DIM_TEMPO_MES_PATH = f"{GOLD_BASE_PATH}/dim_tempo_mes"
DIM_PRODUTO_PATH = f"{GOLD_BASE_PATH}/dim_produto"
DIM_LOCALIDADE_PATH = f"{GOLD_BASE_PATH}/dim_localidade"
DIM_REVENDA_PATH = f"{GOLD_BASE_PATH}/dim_revenda"
FACT_PATH = f"{GOLD_BASE_PATH}/fact_preco_combustivel_mes"

# =========================================================
# Spark Session
# =========================================================
spark = (
    SparkSession.builder
    .appName("GOLD-Star-Schema-Mensal-Combustiveis")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Leitura SILVER + Data Quality (CRÍTICO)
# =========================================================
df_silver = (
    spark.read.parquet(SILVER_PATH)
    .withColumnRenamed("regiao_sigla", "regiao")
    .withColumnRenamed("Estado - Sigla", "estado")
    .withColumnRenamed("Municipio", "municipio")
    .withColumnRenamed("Produto", "produto")
    .withColumnRenamed("Revenda", "revenda")
    .withColumnRenamed("Bandeira", "bandeira")
)


# =========================================================
# DIM_TEMPO_MES (MENSAL)
# =========================================================
dim_tempo_mes = (
    df_silver
    .select(
        year("data_coleta").alias("ano"),
        month("data_coleta").alias("mes")
    )
    .distinct()
    .withColumn("trimestre", expr("ceil(mes / 3)"))
    .withColumn("ano_mes", concat_ws("-", col("ano"), col("mes")))
    .withColumn("id_tempo_mes", monotonically_increasing_id())
)

dim_tempo_mes.write.mode("overwrite").parquet(DIM_TEMPO_MES_PATH)

# =========================================================
# DIM_PRODUTO
# =========================================================
dim_produto = (
    df_silver
    .select("produto")
    .distinct()
    .withColumn("id_produto", monotonically_increasing_id())
)

dim_produto.write.mode("overwrite").parquet(DIM_PRODUTO_PATH)


# =========================================================
# DIM_LOCALIDADE
# =========================================================
dim_localidade = (
    df_silver
    .select("regiao", "estado", "municipio")
    .distinct()
    .withColumn("id_localidade", monotonically_increasing_id())
)

dim_localidade.write.mode("overwrite").parquet(DIM_LOCALIDADE_PATH)


# =========================================================
# DIM_REVENDA
# =========================================================
dim_revenda = (
    df_silver
    .select("revenda", "bandeira")
    .distinct()
    .withColumn("id_revenda", monotonically_increasing_id())
)

dim_revenda.write.mode("overwrite").parquet(DIM_REVENDA_PATH)

# =========================================================
# FATO MENSAL (GRÃO EXPLÍCITO)
# =========================================================
fact_mes = (
    df_silver
    .withColumn("ano", year("data_coleta"))
    .withColumn("mes", month("data_coleta"))
    .join(dim_tempo_mes, ["ano", "mes"], "inner")
    .join(dim_produto, ["produto"], "inner")
    .join(dim_localidade, ["regiao", "estado", "municipio"], "inner")
    .join(dim_revenda, ["revenda", "bandeira"], "inner")
    .groupBy(
        "id_tempo_mes",
        "id_produto",
        "id_localidade",
        "id_revenda"
    )
    .agg(
        avg("valor_venda").alias("preco_venda_medio"),
        count("*").alias("quantidade_registros")
    )
    .withColumn("id_fato", monotonically_increasing_id())
)

fact_mes.write.mode("overwrite").parquet(FACT_PATH)


spark.stop()
