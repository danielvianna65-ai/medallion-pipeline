# Medallion Data Pipeline — Airflow, Spark & HDFS

Pipeline de **Engenharia de Dados** baseado na arquitetura **Medallion (RAW → SILVER → GOLD)**, utilizando **Apache Airflow** para orquestração, **Apache Spark (Standalone)** para processamento distribuído e **HDFS** como camada de armazenamento.

O objetivo do projeto é demonstrar, de forma prática, competências essenciais para a função de **Data Engineer**, incluindo ingestão, transformação, orquestração, governança básica de dados e execução em ambiente containerizado.

---

## Visão Geral da Arquitetura

- **Orquestração**: Apache Airflow  
- **Processamento**: Apache Spark (Standalone Cluster)  
- **Armazenamento**: Hadoop HDFS  
- **Infraestrutura**: Docker Compose  

**Fluxo de dados:**

Fonte CSV → RAW (HDFS) → SILVER (Parquet) → GOLD (Agregações Analíticas)

---

## Estrutura do Repositório

```bash
Medallion-pipeline/
└── airflow-project/
    ├── dags/
    │   └── automotivos_raw_silver_gold_dag.py
    ├── logs/
    │   ├── dag_id=automotivos_raw_silver_gold
    │   ├── dag_id=automotivos_raw_silver_gold_bash
    │   ├── dag_processor_manager
    │   └── scheduler
    ├── spark-jobs/
    │   ├── dados/
    │   │   └── precos_semestrais_automotivos_2025_01.csv
    │   └── spark_raw_silver_gold_com_hdfs.py
    ├── docker-compose.yml
    └── Dockerfile
 ```
---
## Camadas de Dados (Medallion Architecture)
### RAW
- Ingestão direta de dados CSV no HDFS
- Dados preservados no formato original
- Sem tratamento ou validação

### SILVER
- Limpeza e padronização dos dados
- Conversão de tipos (datas, valores numéricos)
- Escrita em formato Parquet
- Dados prontos para uso analítico

### GOLD
- Agregações e métricas consolidadas
- Organização para consumo analítico
- Foco em performance e clareza semântica
- ---

## Orquestração com Airflow
- DAG única responsável por todo o fluxo RAW → SILVER → GOLD
- Execução via SparkSubmitOperator
- Separação clara entre lógica de orquestração (DAG) e lógica de processamento (Spark Job)
- Logs centralizados para auditoria e troubleshooting

---
## Execução do Projeto
### Subir o ambiente
```bash
docker compose up -d
```
### Airflow Web UI
- URL: http://localhost:8080
- Usuário: admin
- Senha: admin

### Execução da DAG
- DAG: automotivos_raw_silver_gold
- Execução manual via interface do Airflow

### Dataset
O dataset bruto utilizado neste projeto não é versionado no repositório devido a restrições de tamanho.

Espera-se que ele esteja disponível localmente no seguinte caminho:
```bash
airflow-project/spark-jobs/dados/
```
Fonte: ANP – Preços de Combustíveis (Brasil)

### Considerações Técnicas Relevantes
- Spark executando em modo Standalone, simulando ambiente distribuído
- Conexão spark_default configurada para:
```bash
spark://spark-master:7077
```
- Persistência de dados no HDFS com controle de permissões
- Diretórios criados explicitamente para evitar AccessControlException
- Spark executando em modo Standalone (não YARN)
- Uso de Parquet para otimização de leitura e escrita
- Ambiente totalmente reproduzível via Docker
- ---

### Tecnologias Utilizadas
Apache Airflow 2.x

Apache Spark 3.4.x (PySpark)

Hadoop HDFS

Docker & Docker Compose

Python

---

#### Autor 
Daniel Lima Viana