# Databricks notebook source
# MAGIC %md
# MAGIC # Source ingestion (replaces the Talend `Load_*` jobs)
# MAGIC
# MAGIC PySpark port of `talend_jobs/Load_DimBranch`, `Load_DimAccount`,
# MAGIC `Load_DimCustomer` and `Load_FactTransaction`:
# MAGIC
# MAGIC * dimensions are read from the operational relational source over JDBC
# MAGIC   (`DimCustomer` joins `customer` + `city` + `state`, uppercasing text fields);
# MAGIC * the fact table unions the relational, CSV and Excel transaction streams
# MAGIC   (Talend `tUnite`) and deduplicates on `transaction_id` (Talend `tUniqRow`);
# MAGIC * everything is written to Delta with `MERGE`-free overwrite semantics.
# MAGIC
# MAGIC Reading `.xlsx` requires the `com.crealytics:spark-excel` library on the cluster;
# MAGIC set the `excel_path` widget to an empty string to skip that source.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dwh", "Schema")
dbutils.widgets.text("jdbc_url", "", "JDBC URL of the operational source")
dbutils.widgets.text("jdbc_user", "", "JDBC user")
dbutils.widgets.text("jdbc_password_scope", "", "Secret scope holding the JDBC password")
dbutils.widgets.text("jdbc_password_key", "", "Secret key holding the JDBC password")
dbutils.widgets.text("csv_path", "dbfs:/FileStore/banking/transaction_csv.csv", "CSV source")
dbutils.widgets.text("excel_path", "dbfs:/FileStore/banking/transaction_excel.xlsx", "Excel source")

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession, functions as F, Window

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
JDBC_URL = dbutils.widgets.get("jdbc_url")
CSV_PATH = dbutils.widgets.get("csv_path")
EXCEL_PATH = dbutils.widgets.get("excel_path")

TRANSACTION_TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"


def read_source_table(spark: SparkSession, table: str) -> DataFrame:
    password = dbutils.secrets.get(
        scope=dbutils.widgets.get("jdbc_password_scope"),
        key=dbutils.widgets.get("jdbc_password_key"),
    )
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", dbutils.widgets.get("jdbc_user"))
        .option("password", password)
        .load()
    )


def write_delta(df: DataFrame, table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{table}")
    )


def normalize_transactions(df: DataFrame) -> DataFrame:
    """Align a raw transaction stream with the FactTransaction schema."""
    transaction_date = F.col("transaction_date")
    if dict(df.dtypes)["transaction_date"] == "string":
        transaction_date = F.to_timestamp(transaction_date, TRANSACTION_TIMESTAMP_FORMAT)

    return df.select(
        F.col("transaction_id").cast("int").alias("TransactionID"),
        F.col("account_id").cast("int").alias("AccountID"),
        transaction_date.cast("timestamp").alias("TransactionDate"),
        F.col("amount").cast("decimal(19,4)").alias("Amount"),
        F.col("transaction_type").cast("string").alias("TransactionType"),
        F.col("branch_id").cast("int").alias("BranchID"),
    )

# COMMAND ----------

# MAGIC %md ## Dimensions

# COMMAND ----------

branch = read_source_table(spark, "branch").select(
    F.col("branch_id").cast("int").alias("BranchID"),
    F.col("branch_name").cast("string").alias("BranchName"),
    F.col("branch_location").cast("string").alias("BranchLocation"),
)
write_delta(branch, "DimBranch")

# COMMAND ----------

account = read_source_table(spark, "account").select(
    F.col("account_id").cast("int").alias("AccountID"),
    F.col("customer_id").cast("int").alias("CustomerID"),
    F.col("account_type").cast("string").alias("AccountType"),
    F.col("balance").cast("decimal(19,4)").alias("Balance"),
    F.col("date_opened").cast("date").alias("DateOpened"),
    F.col("status").cast("string").alias("Status"),
)
write_delta(account, "DimAccount")

# COMMAND ----------

customer_src = read_source_table(spark, "customer").alias("cu")
city = read_source_table(spark, "city").alias("ci")
state = read_source_table(spark, "state").alias("st")

customer = (
    customer_src.join(city, F.col("cu.city_id") == F.col("ci.city_id"), "left")
    .join(state, F.col("ci.state_id") == F.col("st.state_id"), "left")
    .select(
        F.col("cu.customer_id").cast("int").alias("CustomerID"),
        F.upper(F.col("cu.customer_name")).alias("CustomerName"),
        F.upper(F.col("cu.address")).alias("Address"),
        F.upper(F.col("ci.city_name")).alias("CityName"),
        F.upper(F.col("st.state_name")).alias("StateName"),
        F.col("cu.age").cast("int").alias("Age"),
        F.col("cu.gender").cast("string").alias("Gender"),
        F.col("cu.email").cast("string").alias("Email"),
    )
)
write_delta(customer, "DimCustomer")

# COMMAND ----------

# MAGIC %md ## Fact — union of the three transaction sources, deduplicated

# COMMAND ----------

streams = [normalize_transactions(read_source_table(spark, "transaction"))]

if CSV_PATH:
    streams.append(
        normalize_transactions(
            spark.read.option("header", "true").csv(CSV_PATH)
        )
    )

if EXCEL_PATH:
    streams.append(
        normalize_transactions(
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(EXCEL_PATH)
        )
    )

unioned = streams[0]
for stream in streams[1:]:
    unioned = unioned.unionByName(stream)

dedup_window = Window.partitionBy("TransactionID").orderBy(F.col("TransactionDate").desc())
fact = (
    unioned.withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
write_delta(fact, "FactTransaction")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.FactTransaction").orderBy("TransactionID"))
