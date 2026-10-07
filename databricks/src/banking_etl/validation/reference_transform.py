"""Minimal source -> gold reference transform used only to drive the harness live.

The real bronze/silver/gold pipelines are owned by tickets 3-8 and run in parallel, so the
harness cannot depend on them. This module reproduces the four Talend jobs in a few lines of
PySpark (same rules as ``legacy_baseline``) and writes the gold tables into the ticket-scoped
schema. Once the pipeline tickets land, point the harness at ``banking_mig_gold`` instead.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from banking_etl.validation import specs

CSV_DATE_FORMAT = "dd-MM-yyyy HH:mm:ss"
SOURCE_PRIORITY = {"sqlserver": 1, "excel": 2, "csv": 3}  # Talend tUnite merge order


def _project(df: DataFrame, spec: specs.TableSpec) -> DataFrame:
    return df.select([F.col(c.gold).cast(c.dtype).alias(c.gold) for c in spec.columns])


def dim_branch(src: dict[str, DataFrame]) -> DataFrame:
    return _project(src["sqlserver_branch"], specs.DIM_BRANCH)


def dim_account(src: dict[str, DataFrame]) -> DataFrame:
    df = src["sqlserver_account"].withColumn("date_opened", F.to_date("date_opened"))
    return _project(df, specs.DIM_ACCOUNT)


def dim_customer(src: dict[str, DataFrame]) -> DataFrame:
    cust = src["sqlserver_customer"].alias("c")
    city = src["sqlserver_city"].alias("ci")
    state = src["sqlserver_state"].alias("s")
    df = (
        cust.join(city, F.col("c.city_id") == F.col("ci.city_id"), "left")
        .join(state, F.col("ci.state_id") == F.col("s.state_id"), "left")
        .select(
            F.col("c.customer_id").alias("customer_id"),
            F.upper("c.customer_name").alias("customer_name"),
            F.upper("c.address").alias("address"),
            F.col("ci.city_name").alias("city_name"),
            F.col("s.state_name").alias("state_name"),
            F.expr("try_cast(c.age AS INT)").alias("age"),
            F.upper("c.gender").alias("gender"),
            F.col("c.email").alias("email"),
        )
    )
    return _project(df, specs.DIM_CUSTOMER)


def transaction_candidates(src: dict[str, DataFrame]) -> DataFrame:
    """tUnite(sqlserver, excel, csv) + tUniqRow(transaction_id, first wins), before FK checks."""
    cols = ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type", "branch_id"]
    csv = src["file_transaction_csv"].withColumn(
        "transaction_date", F.call_function("try_to_timestamp", F.col("transaction_date"), F.lit(CSV_DATE_FORMAT)))
    union = (
        src["sqlserver_transaction_db"].select(*cols).withColumn("source", F.lit("sqlserver"))
        .unionByName(src["file_transaction_excel"].select(*cols).withColumn("source", F.lit("excel")))
        .unionByName(csv.select(*cols).withColumn("source", F.lit("csv")))
    )
    priority = F.create_map(*[x for k, v in SOURCE_PRIORITY.items() for x in (F.lit(k), F.lit(v))])
    w = Window.partitionBy("transaction_id").orderBy(priority[F.col("source")])
    return (
        union.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("amount", F.col("amount").cast(specs.MONEY))
    )


def split_fact(candidates: DataFrame, accounts: DataFrame, branches: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Apply the legacy FK constraints (Talend DIE_ON_ERROR=false -> row rejected)."""
    acc = accounts.select(F.col("account_id").alias("_acc"))
    br = branches.select(F.col("branch_id").alias("_br"))
    checked = (
        candidates.join(acc, candidates.account_id == acc._acc, "left")
        .join(br, candidates.branch_id == br._br, "left")
        .withColumn(
            "reject_reason",
            F.when(F.col("account_id").isNotNull() & F.col("_acc").isNull(), F.lit("FK_VIOLATION"))
            .when(F.col("branch_id").isNotNull() & F.col("_br").isNull(), F.lit("FK_VIOLATION")),
        )
        .drop("_acc", "_br")
    )
    good = _project(checked.filter(F.col("reject_reason").isNull()), specs.FACT_TRANSACTION)
    rejects = checked.filter(F.col("reject_reason").isNotNull()).select(
        F.lit("Load_FactTransaction").alias("job"),
        "transaction_id", "account_id", "branch_id", "source", F.col("reject_reason").alias("reason"),
    )
    return good, rejects


def build_gold(src: dict[str, DataFrame]) -> tuple[dict[str, DataFrame], DataFrame]:
    """Return ``({gold_table: df}, fact_rejects)`` for a full load."""
    branch, account, customer = dim_branch(src), dim_account(src), dim_customer(src)
    fact, rejects = split_fact(transaction_candidates(src), account, branch)
    return {"dim_branch": branch, "dim_account": account, "dim_customer": customer,
            "fact_transaction": fact}, rejects
