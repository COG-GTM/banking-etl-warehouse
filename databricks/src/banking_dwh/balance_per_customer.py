"""PySpark/Delta conversion of ``sp_BalancePerCustomer``.

Source: ``sql_scripts/02_create_procedures.sql`` lines 50-90.

The T-SQL procedure does three things in one statement:

1. aggregates every row of ``FactTransaction`` into a per-account signed sum
   (``Deposit`` adds, everything else subtracts),
2. joins that aggregate onto ``DimCustomer``/``DimAccount``,
3. filters the result down to one customer with ``LIKE '%' + @customer_name + '%'``
   and ``Status = 'active'``.

Steps 1 and 2 are a full-fact-table aggregation: that is the part that belongs in a
Databricks job writing a Delta gold table. Step 3 is a serving-time predicate and stays
outside the batch job, so the fact scan happens once per refresh instead of once per call.

Semantics preserved from T-SQL (see docs/databricks-migration-assessment.md):

* ``MONEY`` is a fixed-point type (8-byte integer scaled by 10 000), so money columns are
  ``DECIMAL(19, 4)`` here rather than ``DOUBLE``.
* SQL Server's default collation (``SQL_Latin1_General_CP1_CI_AS``) is case-insensitive, so
  ``LIKE`` and ``Status = 'active'`` are case-insensitive. Spark string comparison is
  case-sensitive, so both predicates are lower-cased explicitly.
* ``ISNULL(ts.TotalTransactionAmount, 0)`` becomes ``coalesce(..., 0)`` on a decimal literal.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(19, 4)

GOLD_TABLE = "main.banking_dwh_gold.account_balance"
SILVER_FACT = "main.banking_dwh_silver.fact_transaction"
SILVER_CUSTOMER = "main.banking_dwh_silver.dim_customer"
SILVER_ACCOUNT = "main.banking_dwh_silver.dim_account"


def transaction_summary(fact_transaction: DataFrame) -> DataFrame:
    """T-SQL ``TransactionSummary`` CTE (lines 58-72): signed sum per AccountID."""
    signed_amount = F.when(
        F.lower(F.col("TransactionType")) == F.lit("deposit"), F.col("Amount").cast(MONEY)
    ).otherwise(-F.col("Amount").cast(MONEY))

    return fact_transaction.groupBy("AccountID").agg(
        F.sum(signed_amount).cast(MONEY).alias("TotalTransactionAmount")
    )


def account_balance(
    dim_customer: DataFrame, dim_account: DataFrame, fact_transaction: DataFrame
) -> DataFrame:
    """Unfiltered form of the procedure's final SELECT (lines 74-89).

    Everything except the ``@customer_name`` predicate: this is the batch-computable part
    and is what gets materialised as a Delta gold table.
    """
    summary = transaction_summary(fact_transaction)

    active_accounts = dim_account.where(F.lower(F.col("Status")) == F.lit("active"))

    return (
        dim_customer.alias("c")
        .join(active_accounts.alias("a"), F.col("c.CustomerID") == F.col("a.CustomerID"))
        .join(summary.alias("ts"), F.col("a.AccountID") == F.col("ts.AccountID"), "left")
        .select(
            F.col("c.CustomerID").alias("CustomerID"),
            F.col("a.AccountID").alias("AccountID"),
            F.col("c.CustomerName").alias("CustomerName"),
            F.col("a.AccountType").alias("AccountType"),
            F.col("a.Balance").cast(MONEY).alias("InitialBalance"),
            (
                F.col("a.Balance").cast(MONEY)
                + F.coalesce(F.col("ts.TotalTransactionAmount"), F.lit(0).cast(MONEY))
            )
            .cast(MONEY)
            .alias("CurrentBalance"),
        )
    )


def customer_name_matches(column: Column, customer_name: str) -> Column:
    """``CustomerName LIKE '%' + @customer_name + '%'`` under a case-insensitive collation.

    ``%`` and ``_`` inside ``customer_name`` stay wildcards, as they do in T-SQL.
    """
    return F.lower(column).like("%" + customer_name.lower() + "%")


def balance_per_customer(
    dim_customer: DataFrame,
    dim_account: DataFrame,
    fact_transaction: DataFrame,
    customer_name: str,
) -> DataFrame:
    """Full procedure equivalent, including the ``@customer_name`` filter."""
    return account_balance(dim_customer, dim_account, fact_transaction).where(
        customer_name_matches(F.col("CustomerName"), customer_name)
    ).select("CustomerName", "AccountType", "InitialBalance", "CurrentBalance")


def serve_from_gold(spark: SparkSession, customer_name: str, table: str = GOLD_TABLE) -> DataFrame:
    """Serving path: the point lookup the procedure was really used for, off the gold table."""
    return (
        spark.read.table(table)
        .where(customer_name_matches(F.col("CustomerName"), customer_name))
        .select("CustomerName", "AccountType", "InitialBalance", "CurrentBalance")
    )


def refresh_gold_table(spark: SparkSession, table: str = GOLD_TABLE) -> None:
    """Batch entrypoint: recompute the gold balance table from the silver Delta tables."""
    result = account_balance(
        spark.read.table(SILVER_CUSTOMER),
        spark.read.table(SILVER_ACCOUNT),
        spark.read.table(SILVER_FACT),
    )
    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def main() -> None:
    spark = SparkSession.builder.appName("refresh_account_balance").getOrCreate()
    refresh_gold_table(spark)


if __name__ == "__main__":
    main()
