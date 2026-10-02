"""Spark SQL equivalents of the DWH stored procedures in sql_scripts/02_create_procedures.sql.

python -m etl.analytics daily 2024-01-18 2024-01-20
python -m etl.analytics balance "shelly"
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession

from etl.config import EtlConfig, load_config
from etl.jdbc import read_table
from etl.spark_session import build_spark_session

DAILY_TRANSACTION_SQL = """
SELECT
    CAST(TransactionDate AS DATE) AS `Date`,
    COUNT(TransactionID)          AS TotalTransactions,
    SUM(Amount)                   AS TotalAmount
FROM FactTransaction
WHERE CAST(TransactionDate AS DATE) BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
GROUP BY CAST(TransactionDate AS DATE)
ORDER BY `Date`
"""

# SQL Server's default collation is case-insensitive, so LIKE and Status = 'active' in the
# stored procedure ignore case; UPPER/LOWER reproduce that in Spark.
BALANCE_PER_CUSTOMER_SQL = """
WITH TransactionSummary AS (
    SELECT
        AccountID,
        SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)
            AS TotalTransactionAmount
    FROM FactTransaction
    GROUP BY AccountID
)
SELECT
    c.CustomerName,
    a.AccountType,
    a.Balance AS InitialBalance,
    a.Balance + COALESCE(ts.TotalTransactionAmount, 0) AS CurrentBalance
FROM DimCustomer c
JOIN DimAccount a ON c.CustomerID = a.CustomerID
LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
WHERE UPPER(c.CustomerName) LIKE CONCAT('%', UPPER(:customer_name), '%')
  AND LOWER(a.Status) = 'active'
"""


def register_views(spark: SparkSession, cfg: EtlConfig) -> None:
    t = cfg.target_tables
    for view, table in (
        ("FactTransaction", t.fact_transaction),
        ("DimAccount", t.dim_account),
        ("DimCustomer", t.dim_customer),
        ("DimBranch", t.dim_branch),
    ):
        read_table(spark, cfg.target, table).createOrReplaceTempView(view)


def daily_transaction(spark: SparkSession, start_date: str, end_date: str) -> DataFrame:
    """sp_DailyTransaction @start_date, @end_date (expects views from register_views)."""
    return spark.sql(DAILY_TRANSACTION_SQL, args={"start_date": start_date, "end_date": end_date})


def balance_per_customer(spark: SparkSession, customer_name: str) -> DataFrame:
    """sp_BalancePerCustomer @customer_name (expects views from register_views)."""
    return spark.sql(BALANCE_PER_CUSTOMER_SQL, args={"customer_name": customer_name})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    daily = sub.add_parser("daily", help="sp_DailyTransaction")
    daily.add_argument("start_date")
    daily.add_argument("end_date")
    balance = sub.add_parser("balance", help="sp_BalancePerCustomer")
    balance.add_argument("customer_name")
    args = parser.parse_args(argv)

    cfg = load_config()
    spark = build_spark_session(cfg.spark)
    try:
        register_views(spark, cfg)
        if args.command == "daily":
            df = daily_transaction(spark, args.start_date, args.end_date)
        else:
            df = balance_per_customer(spark, args.customer_name)
        df.show(truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
