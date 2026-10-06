"""sp_BalancePerCustomer: current balance of each active account for matching customers.

CurrentBalance = DimAccount.Balance + sum(Deposit amounts) - sum(all other amounts).

SQL Server's default collation compares case-insensitively, so the LIKE filter on
CustomerName and the Status = 'active' check are made case-insensitive here.

Parameters: customer_name (substring to match), optional output_table, plus
catalog / target_schema.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "jobs"))

from pyspark.sql import DataFrame  # noqa: E402

from common import get_param, get_spark, qualified  # noqa: E402

QUERY = """
WITH TransactionSummary AS (
    SELECT
        AccountID,
        SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END) AS TotalTransactionAmount
    FROM {fact}
    GROUP BY AccountID
)
SELECT
    c.CustomerName,
    a.AccountType,
    a.Balance                                               AS InitialBalance,
    a.Balance + COALESCE(ts.TotalTransactionAmount, 0)      AS CurrentBalance
FROM {customer} c
JOIN {account} a
    ON c.CustomerID = a.CustomerID
LEFT JOIN TransactionSummary ts
    ON a.AccountID = ts.AccountID
WHERE UPPER(c.CustomerName) LIKE CONCAT('%', UPPER(:customer_name), '%')
  AND LOWER(a.Status) = 'active'
"""


def balance_per_customer(customer_name: str, spark=None) -> DataFrame:
    spark = spark or get_spark()
    query = QUERY.format(
        fact=qualified("fact_transaction", spark),
        customer=qualified("dim_customer", spark),
        account=qualified("dim_account", spark),
    )
    return spark.sql(query, args={"customer_name": customer_name})


def main() -> None:
    spark = get_spark()
    customer_name = get_param("customer_name", "Shelly", spark)
    result = balance_per_customer(customer_name, spark)
    result.show(truncate=False)

    output_table = get_param("output_table", "", spark)
    if output_table:
        result.write.format("delta").mode("overwrite").saveAsTable(output_table)
        print(f"Wrote {output_table}")


if __name__ == "__main__":
    main()
