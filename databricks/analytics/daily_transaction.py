"""sp_DailyTransaction: daily transaction count and total amount between two dates.

Parameters: start_date, end_date (yyyy-MM-dd, inclusive), optional output_table to persist
the result as Delta, plus catalog / target_schema.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "jobs"))

from pyspark.sql import DataFrame  # noqa: E402

from common import get_param, get_spark, qualified  # noqa: E402

QUERY = """
SELECT
    CAST(TransactionDate AS DATE) AS `Date`,
    COUNT(TransactionID)          AS TotalTransactions,
    SUM(Amount)                   AS TotalAmount
FROM {fact}
WHERE CAST(TransactionDate AS DATE) BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
GROUP BY CAST(TransactionDate AS DATE)
ORDER BY `Date`
"""


def daily_transaction(start_date: str, end_date: str, spark=None) -> DataFrame:
    spark = spark or get_spark()
    query = QUERY.format(fact=qualified("fact_transaction", spark))
    return spark.sql(query, args={"start_date": start_date, "end_date": end_date})


def main() -> None:
    spark = get_spark()
    start_date = get_param("start_date", "2024-01-18", spark)
    end_date = get_param("end_date", "2024-01-20", spark)
    result = daily_transaction(start_date, end_date, spark)
    result.show(truncate=False)

    output_table = get_param("output_table", "", spark)
    if output_table:
        result.write.format("delta").mode("overwrite").saveAsTable(output_table)
        print(f"Wrote {output_table}")


if __name__ == "__main__":
    main()
