# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics: Daily Transaction
# MAGIC
# MAGIC Databricks replacement for the T-SQL stored procedure `dbo.sp_DailyTransaction(@start_date, @end_date)`
# MAGIC defined in `sql_scripts/02_create_procedures.sql`.
# MAGIC
# MAGIC Source table: `dwh.gold.fact_transaction`.
# MAGIC
# MAGIC Semantics preserved from T-SQL:
# MAGIC * `CAST(TransactionDate AS DATE) AS [Date]` -> `CAST(transaction_date AS DATE) AS date`
# MAGIC * `COUNT(TransactionID)` counts non-null transaction ids (not `COUNT(*)`)
# MAGIC * `SUM(Amount)`: `MONEY` maps to `DECIMAL(19,4)`, so the sum is cast back to
# MAGIC   `DECIMAL(19,4)` to match SQL Server's `MONEY` scale of 4 (Spark widens the
# MAGIC   precision of a decimal `SUM`, which would otherwise change the reported type).
# MAGIC * `BETWEEN` is inclusive on both bounds, on the *date-truncated* timestamp.
# MAGIC * `GROUP BY`/`ORDER BY` on the truncated date.
# MAGIC
# MAGIC Parameters are bound with Spark SQL parameter markers (`spark.sql(query, args=...)`),
# MAGIC never string-interpolated, so the notebook is not exposed to SQL injection.

# COMMAND ----------

from __future__ import annotations

import datetime
from typing import Union

from pyspark.sql import DataFrame, SparkSession

DateLike = Union[str, datetime.date]

FACT_TRANSACTION_TABLE = "dwh.gold.fact_transaction"

_DAILY_TRANSACTION_SQL = """
SELECT
    CAST(transaction_date AS DATE)                  AS date,
    COUNT(transaction_id)                           AS total_transactions,
    CAST(SUM(amount) AS DECIMAL(19, 4))             AS total_amount
FROM {table}
WHERE CAST(transaction_date AS DATE)
      BETWEEN CAST(:start_date AS DATE) AND CAST(:end_date AS DATE)
GROUP BY CAST(transaction_date AS DATE)
ORDER BY date
"""


def daily_transaction(
    spark: SparkSession,
    start_date: DateLike,
    end_date: DateLike,
    table: str = FACT_TRANSACTION_TABLE,
) -> DataFrame:
    """Daily transaction count/amount summary over an inclusive date range.

    Equivalent to ``EXEC sp_DailyTransaction @start_date, @end_date``.

    Args:
        spark: active SparkSession.
        start_date: inclusive lower bound, ``date`` or ``'YYYY-MM-DD'`` string.
        end_date: inclusive upper bound, ``date`` or ``'YYYY-MM-DD'`` string.
        table: fully qualified fact table; overridable for tests.

    Returns:
        DataFrame with columns ``date``, ``total_transactions``, ``total_amount``.
    """
    return spark.sql(
        _DAILY_TRANSACTION_SQL.format(table=table),
        args={"start_date": _as_date_string(start_date), "end_date": _as_date_string(end_date)},
    )


def _as_date_string(value: DateLike) -> str:
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value).strip()


# COMMAND ----------

# Notebook entry point. Guarded so this file stays importable as a plain module
# (unit tests import the pure function above without a Databricks runtime).
if "dbutils" in globals():
    dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD, inclusive)")  # noqa: F821
    dbutils.widgets.text("end_date", "", "End date (YYYY-MM-DD, inclusive)")  # noqa: F821

    result = daily_transaction(
        spark,  # noqa: F821 - provided by the Databricks runtime
        dbutils.widgets.get("start_date"),  # noqa: F821
        dbutils.widgets.get("end_date"),  # noqa: F821
    )
    display(result)  # noqa: F821
