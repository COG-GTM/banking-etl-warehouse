# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics: Balance Per Customer
# MAGIC
# MAGIC Databricks replacement for the T-SQL stored procedure `dbo.sp_BalancePerCustomer(@customer_name)`
# MAGIC defined in `sql_scripts/02_create_procedures.sql`.
# MAGIC
# MAGIC Source tables: `dwh.gold.fact_transaction`, `dwh.silver.dim_customer`, `dwh.silver.dim_account`.
# MAGIC
# MAGIC Semantics preserved from T-SQL:
# MAGIC * CTE sums `CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END` per account.
# MAGIC * `DimCustomer` INNER JOIN `DimAccount`, LEFT JOIN the CTE, so accounts with no
# MAGIC   transactions still appear.
# MAGIC * `ISNULL(ts.TotalTransactionAmount, 0)` -> `coalesce(...)`.
# MAGIC * `MONEY` -> `DECIMAL(19,4)`; the aggregate and the derived balance are cast back to
# MAGIC   `DECIMAL(19,4)` so the output scale matches SQL Server's `MONEY`.
# MAGIC * `CustomerName LIKE '%' + @customer_name + '%'`: the wildcards are added to the *bound
# MAGIC   value*, not to the SQL text, and `%`/`_`/`\` in the user input are escaped so they are
# MAGIC   matched literally (SQL Server would treat them as wildcards; escaping is a deliberate,
# MAGIC   safer deviation, see PR description).
# MAGIC * **Case sensitivity**: SQL Server's default collation (`SQL_Latin1_General_CP1_CI_AS`) is
# MAGIC   case-insensitive, Spark's `LIKE`/`=` are case-sensitive. To preserve the original
# MAGIC   behaviour, the name match and the `'Deposit'` / `'active'` literal comparisons are all
# MAGIC   folded with `lower()`.
# MAGIC
# MAGIC The parameter is bound with a Spark SQL parameter marker (`spark.sql(query, args=...)`);
# MAGIC no user input is ever interpolated into the SQL text.

# COMMAND ----------

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

FACT_TRANSACTION_TABLE = "dwh.gold.fact_transaction"
DIM_CUSTOMER_TABLE = "dwh.silver.dim_customer"
DIM_ACCOUNT_TABLE = "dwh.silver.dim_account"

DEPOSIT_TRANSACTION_TYPE = "deposit"
ACTIVE_ACCOUNT_STATUS = "active"

_BALANCE_PER_CUSTOMER_SQL = """
WITH transaction_summary AS (
    SELECT
        account_id,
        CAST(
            SUM(
                CASE
                    WHEN lower(transaction_type) = '{deposit}' THEN amount
                    ELSE -amount
                END
            ) AS DECIMAL(19, 4)
        ) AS total_transaction_amount
    FROM {fact_transaction}
    GROUP BY account_id
)
SELECT
    c.customer_name                                             AS customer_name,
    a.account_type                                              AS account_type,
    CAST(a.balance AS DECIMAL(19, 4))                           AS initial_balance,
    CAST(a.balance + coalesce(ts.total_transaction_amount, 0)
         AS DECIMAL(19, 4))                                     AS current_balance
FROM {dim_customer} c
JOIN {dim_account} a
    ON c.customer_id = a.customer_id
LEFT JOIN transaction_summary ts
    ON a.account_id = ts.account_id
WHERE lower(c.customer_name) LIKE :customer_name_pattern
  AND lower(a.status) = '{active}'
"""


def balance_per_customer(
    spark: SparkSession,
    customer_name: str,
    fact_transaction_table: str = FACT_TRANSACTION_TABLE,
    dim_customer_table: str = DIM_CUSTOMER_TABLE,
    dim_account_table: str = DIM_ACCOUNT_TABLE,
) -> DataFrame:
    """Current balance of every active account of the customers matching ``customer_name``.

    Equivalent to ``EXEC sp_BalancePerCustomer @customer_name``. The match is a
    case-insensitive substring match, as under SQL Server's default collation.

    Args:
        spark: active SparkSession.
        customer_name: substring of the customer name; may be empty to match everyone.
        fact_transaction_table: fully qualified fact table; overridable for tests.
        dim_customer_table: fully qualified customer dimension; overridable for tests.
        dim_account_table: fully qualified account dimension; overridable for tests.

    Returns:
        DataFrame with columns ``customer_name``, ``account_type``, ``initial_balance``,
        ``current_balance``.
    """
    query = _BALANCE_PER_CUSTOMER_SQL.format(
        fact_transaction=fact_transaction_table,
        dim_customer=dim_customer_table,
        dim_account=dim_account_table,
        deposit=DEPOSIT_TRANSACTION_TYPE,
        active=ACTIVE_ACCOUNT_STATUS,
    )
    return spark.sql(query, args={"customer_name_pattern": build_like_pattern(customer_name)})


def build_like_pattern(customer_name: str) -> str:
    """Build the lower-cased ``%value%`` LIKE pattern for a user-supplied name.

    ``\\``, ``%`` and ``_`` in the input are escaped with the default Spark SQL escape
    character (``\\``) so they match literally instead of acting as wildcards.
    """
    escaped = (
        (customer_name or "")
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )
    return f"%{escaped.lower()}%"


# COMMAND ----------

# Notebook entry point. Guarded so this file stays importable as a plain module
# (unit tests import the pure function above without a Databricks runtime).
if "dbutils" in globals():
    dbutils.widgets.text("customer_name", "", "Customer name (substring match)")  # noqa: F821

    result = balance_per_customer(
        spark,  # noqa: F821 - provided by the Databricks runtime
        dbutils.widgets.get("customer_name"),  # noqa: F821
    )
    display(result)  # noqa: F821
