"""Databricks port of the SQL Server stored procedure ``sp_DailyTransaction``.

Source: ``sql_scripts/02_create_procedures.sql``::

    SELECT CAST(TransactionDate AS DATE) AS [Date],
           COUNT(TransactionID)          AS TotalTransactions,
           SUM(Amount)                   AS TotalAmount
    FROM FactTransaction
    WHERE CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date
    GROUP BY CAST(TransactionDate AS DATE)
    ORDER BY [Date];

Equivalent implementations over ``<catalog>.<schema>.fact_transaction``, all sharing the
same SELECT (``daily_transaction_query``) except the DataFrame one:

* a Unity Catalog SQL table function (``create_table_function_ddl``), callable as
  ``SELECT * FROM <catalog>.gold.sp_daily_transaction(DATE'2024-01-21', DATE'2024-01-25')``;
* a Databricks SQL stored procedure (``create_procedure_ddl``, DBR 17+), callable as
  ``CALL <catalog>.gold.sp_daily_transaction_proc(DATE'2024-01-21', DATE'2024-01-25')``;
* a parameterized PySpark function (``daily_transaction`` / ``run_daily_transaction``).

Output columns map 1:1 to the T-SQL result set: ``Date`` -> ``date`` (DATE),
``TotalTransactions`` -> ``total_transactions`` (INT, like T-SQL ``COUNT``) and
``TotalAmount`` -> ``total_amount`` (DECIMAL(19,4), like ``SUM`` over ``MONEY``).
"""

from __future__ import annotations

import datetime as _dt

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_CATALOG = "banking_dwh"
DEFAULT_SCHEMA = "gold"
DEFAULT_FACT_TABLE = "fact_transaction"
DEFAULT_FUNCTION_NAME = "sp_daily_transaction"
DEFAULT_PROCEDURE_NAME = "sp_daily_transaction_proc"

OUTPUT_COLUMNS = ("date", "total_transactions", "total_amount")
RETURNS_TABLE_SCHEMA = "`date` DATE, total_transactions INT, total_amount DECIMAL(19,4)"

DateLike = _dt.date | str | None


def quote_identifier(*parts: str) -> str:
    """Backtick-quote a (multi-part) Unity Catalog identifier."""
    if not parts or any(not isinstance(p, str) or not p for p in parts):
        raise ValueError(f"Invalid identifier parts: {parts!r}")
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def to_date(value: DateLike) -> _dt.date | None:
    """Coerce a parameter the way SQL Server binds a ``DATE`` argument.

    ``None`` stays ``NULL`` (and, as in T-SQL, makes ``BETWEEN`` match nothing).
    A ``datetime`` is truncated to its date part; strings must be ISO ``YYYY-MM-DD``.
    """
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        return _dt.date.fromisoformat(value.strip())
    raise TypeError(f"Expected a date, ISO date string or None, got {type(value).__name__}")


def daily_transaction_query(fact_table: str, start_ref: str, end_ref: str) -> str:
    """The procedure body as Databricks SQL.

    ``fact_table`` must already be a quoted identifier; ``start_ref``/``end_ref`` are the
    SQL expressions bound to ``@start_date``/``@end_date`` (function parameters in the UC
    table function, ``:start_date``/``:end_date`` markers when run through ``spark.sql``).
    """
    return f"""SELECT
  CAST(transaction_date AS DATE) AS `date`,
  CAST(COUNT(transaction_id) AS INT) AS total_transactions,
  CAST(SUM(amount) AS DECIMAL(19,4)) AS total_amount
FROM {fact_table}
WHERE CAST(transaction_date AS DATE) BETWEEN {start_ref} AND {end_ref}
GROUP BY CAST(transaction_date AS DATE)
ORDER BY `date`"""


def create_table_function_ddl(
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    function_name: str = DEFAULT_FUNCTION_NAME,
    fact_table: str = DEFAULT_FACT_TABLE,
    fact_schema: str | None = None,
) -> str:
    """``CREATE OR REPLACE FUNCTION`` DDL for the Unity Catalog SQL table function."""
    fn = quote_identifier(catalog, schema, function_name)
    source = quote_identifier(catalog, fact_schema or schema, fact_table)
    body = daily_transaction_query(
        source,
        f"{function_name}.start_date",
        f"{function_name}.end_date",
    )
    return f"""CREATE OR REPLACE FUNCTION {fn}(
  start_date DATE COMMENT 'Inclusive lower bound (@start_date)',
  end_date DATE COMMENT 'Inclusive upper bound (@end_date)'
)
RETURNS TABLE ({RETURNS_TABLE_SCHEMA})
COMMENT 'Port of SQL Server sp_DailyTransaction: daily transaction count and total amount between two dates (inclusive).'
RETURN
{body}"""


def run_daily_transaction_sql(
    spark: SparkSession,
    start_date: DateLike,
    end_date: DateLike,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    fact_table: str = DEFAULT_FACT_TABLE,
) -> DataFrame:
    """Run the procedure body through ``spark.sql`` with named parameter markers."""
    query = daily_transaction_query(
        quote_identifier(catalog, schema, fact_table),
        "CAST(:start_date AS DATE)",
        "CAST(:end_date AS DATE)",
    )
    return spark.sql(query, args=_date_args(start_date, end_date))


def _date_param(value: DateLike) -> Column:
    return F.lit(to_date(value)).cast("date")


def _date_args(start_date: DateLike, end_date: DateLike) -> dict:
    return {"start_date": to_date(start_date), "end_date": to_date(end_date)}


def daily_transaction(fact: DataFrame, start_date: DateLike, end_date: DateLike) -> DataFrame:
    """PySpark equivalent of ``sp_DailyTransaction`` over a ``fact_transaction`` DataFrame."""
    txn_date = F.col("transaction_date").cast("date")
    return (
        fact.where(txn_date.between(_date_param(start_date), _date_param(end_date)))
        .groupBy(txn_date.alias("date"))
        .agg(
            F.count("transaction_id").cast("int").alias("total_transactions"),
            F.sum("amount").cast("decimal(19,4)").alias("total_amount"),
        )
        .orderBy("date")
    )


def run_daily_transaction(
    spark: SparkSession,
    start_date: DateLike,
    end_date: DateLike,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    fact_table: str = DEFAULT_FACT_TABLE,
) -> DataFrame:
    """Run the PySpark implementation against ``<catalog>.<schema>.<fact_table>``."""
    fact = spark.table(quote_identifier(catalog, schema, fact_table))
    return daily_transaction(fact, start_date, end_date)


def call_table_function(
    spark: SparkSession,
    start_date: DateLike,
    end_date: DateLike,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    function_name: str = DEFAULT_FUNCTION_NAME,
) -> DataFrame:
    """Invoke the deployed UC table function (ordered by ``date``)."""
    fn = quote_identifier(catalog, schema, function_name)
    return spark.sql(
        f"SELECT * FROM {fn}(CAST(:start_date AS DATE), CAST(:end_date AS DATE)) ORDER BY `date`",
        args=_date_args(start_date, end_date),
    )


def create_table_function(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    function_name: str = DEFAULT_FUNCTION_NAME,
    fact_table: str = DEFAULT_FACT_TABLE,
) -> str:
    """Create (or replace) the UC table function; returns the executed DDL."""
    ddl = create_table_function_ddl(catalog, schema, function_name, fact_table)
    spark.sql(ddl)
    return ddl


def create_procedure_ddl(
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    procedure_name: str = DEFAULT_PROCEDURE_NAME,
    fact_table: str = DEFAULT_FACT_TABLE,
) -> str:
    """``CREATE OR REPLACE PROCEDURE`` DDL (Databricks SQL stored procedures, DBR 17+).

    ``CALL <catalog>.<schema>.sp_daily_transaction_proc(DATE'2024-01-21', DATE'2024-01-25')``
    returns the same result set as the T-SQL ``EXEC sp_DailyTransaction``.
    """
    proc = quote_identifier(catalog, schema, procedure_name)
    source = quote_identifier(catalog, schema, fact_table)
    body = daily_transaction_query(source, "start_date", "end_date")
    return f"""CREATE OR REPLACE PROCEDURE {proc}(
  IN start_date DATE COMMENT 'Inclusive lower bound (@start_date)',
  IN end_date DATE COMMENT 'Inclusive upper bound (@end_date)'
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Port of SQL Server sp_DailyTransaction.'
AS BEGIN
{body};
END"""


def create_procedure(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    procedure_name: str = DEFAULT_PROCEDURE_NAME,
    fact_table: str = DEFAULT_FACT_TABLE,
) -> str:
    """Create (or replace) the SQL stored procedure; returns the executed DDL."""
    ddl = create_procedure_ddl(catalog, schema, procedure_name, fact_table)
    spark.sql(ddl)
    return ddl


def call_procedure(
    spark: SparkSession,
    start_date: DateLike,
    end_date: DateLike,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    procedure_name: str = DEFAULT_PROCEDURE_NAME,
) -> DataFrame:
    """``CALL`` the deployed SQL stored procedure (its result set is ordered by ``date``)."""
    proc = quote_identifier(catalog, schema, procedure_name)
    return spark.sql(
        f"CALL {proc}(CAST(:start_date AS DATE), CAST(:end_date AS DATE))",
        args=_date_args(start_date, end_date),
    )
