"""Databricks port of the T-SQL stored procedure ``sp_BalancePerCustomer``.

Source: ``sql_scripts/02_create_procedures.sql``::

    WITH TransactionSummary AS (
        SELECT AccountID,
               SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)
                   AS TotalTransactionAmount
        FROM FactTransaction GROUP BY AccountID)
    SELECT c.CustomerName, a.AccountType, a.Balance AS InitialBalance,
           a.Balance + ISNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
    FROM DimCustomer c
    JOIN DimAccount a ON c.CustomerID = a.CustomerID
    LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
    WHERE c.CustomerName LIKE '%' + @customer_name + '%' AND a.Status = 'active';

Two equivalent implementations are provided against the gold star schema:

* a Unity Catalog SQL table function ``<catalog>.<gold>.sp_balance_per_customer``
  (plus its helper scalar function ``<catalog>.<gold>.tsql_like_to_regex``),
  created by :func:`register_functions`;
* a parameterized PySpark function, :func:`balance_per_customer`.

SQL Server semantics reproduced (default collation ``SQL_Latin1_General_CP1_CI_AS``):

* ``@customer_name VARCHAR(100)``: input longer than 100 characters is silently
  truncated; ``NULL`` yields ``'%' + NULL + '%' = NULL`` and therefore no rows.
* ``LIKE`` is case-insensitive and accent-sensitive, with T-SQL wildcards inside the
  parameter honoured: ``%``, ``_``, ``[abc]``, ``[a-c]``, ``[^...]``. T-SQL has no
  default escape character, so ``\\`` is a literal. Malformed brackets behave as in
  SQL Server: an unterminated ``[`` or an empty ``[]`` never matches, ``[^]`` matches
  any single character, and reversed ranges such as ``[z-a]`` are empty.
* ``=`` comparisons (``Status = 'active'``, ``TransactionType = 'Deposit'``) are
  case-insensitive and ignore trailing spaces (ANSI padding) but not leading ones.
* ``NULL`` / non-``Deposit`` transaction types subtract; ``NULL`` amounts are ignored by
  ``SUM``; accounts without transactions keep their balance (``ISNULL(..., 0)``).
* ``MONEY`` columns are computed and returned as ``DECIMAL(19,4)``.
* The procedure has no ``ORDER BY``, so row order is unspecified here too.

Known approximation: bracket ranges compare characters case-insensitively by code
point, whereas SQL Server orders them by collation, so ranges spanning accented or
punctuation characters (e.g. ``'á' LIKE '[a-c]'``) can differ. ASCII letter and digit
ranges match exactly.
"""

from __future__ import annotations

import re
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_CATALOG = "banking_dwh"
DEFAULT_GOLD_SCHEMA = "gold"
FUNCTION_NAME = "sp_balance_per_customer"
LIKE_HELPER_NAME = "tsql_like_to_regex"

CUSTOMER_NAME_MAX_LENGTH = 100  # @customer_name VARCHAR(100)
MONEY = "DECIMAL(19,4)"
OUTPUT_COLUMNS = ("customer_name", "account_type", "initial_balance", "current_balance")

NEVER_MATCH = "(?!)"
_TOKEN_RE = re.compile(r"\[[^\]]*\]|.", re.S)
_CLASS_ITEM_RE = re.compile(r".-.|.", re.S)
_REGEX_SAFE = re.compile(r"[A-Za-z0-9]")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_\-]*$")


def _quote(identifier: str) -> str:
    if not identifier or not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid Unity Catalog identifier: {identifier!r}")
    return f"`{identifier}`"


def _schema_ref(catalog: Optional[str], schema: str) -> str:
    return _quote(schema) if catalog is None else f"{_quote(catalog)}.{_quote(schema)}"


# --------------------------------------------------------------------------------------
# T-SQL LIKE -> Java regex (Python implementation, used by the PySpark function)
# --------------------------------------------------------------------------------------
def _escape(ch: str) -> str:
    return ch if _REGEX_SAFE.fullmatch(ch) else "\\" + ch


def _translate_class(token: str) -> str:
    body = token[1:-1]
    negated = body.startswith("^")
    if negated:
        body = body[1:]
    parts = []
    for item in _CLASS_ITEM_RE.findall(body):
        if len(item) == 3:
            low, high = item[0].lower(), item[2].lower()
            if low > high:
                continue  # SQL Server treats a reversed range as empty
            parts.append(f"{_escape(low)}-{_escape(high)}")
        else:
            parts.append(_escape(item))
    if not parts:
        return "." if negated else NEVER_MATCH
    return "[" + ("^" if negated else "") + "".join(parts) + "]"


def tsql_like_to_regex(pattern: Optional[str]) -> Optional[str]:
    """Translate a T-SQL ``LIKE`` pattern into an anchored, case-insensitive Java regex."""
    if pattern is None:
        return None
    tokens = _TOKEN_RE.findall(pattern)
    if "[" in tokens:
        return NEVER_MATCH  # unterminated bracket never matches in SQL Server
    out = []
    for token in tokens:
        if token == "%":
            out.append(".*")
        elif token == "_":
            out.append(".")
        elif len(token) > 1:
            out.append(_translate_class(token))
        else:
            out.append(_escape(token))
    return "(?isu)^" + "".join(out) + r"\z"


def customer_name_pattern(customer_name: Optional[str]) -> Optional[str]:
    """``'%' + @customer_name + '%'`` after VARCHAR(100) truncation."""
    if customer_name is None:
        return None
    return "%" + customer_name[:CUSTOMER_NAME_MAX_LENGTH] + "%"


# --------------------------------------------------------------------------------------
# PySpark implementation
# --------------------------------------------------------------------------------------
def _tsql_equals_ci(col: Column, literal: str) -> Column:
    """``col = literal`` under a case-insensitive collation with ANSI padding."""
    return F.lower(F.rtrim(col)) == F.lit(literal.rstrip(" ").lower())


def balance_per_customer_from_frames(
    dim_customer: DataFrame,
    dim_account: DataFrame,
    fact_transaction: DataFrame,
    customer_name: Optional[str],
) -> DataFrame:
    """Run ``sp_BalancePerCustomer`` logic over the given gold DataFrames."""
    amount = F.col("amount").cast(MONEY)
    transaction_summary = fact_transaction.groupBy("account_id").agg(
        F.sum(
            F.when(_tsql_equals_ci(F.col("transaction_type"), "Deposit"), amount).otherwise(-amount)
        ).alias("total_transaction_amount")
    )

    regex = tsql_like_to_regex(customer_name_pattern(customer_name))
    name_matches = F.col("c.customer_name").rlike(regex) if regex is not None else F.lit(None)

    c = dim_customer.alias("c")
    a = dim_account.alias("a")
    ts = transaction_summary.alias("ts")
    initial_balance = F.col("a.balance").cast(MONEY)
    return (
        c.join(a, F.col("c.customer_id") == F.col("a.customer_id"), "inner")
        .join(ts, F.col("a.account_id") == F.col("ts.account_id"), "left")
        .where(name_matches & _tsql_equals_ci(F.col("a.status"), "active"))
        .select(
            F.col("c.customer_name").alias("customer_name"),
            F.col("a.account_type").alias("account_type"),
            initial_balance.alias("initial_balance"),
            (initial_balance + F.coalesce(F.col("ts.total_transaction_amount"), F.lit(0)))
            .cast(MONEY)
            .alias("current_balance"),
        )
    )


def balance_per_customer(
    spark: SparkSession,
    customer_name: Optional[str],
    catalog: Optional[str] = DEFAULT_CATALOG,
    gold_schema: str = DEFAULT_GOLD_SCHEMA,
) -> DataFrame:
    """PySpark equivalent of ``EXEC sp_BalancePerCustomer @customer_name``."""
    gold = _schema_ref(catalog, gold_schema)
    return balance_per_customer_from_frames(
        spark.table(f"{gold}.`dim_customer`"),
        spark.table(f"{gold}.`dim_account`"),
        spark.table(f"{gold}.`fact_transaction`"),
        customer_name,
    )


# --------------------------------------------------------------------------------------
# Unity Catalog SQL functions
# --------------------------------------------------------------------------------------
# Same algorithm as tsql_like_to_regex() above, expressed in Spark SQL.
_LIKE_HELPER_BODY = r"""
CASE
  WHEN pattern IS NULL THEN NULL
  WHEN array_contains(regexp_extract_all(pattern, r'(?s)\[[^\]]*\]|.', 0), '[') THEN '(?!)'
  ELSE concat('(?isu)^', array_join(transform(
    regexp_extract_all(pattern, r'(?s)\[[^\]]*\]|.', 0),
    tok -> CASE
      WHEN tok = '%' THEN '.*'
      WHEN tok = '_' THEN '.'
      WHEN length(tok) = 1 THEN regexp_replace(tok, r'([^A-Za-z0-9])', r'\\$1')
      ELSE element_at(transform(array(substr(tok, 2, length(tok) - 2)), body ->
        element_at(transform(array(startswith(body, '^')), negated ->
          element_at(transform(array(
            filter(regexp_extract_all(IF(negated, substr(body, 2), body), r'(?s).-.|.', 0),
                   item -> NOT (length(item) = 3
                                AND lower(substr(item, 1, 1)) > lower(substr(item, 3, 1))))),
            items -> CASE
              WHEN size(items) = 0 THEN IF(negated, '.', '(?!)')
              ELSE concat('[', IF(negated, '^', ''), array_join(transform(items,
                item -> IF(length(item) = 3,
                  concat(regexp_replace(lower(substr(item, 1, 1)), r'([^A-Za-z0-9])', r'\\$1'), '-',
                         regexp_replace(lower(substr(item, 3, 1)), r'([^A-Za-z0-9])', r'\\$1')),
                  regexp_replace(item, r'([^A-Za-z0-9])', r'\\$1'))), ''), ']')
            END), 1)), 1)), 1)
    END), ''), r'\z')
END
"""


def like_helper_ddl(catalog: Optional[str] = DEFAULT_CATALOG, gold_schema: str = DEFAULT_GOLD_SCHEMA,
                    temporary: bool = False) -> str:
    name = _quote(LIKE_HELPER_NAME) if temporary else f"{_schema_ref(catalog, gold_schema)}.{_quote(LIKE_HELPER_NAME)}"
    kind = "TEMPORARY FUNCTION" if temporary else "FUNCTION"
    return (
        f"CREATE OR REPLACE {kind} {name}(pattern STRING)\n"
        "RETURNS STRING\n"
        "COMMENT 'Translates a SQL Server LIKE pattern (%, _, [..], [^..]; case-insensitive) "
        "into an anchored Java regex for RLIKE.'\n"
        f"RETURN {_LIKE_HELPER_BODY.strip()}"
    )


def table_function_ddl(catalog: Optional[str] = DEFAULT_CATALOG, gold_schema: str = DEFAULT_GOLD_SCHEMA,
                       temporary: bool = False) -> str:
    gold = _schema_ref(catalog, gold_schema)
    if temporary:
        name, helper = _quote(FUNCTION_NAME), _quote(LIKE_HELPER_NAME)
        kind = "TEMPORARY FUNCTION"
    else:
        name, helper = f"{gold}.{_quote(FUNCTION_NAME)}", f"{gold}.{_quote(LIKE_HELPER_NAME)}"
        kind = "FUNCTION"
    return f"""CREATE OR REPLACE {kind} {name}(
  p_customer_name STRING COMMENT 'Substring of the customer name (T-SQL @customer_name VARCHAR(100))'
)
RETURNS TABLE (
  customer_name STRING,
  account_type STRING,
  initial_balance {MONEY},
  current_balance {MONEY}
)
COMMENT 'Port of SQL Server dbo.sp_BalancePerCustomer: current balance of each active account of
  customers whose name matches %p_customer_name% (case-insensitive).'
RETURN
WITH transaction_summary AS (
  SELECT
    account_id,
    SUM(CASE WHEN lower(rtrim(transaction_type)) = 'deposit' THEN CAST(amount AS {MONEY})
             ELSE -CAST(amount AS {MONEY}) END) AS total_transaction_amount
  FROM {gold}.`fact_transaction`
  GROUP BY account_id
),
name_filter AS (
  SELECT {helper}(concat('%', left(p_customer_name, {CUSTOMER_NAME_MAX_LENGTH}), '%')) AS name_regex
)
SELECT
  c.customer_name,
  a.account_type,
  CAST(a.balance AS {MONEY}) AS initial_balance,
  CAST(CAST(a.balance AS {MONEY}) + coalesce(ts.total_transaction_amount, 0) AS {MONEY}) AS current_balance
FROM {gold}.`dim_customer` c
JOIN {gold}.`dim_account` a ON c.customer_id = a.customer_id
LEFT JOIN transaction_summary ts ON a.account_id = ts.account_id
CROSS JOIN name_filter nf
WHERE c.customer_name RLIKE nf.name_regex
  AND lower(rtrim(a.status)) = 'active'"""


def register_functions(spark: SparkSession, catalog: Optional[str] = DEFAULT_CATALOG,
                       gold_schema: str = DEFAULT_GOLD_SCHEMA, temporary: bool = False) -> str:
    """Create (or replace) the helper and the table function; returns the function name."""
    spark.sql(like_helper_ddl(catalog, gold_schema, temporary))
    spark.sql(table_function_ddl(catalog, gold_schema, temporary))
    if temporary:
        return _quote(FUNCTION_NAME)
    return f"{_schema_ref(catalog, gold_schema)}.{_quote(FUNCTION_NAME)}"


def call_table_function(spark: SparkSession, customer_name: Optional[str],
                        catalog: Optional[str] = DEFAULT_CATALOG,
                        gold_schema: str = DEFAULT_GOLD_SCHEMA, temporary: bool = False) -> DataFrame:
    """``SELECT * FROM <catalog>.<gold>.sp_balance_per_customer(:customer_name)``."""
    name = _quote(FUNCTION_NAME) if temporary else f"{_schema_ref(catalog, gold_schema)}.{_quote(FUNCTION_NAME)}"
    return spark.sql(f"SELECT * FROM {name}(:customer_name)", args={"customer_name": customer_name})
