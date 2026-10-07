"""Unity Catalog SQL table functions ported from ``sql_scripts/02_create_procedures.sql``.

``sql/analytics/*.sql`` each hold one ``${create} ${fn_...}(...) RETURNS TABLE ... RETURN <query>``.
On Databricks they become persistent functions next to the gold tables
(``migration_demo.banking_mig_gold.fn_*``). OSS Spark 4.0 can create persistent SQL table functions but
cannot call them schema-qualified, so local runs (``catalog=None``) create session ``TEMPORARY``
functions with the same body.

=======================  ====================================================================
T-SQL procedure          Databricks
=======================  ====================================================================
sp_DailyTransaction      gold.fn_daily_transaction(start_date DATE, end_date DATE)
sp_BalancePerCustomer    gold.fn_balance_per_customer(customer_name STRING)
=======================  ====================================================================
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from string import Template

from banking_etl.analytics.target import (
    GOLD_DIM_ACCOUNT,
    GOLD_DIM_CUSTOMER,
    GOLD_FACT_TRANSACTION,
    AnalyticsTarget,
)

FN_DAILY_TRANSACTION = "fn_daily_transaction"
FN_BALANCE_PER_CUSTOMER = "fn_balance_per_customer"
FUNCTIONS = (FN_DAILY_TRANSACTION, FN_BALANCE_PER_CUSTOMER)

SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "analytics"


def _temporary(target: AnalyticsTarget, temporary: bool | None) -> bool:
    return not target.unity_catalog if temporary is None else temporary


def short_name(target: AnalyticsTarget, name: str) -> str:
    """Unqualified function name (also the qualifier for its parameters inside the body)."""
    if name not in FUNCTIONS:
        raise ValueError(f"unknown analytics function {name!r}; expected one of {FUNCTIONS}")
    return f"{target.name_prefix}{name}"


def function_name(target: AnalyticsTarget, name: str, *, temporary: bool | None = None) -> str:
    """Name to create / call ``name`` with: schema-qualified, or bare when temporary."""
    short = short_name(target, name)
    return short if _temporary(target, temporary) else f"{target.schema}.{short}"


def function_parameters(target: AnalyticsTarget, *, temporary: bool | None = None) -> dict[str, str]:
    """Placeholder -> value for ``sql/analytics/*.sql``."""
    temp = _temporary(target, temporary)
    return {
        "create": "CREATE OR REPLACE TEMPORARY FUNCTION" if temp else "CREATE OR REPLACE FUNCTION",
        **{fn: function_name(target, fn, temporary=temp) for fn in FUNCTIONS},
        **{f"{fn}_short": short_name(target, fn) for fn in FUNCTIONS},
        "fact_transaction": target.table(GOLD_FACT_TRANSACTION),
        "dim_account": target.table(GOLD_DIM_ACCOUNT),
        "dim_customer": target.table(GOLD_DIM_CUSTOMER),
    }


def sql_files(sql_dir: Path | str = SQL_DIR) -> list[Path]:
    files = sorted(Path(sql_dir).glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no analytics SQL files found in {sql_dir}")
    return files


def render_functions(
    target: AnalyticsTarget, *, temporary: bool | None = None, sql_dir: Path | str = SQL_DIR
) -> list[str]:
    """One ``CREATE OR REPLACE [TEMPORARY] FUNCTION`` per file, in file order (comment lines dropped)."""
    params = function_parameters(target, temporary=temporary)
    statements = []
    for path in sql_files(sql_dir):
        lines = path.read_text(encoding="utf-8").splitlines()
        body = "\n".join(line for line in lines if not line.lstrip().startswith("--"))
        sql = Template(body).substitute(params).strip()
        statements.append(sql[:-1].rstrip() if sql.endswith(";") else sql)
    return statements


def apply_analytics_functions(
    spark, target: AnalyticsTarget, *, temporary: bool | None = None, sql_dir: Path | str = SQL_DIR
) -> list[str]:
    """Create (or replace) both functions. Idempotent. Returns the executed statements."""
    statements = render_functions(target, temporary=temporary, sql_dir=sql_dir)
    for statement in statements:
        spark.sql(statement)
    return statements


def as_date(value: dt.date | str | None) -> dt.date | None:
    if value is None or isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value.strip())


def daily_transaction(
    spark, target: AnalyticsTarget, start_date: dt.date | str | None, end_date: dt.date | str | None,
    *, temporary: bool | None = None,
):
    """``EXEC sp_DailyTransaction @start_date, @end_date`` -> DataFrame[date, total_transactions, total_amount]."""
    fn = function_name(target, FN_DAILY_TRANSACTION, temporary=temporary)
    return spark.sql(
        f"SELECT * FROM {fn}(:start_date, :end_date) ORDER BY `date`",
        args={"start_date": as_date(start_date), "end_date": as_date(end_date)},
    )


def balance_per_customer(spark, target: AnalyticsTarget, customer_name: str | None, *, temporary: bool | None = None):
    """``EXEC sp_BalancePerCustomer @customer_name`` -> DataFrame[customer_name, account_type, initial_balance, current_balance]."""
    fn = function_name(target, FN_BALANCE_PER_CUSTOMER, temporary=temporary)
    return spark.sql(f"SELECT * FROM {fn}(:customer_name)", args={"customer_name": customer_name})
