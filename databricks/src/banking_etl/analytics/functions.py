"""Unity Catalog SQL table-valued functions ported from ``sql_scripts/02_create_procedures.sql``.

``sql/analytics/*.sql`` hold one ``${create} ${fn_...}(...) RETURNS TABLE ... RETURN <query>`` each.
On Databricks they become persistent functions in the gold schema (``<catalog>.<prefix>gold.fn_*``).
OSS Spark 4.0 can create persistent SQL table functions but cannot call them with a schema-qualified
name, so local runs (``Settings(catalog=None)``) create session-scoped ``TEMPORARY`` functions with
the same body instead.

=======================  =============================================================
T-SQL procedure          Databricks
=======================  =============================================================
sp_DailyTransaction      gold.fn_daily_transaction(start_date DATE, end_date DATE)
sp_BalancePerCustomer    gold.fn_balance_per_customer(customer_name STRING)
=======================  =============================================================
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from string import Template

from banking_etl.config import GOLD_DIM_ACCOUNT, GOLD_DIM_CUSTOMER, GOLD_FACT_TRANSACTION, Settings

FN_DAILY_TRANSACTION = "fn_daily_transaction"
FN_BALANCE_PER_CUSTOMER = "fn_balance_per_customer"
FUNCTIONS = (FN_DAILY_TRANSACTION, FN_BALANCE_PER_CUSTOMER)

SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "analytics"


def _temporary(settings: Settings, temporary: bool | None) -> bool:
    return settings.catalog is None if temporary is None else temporary


def function_name(settings: Settings, name: str, *, temporary: bool | None = None) -> str:
    """Name to create/call ``name`` with: ``<catalog>.<prefix>gold.<name>``, or bare when temporary."""
    if name not in FUNCTIONS:
        raise ValueError(f"unknown analytics function {name!r}; expected one of {FUNCTIONS}")
    return name if _temporary(settings, temporary) else settings.table("gold", name)


def function_parameters(settings: Settings, *, temporary: bool | None = None) -> dict[str, str]:
    """Placeholder -> value for ``sql/analytics/*.sql``."""
    temp = _temporary(settings, temporary)
    return {
        "create": "CREATE OR REPLACE TEMPORARY FUNCTION" if temp else "CREATE OR REPLACE FUNCTION",
        **{fn: function_name(settings, fn, temporary=temp) for fn in FUNCTIONS},
        "fact_transaction": settings.table("gold", GOLD_FACT_TRANSACTION),
        "dim_account": settings.table("gold", GOLD_DIM_ACCOUNT),
        "dim_customer": settings.table("gold", GOLD_DIM_CUSTOMER),
    }


def sql_files(sql_dir: Path | str = SQL_DIR) -> list[Path]:
    files = sorted(Path(sql_dir).glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no analytics SQL files found in {sql_dir}")
    return files


def render_functions(
    settings: Settings, *, temporary: bool | None = None, sql_dir: Path | str = SQL_DIR
) -> list[str]:
    """One ``CREATE OR REPLACE [TEMPORARY] FUNCTION`` statement per file, in file order (header comments dropped)."""
    params = function_parameters(settings, temporary=temporary)
    statements = []
    for path in sql_files(sql_dir):
        lines = path.read_text(encoding="utf-8").splitlines()
        body = "\n".join(line for line in lines if not line.lstrip().startswith("--"))
        sql = Template(body).substitute(params).strip()
        statements.append(sql[:-1].rstrip() if sql.endswith(";") else sql)
    return statements


def apply_analytics_functions(
    spark, settings: Settings, *, temporary: bool | None = None, sql_dir: Path | str = SQL_DIR
) -> list[str]:
    """Create (or replace) the analytics functions. Idempotent. Returns the executed statements."""
    statements = render_functions(settings, temporary=temporary, sql_dir=sql_dir)
    for statement in statements:
        spark.sql(statement)
    return statements


def _as_date(value: dt.date | str | None) -> dt.date | None:
    if value is None or isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value.strip())


def daily_transaction(
    spark,
    settings: Settings,
    start_date: dt.date | str | None,
    end_date: dt.date | str | None,
    *,
    temporary: bool | None = None,
):
    """``EXEC sp_DailyTransaction @start_date, @end_date`` -> DataFrame[Date, TotalTransactions, TotalAmount]."""
    fn = function_name(settings, FN_DAILY_TRANSACTION, temporary=temporary)
    return spark.sql(
        f"SELECT * FROM {fn}(:start_date, :end_date) ORDER BY `Date`",
        args={"start_date": _as_date(start_date), "end_date": _as_date(end_date)},
    )


def balance_per_customer(spark, settings: Settings, customer_name: str | None, *, temporary: bool | None = None):
    """``EXEC sp_BalancePerCustomer @customer_name`` -> DataFrame[CustomerName, AccountType, InitialBalance, CurrentBalance]."""
    fn = function_name(settings, FN_BALANCE_PER_CUSTOMER, temporary=temporary)
    return spark.sql(f"SELECT * FROM {fn}(:customer_name)", args={"customer_name": customer_name})
