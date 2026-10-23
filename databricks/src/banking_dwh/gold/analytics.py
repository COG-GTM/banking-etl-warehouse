"""Gold analytics objects replacing the T-SQL stored procedures (Ticket 9).

``sql_scripts/02_create_procedures.sql`` defines two procedures on the legacy DWH:

* ``sp_DailyTransaction @start_date, @end_date``: ``CAST(TransactionDate AS DATE)``, ``COUNT``,
  ``SUM(Amount)`` over an inclusive ``BETWEEN`` range, ordered by date.
* ``sp_BalancePerCustomer @customer_name``: CTE summing ``CASE WHEN TransactionType = 'Deposit'
  THEN Amount ELSE -Amount END`` per account, ``Balance + ISNULL(sum, 0)`` for every account with
  ``Status = 'active'`` whose owner's ``CustomerName LIKE '%' + @customer_name + '%'``.

They become Unity Catalog objects in the gold schema, named and typed by ``GOLD_OBJECTS`` in
``banking_dwh.contracts.tables``:

* ``v_daily_transaction``: view, the unfiltered daily summary.
* ``fn_daily_transaction(start_date DATE, end_date DATE)``: SQL table function.
* ``fn_balance_per_customer(customer_name STRING)``: SQL table function.

T-SQL semantics kept (verified against SQL Server 2022 by ``fixtures/t9_legacy_parity``):

* The legacy DB collation is ``SQL_Latin1_General_CP1_CI_AS``: case-insensitive, accent-sensitive,
  and ``=`` ignores trailing spaces. So ``Status = 'active'`` and ``TransactionType = 'Deposit'``
  become ``lower(rtrim(col)) = 'active'``, and the name filter uses ``ILIKE``.
* ``@customer_name`` is ``VARCHAR(100)``: longer input is truncated to 100 characters
  (``left(name, 100)``). ``%`` and ``_`` in the argument stay wildcards, as in T-SQL. A backslash
  is literal in T-SQL but the escape character in Spark ``LIKE``, so it is doubled. Runs of ``%``
  are collapsed to one, because Spark compiles ``LIKE`` to a backtracking Java regex.
* A NULL argument returns no rows; an empty string returns every active account.
* ``ISNULL`` becomes ``coalesce``; ``SUM`` over only NULL amounts is NULL.
* ``CAST(TransactionDate AS DATE)``: legacy ``DATETIME`` values are wall-clock times, stored in
  silver as ``TIMESTAMP`` written with session time zone UTC. The date is the UTC calendar day of
  the instant, computed from epoch seconds so the caller's session time zone never shifts it.

Known divergence: T-SQL ``LIKE`` bracket classes (``[a-c]``, ``[^x]``) have no Spark ``LIKE``
equivalent. In the argument, ``[`` and ``]`` match literally.

    python -m banking_dwh.gold.analytics --target prod > ddl/04_gold_objects.sql
"""

from __future__ import annotations

import argparse

from ..common.config import EnvConfig
from ..contracts.tables import GOLD_OBJECTS, Column, GoldObject

CUSTOMER_NAME_MAX_LENGTH = 100  # sp_BalancePerCustomer @customer_name VARCHAR(100)

V_DAILY_TRANSACTION = "v_daily_transaction"
FN_DAILY_TRANSACTION = "fn_daily_transaction"
FN_BALANCE_PER_CUSTOMER = "fn_balance_per_customer"


def gold_object(name: str) -> GoldObject:
    for obj in GOLD_OBJECTS:
        if obj.name == name:
            return obj
    raise KeyError(f"No gold object contract named {name!r}")


def _q(text: str) -> str:
    return text.replace("\\", "\\\\").replace("'", "\\'")


def _comment(c: Column) -> str:
    parts = [p for p in (f"Legacy: {c.legacy_name}" if c.legacy_name else "", c.comment) if p]
    return f" COMMENT '{_q('. '.join(parts))}'" if parts else ""


def _txn_date(col: str = "transaction_date") -> str:
    """CAST(TransactionDate AS DATE) on the legacy wall clock, independent of the session time zone."""
    # Epoch arithmetic: unix_seconds/date_from_unix_date ignore spark.sql.session.timeZone, unlike
    # CAST(... AS DATE) or current_timezone()-based conversions (also inside captured view/function configs).
    return f"date_from_unix_date(CAST(floor(unix_seconds({col}) / 86400) AS INT))"


def _ci_equals(col: str, value: str) -> str:
    """SQL Server CI collation equality: case-insensitive, trailing spaces ignored."""
    return f"lower(rtrim({col})) = '{_q(value.lower())}'"


def name_pattern(arg: str) -> str:
    """Spark ILIKE pattern equivalent to T-SQL '%' + @customer_name + '%' (see module docstring)."""
    truncated = f"left({arg}, {CUSTOMER_NAME_MAX_LENGTH})"
    escaped = f"replace({truncated}, '\\\\', '\\\\\\\\')"
    return f"regexp_replace(concat('%', {escaped}, '%'), '%+', '%')"


def daily_transaction_query(cfg: EnvConfig, where: str | None = None, *, order: bool = False) -> str:
    fact = cfg.table("silver", "fact_transaction")
    sql = (
        f"SELECT\n  {_txn_date()} AS transaction_date,\n"
        "  count(transaction_id) AS total_transactions,\n"
        "  sum(amount) AS total_amount\n"
        f"FROM {fact}\n"
    )
    if where:
        sql += f"WHERE {where}\n"
    sql += f"GROUP BY {_txn_date()}"
    if order:
        sql += "\nORDER BY transaction_date"
    return sql


def balance_per_customer_query(cfg: EnvConfig, name_arg: str) -> str:
    return (
        "WITH transaction_summary AS (\n"
        "  SELECT\n    account_id,\n"
        f"    sum(CASE WHEN {_ci_equals('transaction_type', 'Deposit')} THEN amount ELSE -amount END)"
        " AS total_transaction_amount\n"
        f"  FROM {cfg.table('silver', 'fact_transaction')}\n"
        "  GROUP BY account_id\n"
        ")\n"
        "SELECT\n"
        "  c.customer_name,\n"
        "  a.account_type,\n"
        "  a.balance AS initial_balance,\n"
        "  CAST(a.balance + coalesce(ts.total_transaction_amount, 0) AS DECIMAL(20,4)) AS current_balance\n"
        f"FROM {cfg.table('silver', 'dim_customer')} c\n"
        f"JOIN {cfg.table('silver', 'dim_account')} a ON c.customer_id = a.customer_id\n"
        "LEFT JOIN transaction_summary ts ON a.account_id = ts.account_id\n"
        f"WHERE c.customer_name ILIKE {name_pattern(name_arg)}\n"
        f"  AND {_ci_equals('a.status', 'active')}"
    )


def _fn_header(cfg: EnvConfig, obj: GoldObject, temporary: bool) -> str:
    name = obj.name if temporary else cfg.table("gold", obj.name)
    params = ",\n".join(f"  {p.name} {p.type}{_comment(p)}" for p in obj.parameters)
    returns = ",\n".join(f"  {c.name} {c.type}{_comment(c)}" for c in obj.output)
    kind = "TEMPORARY FUNCTION" if temporary else "FUNCTION"
    return (
        f"CREATE OR REPLACE {kind} {name}(\n{params}\n)\nRETURNS TABLE (\n{returns}\n)\n"
        f"COMMENT '{_q(f'{obj.comment} Replaces {obj.replaces}.')}'\nRETURN\n"
    )


def create_view_sql(cfg: EnvConfig) -> str:
    obj = gold_object(V_DAILY_TRANSACTION)
    cols = ",\n".join(f"  {c.name}{_comment(c)}" for c in obj.output)
    return (
        f"CREATE OR REPLACE VIEW {cfg.table('gold', obj.name)} (\n{cols}\n)\n"
        f"COMMENT '{_q(f'{obj.comment} Replaces {obj.replaces}.')}'\n"
        "TBLPROPERTIES ('banking_dwh.layer' = 'gold', 'banking_dwh.owner_ticket' = '9')\n"
        f"AS\n{daily_transaction_query(cfg)}"
    )


def create_fn_daily_transaction_sql(cfg: EnvConfig, *, temporary: bool = False) -> str:
    obj = gold_object(FN_DAILY_TRANSACTION)
    # Parameters are qualified with the function name so a future column of the same name
    # cannot shadow them.
    q = obj.name
    where = f"{_txn_date()} BETWEEN {q}.start_date AND {q}.end_date"
    return _fn_header(cfg, obj, temporary) + daily_transaction_query(cfg, where, order=True)


def create_fn_balance_per_customer_sql(cfg: EnvConfig, *, temporary: bool = False) -> str:
    obj = gold_object(FN_BALANCE_PER_CUSTOMER)
    # dim_customer also has a customer_name column: the parameter must be qualified.
    return _fn_header(cfg, obj, temporary) + balance_per_customer_query(cfg, f"{obj.name}.customer_name")


def gold_object_statements(cfg: EnvConfig, *, temporary_functions: bool = False) -> list[str]:
    """DDL for every GOLD_OBJECTS entry, in contract order.

    ``temporary_functions=True`` creates the functions as session-scoped TEMPORARY FUNCTIONs with
    the same body: OSS Spark 4.0 can create persistent SQL table functions but cannot call them
    schema-qualified, so local tests use this form. Databricks/Unity Catalog uses the default.
    """
    return [
        create_view_sql(cfg),
        create_fn_daily_transaction_sql(cfg, temporary=temporary_functions),
        create_fn_balance_per_customer_sql(cfg, temporary=temporary_functions),
    ]


def call_daily_transaction_sql(cfg: EnvConfig | None) -> str:
    """Parameterized call (named markers :start_date / :end_date), the EXEC replacement.

    ``cfg=None`` calls the unqualified (temporary) function.
    """
    fn = FN_DAILY_TRANSACTION if cfg is None else cfg.table("gold", FN_DAILY_TRANSACTION)
    return f"SELECT * FROM {fn}(:start_date, :end_date) ORDER BY transaction_date"


def call_balance_per_customer_sql(cfg: EnvConfig | None) -> str:
    fn = FN_BALANCE_PER_CUSTOMER if cfg is None else cfg.table("gold", FN_BALANCE_PER_CUSTOMER)
    return f"SELECT * FROM {fn}(:customer_name)"


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Render the gold analytics DDL (Ticket 9).")
    p.add_argument("--target", default="prod")
    a = p.parse_args(argv)
    cfg = EnvConfig.from_target(a.target)
    header = (
        f"-- GENERATED by `python -m banking_dwh.gold.analytics --target {a.target}`; do not edit by hand.\n"
        "-- Source of truth: databricks/src/banking_dwh/gold/analytics.py and GOLD_OBJECTS in\n"
        "-- databricks/src/banking_dwh/contracts/tables.py. Replaces sql_scripts/02_create_procedures.sql."
    )
    body = ";\n\n".join(gold_object_statements(cfg))
    print(f"{header}\n\n{body};\n", end="")


if __name__ == "__main__":
    main()
