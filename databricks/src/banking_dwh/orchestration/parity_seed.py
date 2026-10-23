"""Live-validation harness for the reconciliation gate (Ticket 10).

Seeds a ticket *scratch* namespace (schema prefix like ``bdwh_t10_``) so that
``notebooks/orchestration/01_reconcile`` can be exercised before Tickets 3-9 land:

* Silver tables are loaded from the legacy parity fixtures (``fixtures/parity/``).
* Gold objects are T9 stand-ins computed from that Silver data with the logic of
  ``sql_scripts/02_create_procedures.sql`` (never loaded from fixtures).
* ``break_scenarios`` corrupts the seeded data to prove the gate fails.

Every entry point refuses non-scratch namespaces (prod, or the shared ``bdwh_`` dev prefix).
"""

from __future__ import annotations

import re
from pathlib import Path

from ..common.config import EnvConfig
from ..contracts.ddl import create_table_sql
from ..contracts.tables import SILVER_TABLES
from ..dq.reconciliation import FIXTURES_DIR, default_specs, expected_df

SCRATCH_PREFIX = re.compile(r"^([A-Za-z0-9_]*_)?t\d+_$")


class NotScratchNamespaceError(ValueError):
    pass


def assert_scratch(cfg: EnvConfig) -> None:
    if cfg.target == "prod" or not SCRATCH_PREFIX.match(cfg.schema_prefix or ""):
        raise NotScratchNamespaceError(
            f"Refusing to seed {cfg.catalog}.{cfg.schema_prefix}* (target={cfg.target}): parity seeding is only "
            "allowed in a ticket scratch namespace, e.g. schema_prefix=bdwh_t10_")


def _uc(cfg: EnvConfig) -> bool:
    return cfg.catalog != "spark_catalog"


def seed_silver(spark, cfg: EnvConfig, fixtures_dir: Path | str = FIXTURES_DIR) -> dict[str, int]:
    """Overwrite the scratch Silver tables with the legacy DWH rows (dims first, then the fact)."""
    from pyspark.sql import functions as F

    assert_scratch(cfg)
    specs = {s.name: s for s in default_specs(["silver"])}
    counts = {}
    for t in SILVER_TABLES:
        spark.sql(create_table_sql(cfg, t, unity_catalog=_uc(cfg)))
        df = expected_df(spark, specs[f"silver.{t.name}"], fixtures_dir)
        meta = {"_loaded_at": F.current_timestamp(), "_source_system": F.lit(None).cast("string")}
        df = df.select(*[F.col(c) if c in df.columns else meta[c].alias(c) for c in t.column_names])
        df.write.mode("overwrite").insertInto(cfg.table("silver", t.name))
        counts[t.name] = spark.table(cfg.table("silver", t.name)).count()
    return counts


def gold_standin_statements(cfg: EnvConfig, *, temporary: bool = False) -> list[str]:
    """T9 stand-ins for v_daily_transaction / fn_daily_transaction / fn_balance_per_customer.

    ``temporary=True`` renders session-scoped objects for local OSS Spark, which cannot call
    schema-qualified SQL table functions.
    """
    assert_scratch(cfg)
    fact, acct, cust = (cfg.table("silver", n) for n in ("fact_transaction", "dim_account", "dim_customer"))
    if temporary:
        view, fn = "OR REPLACE TEMPORARY VIEW v_daily_transaction", "OR REPLACE TEMPORARY FUNCTION "
        daily_fn, bal_fn = "fn_daily_transaction", "fn_balance_per_customer"
    else:
        view = f"OR REPLACE VIEW {cfg.table('gold', 'v_daily_transaction')}"
        fn, daily_fn, bal_fn = "OR REPLACE FUNCTION ", cfg.table("gold", "fn_daily_transaction"), \
            cfg.table("gold", "fn_balance_per_customer")
    daily_select = (f"SELECT CAST(transaction_date AS DATE) AS transaction_date, "
                    f"COUNT(transaction_id) AS total_transactions, SUM(amount) AS total_amount FROM {fact}")
    return [
        f"CREATE {view} COMMENT 'T10 stand-in for T9 (sp_DailyTransaction, unfiltered)' AS "
        f"{daily_select} GROUP BY CAST(transaction_date AS DATE)",
        f"CREATE {fn}{daily_fn}(start_date DATE, end_date DATE) "
        f"RETURNS TABLE (transaction_date DATE, total_transactions BIGINT, total_amount DECIMAL(29,4)) "
        f"RETURN {daily_select} WHERE CAST(transaction_date AS DATE) BETWEEN start_date AND end_date "
        f"GROUP BY CAST(transaction_date AS DATE)",
        f"CREATE {fn}{bal_fn}(p_customer_name STRING) "
        f"RETURNS TABLE (customer_name STRING, account_type STRING, initial_balance DECIMAL(19,4), "
        f"current_balance DECIMAL(20,4)) RETURN "
        f"WITH ts AS (SELECT account_id, SUM(CASE WHEN transaction_type = 'Deposit' THEN amount ELSE -amount END) "
        f"AS total FROM {fact} GROUP BY account_id) "
        f"SELECT c.customer_name, a.account_type, a.balance AS initial_balance, "
        f"CAST(a.balance + COALESCE(ts.total, 0) AS DECIMAL(20,4)) AS current_balance "
        f"FROM {cust} c JOIN {acct} a ON c.customer_id = a.customer_id "
        f"LEFT JOIN ts ON a.account_id = ts.account_id "
        f"WHERE instr(upper(c.customer_name), upper(p_customer_name)) > 0 AND a.status = 'active'",
    ]


def create_gold_standins(spark, cfg: EnvConfig, *, temporary: bool = False) -> int:
    stmts = gold_standin_statements(cfg, temporary=temporary)
    for s in stmts:
        spark.sql(s)
    return len(stmts)


# Deliberate corruptions for the "broken run" demo: each must make the reconciliation fail.
# (layer, table, key column, MIN|MAX row picked, statement with {t} = table and {k} = key value)
BREAK_SCENARIOS = {
    "fact_row_missing": ("silver", "fact_transaction", "transaction_id", "MAX",
                         "DELETE FROM {t} WHERE transaction_id = {k}"),
    "balance_drift": ("silver", "dim_account", "account_id", "MIN",
                      "UPDATE {t} SET balance = balance + 0.01 WHERE account_id = {k}"),
    "customer_not_upcased": ("silver", "dim_customer", "customer_id", "MIN",
                             "UPDATE {t} SET customer_name = initcap(customer_name) WHERE customer_id = {k}"),
}


def break_scenarios(spark, cfg: EnvConfig, scenarios: list[str]) -> list[dict]:
    assert_scratch(cfg)
    unknown = set(scenarios) - set(BREAK_SCENARIOS)
    if unknown:
        raise ValueError(f"Unknown scenario(s) {sorted(unknown)}; expected {sorted(BREAK_SCENARIOS)}")
    applied = []
    for name in scenarios:
        layer, table, key, agg, sql = BREAK_SCENARIOS[name]
        fq = cfg.table(layer, table)
        k = spark.sql(f"SELECT {agg}({key}) AS k FROM {fq}").collect()[0]["k"]
        spark.sql(sql.format(t=fq, k=int(k)))
        applied.append({"scenario": name, "table": fq, key: int(k)})
    return applied
