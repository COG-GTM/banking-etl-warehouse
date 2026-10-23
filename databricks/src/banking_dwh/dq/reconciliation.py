"""Reconciliation of the Lakehouse against the legacy SQL Server DWH (Ticket 10).

The legacy output (DWH.dbo.Dim*/FactTransaction loaded by the Talend jobs, plus the two
stored-procedure result sets) is captured as CSV parity fixtures in ``fixtures/parity/``
by ``fixtures/parity/build_parity.py``. For every Silver table and Gold object this module
compares, per object:

* ``row_count``                      - number of rows
* ``distinct_keys``                  - distinct values of the key columns
* ``sum_<column>``                   - SUM of every DECIMAL (money) column: amount, balance, ...
* ``rows_missing`` / ``rows_unexpected`` - full-row multiset diff on the business columns
  (legacy rows absent from the Lakehouse / Lakehouse rows absent from legacy).

Fixtures are plain strings; both sides are cast to the contract column types with
``try_cast`` (Spark SQL, so it works on Spark Connect / serverless) before comparing.
Comparisons are exact: money is DECIMAL on both sides, so there is no tolerance.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from ..common.config import EnvConfig, validate_identifier
from ..contracts.tables import GOLD_OBJECTS, RECONCILIATION_RESULTS, SILVER_TABLES, Column, GoldObject
from .checks import DQResult

FIXTURES_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "parity"
SAMPLE_LIMIT = 5


class ReconciliationError(AssertionError):
    pass


@dataclass(frozen=True)
class ReconSpec:
    """One Lakehouse object reconciled against one legacy fixture."""

    name: str                      # e.g. silver.dim_account, gold.fn_daily_transaction(...)
    layer: str
    fixture: str                   # CSV file in fixtures/parity/
    legacy_object: str             # e.g. DWH.dbo.DimAccount, EXEC sp_DailyTransaction ...
    columns: tuple[Column, ...]    # business columns (name, type, legacy_name)
    key: tuple[str, ...]
    relation: Callable[[EnvConfig], str] = field(compare=False)

    @property
    def sum_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.type.upper().startswith("DECIMAL")]


@dataclass(frozen=True)
class ReconResult:
    check_name: str
    layer: str
    object_name: str
    legacy_object: str
    metric: str
    expected: str | None
    actual: str | None
    passed: bool
    details: str | None = None


def _silver_specs() -> list[ReconSpec]:
    specs = []
    for t in SILVER_TABLES:
        cols = tuple(c for c in t.columns if not c.name.startswith("_"))
        specs.append(ReconSpec(
            name=f"silver.{t.name}", layer="silver", fixture=f"{t.name}.csv",
            legacy_object=f"DWH.dbo.{t.legacy_name}", columns=cols, key=t.primary_key,
            relation=lambda cfg, n=t.name: cfg.table("silver", n),
        ))
    return specs


def _gold(name: str) -> GoldObject:
    return next(g for g in GOLD_OBJECTS if g.name == name)


def _sql_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _gold_specs() -> list[ReconSpec]:
    daily, daily_fn, balance = _gold("v_daily_transaction"), _gold("fn_daily_transaction"), _gold("fn_balance_per_customer")
    bal_key = ("customer_name", "account_type")

    def fn(name: str, *args: str) -> Callable[[EnvConfig], str]:
        return lambda cfg: f"{cfg.table('gold', name)}({', '.join(_sql_str(a) for a in args)})"

    return [
        ReconSpec("gold.v_daily_transaction", "gold", "sp_daily_transaction__all.csv",
                  "sp_DailyTransaction over all dates", daily.output, ("transaction_date",),
                  lambda cfg: cfg.table("gold", "v_daily_transaction")),
        ReconSpec("gold.fn_daily_transaction('2024-01-18','2024-01-20')", "gold",
                  "sp_daily_transaction__2024-01-18__2024-01-20.csv",
                  "EXEC sp_DailyTransaction @start_date='2024-01-18', @end_date='2024-01-20'",
                  daily_fn.output, ("transaction_date",),
                  fn("fn_daily_transaction", "2024-01-18", "2024-01-20")),
        ReconSpec("gold.fn_balance_per_customer('')", "gold", "sp_balance_per_customer__all.csv",
                  "EXEC sp_BalancePerCustomer @customer_name=''", balance.output, bal_key,
                  fn("fn_balance_per_customer", "")),
        ReconSpec("gold.fn_balance_per_customer('Shelly')", "gold", "sp_balance_per_customer__shelly.csv",
                  "EXEC sp_BalancePerCustomer @customer_name='Shelly'", balance.output, bal_key,
                  fn("fn_balance_per_customer", "Shelly")),
    ]


def default_specs(layers: Iterable[str] = ("silver", "gold")) -> list[ReconSpec]:
    layers = set(layers)
    if not layers:
        raise ValueError("At least one reconciliation layer (silver, gold) is required")
    unknown = layers - {"silver", "gold"}
    if unknown:
        raise ValueError(f"Unknown reconciliation layer(s) {sorted(unknown)}; expected silver and/or gold")
    specs = []
    if "silver" in layers:
        specs += _silver_specs()
    if "gold" in layers:
        specs += _gold_specs()
    return specs


# ------------------------------------------------------------------------------------------
# Loading both sides
# ------------------------------------------------------------------------------------------
def read_fixture_rows(spec: ReconSpec, fixtures_dir: Path | str = FIXTURES_DIR) -> list[tuple]:
    """Read a fixture CSV and return tuples in ``spec.columns`` order (blank -> NULL)."""
    path = Path(fixtures_dir) / spec.fixture
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing = [c.legacy_name for c in spec.columns if c.legacy_name not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{path.name}: missing legacy columns {missing}")
        return [tuple(row[c.legacy_name] if row[c.legacy_name] != "" else None for c in spec.columns)
                for row in reader]


def _typed(df, columns: Iterable[Column]):
    return df.selectExpr(*[f"try_cast(`{c.name}` AS {c.type}) AS `{c.name}`" for c in columns])


def expected_df(spark, spec: ReconSpec, fixtures_dir: Path | str = FIXTURES_DIR):
    schema = ", ".join(f"`{c.name}` STRING" for c in spec.columns)
    raw = spark.createDataFrame(read_fixture_rows(spec, fixtures_dir), schema)
    return _typed(raw, spec.columns)


def actual_df(spark, cfg: EnvConfig, spec: ReconSpec):
    for c in spec.columns:
        validate_identifier(c.name, "column")
    rel = spec.relation(cfg)
    df = spark.sql(f"SELECT {', '.join('`' + c.name + '`' for c in spec.columns)} FROM {rel}")
    return _typed(df, spec.columns)


# ------------------------------------------------------------------------------------------
# Comparison
# ------------------------------------------------------------------------------------------
def profile(df, spec: ReconSpec) -> dict[str, str | None]:
    from pyspark.sql import functions as F

    aggs = [F.count(F.lit(1)).alias("row_count"),
            F.countDistinct(*[F.col(k) for k in spec.key]).alias("distinct_keys")]
    aggs += [F.sum(F.col(c)).alias(f"sum_{c}") for c in spec.sum_columns]
    row = df.agg(*aggs).collect()[0].asDict()
    return {k: (None if v is None else str(v)) for k, v in row.items()}


def _sample(df) -> list[dict]:
    return [r.asDict() for r in df.limit(SAMPLE_LIMIT).collect()]


def reconcile_spec(spark, cfg: EnvConfig, spec: ReconSpec, fixtures_dir: Path | str = FIXTURES_DIR) -> list[ReconResult]:
    rel_name = spec.relation(cfg)
    exp = expected_df(spark, spec, fixtures_dir)
    try:
        act = actual_df(spark, cfg, spec)
        act.limit(0).collect()
    except Exception as e:  # missing table/view/function or contract column
        msg = str(e).splitlines()[0][:500]
        return [ReconResult(f"{spec.name}.exists", spec.layer, rel_name, spec.legacy_object, "exists",
                            "true", "false", False, json.dumps({"error": msg}))]

    e_prof, a_prof = profile(exp, spec), profile(act, spec)
    results = [ReconResult(f"{spec.name}.exists", spec.layer, rel_name, spec.legacy_object, "exists",
                           "true", "true", True)]
    for metric, e_val in e_prof.items():
        a_val = a_prof.get(metric)
        results.append(ReconResult(f"{spec.name}.{metric}", spec.layer, rel_name, spec.legacy_object,
                                   metric, e_val, a_val, e_val == a_val))

    missing, unexpected = exp.exceptAll(act), act.exceptAll(exp)
    for metric, diff in (("rows_missing", missing), ("rows_unexpected", unexpected)):
        n = diff.count()
        details = json.dumps({"sample": _sample(diff)}, default=str) if n else None
        results.append(ReconResult(f"{spec.name}.{metric}", spec.layer, rel_name, spec.legacy_object,
                                   metric, "0", str(n), n == 0, details))
    return results


def run_reconciliation(spark, cfg: EnvConfig, specs: Iterable[ReconSpec] | None = None,
                       fixtures_dir: Path | str = FIXTURES_DIR) -> list[ReconResult]:
    results: list[ReconResult] = []
    for spec in default_specs() if specs is None else specs:
        results += reconcile_spec(spark, cfg, spec, fixtures_dir)
    return results


# ------------------------------------------------------------------------------------------
# Persistence / gating
# ------------------------------------------------------------------------------------------
def write_results(spark, cfg: EnvConfig, results: list[ReconResult], run_id: str) -> int:
    """Append to ``ops.reconciliation_results`` (create it if needed)."""
    from ..contracts.ddl import create_table_sql

    if not results:
        return 0
    spark.sql(create_table_sql(cfg, RECONCILIATION_RESULTS, unity_catalog=cfg.catalog != "spark_catalog"))
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(run_id, r.check_name, r.layer, r.object_name, r.legacy_object, r.metric,
             r.expected, r.actual, r.passed, r.details, now) for r in results]
    spark.createDataFrame(rows, RECONCILIATION_RESULTS.ddl_schema()).write.format("delta") \
        .mode("append").saveAsTable(cfg.table("ops", RECONCILIATION_RESULTS.name))
    return len(rows)


def as_dq_results(results: list[ReconResult]) -> list[DQResult]:
    """One ``ops.dq_results`` row per reconciled object (check_type=reconciliation)."""
    by_obj: dict[str, list[ReconResult]] = {}
    for r in results:
        by_obj.setdefault(r.object_name, []).append(r)
    out = []
    for obj, rs in by_obj.items():
        failed = [r for r in rs if not r.passed]
        out.append(DQResult(
            f"reconciliation.{rs[0].check_name.rsplit('.', 1)[0]}", "reconciliation", obj, not failed,
            len(failed),
            json.dumps({"legacy_object": rs[0].legacy_object,
                        "failed": [{"metric": r.metric, "expected": r.expected, "actual": r.actual}
                                   for r in failed]}),
        ))
    return out


def summarize(results: list[ReconResult]) -> dict:
    objects: dict[str, dict] = {}
    for r in results:
        o = objects.setdefault(r.check_name.rsplit(".", 1)[0], {"passed": True, "metrics": {}})
        o["metrics"][r.metric] = r.actual if r.passed else {"expected": r.expected, "actual": r.actual}
        o["passed"] = o["passed"] and r.passed
    failed = [r for r in results if not r.passed]
    return {"passed": not failed, "checks": len(results), "failed_checks": len(failed), "objects": objects}


def assert_reconciled(results: list[ReconResult]) -> None:
    if not results:
        raise ReconciliationError("No reconciliation checks ran")
    failed = [r for r in results if not r.passed]
    if failed:
        msg = "; ".join(f"{r.check_name}: expected {r.expected}, got {r.actual}" for r in failed)
        raise ReconciliationError(f"{len(failed)} reconciliation check(s) failed: {msg}")
