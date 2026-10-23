"""Data-quality checks for the Silver star schema (Ticket 2).

Delta / Unity Catalog do not enforce PRIMARY KEY or FOREIGN KEY constraints, so the
legacy guarantees from ``sql_scripts/01_create_tables.sql`` are re-implemented here:

* ``pk_not_null.<table>`` / ``pk_unique.<table>`` for every Silver table with a key.
* ``FK_FactTransaction_DimAccount`` and ``FK_FactTransaction_DimBranch``: fact rows whose
  non-null foreign key has no matching dimension row (SQL Server semantics: NULL FKs pass).

Ticket 10 extends this module with row-count and reconciliation checks.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable

from ..common.config import EnvConfig
from ..contracts.tables import DQ_RESULTS, SILVER_TABLES, TableContract

SAMPLE_LIMIT = 10


@dataclass(frozen=True)
class DQResult:
    check_name: str
    check_type: str
    table_name: str
    passed: bool
    failed_rows: int
    details: str | None = None


class DataQualityError(AssertionError):
    pass


def _sample(df, cols: list[str]) -> list[dict]:
    return [r.asDict() for r in df.select(*cols).limit(SAMPLE_LIMIT).collect()]


def primary_key_checks(spark, cfg: EnvConfig, tables: Iterable[TableContract] = SILVER_TABLES) -> list[DQResult]:
    from pyspark.sql import functions as F

    results = []
    for t in tables:
        if not t.primary_key:
            continue
        fq = cfg.table(t.layer, t.name)
        df = spark.table(fq)
        pk = list(t.primary_key)
        nulls = df.filter(" OR ".join(f"{c} IS NULL" for c in pk))
        n_null = nulls.count()
        results.append(DQResult(
            f"pk_not_null.{t.name}", "pk_not_null", fq, n_null == 0, n_null,
            json.dumps({"key": pk}),
        ))
        dups = df.groupBy(*pk).agg(F.count(F.lit(1)).alias("n")).filter("n > 1")
        n_dup = dups.count()
        results.append(DQResult(
            f"pk_unique.{t.name}", "pk_unique", fq, n_dup == 0, n_dup,
            json.dumps({"key": pk, "sample": _sample(dups, pk + ["n"])}, default=str),
        ))
    return results


def referential_integrity_checks(spark, cfg: EnvConfig, tables: Iterable[TableContract] = SILVER_TABLES) -> list[DQResult]:
    from pyspark.sql import functions as F

    results = []
    for t in tables:
        child_fq = cfg.table(t.layer, t.name)
        for fk in t.foreign_keys:
            parent_fq = cfg.table(t.layer, fk.ref_table)
            child = spark.table(child_fq)
            parent = spark.table(parent_fq).select(
                *[F.col(rc).alias(c) for c, rc in zip(fk.columns, fk.ref_columns)]
            ).distinct()
            non_null = child.filter(" AND ".join(f"{c} IS NOT NULL" for c in fk.columns))
            orphans = non_null.join(parent, on=list(fk.columns), how="left_anti")
            n = orphans.count()
            sample_cols = list(t.primary_key) + [c for c in fk.columns if c not in t.primary_key]
            results.append(DQResult(
                fk.name, "referential_integrity", child_fq, n == 0, n,
                json.dumps({
                    "columns": list(fk.columns),
                    "references": f"{parent_fq}({', '.join(fk.ref_columns)})",
                    "sample": _sample(orphans, sample_cols),
                }, default=str),
            ))
    return results


def run_star_schema_checks(spark, cfg: EnvConfig) -> list[DQResult]:
    return primary_key_checks(spark, cfg) + referential_integrity_checks(spark, cfg)


def write_results(spark, cfg: EnvConfig, results: list[DQResult], run_id: str) -> int:
    if not results:
        return 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(run_id, r.check_name, r.check_type, r.table_name, r.passed, int(r.failed_rows), r.details, now)
            for r in results]
    df = spark.createDataFrame(rows, DQ_RESULTS.ddl_schema())
    df.write.format("delta").mode("append").saveAsTable(cfg.table("ops", DQ_RESULTS.name))
    return len(rows)


def assert_all_passed(results: list[DQResult]) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        msg = "; ".join(f"{r.check_name} ({r.table_name}): {r.failed_rows} failing rows" for r in failed)
        raise DataQualityError(f"{len(failed)} data-quality check(s) failed: {msg}")


def results_as_dicts(results: list[DQResult]) -> list[dict]:
    return [asdict(r) for r in results]
