"""Dry-run cutover from the legacy Talend + SQL Server DWH to Delta gold.

Implements the runbook in ``docs/cutover_runbook.md`` end to end against a ticket-scoped
schema: seed -> historical load -> freeze -> final incremental -> reconcile gate (100% match)
-> switch consumers (or rollback) -> rollback rehearsal -> Talend decommission checklist.
Every step is logged to ``ops.cutover_log``; reconciliation rows go to
``ops.reconciliation_results``.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from banking_etl.validation import fixtures, procs, reference_transform, specs
from banking_etl.validation.reconcile import ReconciliationReport, reconcile
from banking_etl.validation.tables import Schemas, ensure_schemas, write_table

RESULTS_TABLE = "reconciliation_results"
CUTOVER_LOG_TABLE = "cutover_log"
DEFAULT_HISTORICAL_CUTOFF = "2024-01-22 00:00:00"  # final CSV batch (22-01-2024) arrives as the "final incremental"

TALEND_DECOMMISSION_CHECKLIST = [
    "Disable Talend schedules/triggers for Load_DimBranch, Load_DimAccount, Load_DimCustomer, Load_FactTransaction",
    "Revoke Talend service account write access on DWH (INSERT/TRUNCATE on Dim*/FactTransaction)",
    "Archive talend_jobs/*.zip exports and DWH_DB_Connection / Sample_DB_Connection metadata",
    "Take final DWH backup (BACKUP DATABASE DWH) and retain per policy",
    "Set DWH read-only after the rollback window (ALTER DATABASE DWH SET READ_ONLY)",
    "Remove Talend job servers / JobServer agents after rollback window closes",
]

LOG_SCHEMA = T.StructType([
    T.StructField("cutover_id", T.StringType(), False),
    T.StructField("step_no", T.IntegerType(), False),
    T.StructField("step", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("dry_run", T.BooleanType(), False),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("finished_at", T.TimestampType()),
    T.StructField("details", T.StringType()),
])


@dataclass
class CutoverReport:
    cutover_id: str
    dry_run: bool
    steps: list[dict] = field(default_factory=list)
    reconciliation: ReconciliationReport | None = None
    consumers_on: str = "legacy"

    @property
    def succeeded(self) -> bool:
        return bool(self.reconciliation and self.reconciliation.green) and self.consumers_on == "gold"

    def summary(self) -> dict:
        return {"cutover_id": self.cutover_id, "dry_run": self.dry_run, "succeeded": self.succeeded,
                "consumers_on": self.consumers_on,
                "steps": [(s["step_no"], s["step"], s["status"]) for s in self.steps],
                "reconciliation": self.reconciliation.summary() if self.reconciliation else None}


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


class _Log:
    def __init__(self, report: CutoverReport):
        self.report = report

    def step(self, name: str, fn: Callable[[], tuple[str, dict]]):
        started = _now()
        status, details = fn()
        self.report.steps.append({
            "cutover_id": self.report.cutover_id, "step_no": len(self.report.steps) + 1, "step": name,
            "status": status, "dry_run": self.report.dry_run, "started_at": started, "finished_at": _now(),
            "details": json.dumps(details, default=str, sort_keys=True),
        })
        return status, details


def point_consumers(spark: SparkSession, names: Schemas, at: str) -> None:
    """(Re)create the consumer views. ``at`` is ``gold`` or ``legacy``; contract = gold column names."""
    for spec in specs.GOLD_TABLES:
        src = names.work_table(spec.gold if at == "gold" else f"legacy_{spec.gold}")
        cols = ", ".join(spec.gold_columns)
        spark.sql(f"CREATE OR REPLACE VIEW {names.work_table(f'consumer_{spec.gold}')} "
                  f"COMMENT 'cutover consumer view -> {at}' AS SELECT {cols} FROM {src}")


def consumers_target(spark: SparkSession, names: Schemas) -> str:
    """Which side the consumer views currently read, from the view comment."""
    row = spark.sql(f"DESCRIBE TABLE EXTENDED {names.work_table('consumer_fact_transaction')}") \
        .filter(F.col("col_name") == "Comment").first()
    return row["data_type"].rsplit("-> ", 1)[-1] if row else "unknown"


def seed(spark: SparkSession, names: Schemas, fixtures_path: str) -> dict:
    """Materialise source snapshots as ``bronze_*`` tables and legacy DWH snapshots as ``legacy_*``."""
    counts = {}
    for name, df in fixtures.load_sources(spark, fixtures_path).items():
        write_table(df, names.work_table(f"bronze_{name}"))
        counts[f"bronze_{name}"] = df.count()
    for name, df in fixtures.load_legacy_dwh(spark, fixtures_path).items():
        write_table(df, names.work_table(f"legacy_{name}"))
        counts[f"legacy_{name}"] = df.count()
    for name, df in fixtures.load_legacy_procs(spark, fixtures_path).items():
        write_table(df, names.work_table(f"legacy_proc_{name.lower()}"))
    write_table(fixtures.load_talend_rejects(spark, fixtures_path), names.work_table("legacy_talend_rejects"))
    return counts


def _sources(spark: SparkSession, names: Schemas) -> dict[str, DataFrame]:
    return {n: spark.table(names.work_table(f"bronze_{n}")) for n in specs.SOURCE_SCHEMAS}


def _legacy(spark: SparkSession, names: Schemas) -> dict[str, DataFrame]:
    return {s.gold: spark.table(names.work_table(f"legacy_{s.gold}")) for s in specs.GOLD_TABLES}


def _gold(spark: SparkSession, names: Schemas) -> dict[str, DataFrame]:
    return {s.gold: spark.table(names.work_table(s.gold)) for s in specs.GOLD_TABLES}


def historical_load(spark: SparkSession, names: Schemas, cutoff: str) -> dict:
    gold, rejects = reference_transform.build_gold(_sources(spark, names))
    gold["fact_transaction"] = gold["fact_transaction"].filter(F.col("transaction_date") < F.lit(cutoff).cast("timestamp"))
    for name, df in gold.items():
        write_table(df, names.work_table(name))
    write_table(rejects.filter(F.lit(False)), names.work_table("fact_transaction_rejects"))
    return {name: spark.table(names.work_table(name)).count() for name in gold}


def freeze(spark: SparkSession, names: Schemas) -> dict:
    """Capture the legacy freeze point (Talend schedules paused, source writes frozen)."""
    legacy = _legacy(spark, names)
    hw = legacy["fact_transaction"].agg(F.max("transaction_date").alias("max_ts"),
                                        F.max("transaction_id").alias("max_id")).first()
    return {"legacy_row_counts": {k: v.count() for k, v in legacy.items()},
            "legacy_high_watermark": {"transaction_date": hw["max_ts"], "transaction_id": hw["max_id"]}}


def final_incremental(spark: SparkSession, names: Schemas) -> dict:
    """Refresh dims (SCD-1 overwrite) and MERGE fact rows newer than the Delta watermark."""
    src = _sources(spark, names)
    gold, rejects = reference_transform.build_gold(src)
    for name in ("dim_branch", "dim_account", "dim_customer"):
        write_table(gold[name], names.work_table(name))
    fact_tbl = names.work_table("fact_transaction")
    watermark = spark.table(fact_tbl).agg(F.max("transaction_date")).first()[0]
    increment = gold["fact_transaction"]
    if watermark is not None:
        increment = increment.filter(F.col("transaction_date") > F.lit(watermark))
    n_incr = increment.count()
    increment.createOrReplaceTempView("t10_final_increment")
    cols = specs.FACT_TRANSACTION.gold_columns
    spark.sql(
        f"MERGE INTO {fact_tbl} t USING t10_final_increment s ON t.transaction_id = s.transaction_id "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"
    )
    write_table(rejects, names.work_table("fact_transaction_rejects"))
    return {"delta_watermark_before": watermark, "rows_in_increment": n_incr,
            "fact_rows_after": spark.table(fact_tbl).count(), "rejects": rejects.count()}


def run_reconciliation(spark: SparkSession, names: Schemas, run_label: str, run_id: str | None = None,
                       write: bool = True) -> ReconciliationReport:
    gold = _gold(spark, names)
    report = reconcile(
        spark, _legacy(spark, names), gold, run_label=run_label, run_id=run_id,
        legacy_procs={"sp_DailyTransaction": spark.table(names.work_table("legacy_proc_sp_dailytransaction")),
                      "sp_BalancePerCustomer": spark.table(names.work_table("legacy_proc_sp_balancepercustomer"))},
        target_procs=procs.run_all(gold),
        legacy_rejects=spark.table(names.work_table("legacy_talend_rejects")),
        target_rejects=spark.table(names.work_table("fact_transaction_rejects")),
    )
    if write:
        report.write(spark, names.ops_table(RESULTS_TABLE))
    return report


def _consumer_smoke(spark: SparkSession, names: Schemas) -> dict:
    legacy = _legacy(spark, names)
    out = {}
    for s in specs.GOLD_TABLES:
        out[s.gold] = {"consumer": spark.table(names.work_table(f"consumer_{s.gold}")).count(),
                       "legacy": legacy[s.gold].count()}
    return out


def dry_run_cutover(
    spark: SparkSession,
    names: Schemas,
    fixtures_path: str = fixtures.DEFAULT_FIXTURES,
    *,
    historical_cutoff: str = DEFAULT_HISTORICAL_CUTOFF,
    cutover_id: str | None = None,
    dry_run: bool = True,
    before_gate: Callable[[SparkSession, Schemas], None] | None = None,
) -> CutoverReport:
    """Run every runbook step. ``before_gate`` lets tests corrupt gold to exercise rollback."""
    report = CutoverReport(cutover_id or uuid.uuid4().hex, dry_run)
    log = _Log(report)
    ensure_schemas(spark, names.work, names.ops)

    log.step("seed_legacy_and_sources", lambda: ("OK", seed(spark, names, fixtures_path)))
    point_consumers(spark, names, "legacy")
    log.step("consumers_on_legacy", lambda: ("OK", {"consumers_on": consumers_target(spark, names)}))
    log.step("historical_load", lambda: ("OK", {"cutoff": historical_cutoff,
                                                "rows": historical_load(spark, names, historical_cutoff)}))
    log.step("freeze_legacy", lambda: ("OK", freeze(spark, names)))
    log.step("final_incremental", lambda: ("OK", final_incremental(spark, names)))
    if before_gate:
        before_gate(spark, names)

    def gate():
        rec = run_reconciliation(spark, names, run_label=f"cutover:{report.cutover_id}")
        report.reconciliation = rec
        return ("GREEN" if rec.green else "RED"), rec.summary()

    status, _ = log.step("reconcile_gate", gate)
    if status == "GREEN":
        def switch():
            point_consumers(spark, names, "gold")
            report.consumers_on = consumers_target(spark, names)
            return "OK", {"consumers_on": report.consumers_on, "smoke": _consumer_smoke(spark, names)}
        log.step("switch_consumers", switch)

        def rehearse():
            point_consumers(spark, names, "legacy")
            on_legacy = consumers_target(spark, names)
            smoke_legacy = _consumer_smoke(spark, names)
            point_consumers(spark, names, "gold")
            report.consumers_on = consumers_target(spark, names)
            ok = on_legacy == "legacy" and report.consumers_on == "gold"
            return ("OK" if ok else "FAILED"), {"rolled_back_to": on_legacy, "smoke_on_legacy": smoke_legacy,
                                                "re_switched_to": report.consumers_on}
        log.step("rollback_rehearsal", rehearse)
        log.step("talend_decommission",
                 lambda: ("SKIPPED_DRY_RUN" if dry_run else "PENDING_MANUAL",
                          {"checklist": TALEND_DECOMMISSION_CHECKLIST}))
    else:
        def rollback():
            point_consumers(spark, names, "legacy")
            report.consumers_on = consumers_target(spark, names)
            return "ROLLED_BACK", {"consumers_on": report.consumers_on,
                                   "failed_checks": [(r["table_name"], r["check_type"], r["check_name"])
                                                     for r in report.reconciliation.failed]}
        log.step("rollback", rollback)
        log.step("talend_decommission", lambda: ("BLOCKED", {"reason": "reconcile gate red; Talend stays live"}))

    write_table(spark.createDataFrame([tuple(s[f.name] for f in LOG_SCHEMA) for s in report.steps], LOG_SCHEMA),
                names.ops_table(CUTOVER_LOG_TABLE), mode="append")
    return report
