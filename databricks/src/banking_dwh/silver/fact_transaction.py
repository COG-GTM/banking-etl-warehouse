"""Silver ``fact_transaction`` load (Ticket 8), port of Talend ``Load_FactTransaction``.

Talend job (``talend_jobs/Load_FactTransaction.zip`` -> ``Load_FactTransaction_0.1.item``)::

    tDBInput_1 (sample.dbo.transaction_db)  --row1--+
    tFileInputExcel_1 (Sheet1, header 1)    --row2--+--> tUnite_1 --row4--> tUniqRow_1 (key transaction_id)
    tFileInputDelimited_1 (CSV, header 1)   --row3--+        --UNIQUE row5--> tMap_1 (1:1 rename)
                                                             --> tDBOutput_1 (DWH.FactTransaction,
                                                                 TABLE_ACTION=TRUNCATE, DATA_ACTION=INSERT,
                                                                 DIE_ON_ERROR=false)

Port, step by step:

1. **Read** the three Bronze contracts. SQL Server: only the latest extraction batch (``_batch_id`` of
   the newest ``_ingested_at``), which is what Talend's full-table SELECT saw. Files: all ingested rows
   (the Bronze file tables are already typed, nothing is re-parsed).
2. **Conform** to the ``FACT_TRANSACTION`` columns and types (``amount`` INT -> DECIMAL(19,4)) and tag each
   row with ``_source_system`` and a source priority that mirrors tUnite's input order
   ``row1`` SQL (1), ``row2`` Excel (2), ``row3`` CSV (3).
3. **Union** with ``unionByName`` (tUnite).
4. **Input rejects**: rows Talend could never have inserted are quarantined before dedup, like the
   file components' unconnected REJECT flows (``DIE_ON_ERROR=false``): NULL ``transaction_id`` (PK) and
   Bronze rows with ``_rescued_data`` (a value failed to parse).
5. **Deduplicate** on ``transaction_id`` (tUniqRow keeps the first occurrence in tUnite order):
   ``row_number()`` over ``transaction_id`` ordered by source priority, then newest ``_ingested_at``,
   then ``_source_file`` and the business columns so the winner is deterministic.
6. **Referential integrity**: legacy ``FactTransaction`` had enforced FKs to ``DimAccount`` /
   ``DimBranch``, so an orphan row failed its INSERT and was dropped (DIE_ON_ERROR=false, no reject
   flow). Default ``orphan_policy="quarantine"`` reproduces that result but records each orphan in
   ``ops.fact_transaction_rejects``; ``"load"`` loads them anyway (the post-load RI check then fails),
   ``"fail"`` aborts before writing.
7. **Write** with a Delta MERGE on ``transaction_id`` that inserts new keys, updates changed rows and
   deletes keys no longer produced by the sources (``WHEN NOT MATCHED BY SOURCE``). The table ends up
   exactly as Talend's TRUNCATE + INSERT would leave it, but unchanged rows are not rewritten, so a
   rerun on the same input changes nothing and the Change Data Feed only shows real changes.
8. **DQ**: Ticket 2's ``pk_*`` and ``FK_FactTransaction_*`` checks on the loaded table, plus one
   ``<FK>.quarantined`` result per foreign key, appended to ``ops.dq_results``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, replace
from typing import Any

from ..common.config import EnvConfig
from ..contracts.ddl import create_table_sql
from ..contracts.tables import (
    BRONZE_FILE_TRANSACTION_CSV,
    BRONZE_FILE_TRANSACTION_EXCEL,
    BRONZE_SQLSERVER_TRANSACTION,
    DQ_RESULTS,
    FACT_TRANSACTION,
    Column,
    TableContract,
)
from ..dq.checks import DataQualityError, DQResult, primary_key_checks, referential_integrity_checks, write_results


@dataclass(frozen=True)
class SourceStream:
    source_system: str
    contract: TableContract
    priority: int  # tUnite input order: lower wins in tUniqRow


# Order of the incoming tUnite_1 connections in the .item XML: row1 (SQL), row2 (Excel), row3 (CSV).
SOURCES = (
    SourceStream("sqlserver", BRONZE_SQLSERVER_TRANSACTION, 1),
    SourceStream("excel", BRONZE_FILE_TRANSACTION_EXCEL, 2),
    SourceStream("csv", BRONZE_FILE_TRANSACTION_CSV, 3),
)

BUSINESS_COLUMNS = FACT_TRANSACTION.business_columns
ORPHAN_POLICIES = ("quarantine", "load", "fail")

REJECT_NULL_KEY = "null_transaction_id"
REJECT_RESCUED = "bronze_rescued_data"
REJECT_DUPLICATE = "duplicate_transaction_id"

FACT_TRANSACTION_REJECTS = TableContract(
    "ops", "fact_transaction_rejects",
    (
        Column("run_id", "STRING", False, "Load run that produced this reject."),
        Column("reject_reason", "STRING", False,
               "null_transaction_id / bronze_rescued_data / duplicate_transaction_id / "
               "FK_FactTransaction_DimAccount / FK_FactTransaction_DimBranch"),
        *[replace(c, nullable=True) for c in FACT_TRANSACTION.columns if c.name in BUSINESS_COLUMNS],
        Column("_source_system", "STRING", False, "Source stream of the rejected row: sqlserver / excel / csv."),
        Column("_source_file", "STRING", True, "Landed file, or sqlserver:<table>#<batch_id>."),
        Column("_ingested_at", "TIMESTAMP", True, "Bronze ingestion timestamp of the rejected row."),
        Column("details", "STRING", True, "JSON with reason-specific details."),
        Column("rejected_at", "TIMESTAMP", False, ""),
    ),
    "Rows the fact_transaction load did not insert, with the reason (current state; each run replaces it). "
    "Orphans here are rows legacy SQL Server rejected through FK_FactTransaction_* on INSERT.",
    owner_ticket=8,
)

_STAGE_EXTRA = ("_source_system", "_source_file", "_ingested_at", "_rescued_data", "_source_priority")


# --------------------------------------------------------------------------------------
# Read + conform
# --------------------------------------------------------------------------------------
def _cast_to(col_name: str, current_type: str, target_type: str):
    from pyspark.sql import functions as F

    col = F.col(col_name)
    return (col if current_type.lower() == target_type.lower() else col.cast(target_type)).alias(col_name)


def conform_stream(df, stream: SourceStream):
    """Bronze rows of one stream -> FACT_TRANSACTION business columns + lineage columns."""
    from pyspark.sql import functions as F

    types = dict(df.dtypes)
    target = {c.name: c.type for c in FACT_TRANSACTION.columns}
    cols = [_cast_to(c, types[c], target[c]) for c in BUSINESS_COLUMNS]
    if "_source_file" in types:
        source_file = F.col("_source_file")
    else:
        source_file = F.concat(F.lit(f"sqlserver:{stream.contract.name}#"), F.col("_batch_id"))
    rescued = F.col("_rescued_data") if "_rescued_data" in types else F.lit(None).cast("string")
    return df.select(
        *cols,
        F.lit(stream.source_system).alias("_source_system"),
        source_file.cast("string").alias("_source_file"),
        F.col("_ingested_at").cast("timestamp").alias("_ingested_at"),
        rescued.cast("string").alias("_rescued_data"),
        F.lit(stream.priority).alias("_source_priority"),
    )


def latest_batch(df):
    """Rows of the newest JDBC extraction batch (Bronze JDBC tables are append-only snapshots)."""
    from pyspark.sql import functions as F

    newest = df.orderBy(F.col("_ingested_at").desc(), F.col("_batch_id").desc()).select("_batch_id").limit(1)
    return df.join(newest, on="_batch_id", how="left_semi")


def read_streams(spark, cfg: EnvConfig) -> dict[str, Any]:
    streams = {}
    for s in SOURCES:
        df = spark.table(cfg.table(s.contract.layer, s.contract.name))
        if s.source_system == "sqlserver":
            df = latest_batch(df)
        streams[s.source_system] = conform_stream(df, s)
    return streams


# --------------------------------------------------------------------------------------
# Union, input rejects, dedup, RI
# --------------------------------------------------------------------------------------
def union_streams(streams):
    """tUnite: union by column name, SQL / Excel / CSV."""
    streams = list(streams)
    out = streams[0]
    for df in streams[1:]:
        out = out.unionByName(df)
    return out


def split_input_rejects(df):
    """(valid, rejects): rows Talend could not have inserted (NULL PK, unparsable Bronze values)."""
    from pyspark.sql import functions as F

    reason = (
        F.when(F.col("transaction_id").isNull(), F.lit(REJECT_NULL_KEY))
        .when(F.col("_rescued_data").isNotNull(), F.lit(REJECT_RESCUED))
    )
    tagged = df.withColumn("_reject_reason", reason)
    rejects = tagged.filter(F.col("_reject_reason").isNotNull()).select(
        "*", F.to_json(F.struct(F.col("_rescued_data").alias("rescued_data"))).alias("_details")
    )
    return tagged.filter(F.col("_reject_reason").isNull()).drop("_reject_reason"), rejects


def deduplicate(df):
    """tUniqRow on transaction_id, first occurrence in tUnite order wins. Returns (winners, losers)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = [
        F.col("_source_priority").asc(),
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
        *[F.col(c).asc_nulls_last() for c in BUSINESS_COLUMNS if c != "transaction_id"],
    ]
    w = Window.partitionBy("transaction_id").orderBy(*order)
    ranked = df.withColumn("_rn", F.row_number().over(w)).withColumn(
        "_winner_source", F.first("_source_system").over(w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    )
    winners = ranked.filter("_rn = 1").drop("_rn", "_winner_source")
    losers = ranked.filter("_rn > 1").select(
        *[c for c in df.columns],
        F.lit(REJECT_DUPLICATE).alias("_reject_reason"),
        F.to_json(F.struct(F.col("_winner_source").alias("kept_source"))).alias("_details"),
    )
    return winners, losers


def split_orphans(spark, cfg: EnvConfig, df):
    """(valid, orphan_rejects): one reject row per violated FK (NULL FKs pass, as in SQL Server)."""
    from pyspark.sql import functions as F

    valid = df
    rejects = None
    for fk in FACT_TRANSACTION.foreign_keys:
        parent = spark.table(cfg.table(FACT_TRANSACTION.layer, fk.ref_table)).select(
            *[F.col(rc).alias(c) for c, rc in zip(fk.columns, fk.ref_columns)]
        ).distinct()
        non_null = df.filter(" AND ".join(f"{c} IS NOT NULL" for c in fk.columns))
        orphans = non_null.join(parent, on=list(fk.columns), how="left_anti").select(
            *df.columns,
            F.lit(fk.name).alias("_reject_reason"),
            F.to_json(F.struct(
                F.lit(",".join(fk.columns)).alias("columns"),
                F.lit(f"{cfg.table(FACT_TRANSACTION.layer, fk.ref_table)}({', '.join(fk.ref_columns)})").alias("references"),
            )).alias("_details"),
        )
        rejects = orphans if rejects is None else rejects.unionByName(orphans)
        valid = valid.join(orphans.select("transaction_id"), on="transaction_id", how="left_anti")
    return valid, rejects


# --------------------------------------------------------------------------------------
# Writes
# --------------------------------------------------------------------------------------
def ensure_rejects_table(spark, cfg: EnvConfig, *, unity_catalog: bool = True) -> str:
    spark.sql(create_table_sql(cfg, FACT_TRANSACTION_REJECTS, unity_catalog=unity_catalog))
    return cfg.table(FACT_TRANSACTION_REJECTS.layer, FACT_TRANSACTION_REJECTS.name)


def write_rejects(spark, cfg: EnvConfig, rejects, run_id: str) -> int:
    """Replace the rejects table with this run's rejects (current state, idempotent on rerun)."""
    from pyspark.sql import functions as F

    target = cfg.table(FACT_TRANSACTION_REJECTS.layer, FACT_TRANSACTION_REJECTS.name)
    types = {c.name: c.type for c in FACT_TRANSACTION_REJECTS.columns}
    out = rejects.select(
        F.lit(run_id).alias("run_id"),
        F.col("_reject_reason").alias("reject_reason"),
        *BUSINESS_COLUMNS,
        "_source_system", "_source_file", "_ingested_at",
        F.col("_details").alias("details"),
        F.current_timestamp().alias("rejected_at"),
    ).select(*[F.col(c).cast(types[c]).alias(c) for c in FACT_TRANSACTION_REJECTS.column_names])
    out.write.insertInto(target, overwrite=True)
    return spark.table(target).count()


def merge_fact(spark, cfg: EnvConfig, df) -> dict[str, int]:
    """MERGE the deduplicated rows into silver.fact_transaction; returns Delta operation metrics."""
    target = cfg.table(FACT_TRANSACTION.layer, FACT_TRANSACTION.name)
    view = f"t8_fact_transaction_src_{uuid.uuid4().hex}"
    df.select(*BUSINESS_COLUMNS, "_source_system").createOrReplaceTempView(view)
    compared = [*BUSINESS_COLUMNS[1:], "_source_system"]
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in compared)
    cols = [*BUSINESS_COLUMNS, "_source_system"]
    try:
        spark.sql(f"""
            MERGE INTO {target} AS t
            USING {view} AS s
            ON t.transaction_id = s.transaction_id
            WHEN MATCHED AND ({changed}) THEN UPDATE SET
              {", ".join(f"t.{c} = s.{c}" for c in compared)}, t._loaded_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}, _loaded_at)
              VALUES ({", ".join(f"s.{c}" for c in cols)}, current_timestamp())
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)
    finally:
        spark.catalog.dropTempView(view)
    hist = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()[0]
    m = hist["operationMetrics"] or {}
    if hist["operation"] != "MERGE":
        return {"inserted": 0, "updated": 0, "deleted": 0}
    return {
        "inserted": int(m.get("numTargetRowsInserted", 0)),
        "updated": int(m.get("numTargetRowsUpdated", 0)),
        "deleted": int(m.get("numTargetRowsDeleted", 0)),
    }


# --------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------
def _count_by(df, col: str) -> dict[str, int]:
    return {r[0]: int(r[1]) for r in df.groupBy(col).count().collect()}


def quarantine_results(cfg: EnvConfig, orphan_counts: dict[str, int]) -> list[DQResult]:
    rejects_fq = cfg.table(FACT_TRANSACTION_REJECTS.layer, FACT_TRANSACTION_REJECTS.name)
    out = []
    for fk in FACT_TRANSACTION.foreign_keys:
        n = int(orphan_counts.get(fk.name, 0))
        out.append(DQResult(
            f"{fk.name}.quarantined", "referential_integrity_quarantine", rejects_fq, n == 0, n,
            json.dumps({"fact": cfg.table(FACT_TRANSACTION.layer, FACT_TRANSACTION.name),
                        "rejects": rejects_fq, "reject_reason": fk.name}),
        ))
    return out


def load_fact_transaction(
    spark,
    cfg: EnvConfig,
    *,
    run_id: str | None = None,
    orphan_policy: str = "quarantine",
    unity_catalog: bool = True,
    write_dq: bool = True,
) -> dict[str, Any]:
    """Run the full load; returns a JSON-serialisable summary (counts, merge metrics, DQ results)."""
    if orphan_policy not in ORPHAN_POLICIES:
        raise ValueError(f"orphan_policy must be one of {ORPHAN_POLICIES}, got {orphan_policy!r}")
    run_id = run_id or str(uuid.uuid4())
    ensure_rejects_table(spark, cfg, unity_catalog=unity_catalog)

    streams = read_streams(spark, cfg)
    source_counts = {k: v.count() for k, v in streams.items()}
    unioned = union_streams(streams[s.source_system] for s in SOURCES)
    union_count = unioned.count()

    valid, input_rejects = split_input_rejects(unioned)
    winners, losers = deduplicate(valid)
    dedup_count = winners.count()
    loadable, orphans = split_orphans(spark, cfg, winners)
    orphan_counts = _count_by(orphans, "_reject_reason")
    orphan_rows = orphans.select("transaction_id").distinct().count()

    if orphan_policy == "fail" and orphan_rows:
        raise DataQualityError(f"{orphan_rows} fact_transaction row(s) violate foreign keys: {orphan_counts}")
    if orphan_policy == "load":
        loadable = winners
        rejects = input_rejects.unionByName(losers)
    else:
        rejects = input_rejects.unionByName(losers).unionByName(orphans)

    merge_metrics = merge_fact(spark, cfg, loadable)
    rejects_count = write_rejects(spark, cfg, rejects, run_id)

    fact_fq = cfg.table(FACT_TRANSACTION.layer, FACT_TRANSACTION.name)
    fact = spark.table(fact_fq)
    final_count = fact.count()

    results = primary_key_checks(spark, cfg, [FACT_TRANSACTION]) + referential_integrity_checks(
        spark, cfg, [FACT_TRANSACTION]
    )
    if orphan_policy == "quarantine":
        results += quarantine_results(cfg, orphan_counts)
    if write_dq:
        write_results(spark, cfg, results, run_id)

    rejects_fq = cfg.table(FACT_TRANSACTION_REJECTS.layer, FACT_TRANSACTION_REJECTS.name)
    return {
        "run_id": run_id,
        "orphan_policy": orphan_policy,
        "fact_table": fact_fq,
        "rejects_table": rejects_fq,
        "source_counts": source_counts,
        "union_count": union_count,
        "input_rejects": input_rejects.count(),
        "dedup_count": dedup_count,
        "duplicates_removed": losers.count(),
        "orphan_rows": orphan_rows,
        "orphans_by_fk": orphan_counts,
        "merge": merge_metrics,
        "final_count": final_count,
        "winners_by_source": _count_by(fact, "_source_system"),
        "rejects_by_reason": _count_by(spark.table(rejects_fq), "reject_reason"),
        "rejects_count": rejects_count,
        "dq": [
            {"check_name": r.check_name, "check_type": r.check_type, "passed": r.passed, "failed_rows": r.failed_rows}
            for r in results
        ],
        "dq_table": cfg.table(DQ_RESULTS.layer, DQ_RESULTS.name) if write_dq else None,
    }


def dq_failures(summary: dict[str, Any], *, include_quarantine: bool = False) -> list[dict]:
    """Failed checks from a load summary; quarantine results only when asked for."""
    return [
        r for r in summary["dq"]
        if not r["passed"] and (include_quarantine or r["check_type"] != "referential_integrity_quarantine")
    ]
