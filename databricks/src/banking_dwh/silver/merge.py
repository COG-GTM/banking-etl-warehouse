"""SCD-1 MERGE helper for the Silver dimension loads of Ticket 6 (dim_branch, dim_account).

The Talend jobs ``Load_DimBranch`` / ``Load_DimAccount`` did a plain INSERT with
``DIE_ON_ERROR=false``: a rerun hit the primary key on every row and Talend dropped those rows,
so the DWH never picked up a changed source row. This port does a Delta ``MERGE INTO`` on the
natural key instead (SCD type 1):

* new key       -> INSERT
* changed row   -> UPDATE every business column and ``_loaded_at``
* unchanged row -> left alone (``_loaded_at`` keeps the time the row last changed), so reruns
  are idempotent and write no new data
* key missing from the source -> left alone (Talend never deleted either)

Bronze is append-only, so the source is first reduced to one row per key: the most recently
ingested row wins (``_ingested_at`` then ``_batch_id`` descending). Rows with a NULL key are
dropped and counted. Talend would have rejected them on the NOT NULL primary key.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Sequence

from ..common.config import EnvConfig, validate_identifier
from ..contracts.ddl import create_table_sql
from ..contracts.tables import TableContract

LOADED_AT = "_loaded_at"
_FQ_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){1,2}$")


@dataclass(frozen=True)
class MergeResult:
    target: str
    source_rows: int
    null_key_rows: int
    duplicate_key_rows: int
    merge_rows: int
    inserted: int
    updated: int
    target_rows: int
    loaded_at: str

    def as_dict(self) -> dict:
        return asdict(self)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def latest_per_key(df, key: Sequence[str], order_by: Sequence[str] = ("_ingested_at", "_batch_id")):
    """One row per key: the most recent by ``order_by`` (descending), ties broken by all columns."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    present = [c for c in order_by if c in df.columns]
    rest = [c for c in df.columns if c not in present and c not in key]
    w = Window.partitionBy(*key).orderBy(
        *[F.col(c).desc_nulls_last() for c in present], *[F.col(c).asc_nulls_last() for c in rest]
    )
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")


def ensure_table(spark, cfg: EnvConfig, contract: TableContract, *, unity_catalog: bool = True) -> str:
    spark.sql(create_table_sql(cfg, contract, unity_catalog=unity_catalog))
    return cfg.table(contract.layer, contract.name)


def merge_sql(target: str, source_view: str, key: Sequence[str], columns: Sequence[str]) -> str:
    """Delta MERGE statement: update on any business-column change (null-safe), insert new keys."""
    if not _FQ_NAME.match(target):
        raise ValueError(f"Invalid target table name: {target!r}")
    validate_identifier(source_view, "source view")
    for c in (*key, *columns):
        validate_identifier(c, "column")
    compare = [c for c in columns if c not in key and c != LOADED_AT]
    on = " AND ".join(f"t.{c} = s.{c}" for c in key)
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in compare) or "FALSE"
    updates = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in key)
    cols = ", ".join(columns)
    vals = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} AS t\nUSING {source_view} AS s\nON {on}\n"
        f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
    )


def _metric(row: dict, *names: str) -> int:
    for n in names:
        if row.get(n) is not None:
            return int(row[n])
    return 0


def scd1_merge(spark, cfg: EnvConfig, contract: TableContract, source_df, *,
               loaded_at: datetime | None = None, unity_catalog: bool = True) -> MergeResult:
    """MERGE ``source_df`` (already mapped to the contract's business columns) into the contract table."""
    from pyspark.sql import functions as F

    key = list(contract.primary_key)
    business = contract.business_columns
    columns = business + [LOADED_AT]
    loaded_at = loaded_at or utc_now()

    source_rows = source_df.count()
    keyed = source_df.filter(" AND ".join(f"{c} IS NOT NULL" for c in key))
    keyed_rows = keyed.count()
    deduped = latest_per_key(keyed, key)
    merge_rows = deduped.count()

    src = deduped.select(*business).withColumn(LOADED_AT, F.lit(loaded_at).cast("timestamp"))
    target = ensure_table(spark, cfg, contract, unity_catalog=unity_catalog)
    view = f"__t6_src_{contract.name}_{uuid.uuid4().hex[:8]}"
    src.createOrReplaceTempView(view)
    try:
        out = spark.sql(merge_sql(target, view, key, columns)).collect()
    finally:
        spark.catalog.dropTempView(view)
    m = out[0].asDict() if out else {}
    return MergeResult(
        target=target,
        source_rows=source_rows,
        null_key_rows=source_rows - keyed_rows,
        duplicate_key_rows=keyed_rows - merge_rows,
        merge_rows=merge_rows,
        inserted=_metric(m, "num_inserted_rows"),
        updated=_metric(m, "num_updated_rows"),
        target_rows=spark.table(target).count(),
        loaded_at=loaded_at.isoformat(sep=" "),
    )


def run_pk_checks(spark, cfg: EnvConfig, contract: TableContract, *, dq_run_id: str | None = None,
                  fail_on_error: bool = True) -> list[dict]:
    """T2 primary-key DQ checks for one table; optionally appended to ops.dq_results."""
    from ..dq.checks import assert_all_passed, primary_key_checks, results_as_dicts, write_results

    results = primary_key_checks(spark, cfg, [contract])
    if dq_run_id:
        write_results(spark, cfg, results, dq_run_id)
    if fail_on_error:
        assert_all_passed(results)
    return results_as_dicts(results)
