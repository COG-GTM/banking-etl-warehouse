"""Shared plumbing for the job entrypoints (logging, dimension load semantics)."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from .config import EtlConfig
from .io import execute_jdbc, read_jdbc, table_exists, write_jdbc

log = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def load_table(
    spark: SparkSession,
    cfg: EtlConfig,
    df: DataFrame,
    table: str,
    key: str,
    mode: str,
    referenced_by_fk: bool = True,
) -> int:
    """Loads ``df`` into an existing DWH table and returns the number of rows inserted.

    ``append`` mirrors the Talend tMSSqlOutput setup (INSERT, die-on-error off): rows whose
    key already exists are skipped instead of failing the load, so re-runs are idempotent.
    ``overwrite`` replaces the table contents. Tables nobody references (FactTransaction) are
    TRUNCATEd, like Talend's TABLE_ACTION=TRUNCATE. SQL Server refuses TRUNCATE on tables
    referenced by a foreign key (the dimensions), so those use DELETE + INSERT, which only
    succeeds once FactTransaction no longer references the rows (see run_all
    ``--full-refresh``).
    """
    if mode == "overwrite" and not referenced_by_fk:
        df = df.cache()
        count = df.count()
        write_jdbc(df, cfg.dwh_db, table, mode="overwrite")
        log.info("%s: truncated and inserted %d rows", table, count)
        df.unpersist()
        return count
    if mode == "overwrite":
        execute_jdbc(spark, cfg.dwh_db, [f"DELETE FROM {cfg.dwh_db.qualified(table)}"])
        to_insert = df
    elif mode == "append":
        to_insert = df
        if table_exists(spark, cfg.dwh_db, table):
            existing = read_jdbc(spark, cfg.dwh_db, table=table).select(key)
            to_insert = df.join(existing, on=key, how="left_anti")
    else:
        raise ValueError(f"Unsupported mode {mode!r}")

    to_insert = to_insert.cache()
    count = to_insert.count()
    skipped = df.count() - count
    write_jdbc(to_insert, cfg.dwh_db, table, mode="append")
    log.info("%s: inserted %d rows (%d existing keys skipped)", table, count, skipped)
    to_insert.unpersist()
    return count
