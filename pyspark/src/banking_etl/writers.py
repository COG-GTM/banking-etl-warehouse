"""Delta Lake table management replacing the Talend tMSSqlOutput components."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from banking_etl.config import WarehouseSettings
from banking_etl.readers import conform
from banking_etl.schemas import TableSpec

log = logging.getLogger(__name__)


def table_identifier(warehouse: WarehouseSettings, spec: TableSpec) -> str:
    if warehouse.path_based:
        return f"delta.`{warehouse.table_path(spec.name)}`"
    return warehouse.table_name(spec.name)


def ensure_table(spark: SparkSession, warehouse: WarehouseSettings, spec: TableSpec) -> str:
    """Create the Delta table (CREATE_IF_NOT_EXISTS, as in the Talend jobs)."""
    if not warehouse.path_based:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {warehouse.schema_name()}")
    identifier = table_identifier(warehouse, spec)
    comment = spec.comment.replace("'", "\\'")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {identifier} ({spec.ddl_columns()}) USING DELTA COMMENT '{comment}'"
    )
    return identifier


def read_table(spark: SparkSession, warehouse: WarehouseSettings, spec: TableSpec) -> DataFrame:
    return spark.table(table_identifier(warehouse, spec))


def write_table(
    spark: SparkSession,
    df: DataFrame,
    warehouse: WarehouseSettings,
    spec: TableSpec,
    write_mode: str,
) -> int:
    """Write ``df`` to the warehouse table and return the number of rows written.

    ``merge`` upserts on the primary key, ``overwrite`` replaces all rows.
    """
    identifier = ensure_table(spark, warehouse, spec)
    staged = conform(df, spec.struct).cache()
    rows = staged.count()
    view = f"_stage_{spec.name}"
    staged.createOrReplaceTempView(view)
    columns = ", ".join(spec.column_names)
    try:
        if write_mode == "overwrite":
            spark.sql(f"INSERT OVERWRITE TABLE {identifier} ({columns}) SELECT {columns} FROM {view}")
        elif write_mode == "merge":
            condition = " AND ".join(f"t.{key} = s.{key}" for key in spec.primary_key)
            updates = ", ".join(f"t.{c} = s.{c}" for c in spec.column_names if c not in spec.primary_key)
            values = ", ".join(f"s.{c}" for c in spec.column_names)
            spark.sql(
                f"MERGE INTO {identifier} t USING {view} s ON {condition} "
                f"WHEN MATCHED THEN UPDATE SET {updates} "
                f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
            )
        else:
            raise ValueError(f"Unsupported write mode: {write_mode}")
    finally:
        spark.catalog.dropTempView(view)
        staged.unpersist()
    log.info("Wrote %d rows to %s (%s)", rows, identifier, write_mode)
    return rows
