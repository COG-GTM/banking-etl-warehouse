"""Create the bronze / silver / gold Delta tables from the DDL files in this directory.

Usage as a Databricks job / notebook task::

    python -m databricks.schema.create_tables --catalog main --schema banking_dwh

Schema naming: gold tables are created in ``<schema>``, bronze in ``<schema>_bronze``
and silver in ``<schema>_silver`` (override with ``layer_schemas``).
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from pyspark.sql import SparkSession

SCHEMA_DIR = Path(__file__).resolve().parent
LAYERS = ("bronze", "silver", "gold")

_STATEMENT_SPLIT = re.compile(r";\s*(?:\n|$)")
_COMMENT_LINE = re.compile(r"^\s*--.*$", re.MULTILINE)
_CONSTRAINT_LINE = re.compile(r",?\s*CONSTRAINT\s+\w+\s+(?:PRIMARY|FOREIGN)\s+KEY[^\n]*NOT ENFORCED", re.IGNORECASE)
_USING_DELTA = re.compile(r"\bUSING\s+DELTA\b", re.IGNORECASE)


def default_layer_schemas(schema: str) -> dict[str, str]:
    return {"bronze": f"{schema}_bronze", "silver": f"{schema}_silver", "gold": schema}


def read_ddl(layer: str) -> str:
    return (SCHEMA_DIR / f"{layer}_ddl.sql").read_text(encoding="utf-8")


def render_ddl(ddl: str, catalog: str | None, schema: str, *, local: bool = False) -> str:
    """Substitute ``${catalog}`` / ``${schema}`` placeholders.

    With ``local=True`` the DDL is rewritten so it runs on a plain local SparkSession
    (no Unity Catalog, no Delta): the catalog qualifier is dropped, informational
    constraints are removed and ``USING DELTA`` becomes ``USING PARQUET``.
    """
    if local or not catalog:
        ddl = ddl.replace("${catalog}.", "")
    else:
        ddl = ddl.replace("${catalog}", catalog)
    ddl = ddl.replace("${schema}", schema)
    if local:
        ddl = _CONSTRAINT_LINE.sub("", ddl)
        ddl = _USING_DELTA.sub("USING PARQUET", ddl)
    return ddl


def split_statements(ddl: str) -> list[str]:
    ddl = _COMMENT_LINE.sub("", ddl)
    return [s.strip() for s in _STATEMENT_SPLIT.split(ddl) if s.strip()]


def create_layer_tables(
    spark: SparkSession, layer: str, catalog: str | None, schema: str, *, local: bool = False
) -> list[str]:
    statements = split_statements(render_ddl(read_ddl(layer), catalog, schema, local=local))
    for stmt in statements:
        spark.sql(stmt)
    return statements


def create_all_tables(
    spark: SparkSession,
    catalog: str | None,
    schema: str,
    *,
    layer_schemas: dict[str, str] | None = None,
    local: bool = False,
) -> dict[str, list[str]]:
    """Execute bronze, silver and gold DDL. Returns the statements run per layer."""
    layer_schemas = layer_schemas or default_layer_schemas(schema)
    return {
        layer: create_layer_tables(spark, layer, catalog, layer_schemas[layer], local=local)
        for layer in LAYERS
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", required=True, help="Unity Catalog name, e.g. main")
    parser.add_argument("--schema", required=True, help="Gold schema name, e.g. banking_dwh")
    parser.add_argument("--local", action="store_true", help="Rewrite DDL for a local non-UC Spark session")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    session = SparkSession.builder.appName("banking-dwh-create-tables").getOrCreate()
    result = create_all_tables(session, args.catalog, args.schema, local=args.local)
    for layer_name, stmts in result.items():
        print(f"{layer_name}: executed {len(stmts)} statements")
