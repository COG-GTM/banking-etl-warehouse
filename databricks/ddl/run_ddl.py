"""Execute the warehouse DDL in dependency order.

On Databricks the statements run as written against Unity Catalog. Locally
(``--local``) the same files run against an open-source Delta session by
rewriting the three-level names ``banking.<schema>.<table>`` into two-level
``banking_<schema>.<table>`` names, skipping ``CREATE CATALOG`` and skipping the
Unity-Catalog-only informational constraints.

    python databricks/ddl/run_ddl.py --local --warehouse-dir /tmp/spark-warehouse
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Iterable, List

DDL_DIR = Path(__file__).resolve().parent
CATALOG = "banking"
SCHEMAS = ("bronze", "silver", "gold")

# Executed in dependency order: containers, then bronze, silver, gold, then the
# constraints that reference the gold tables.
DDL_FILES: tuple[str, ...] = (
    "00_catalog_and_schemas.sql",
    "01_bronze.sql",
    "02_silver.sql",
    "03_gold.sql",
    "04_constraints.sql",
)

# Unity Catalog only: open-source Delta rejects PRIMARY KEY / FOREIGN KEY.
LOCAL_SKIPPED_FILES = frozenset({"04_constraints.sql"})


def split_statements(sql: str) -> List[str]:
    """Split a SQL script into statements, ignoring ``--`` comments."""
    statements: List[str] = []
    current: List[str] = []
    in_string = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if in_string:
            current.append(char)
            if char == "'":
                in_string = False
            i += 1
        elif char == "'":
            in_string = True
            current.append(char)
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = len(sql) if end == -1 else end
        elif char == ";":
            statements.append("".join(current))
            current = []
            i += 1
        else:
            current.append(char)
            i += 1
    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def localize(statement: str) -> str | None:
    """Rewrite a Unity Catalog statement for a plain local Delta session."""
    if re.match(r"(?is)^\s*CREATE\s+CATALOG\b", statement):
        return None
    for schema in SCHEMAS:
        statement = statement.replace(f"{CATALOG}.{schema}", f"{CATALOG}_{schema}")
    return statement


def statements_for(path: Path, local: bool) -> List[str]:
    if local and path.name in LOCAL_SKIPPED_FILES:
        return []
    statements = split_statements(path.read_text())
    if not local:
        return statements
    return [s for s in (localize(s) for s in statements) if s]


def run_ddl(spark, ddl_dir: Path = DDL_DIR, local: bool = False, files: Iterable[str] = DDL_FILES) -> List[str]:
    """Run every DDL statement and return the statements that were executed."""
    executed: List[str] = []
    for name in files:
        for statement in statements_for(ddl_dir / name, local):
            spark.sql(statement)
            executed.append(statement)
    return executed


def local_session_builder(warehouse_dir: str, app_name: str = "banking-ddl"):
    """Builder for a local Delta-enabled Spark session.

    Delta's jars are normally resolved from Maven by ``delta-spark``. Set
    ``DELTA_JARS`` to a colon-separated list of local jar paths when Maven is
    not reachable.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
    )
    delta_jars = os.environ.get("DELTA_JARS")
    if delta_jars:
        return builder.config("spark.jars", ",".join(delta_jars.split(":")))

    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(builder)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="run against a local Delta-enabled Spark session")
    parser.add_argument("--warehouse-dir", default="spark-warehouse", help="local warehouse directory")
    args = parser.parse_args()

    if args.local:
        spark = local_session_builder(args.warehouse_dir).getOrCreate()
    else:  # on Databricks the notebook/job session is already available
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    executed = run_ddl(spark, local=args.local)
    print(f"executed {len(executed)} statements")


if __name__ == "__main__":
    main()
