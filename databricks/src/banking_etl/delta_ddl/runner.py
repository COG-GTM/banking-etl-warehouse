"""Apply the gold Delta DDL: ``python runner.py --catalog banking_dwh --schema gold``."""

from __future__ import annotations

import argparse
import inspect
import sys
from pathlib import Path

try:
    from banking_etl.delta_ddl import DEFAULT_CATALOG, DEFAULT_SCHEMA, apply_ddl
except (
    ImportError
):  # run as a plain file (spark_python_task): put databricks/src on sys.path
    # Serverless spark_python_task exec()s the file without defining __file__.
    _this_file = globals().get("__file__") or inspect.currentframe().f_code.co_filename
    sys.path.insert(0, str(Path(_this_file).resolve().parents[2]))
    from banking_etl.delta_ddl import DEFAULT_CATALOG, DEFAULT_SCHEMA, apply_ddl


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument(
        "--skip-constraints",
        action="store_true",
        help="Do not add the informational PK/FK constraints (needed outside Unity Catalog).",
    )
    parser.add_argument(
        "--skip-create-schema",
        action="store_true",
        help="Assume the schema already exists.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, spark=None) -> list[str]:
    args = parse_args(argv)
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    statements = apply_ddl(
        spark,
        catalog=args.catalog,
        schema=args.schema,
        include_schema=not args.skip_create_schema,
        include_constraints=not args.skip_constraints,
    )
    print(f"Applied {len(statements)} DDL statements to {args.catalog}.{args.schema}")
    return statements


if __name__ == "__main__":
    main()
