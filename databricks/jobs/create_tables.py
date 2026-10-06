"""Create the dwh schema and Delta tables from ddl/01_create_tables.sql (idempotent)."""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import get_param, get_spark  # noqa: E402

DDL_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ddl", "01_create_tables.sql")


def main() -> None:
    spark = get_spark()
    catalog = get_param("catalog", spark=spark)
    schema = get_param("target_schema", spark=spark)
    target = f"{catalog}.{schema}" if catalog else schema

    with open(get_param("ddl_path", DDL_FILE, spark=spark)) as fh:
        sql = re.sub(r"--[^\n]*", "", fh.read())
    for stmt in filter(None, (s.strip() for s in sql.split(";"))):
        stmt = re.sub(r"\bdwh\b", target, stmt)
        print(stmt.splitlines()[0])
        spark.sql(stmt)


if __name__ == "__main__":
    main()
