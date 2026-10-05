#!/usr/bin/env python3
"""GUARDED: create the Lakehouse Federation SQL Server connection + foreign catalog (prod alternative to JDBC).

Prints the SQL by default; runs it only with --execute (needs CREATE CONNECTION / CREATE FOREIGN CATALOG).

  python scripts/bronze/create_federation.py --host sql.example.internal [--port 1433] [--database sample] \
      [--connection banking_etl_sqlserver] [--foreign-catalog banking_etl_sqlserver_sample] \
      [--secret-scope banking-etl-sqlserver] [--execute --warehouse-id 565cd2fd713738c4]
"""
import argparse

import _bootstrap  # noqa: F401

from banking_etl.bronze.federation import FederationOptions, federation_statements
from banking_etl.setup.cli import warehouse_executor


def main() -> None:
    d = FederationOptions(host="")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", default=d.port)
    ap.add_argument("--database", default=d.database)
    ap.add_argument("--connection", default=d.connection)
    ap.add_argument("--foreign-catalog", default=d.foreign_catalog)
    ap.add_argument("--secret-scope", default=d.secret_scope)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--warehouse-id")
    a = ap.parse_args()

    opts = FederationOptions(a.host, a.port, a.database, a.connection, a.foreign_catalog, a.secret_scope)
    statements = federation_statements(opts)
    if not a.execute:
        print(";\n\n".join(statements) + ";")
        return
    if not a.warehouse_id:
        ap.error("--warehouse-id is required with --execute")
    execute = warehouse_executor(a.warehouse_id)
    for stmt in statements:
        print(stmt.splitlines()[0])
        execute(stmt)


if __name__ == "__main__":
    main()
