#!/usr/bin/env python3
"""Create (if missing) the SQL Server JDBC secret scope and put its keys from env vars.

Required env vars (never commit their values):
  BANKING_ETL_JDBC_HOST, BANKING_ETL_JDBC_PORT, BANKING_ETL_JDBC_DATABASE,
  BANKING_ETL_JDBC_USER, BANKING_ETL_JDBC_PASSWORD

  python scripts/setup/create_secret_scope.py [--scope banking-etl-sqlserver] [--reader <principal> ...]
"""
import argparse

import _bootstrap  # noqa: F401

from banking_etl.setup.secrets import SECRET_KEYS, ensure_scope, grant_read, put_values, read_values


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="banking-etl-sqlserver")
    ap.add_argument("--reader", action="append", default=[], help="principal granted READ on the scope (repeatable)")
    a = ap.parse_args()

    values = read_values()
    created = ensure_scope(a.scope)
    put_values(a.scope, values)
    grant_read(a.scope, a.reader)
    print(f"scope {a.scope}: {'created' if created else 'exists'}; keys put: {', '.join(SECRET_KEYS)}")


if __name__ == "__main__":
    main()
