"""Lakehouse Federation (SQL Server connection + foreign catalog) -- prod alternative to JDBC.

Guarded: nothing runs this automatically (``banking_etl.setup.provision`` does not read
``sql/setup/20_federation_sqlserver.sql``). Render it with :func:`federation_statements` and run it
explicitly (``scripts/bronze/create_federation.py --execute``). Needs ``CREATE CONNECTION`` and
``CREATE FOREIGN CATALOG`` on the metastore.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from string import Template

from banking_etl.setup.provision import quote, split_statements

SQL_FILE = Path(__file__).resolve().parents[3] / "sql" / "setup" / "20_federation_sqlserver.sql"


@dataclass(frozen=True)
class FederationOptions:
    host: str
    port: str = "1433"
    database: str = "sample"
    connection: str = "banking_etl_sqlserver"
    foreign_catalog: str = "banking_etl_sqlserver_sample"
    secret_scope: str = "banking-etl-sqlserver"


def _literal(value: str) -> str:
    if "'" in value or "\\" in value or not value:
        raise ValueError(f"invalid option value {value!r}")
    return f"'{value}'"


def federation_statements(options: FederationOptions, sql_file: Path | str = SQL_FILE) -> list[str]:
    variables = {
        "connection": quote(options.connection),
        "foreign_catalog": quote(options.foreign_catalog),
        "host": _literal(options.host),
        "port": _literal(str(options.port)),
        "database": _literal(options.database),
        "secret_scope": _literal(options.secret_scope),
    }
    return split_statements(Template(Path(sql_file).read_text(encoding="utf-8")).substitute(variables))


def foreign_table(options: FederationOptions, table: str) -> str:
    """``<foreign_catalog>.dbo.<table>``: drop-in source for ``read_jdbc`` when federating."""
    return f"{quote(options.foreign_catalog)}.dbo.{quote(table)}"
