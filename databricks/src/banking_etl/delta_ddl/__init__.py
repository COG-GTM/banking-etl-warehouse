"""Unity Catalog Delta DDL for the gold star schema.

Translated from the SQL Server DDL in ``sql_scripts/01_create_tables.sql``
(DimAccount, DimBranch, DimCustomer, FactTransaction). The SQL lives in
``sql/`` with ``${catalog}`` / ``${schema}`` placeholders.
"""

from __future__ import annotations

import re
from pathlib import Path
from string import Template

SQL_DIR = Path(__file__).resolve().parent / "sql"
SCHEMA_FILE = "00_schema.sql"
TABLE_FILES = (
    "01_dim_account.sql",
    "02_dim_branch.sql",
    "03_dim_customer.sql",
    "04_fact_transaction.sql",
)
CONSTRAINTS_FILE = "90_constraints.sql"

DEFAULT_CATALOG = "banking_dwh"
DEFAULT_SCHEMA = "gold"

GOLD_TABLES = ("dim_account", "dim_branch", "dim_customer", "fact_transaction")

SQLSERVER_TO_DELTA_TYPES = {
    "INT": "INT",
    "MONEY": "DECIMAL(19,4)",
    "DATETIME": "TIMESTAMP",
    "DATE": "DATE",
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VARCHAR = re.compile(r"^VARCHAR\s*\(\s*(\d+)\s*\)$", re.IGNORECASE)


def delta_type_for(sqlserver_type: str) -> str:
    """Map a SQL Server column type from the source DDL to its Delta type."""
    normalized = sqlserver_type.strip().upper()
    varchar = _VARCHAR.match(normalized)
    if varchar:
        return f"VARCHAR({varchar.group(1)})"
    try:
        return SQLSERVER_TO_DELTA_TYPES[normalized]
    except KeyError:
        raise ValueError(
            f"No Delta mapping for SQL Server type {sqlserver_type!r}"
        ) from None


def quote_identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Invalid catalog/schema identifier: {name!r}")
    return f"`{name}`"


def split_statements(sql: str) -> list[str]:
    """Split a SQL script on ``;`` outside quotes, dropping ``--`` comments."""
    statements, current, quote = [], [], None
    i = 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            current.append(ch)
            if ch == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    current.append(sql[i + 1])
                    i += 1
                else:
                    quote = None
        elif ch in ("'", "`", '"'):
            quote = ch
            current.append(ch)
        elif sql.startswith("--", i):
            newline = sql.find("\n", i)
            i = len(sql) if newline == -1 else newline
            continue
        elif ch == ";":
            statements.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1
    statements.append("".join(current).strip())
    return [s for s in statements if s]


def render_file(name: str, catalog: str, schema: str) -> list[str]:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    rendered = Template(text).substitute(
        catalog=quote_identifier(catalog), schema=quote_identifier(schema)
    )
    return split_statements(rendered)


def render_statements(
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    include_schema: bool = True,
    include_constraints: bool = True,
) -> list[str]:
    files = ([SCHEMA_FILE] if include_schema else []) + list(TABLE_FILES)
    if include_constraints:
        files.append(CONSTRAINTS_FILE)
    return [stmt for name in files for stmt in render_file(name, catalog, schema)]


def apply_ddl(
    spark,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    include_schema: bool = True,
    include_constraints: bool = True,
) -> list[str]:
    """Create the gold schema and tables (idempotent) and return the executed SQL.

    ``include_constraints=False`` skips the informational PK/FK constraints,
    which only Unity Catalog supports (open-source Delta cannot parse them).
    """
    statements = render_statements(catalog, schema, include_schema, include_constraints)
    for statement in statements:
        spark.sql(statement)
    return statements
