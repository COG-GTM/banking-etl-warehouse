"""Apply the gold star schema DDL (``sql/gold/*.sql``) for a :class:`Settings`.

The SQL files use ``${name}`` placeholders (see :func:`ddl_parameters`) and wrap
Unity Catalog-only clauses (informational PRIMARY KEY / FOREIGN KEY) in
``/*uc:begin*/ ... /*uc:end*/`` so the same files also run on OSS Delta locally.

OSS Spark 4.0 + Delta 4.0 reject ``GENERATED ... AS IDENTITY`` in SQL DDL (the Delta catalog
does not advertise the capability) but support it through ``DeltaTableBuilder``. With
``unity_catalog=False`` such ``CREATE TABLE`` statements are therefore executed through the
builder (same columns, comments, nullability, identity, table comment and properties).
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from string import Template

from banking_etl.config import (
    GOLD_DIM_ACCOUNT,
    GOLD_DIM_BRANCH,
    GOLD_DIM_CUSTOMER,
    GOLD_FACT_TRANSACTION,
    Settings,
)

OPS_FACT_TRANSACTION_REJECTS = "fact_transaction_rejects"

SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "gold"

_UC_BLOCK = re.compile(r"/\*uc:begin\*/.*?/\*uc:end\*/", re.DOTALL)
_STATEMENT_END = re.compile(r";[ \t]*$", re.MULTILINE)
_IDENTITY = re.compile(r"\s+GENERATED\s+BY\s+DEFAULT\s+AS\s+IDENTITY\b", re.IGNORECASE)
_CREATE = re.compile(
    r"^CREATE TABLE IF NOT EXISTS\s+(?P<name>\S+)\s*\((?P<columns>.*)\)\s*USING DELTA\s*"
    r"(?:COMMENT\s+'(?P<comment>(?:[^']|'')*)')?\s*(?:TBLPROPERTIES\s*\((?P<props>.*)\))?\s*$",
    re.DOTALL | re.IGNORECASE,
)
_PROPERTY = re.compile(r"'([^']+)'\s*=\s*'([^']*)'")


def ddl_parameters(settings: Settings) -> dict[str, str]:
    """Placeholder -> fully qualified table name."""
    return {
        "dim_branch": settings.table("gold", GOLD_DIM_BRANCH),
        "dim_account": settings.table("gold", GOLD_DIM_ACCOUNT),
        "dim_customer": settings.table("gold", GOLD_DIM_CUSTOMER),
        "fact_transaction": settings.table("gold", GOLD_FACT_TRANSACTION),
        "fact_transaction_rejects": settings.table("ops", OPS_FACT_TRANSACTION_REJECTS),
    }


def ddl_files(sql_dir: Path | str = SQL_DIR) -> list[Path]:
    files = sorted(Path(sql_dir).glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no DDL files found in {sql_dir}")
    return files


def _is_blank(statement: str) -> bool:
    return not _strip_line_comments(statement).strip()


def render_statements(sql: str, settings: Settings, *, unity_catalog: bool) -> list[str]:
    """Substitute names, optionally strip UC-only blocks and split into statements."""
    sql = Template(sql).substitute(ddl_parameters(settings))
    sql = sql.replace("/*uc:begin*/", "").replace("/*uc:end*/", "") if unity_catalog else _UC_BLOCK.sub("", sql)
    return [s.strip() for s in _STATEMENT_END.split(sql) if not _is_blank(s)]


def render_ddl(settings: Settings, *, unity_catalog: bool | None = None, sql_dir: Path | str = SQL_DIR) -> list[str]:
    """All DDL statements in file order. ``unity_catalog`` defaults to ``settings.catalog is not None``."""
    uc = settings.catalog is not None if unity_catalog is None else unity_catalog
    statements: list[str] = []
    for path in ddl_files(sql_dir):
        statements.extend(render_statements(path.read_text(encoding="utf-8"), settings, unity_catalog=uc))
    return statements


def _strip_line_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))


def _create_with_delta_builder(spark, statement: str) -> bool:
    """Run an identity-column CREATE TABLE through DeltaTableBuilder (OSS Delta). False if not applicable."""
    body = _strip_line_comments(statement).strip()
    match = _CREATE.match(body)
    if not match or not _IDENTITY.search(match["columns"]):
        return False
    from delta.tables import DeltaTable, IdentityGenerator
    from pyspark.sql.types import StructType

    columns = match["columns"]
    identity = {
        m.group(1)
        for m in re.finditer(r"(\w+)\s+BIGINT" + _IDENTITY.pattern, columns, re.IGNORECASE)
    }
    jschema = spark._jvm.org.apache.spark.sql.types.StructType.fromDDL(_IDENTITY.sub("", columns))
    schema = StructType.fromJson(json.loads(jschema.json()))

    builder = DeltaTable.createIfNotExists(spark).tableName(match["name"])
    for field in schema.fields:
        builder = builder.addColumn(
            field.name,
            field.dataType,
            nullable=field.nullable,
            comment=field.metadata.get("comment"),
            generatedByDefaultAs=IdentityGenerator() if field.name in identity else None,
        )
    if match["comment"]:
        builder = builder.comment(match["comment"].replace("''", "'"))
    for key, value in _PROPERTY.findall(match["props"] or ""):
        builder = builder.property(key, value)
    builder.execute()
    return True


def apply_star_schema(
    spark,
    settings: Settings,
    *,
    unity_catalog: bool | None = None,
    ensure_schemas: bool = True,
    sql_dir: Path | str = SQL_DIR,
) -> list[str]:
    """Create gold dims/fact and ops.fact_transaction_rejects if missing. Idempotent.

    ``ensure_schemas`` runs ``CREATE SCHEMA IF NOT EXISTS`` for gold and ops (ticket 1 owns
    their full setup; this only guards against running before it).
    Returns the statements executed.
    """
    executed: list[str] = []
    if ensure_schemas:
        for layer in ("gold", "ops"):
            executed.append(f"CREATE SCHEMA IF NOT EXISTS {settings.schema(layer)}")
            spark.sql(executed[-1])
    uc = settings.catalog is not None if unity_catalog is None else unity_catalog
    for statement in render_ddl(settings, unity_catalog=uc, sql_dir=sql_dir):
        if uc or not _create_with_delta_builder(spark, statement):
            spark.sql(statement)
        executed.append(statement)
    return executed
