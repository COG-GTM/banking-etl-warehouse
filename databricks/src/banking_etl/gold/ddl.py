"""Delta Lake DDL for the banking DWH (port of ``sql_scripts/01_create_tables.sql``).

The legacy SQL Server star schema (``DimAccount``, ``DimBranch``, ``DimCustomer``,
``FactTransaction``) is declared once here as :data:`TABLES` and rendered to Delta DDL
for two targets:

* ``unity_catalog=True`` (Databricks): three-part names, informational
  ``PRIMARY KEY`` / ``FOREIGN KEY ... NOT ENFORCED`` constraints.
* ``unity_catalog=False`` (local OSS Delta / pytest): two-part names, no PK/FK clauses
  (OSS Delta rejects them); the keys are kept in the ``banking_etl.primary_key`` /
  ``banking_etl.foreign_keys`` table properties instead so they stay inspectable.

Both targets get the same columns, types, NOT NULL, comments, ``CHECK`` constraints
(``VARCHAR(n)`` length limits) and Delta properties (CDF, auto-optimize).

``databricks/sql/ddl/*.sql`` is generated from this module with
``python -m banking_etl.gold.ddl --write-sql <dir>``; a test keeps them in sync.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_CATALOG = "migration_demo"
DEFAULT_SCHEMA_PREFIX = "banking_mig_"
LAYERS = ("silver", "gold")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

TABLE_PROPERTIES: dict[str, str] = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

LAYER_COMMENTS = {
    "silver": "Silver: cleansed, typed and deduplicated entities (branch, account, customer, transaction).",
    "gold": "Gold: star schema replacing the legacy DWH (dim_branch, dim_account, dim_customer, fact_transaction) and analytics functions.",
}


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    comment: str
    legacy_name: str | None = None
    legacy_type: str | None = None
    nullable: bool = True
    max_length: int | None = None

    def ddl(self) -> str:
        not_null = " NOT NULL" if not self.nullable else ""
        return f"{self.name} {self.data_type}{not_null} COMMENT {_sql_str(self.full_comment)}"

    @property
    def full_comment(self) -> str:
        if self.legacy_name:
            return f"{self.comment} Legacy: {self.legacy_name} {self.legacy_type}."
        return self.comment


@dataclass(frozen=True)
class ForeignKey:
    name: str
    column: str
    ref_table: str
    ref_column: str


@dataclass(frozen=True)
class Table:
    layer: str
    name: str
    comment: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    legacy_name: str | None = None
    foreign_keys: tuple[ForeignKey, ...] = field(default_factory=tuple)

    @property
    def pk_name(self) -> str:
        return f"pk_{self.name}"

    def check_constraints(self) -> dict[str, str]:
        return {
            f"ck_{self.name}_{c.name}_len": f"{c.name} IS NULL OR length({c.name}) <= {c.max_length}"
            for c in self.columns
            if c.max_length is not None
        }


def _sql_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _varchar(name, n, comment, legacy_name, nullable=True) -> Column:
    return Column(name, "STRING", f"{comment} Max length {n} (CHECK).", legacy_name, f"VARCHAR({n})", nullable, n)


def _int(name, comment, legacy_name, nullable=True) -> Column:
    return Column(name, "INT", comment, legacy_name, "INT", nullable)


def _money(name, comment, legacy_name) -> Column:
    return Column(name, "DECIMAL(19,4)", comment, legacy_name, "MONEY")


def _account_columns() -> tuple[Column, ...]:
    return (
        _int("account_id", "Account natural key (primary key).", "AccountID", nullable=False),
        _int("customer_id", "Owning customer (joins dim_customer.customer_id).", "CustomerID"),
        _varchar("account_type", 50, "Account product type.", "AccountType"),
        _money("balance", "Account balance.", "Balance"),
        Column("date_opened", "DATE", "Date the account was opened.", "DateOpened", "DATE"),
        _varchar("status", 50, "Account status; sp_BalancePerCustomer filters status = 'active'.", "Status"),
    )


def _branch_columns() -> tuple[Column, ...]:
    return (
        _int("branch_id", "Branch natural key (primary key).", "BranchID", nullable=False),
        _varchar("branch_name", 100, "Branch name.", "BranchName"),
        _varchar("branch_location", 255, "Branch location.", "BranchLocation"),
    )


def _customer_columns() -> tuple[Column, ...]:
    return (
        _int("customer_id", "Customer natural key (primary key).", "CustomerID", nullable=False),
        _varchar("customer_name", 100, "Customer name (upper-cased by Load_DimCustomer tMap).", "CustomerName"),
        _varchar("address", 255, "Street address (upper-cased by Load_DimCustomer tMap).", "Address"),
        _varchar("city_name", 100, "City name, denormalised from source city (upper-cased).", "CityName"),
        _varchar("state_name", 100, "State name, denormalised from source state (upper-cased).", "StateName"),
        _int("age", "Customer age in years.", "Age"),
        _varchar("gender", 10, "Customer gender (upper-cased by Load_DimCustomer tMap).", "Gender"),
        _varchar("email", 100, "Customer e-mail address.", "Email"),
    )


def _transaction_columns() -> tuple[Column, ...]:
    return (
        _int("transaction_id", "Transaction natural key (primary key; unique after tUniqRow dedup).", "TransactionID", nullable=False),
        _int("account_id", "Account of the transaction (FK to dim_account.account_id).", "AccountID"),
        Column(
            "transaction_date",
            "TIMESTAMP",
            "Transaction timestamp (session time zone UTC; microsecond precision vs DATETIME 3.33 ms).",
            "TransactionDate",
            "DATETIME",
        ),
        _money("amount", "Transaction amount (unsigned; sign is derived from transaction_type).", "Amount"),
        _varchar("transaction_type", 50, "Transaction type; 'Deposit' adds, anything else subtracts in sp_BalancePerCustomer.", "TransactionType"),
        _int("branch_id", "Branch of the transaction (FK to dim_branch.branch_id).", "BranchID"),
    )


SILVER_LINEAGE = (
    Column("_source_system", "STRING", "Source stream the row came from: sqlserver, excel or csv (transactions); sqlserver for master data.", nullable=False),
    Column("_ingested_at", "TIMESTAMP", "Timestamp the row was written to silver."),
)

TABLES: tuple[Table, ...] = (
    Table(
        "silver", "branch",
        "Silver conformed branch entity from sample.dbo.branch; source of gold.dim_branch.",
        _branch_columns() + SILVER_LINEAGE, ("branch_id",),
    ),
    Table(
        "silver", "account",
        "Silver conformed account entity from sample.dbo.account; source of gold.dim_account.",
        _account_columns() + SILVER_LINEAGE, ("account_id",),
    ),
    Table(
        "silver", "customer",
        "Silver conformed customer entity (customer joined to city and state); source of gold.dim_customer.",
        _customer_columns() + SILVER_LINEAGE, ("customer_id",),
    ),
    Table(
        "silver", "transaction",
        "Silver conformed transactions: union of sample.dbo.transaction_db, transaction_excel.xlsx and transaction_csv.csv, deduplicated on transaction_id; source of gold.fact_transaction.",
        _transaction_columns() + SILVER_LINEAGE, ("transaction_id",),
    ),
    Table(
        "gold", "dim_branch",
        "Branch dimension. Port of DWH.dbo.DimBranch (Talend Load_DimBranch).",
        _branch_columns(), ("branch_id",), "DimBranch",
    ),
    Table(
        "gold", "dim_account",
        "Account dimension. Port of DWH.dbo.DimAccount (Talend Load_DimAccount).",
        _account_columns(), ("account_id",), "DimAccount",
    ),
    Table(
        "gold", "dim_customer",
        "Customer dimension (customer + city + state). Port of DWH.dbo.DimCustomer (Talend Load_DimCustomer).",
        _customer_columns(), ("customer_id",), "DimCustomer",
    ),
    Table(
        "gold", "fact_transaction",
        "Transaction fact. Port of DWH.dbo.FactTransaction (Talend Load_FactTransaction). PK/FKs are informational (NOT ENFORCED): dedup and RI are the ETL's job.",
        _transaction_columns(), ("transaction_id",), "FactTransaction",
        (
            ForeignKey("fk_fact_transaction_dim_account", "account_id", "dim_account", "account_id"),
            ForeignKey("fk_fact_transaction_dim_branch", "branch_id", "dim_branch", "branch_id"),
        ),
    ),
)


def get_table(layer: str, name: str) -> Table:
    for t in TABLES:
        if t.layer == layer and t.name == name:
            return t
    raise KeyError(f"{layer}.{name}")


def _identifier(value: str, what: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid {what} {value!r}: expected [A-Za-z_][A-Za-z0-9_]*")
    return value


def schema_name(layer: str, catalog: str | None = DEFAULT_CATALOG, schema_prefix: str = DEFAULT_SCHEMA_PREFIX) -> str:
    schema = _identifier(f"{schema_prefix}{layer}", "schema name")
    if catalog:
        _identifier(catalog, "catalog")
    return f"{catalog}.{schema}" if catalog else schema


def qualified_name(table: Table, catalog: str | None = DEFAULT_CATALOG, schema_prefix: str = DEFAULT_SCHEMA_PREFIX) -> str:
    return f"{schema_name(table.layer, catalog, schema_prefix)}.{table.name}"


def _tblproperties(table: Table, unity_catalog: bool) -> dict[str, str]:
    props = dict(TABLE_PROPERTIES)
    if not unity_catalog:
        props["banking_etl.primary_key"] = ",".join(table.primary_key)
        if table.foreign_keys:
            props["banking_etl.foreign_keys"] = ",".join(
                f"{fk.column}->{fk.ref_table}.{fk.ref_column}" for fk in table.foreign_keys
            )
    return props


def render_create_table(
    table: Table,
    catalog: str | None = DEFAULT_CATALOG,
    schema_prefix: str = DEFAULT_SCHEMA_PREFIX,
    unity_catalog: bool = True,
) -> str:
    body = [f"  {c.ddl()}" for c in table.columns]
    if unity_catalog:
        body.append(f"  CONSTRAINT {table.pk_name} PRIMARY KEY ({', '.join(table.primary_key)}) NOT ENFORCED")
        for fk in table.foreign_keys:
            ref = f"{schema_name(table.layer, catalog, schema_prefix)}.{fk.ref_table}"
            body.append(
                f"  CONSTRAINT {fk.name} FOREIGN KEY ({fk.column}) REFERENCES {ref} ({fk.ref_column}) NOT ENFORCED"
            )
    props = ",\n".join(f"  {_sql_str(k)} = {_sql_str(v)}" for k, v in _tblproperties(table, unity_catalog).items())
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_name(table, catalog, schema_prefix)} (\n"
        + ",\n".join(body)
        + "\n)\nUSING DELTA\n"
        + f"COMMENT {_sql_str(table.comment)}\n"
        + f"TBLPROPERTIES (\n{props}\n)"
    )


def render_check_constraints(
    table: Table, catalog: str | None = DEFAULT_CATALOG, schema_prefix: str = DEFAULT_SCHEMA_PREFIX
) -> list[str]:
    fqn = qualified_name(table, catalog, schema_prefix)
    return [f"ALTER TABLE {fqn} ADD CONSTRAINT {name} CHECK ({expr})" for name, expr in table.check_constraints().items()]


def render_create_schema(layer: str, catalog: str | None = DEFAULT_CATALOG, schema_prefix: str = DEFAULT_SCHEMA_PREFIX) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {schema_name(layer, catalog, schema_prefix)} COMMENT {_sql_str(LAYER_COMMENTS[layer])}"


def tables_for(layers: Iterable[str] = LAYERS) -> list[Table]:
    wanted = set(layers)
    unknown = wanted - set(LAYERS)
    if unknown:
        raise ValueError(f"unknown layers: {sorted(unknown)}")
    return [t for t in TABLES if t.layer in wanted]


def render_statements(
    catalog: str | None = DEFAULT_CATALOG,
    schema_prefix: str = DEFAULT_SCHEMA_PREFIX,
    unity_catalog: bool = True,
    layers: Sequence[str] = LAYERS,
    create_schemas: bool = True,
) -> list[str]:
    """All DDL statements in dependency order (dims before the fact that references them).

    CHECK constraints are returned as plain ``ALTER TABLE ... ADD CONSTRAINT``; use
    :func:`apply_star_schema` for an idempotent apply.
    """
    tables = tables_for(layers)
    stmts = [render_create_schema(layer, catalog, schema_prefix) for layer in layers] if create_schemas else []
    for t in tables:
        stmts.append(render_create_table(t, catalog, schema_prefix, unity_catalog))
        stmts.extend(render_check_constraints(t, catalog, schema_prefix))
    return stmts


def _existing_check_constraints(spark, fqn: str) -> set[str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
    prefix = "delta.constraints."
    return {r["key"][len(prefix):] for r in rows if r["key"].startswith(prefix)}


def apply_star_schema(
    spark,
    catalog: str | None = DEFAULT_CATALOG,
    schema_prefix: str = DEFAULT_SCHEMA_PREFIX,
    unity_catalog: bool = True,
    layers: Sequence[str] = LAYERS,
    create_schemas: bool = True,
) -> list[str]:
    """Create the silver/gold tables idempotently and return their qualified names.

    Existing tables are left as-is (``CREATE TABLE IF NOT EXISTS``); missing CHECK
    constraints are added. Use ``unity_catalog=False`` with ``catalog=None`` locally.
    """
    tables = tables_for(layers)
    if create_schemas:
        for layer in layers:
            spark.sql(render_create_schema(layer, catalog, schema_prefix))
    created = []
    for t in tables:
        fqn = qualified_name(t, catalog, schema_prefix)
        spark.sql(render_create_table(t, catalog, schema_prefix, unity_catalog))
        existing = _existing_check_constraints(spark, fqn)
        for name, expr in t.check_constraints().items():
            if name.lower() not in existing:
                spark.sql(f"ALTER TABLE {fqn} ADD CONSTRAINT {name} CHECK ({expr})")
        created.append(fqn)
    return created


def render_sql_files(
    catalog: str = DEFAULT_CATALOG, schema_prefix: str = DEFAULT_SCHEMA_PREFIX
) -> dict[str, str]:
    """Unity Catalog SQL scripts, keyed by file name."""
    header = (
        "-- GENERATED by `python -m banking_etl.gold.ddl --write-sql databricks/sql/ddl`; do not edit by hand.\n"
        "-- Source of truth: databricks/src/banking_etl/gold/ddl.py (port of sql_scripts/01_create_tables.sql).\n"
        "-- Type mapping: docs/type_mapping.md. Re-running ADD CONSTRAINT on an existing table fails;\n"
        "-- use banking_etl.gold.ddl.apply_star_schema for an idempotent apply.\n\n"
    )
    files = {"00_schemas.sql": header + ";\n\n".join(render_create_schema(l, catalog, schema_prefix) for l in LAYERS) + ";\n"}
    for idx, layer in ((10, "silver"), (20, "gold")):
        parts = []
        for t in tables_for([layer]):
            parts.append(render_create_table(t, catalog, schema_prefix) + ";")
            parts.extend(s + ";" for s in render_check_constraints(t, catalog, schema_prefix))
        files[f"{idx}_{layer}_tables.sql"] = header + "\n\n".join(parts) + "\n"
    return files


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write-sql", type=Path, help="directory to write the generated UC SQL scripts to")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema-prefix", default=DEFAULT_SCHEMA_PREFIX)
    args = parser.parse_args(argv)
    files = render_sql_files(args.catalog, args.schema_prefix)
    if args.write_sql:
        args.write_sql.mkdir(parents=True, exist_ok=True)
        for name, text in files.items():
            (args.write_sql / name).write_text(text)
    else:
        print("\n".join(files.values()))


if __name__ == "__main__":
    main()
