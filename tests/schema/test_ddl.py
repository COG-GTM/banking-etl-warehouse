"""Verify the Databricks DDL against the original T-SQL and the StructType definitions."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from databricks.schema import schemas
from databricks.schema.create_tables import (
    LAYERS,
    read_ddl,
    render_ddl,
    split_statements,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TSQL_PATH = REPO_ROOT / "sql_scripts" / "01_create_tables.sql"

# Expected Spark type for each T-SQL base type
TSQL_TO_SPARK = {
    "INT": "INT",
    "VARCHAR": "STRING",
    "MONEY": "DECIMAL(19, 4)",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
}


def _parse_tsql_tables() -> dict[str, dict[str, str]]:
    """Return {table: {column: tsql_base_type}} from the original T-SQL script."""
    text = TSQL_PATH.read_text(encoding="utf-8")
    # strip PRINT / IF ... BEGIN ... END / USE / GO blocks that sqlglot's tsql parser may choke on
    text = re.sub(r"^\s*PRINT .*?;\s*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"IF NOT EXISTS .*?END", "", text, flags=re.DOTALL)
    text = re.sub(r"^\s*(GO|USE DWH;)\s*$", "", text, flags=re.MULTILINE)
    tables: dict[str, dict[str, str]] = {}
    for stmt in sqlglot.parse(text, read="tsql"):
        if not isinstance(stmt, exp.Create) or stmt.kind != "TABLE":
            continue
        name = stmt.this.this.name
        cols = {}
        for col in stmt.this.expressions:
            if isinstance(col, exp.ColumnDef):
                cols[col.name] = col.kind.this.name.upper()
        tables[name] = cols
    return tables


def _parse_databricks_ddl(layer: str) -> dict[str, dict[str, str]]:
    """Return {table: {column: spark_type_sql}} by parsing the rendered DDL with sqlglot."""
    ddl = render_ddl(read_ddl(layer), "cat", "sch")
    tables: dict[str, dict[str, str]] = {}
    for stmt in split_statements(ddl):
        parsed = sqlglot.parse_one(stmt, read="databricks")
        if not isinstance(parsed, exp.Create) or parsed.kind != "TABLE":
            continue
        table = parsed.this.this
        assert table.catalog == "cat" and table.db == "sch", f"{table.sql()} not fully qualified"
        cols = {}
        for col in parsed.this.expressions:
            if isinstance(col, exp.ColumnDef):
                cols[col.name] = col.kind.sql(dialect="databricks").upper()
        tables[table.name] = cols
    return tables


def _spark_type_sql(dtype) -> str:
    return dtype.simpleString().upper().replace("DECIMAL(19,4)", "DECIMAL(19, 4)")


# ---------------------------------------------------------------------------
# Gold vs. original T-SQL
# ---------------------------------------------------------------------------
def test_tsql_source_parsed():
    tables = _parse_tsql_tables()
    assert set(tables) == {"DimAccount", "DimBranch", "DimCustomer", "FactTransaction"}
    assert tables["FactTransaction"]["Amount"] == "MONEY"


def test_gold_ddl_contains_every_tsql_column_with_mapped_type():
    tsql = _parse_tsql_tables()
    gold = _parse_databricks_ddl("gold")
    assert set(gold) == set(tsql)
    for table, cols in tsql.items():
        assert list(gold[table]) == list(cols), f"{table} column order/names differ"
        for col, tsql_type in cols.items():
            expected = TSQL_TO_SPARK[tsql_type]
            assert gold[table][col] == expected, f"{table}.{col}: {gold[table][col]} != {expected}"


def test_gold_ddl_declares_constraints_and_delta():
    ddl = read_ddl("gold")
    assert ddl.count("USING DELTA") == 4
    assert len(re.findall(r"CONSTRAINT \w+ PRIMARY KEY", ddl)) == 4
    for fk in ("FK_FactTransaction_DimAccount", "FK_FactTransaction_DimBranch"):
        assert fk in ddl
    assert len(re.findall(r"KEY \([^)]*\)(?: REFERENCES [^\n]*\))? NOT ENFORCED", ddl)) == 6
    for pk_col in ("AccountID INT NOT NULL", "BranchID INT NOT NULL", "CustomerID INT NOT NULL", "TransactionID INT NOT NULL"):
        assert pk_col in ddl
    assert ddl.count("COMMENT '") >= 4


# ---------------------------------------------------------------------------
# StructTypes vs. DDL for every layer
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("layer", LAYERS)
def test_structtypes_match_ddl(layer):
    ddl_tables = _parse_databricks_ddl(layer)
    struct_tables = schemas.ALL_SCHEMAS[layer]
    assert set(ddl_tables) == set(struct_tables), f"{layer}: table sets differ"
    for table, struct in struct_tables.items():
        ddl_cols = ddl_tables[table]
        struct_cols = {f.name: _spark_type_sql(f.dataType) for f in struct.fields}
        assert struct_cols == ddl_cols, f"{layer}.{table} columns differ"


@pytest.mark.parametrize("table", schemas.BRONZE_SCHEMAS)
def test_bronze_tables_have_metadata_columns(table):
    names = schemas.BRONZE_SCHEMAS[table].fieldNames()
    assert names[-2:] == ["_ingest_ts", "_source_file"]


def test_ddl_type_map_documents_expected_mapping():
    assert schemas.DDL_TYPE_MAP == {
        "INT": "INT",
        "VARCHAR(n)": "STRING",
        "MONEY": "DECIMAL(19,4)",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP",
    }


def test_source_file_headers_match_bronze_file_tables():
    header = (REPO_ROOT / "data_sources" / "transaction_csv.csv").read_text().splitlines()[0].split(",")
    for table in ("transaction_csv", "transaction_excel"):
        assert schemas.BRONZE_SCHEMAS[table].fieldNames()[: len(header)] == header


# ---------------------------------------------------------------------------
# Execute the DDL against a local SparkSession
# ---------------------------------------------------------------------------
def test_create_all_tables_local(spark):
    from databricks.schema.create_tables import create_all_tables

    result = create_all_tables(spark, None, "dwh_test", local=True)
    assert set(result) == set(LAYERS)
    for layer, layer_schema in (("bronze", "dwh_test_bronze"), ("silver", "dwh_test_silver"), ("gold", "dwh_test")):
        for table, struct in schemas.ALL_SCHEMAS[layer].items():
            actual = spark.table(f"{layer_schema}.{table}").schema
            assert [(f.name.lower(), f.dataType) for f in actual] == [
                (f.name.lower(), f.dataType) for f in struct.fields
            ], f"{layer_schema}.{table}"
