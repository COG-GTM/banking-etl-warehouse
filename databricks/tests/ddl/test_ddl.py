"""Run the warehouse DDL against a local Delta session and assert the schemas.

The expected schemas below are the migration contract: they mirror the Talend
job output schemas documented in docs/migration/01-talend-mapping-spec.md.
"""

from pathlib import Path

import pytest

from run_ddl import DDL_DIR, DDL_FILES, localize, split_statements, statements_for

EXPECTED_BRONZE_TRANSACTION_FILE_COLUMNS = {
    "transaction_id": "string",
    "account_id": "string",
    "transaction_date": "string",
    "amount": "string",
    "transaction_type": "string",
    "branch_id": "string",
    "_ingest_ts": "timestamp",
    "_source_file": "string",
}

EXPECTED_SCHEMAS = {
    "banking_bronze.mssql_transaction_db": {
        "transaction_id": "int",
        "account_id": "int",
        "transaction_date": "timestamp",
        "amount": "int",
        "transaction_type": "string",
        "branch_id": "int",
        "_ingest_ts": "timestamp",
        "_source_file": "string",
    },
    "banking_bronze.csv_transaction": EXPECTED_BRONZE_TRANSACTION_FILE_COLUMNS,
    "banking_bronze.excel_transaction": EXPECTED_BRONZE_TRANSACTION_FILE_COLUMNS,
    "banking_bronze.mssql_account": {
        "account_id": "int",
        "customer_id": "int",
        "account_type": "string",
        "balance": "int",
        "date_opened": "timestamp",
        "status": "string",
        "_ingest_ts": "timestamp",
        "_source_file": "string",
    },
    "banking_bronze.mssql_branch": {
        "branch_id": "int",
        "branch_name": "string",
        "branch_location": "string",
        "_ingest_ts": "timestamp",
        "_source_file": "string",
    },
    "banking_bronze.mssql_customer": {
        "customer_id": "int",
        "customer_name": "string",
        "address": "string",
        "city_id": "int",
        "age": "string",
        "gender": "string",
        "email": "string",
        "_ingest_ts": "timestamp",
        "_source_file": "string",
    },
    "banking_bronze.mssql_city": {
        "city_id": "int",
        "city_name": "string",
        "state_id": "int",
        "_ingest_ts": "timestamp",
        "_source_file": "string",
    },
    "banking_bronze.mssql_state": {
        "state_id": "int",
        "state_name": "string",
        "_ingest_ts": "timestamp",
        "_source_file": "string",
    },
    "banking_silver.account": {
        "account_id": "int",
        "customer_id": "int",
        "account_type": "string",
        "balance": "decimal(19,4)",
        "date_opened": "date",
        "status": "string",
        "_ingest_ts": "timestamp",
    },
    "banking_silver.branch": {
        "branch_id": "int",
        "branch_name": "string",
        "branch_location": "string",
        "_ingest_ts": "timestamp",
    },
    "banking_silver.city": {
        "city_id": "int",
        "city_name": "string",
        "state_id": "int",
        "_ingest_ts": "timestamp",
    },
    "banking_silver.state": {
        "state_id": "int",
        "state_name": "string",
        "_ingest_ts": "timestamp",
    },
    "banking_silver.customer": {
        "customer_id": "int",
        "customer_name": "string",
        "address": "string",
        "city_id": "int",
        "city_name": "string",
        "state_name": "string",
        "age": "int",
        "gender": "string",
        "email": "string",
        "_ingest_ts": "timestamp",
    },
    "banking_silver.transaction": {
        "transaction_id": "int",
        "account_id": "int",
        "transaction_date": "timestamp",
        "amount": "decimal(19,4)",
        "transaction_type": "string",
        "branch_id": "int",
        "_source_system": "string",
        "_ingest_ts": "timestamp",
    },
    "banking_gold.dim_account": {
        "account_id": "int",
        "customer_id": "int",
        "account_type": "string",
        "balance": "decimal(19,4)",
        "date_opened": "date",
        "status": "string",
    },
    "banking_gold.dim_branch": {
        "branch_id": "int",
        "branch_name": "string",
        "branch_location": "string",
    },
    "banking_gold.dim_customer": {
        "customer_id": "int",
        "customer_name": "string",
        "address": "string",
        "city_name": "string",
        "state_name": "string",
        "age": "int",
        "gender": "string",
        "email": "string",
    },
    "banking_gold.fact_transaction": {
        "transaction_id": "int",
        "account_id": "int",
        "transaction_date": "timestamp",
        "amount": "decimal(19,4)",
        "transaction_type": "string",
        "branch_id": "int",
    },
}


def actual_schema(spark, table):
    df = spark.table(table)
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


def test_schemas_exist(warehouse):
    databases = {row.namespace for row in warehouse.sql("SHOW DATABASES").collect()}
    assert {"banking_bronze", "banking_silver", "banking_gold"} <= databases


@pytest.mark.parametrize("table", sorted(EXPECTED_SCHEMAS))
def test_table_schema(warehouse, table):
    assert actual_schema(warehouse, table) == EXPECTED_SCHEMAS[table]


@pytest.mark.parametrize("table", sorted(EXPECTED_SCHEMAS))
def test_table_column_order(warehouse, table):
    assert list(actual_schema(warehouse, table)) == list(EXPECTED_SCHEMAS[table])


@pytest.mark.parametrize("table", sorted(EXPECTED_SCHEMAS))
def test_table_is_delta_with_cdf(warehouse, table):
    detail = warehouse.sql(f"DESCRIBE DETAIL {table}").collect()[0]
    assert detail["format"] == "delta"
    assert detail["properties"].get("delta.enableChangeDataFeed") == "true"


@pytest.mark.parametrize("table", sorted(EXPECTED_SCHEMAS))
def test_business_key_columns_are_not_null(warehouse, table):
    """Every table's first column is its business key and must be NOT NULL."""
    if table.startswith("banking_bronze."):
        pytest.skip("bronze keeps raw nullability")
    key = next(iter(EXPECTED_SCHEMAS[table]))
    field = warehouse.table(table).schema[key]
    assert field.nullable is False


def test_column_comments_carry_legacy_types(warehouse):
    rows = warehouse.sql("DESCRIBE TABLE banking_gold.fact_transaction").collect()
    comments = {r["col_name"]: r["comment"] for r in rows if r["col_name"]}
    assert comments["amount"].startswith("T-SQL: MONEY")
    assert comments["transaction_date"].startswith("T-SQL: DATETIME")
    assert "FK -> DimAccount" in comments["account_id"]


def test_every_ddl_file_is_parsed():
    for name in DDL_FILES:
        assert statements_for(Path(DDL_DIR) / name, local=False), name


def test_local_mode_rewrites_names_and_skips_unity_only_statements():
    catalog_statements = statements_for(Path(DDL_DIR) / "00_catalog_and_schemas.sql", local=True)
    assert not any(s.upper().startswith("CREATE CATALOG") for s in catalog_statements)
    assert any("banking_gold" in s for s in catalog_statements)
    assert statements_for(Path(DDL_DIR) / "04_constraints.sql", local=True) == []


def test_constraints_reference_the_gold_tables():
    statements = statements_for(Path(DDL_DIR) / "04_constraints.sql", local=False)
    added = [s for s in statements if "ADD CONSTRAINT" in s]
    assert sum("PRIMARY KEY" in s for s in added) == 4
    foreign_keys = [s for s in added if "FOREIGN KEY" in s]
    assert len(foreign_keys) == 3
    assert all(s.rstrip().endswith("NOT ENFORCED") for s in foreign_keys)


def test_split_statements_ignores_comments_and_keeps_string_literals():
    sql = "-- a comment;\nCREATE TABLE t (c STRING COMMENT 'has ; semicolon');\nSELECT 1;"
    assert split_statements(sql) == [
        "CREATE TABLE t (c STRING COMMENT 'has ; semicolon')",
        "SELECT 1",
    ]


def test_localize_drops_catalog_creation():
    assert localize("CREATE CATALOG IF NOT EXISTS banking") is None
    assert localize("SELECT * FROM banking.gold.dim_branch") == "SELECT * FROM banking_gold.dim_branch"
