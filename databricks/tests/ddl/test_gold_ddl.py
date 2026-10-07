import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from banking_etl.gold import ddl

LOCAL_PREFIX = "t_ddl_"
SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl"
LEGACY_DDL = Path(__file__).resolve().parents[3] / "sql_scripts" / "01_create_tables.sql"

TYPE_MAP = {"INT": "INT", "MONEY": "DECIMAL(19,4)", "DATE": "DATE", "DATETIME": "TIMESTAMP"}


def _snake(name: str) -> str:
    return re.sub(r"(?<=[a-z])(?=[A-Z])", "_", name).replace("ID", "_id").replace("__", "_").lower().strip("_")


def _parse_legacy():
    tables = {}
    for name, body in re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", LEGACY_DDL.read_text(), re.S):
        cols = []
        for line in body.splitlines():
            m = re.match(r"\s+(\w+) (INT|MONEY|DATE|DATETIME|VARCHAR\((\d+)\))( PRIMARY KEY)?,?\s*$", line)
            if m:
                cols.append((m.group(1), m.group(2), m.group(3), bool(m.group(4))))
        fks = re.findall(r"FOREIGN KEY \((\w+)\) REFERENCES (\w+)\((\w+)\)", body)
        tables[name] = (cols, fks)
    return tables


@pytest.fixture(scope="module")
def applied(ddl_spark):
    names = ddl.apply_star_schema(ddl_spark, catalog=None, schema_prefix=LOCAL_PREFIX, unity_catalog=False)
    return ddl_spark, names


def _schema(spark, fqn):
    return {f.name: f for f in spark.table(fqn).schema.fields}


def _props(spark, fqn):
    return {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()}


def test_spec_matches_legacy_sql_server_ddl():
    legacy = _parse_legacy()
    assert set(legacy) == {"DimAccount", "DimBranch", "DimCustomer", "FactTransaction"}
    for legacy_name, (cols, fks) in legacy.items():
        table = next(t for t in ddl.TABLES if t.legacy_name == legacy_name)
        assert [(c.legacy_name, c.legacy_type) for c in table.columns] == [(c[0], c[1]) for c in cols]
        for (lname, ltype, length, is_pk), col in zip(cols, table.columns):
            assert col.name == _snake(lname)
            if length:
                assert col.data_type == "STRING" and col.max_length == int(length)
            else:
                assert col.data_type == TYPE_MAP[ltype] and col.max_length is None
            assert (col.name in table.primary_key) == is_pk
            assert col.nullable is not is_pk
        assert {(fk.column, fk.ref_table, fk.ref_column) for fk in table.foreign_keys} == {
            (_snake(c), "dim_" + _snake(t.replace("Dim", "")), _snake(rc)) for c, t, rc in fks
        }


def test_silver_tables_carry_gold_columns_plus_lineage():
    for silver_name, gold_name in [("branch", "dim_branch"), ("account", "dim_account"),
                                   ("customer", "dim_customer"), ("transaction", "fact_transaction")]:
        silver, gold = ddl.get_table("silver", silver_name), ddl.get_table("gold", gold_name)
        assert silver.columns[: len(gold.columns)] == gold.columns
        assert [c.name for c in silver.columns[len(gold.columns):]] == ["_source_system", "_ingested_at"]
        assert silver.primary_key == gold.primary_key


def test_apply_creates_all_tables_with_types_and_nullability(applied):
    spark, names = applied
    assert names == [f"{LOCAL_PREFIX}{t.layer}.{t.name}" for t in ddl.TABLES]
    for table, fqn in zip(ddl.TABLES, names):
        fields = _schema(spark, fqn)
        assert list(fields) == [c.name for c in table.columns]
        for col in table.columns:
            assert fields[col.name].dataType.simpleString().upper() == col.data_type.replace(" ", "")
            assert fields[col.name].nullable == col.nullable
            assert fields[col.name].metadata.get("comment") == col.full_comment


def test_table_comment_and_delta_properties(applied):
    spark, names = applied
    for table, fqn in zip(ddl.TABLES, names):
        props = _props(spark, fqn)
        for key, value in ddl.TABLE_PROPERTIES.items():
            assert props[key] == value
        assert props["banking_etl.primary_key"] == ",".join(table.primary_key)
        detail = spark.sql(f"DESCRIBE DETAIL {fqn}").first()
        assert detail["format"] == "delta"
        assert detail["description"] == table.comment
    fact_props = _props(spark, f"{LOCAL_PREFIX}gold.fact_transaction")
    assert fact_props["banking_etl.foreign_keys"] == "account_id->dim_account.account_id,branch_id->dim_branch.branch_id"


def test_check_constraints_registered(applied):
    spark, _ = applied
    for table in ddl.TABLES:
        props = _props(spark, f"{LOCAL_PREFIX}{table.layer}.{table.name}")
        registered = {k[len("delta.constraints."):]: re.sub(r"\s+", "", v)
                      for k, v in props.items() if k.startswith("delta.constraints.")}
        assert registered == {k: re.sub(r"\s+", "", v) for k, v in table.check_constraints().items()}
    total = sum(len(t.check_constraints()) for t in ddl.TABLES)
    assert total == 2 * (2 + 2 + 6 + 1)


def test_apply_is_idempotent(applied):
    spark, names = applied
    before = {fqn: _props(spark, fqn) for fqn in names}
    assert ddl.apply_star_schema(spark, catalog=None, schema_prefix=LOCAL_PREFIX, unity_catalog=False) == names
    assert {fqn: _props(spark, fqn) for fqn in names} == before


def test_valid_rows_round_trip_with_exact_money(applied):
    spark, _ = applied
    fqn = f"{LOCAL_PREFIX}gold.fact_transaction"
    spark.createDataFrame(
        [(1, 10, datetime(2024, 1, 2, 3, 4, 5, 123000), Decimal("922337203685477.5807"), "Deposit", 2)],
        spark.table(fqn).schema,
    ).write.format("delta").mode("append").saveAsTable(fqn)
    row = spark.table(fqn).where("transaction_id = 1").first()
    assert row["amount"] == Decimal("922337203685477.5807")
    assert row["transaction_date"] == datetime(2024, 1, 2, 3, 4, 5, 123000)

    acct = f"{LOCAL_PREFIX}gold.dim_account"
    spark.sql(f"INSERT INTO {acct} VALUES (1, 7, 'saving', 1000.5, DATE'2020-05-01', 'active')")
    assert spark.table(acct).first()["date_opened"] == date(2020, 5, 1)


def test_varchar_length_check_rejects_overlong_values(applied):
    spark, _ = applied
    fqn = f"{LOCAL_PREFIX}gold.dim_branch"
    spark.sql(f"INSERT INTO {fqn} VALUES (1, 'Branch', '{'x' * 255}')")
    with pytest.raises(Exception, match="(?i)check constraint|CHECK_CONSTRAINT|DELTA_VIOLATE_CONSTRAINT"):
        spark.sql(f"INSERT INTO {fqn} VALUES (2, 'Branch', '{'x' * 256}')")
    assert spark.table(fqn).count() == 1


def test_primary_key_column_is_not_null(applied):
    spark, _ = applied
    with pytest.raises(Exception, match="(?i)null"):
        spark.sql(f"INSERT INTO {LOCAL_PREFIX}silver.branch VALUES (NULL, 'b', 'l', 'sqlserver', current_timestamp())")


def test_unity_catalog_render_has_informational_constraints():
    sql = ddl.render_create_table(ddl.get_table("gold", "fact_transaction"))
    assert sql.startswith("CREATE TABLE IF NOT EXISTS migration_demo.banking_mig_gold.fact_transaction (")
    assert "CONSTRAINT pk_fact_transaction PRIMARY KEY (transaction_id) NOT ENFORCED" in sql
    assert ("CONSTRAINT fk_fact_transaction_dim_account FOREIGN KEY (account_id) REFERENCES "
            "migration_demo.banking_mig_gold.dim_account (account_id) NOT ENFORCED") in sql
    assert "banking_etl.primary_key" not in sql
    local = ddl.render_create_table(ddl.get_table("gold", "fact_transaction"), None, "x_", unity_catalog=False)
    assert "PRIMARY KEY" not in local and "FOREIGN KEY" not in local
    assert local.startswith("CREATE TABLE IF NOT EXISTS x_gold.fact_transaction (")


def test_render_is_parametrised_and_ordered():
    stmts = ddl.render_statements(catalog="cat", schema_prefix="p_", layers=["gold"])
    assert stmts[0] == f"CREATE SCHEMA IF NOT EXISTS cat.p_gold COMMENT '{ddl.LAYER_COMMENTS['gold']}'"
    creates = [s.split()[5] for s in stmts if s.startswith("CREATE TABLE")]
    assert creates == ["cat.p_gold.dim_branch", "cat.p_gold.dim_account", "cat.p_gold.dim_customer", "cat.p_gold.fact_transaction"]
    with pytest.raises(ValueError):
        ddl.tables_for(["platinum"])



@pytest.mark.parametrize("catalog, prefix", [("cat; DROP TABLE x", "p_"), ("cat", "p_`x"), ("cat", "p.q_"), ("", "1p_")])
def test_unsafe_identifiers_are_rejected(catalog, prefix):
    with pytest.raises(ValueError, match="invalid"):
        ddl.render_statements(catalog=catalog or None, schema_prefix=prefix)


def test_unknown_layer_fails_before_any_sql_runs():
    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, stmt):
            self.statements.append(stmt)

    spark = RecordingSpark()
    with pytest.raises(ValueError, match="unknown layers"):
        ddl.apply_star_schema(spark, layers=("silver", "platinum"))
    assert spark.statements == []

def test_generated_sql_files_are_in_sync():
    expected = ddl.render_sql_files()
    assert sorted(p.name for p in SQL_DIR.glob("*.sql")) == sorted(expected)
    for name, text in expected.items():
        assert (SQL_DIR / name).read_text() == text, f"{name} is stale: run python -m banking_etl.gold.ddl --write-sql databricks/sql/ddl"
