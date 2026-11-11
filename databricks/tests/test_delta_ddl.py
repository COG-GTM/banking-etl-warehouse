"""Gold Delta DDL must match the SQL Server star schema in sql_scripts/01_create_tables.sql."""

from __future__ import annotations

import os
import re
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from banking_etl.delta_ddl import (
    GOLD_TABLES,
    apply_ddl,
    delta_type_for,
    render_file,
    render_statements,
    runner,
    split_statements,
)

SOURCE_DDL = (
    Path(__file__).resolve().parents[2] / "sql_scripts" / "01_create_tables.sql"
)

SOURCE_TO_GOLD = {
    "DimAccount": "dim_account",
    "DimBranch": "dim_branch",
    "DimCustomer": "dim_customer",
    "FactTransaction": "fact_transaction",
}


def snake_case(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def parse_source_ddl() -> dict:
    """Parse the SQL Server CREATE TABLE statements into columns, PKs and FKs."""
    text = SOURCE_DDL.read_text(encoding="utf-8")
    tables = {}
    for match in re.finditer(r"CREATE TABLE (\w+) \((.*?)\n\);", text, re.DOTALL):
        name, body = match.groups()
        columns, pk, fks = [], [], []
        for raw in body.splitlines():
            line = raw.split("--")[0].strip().rstrip(",")
            if not line:
                continue
            fk = re.match(
                r"CONSTRAINT (\w+) FOREIGN KEY \((\w+)\) REFERENCES (\w+)\((\w+)\)",
                line,
            )
            if fk:
                fks.append(fk.groups())
                continue
            col = re.match(r"(\w+) (\w+(?:\(\d+\))?)( PRIMARY KEY)?$", line)
            assert col, f"Unparsed source line in {name}: {line!r}"
            columns.append((col.group(1), col.group(2)))
            if col.group(3):
                pk.append(col.group(1))
        tables[name] = {"columns": columns, "pk": pk, "fks": fks}
    return tables


SOURCE = parse_source_ddl()


# --------------------------------------------------------------------------- pure tests


def test_source_ddl_has_the_four_star_schema_tables():
    assert set(SOURCE) == set(SOURCE_TO_GOLD)
    assert set(SOURCE_TO_GOLD.values()) == set(GOLD_TABLES)
    assert SOURCE["FactTransaction"]["fks"] == [
        ("FK_FactTransaction_DimAccount", "AccountID", "DimAccount", "AccountID"),
        ("FK_FactTransaction_DimBranch", "BranchID", "DimBranch", "BranchID"),
    ]


@pytest.mark.parametrize(
    "sqlserver, delta",
    [
        ("INT", "INT"),
        ("MONEY", "DECIMAL(19,4)"),
        ("DATETIME", "TIMESTAMP"),
        ("DATE", "DATE"),
        ("VARCHAR(50)", "VARCHAR(50)"),
        ("varchar(255)", "VARCHAR(255)"),
    ],
)
def test_type_mapping(sqlserver, delta):
    assert delta_type_for(sqlserver) == delta


def test_unknown_type_is_rejected():
    with pytest.raises(ValueError):
        delta_type_for("XML")


@pytest.mark.parametrize("bad", ["gold; DROP TABLE x", "a.b", "", "1gold", "go`ld"])
def test_invalid_identifiers_are_rejected(bad):
    with pytest.raises(ValueError):
        render_statements(catalog=bad)
    with pytest.raises(ValueError):
        render_statements(schema=bad)


def test_catalog_and_schema_are_parameterized():
    statements = render_statements(catalog="my_cat", schema="my_gold")
    assert statements[0].startswith("CREATE SCHEMA IF NOT EXISTS `my_cat`.`my_gold`")
    for table in GOLD_TABLES:
        assert any(f"`my_cat`.`my_gold`.{table}" in s for s in statements)
    joined = "\n".join(statements)
    assert "${" not in joined
    assert "banking_dwh" not in joined
    assert not re.search(r"`gold`|\.gold\.", joined)


def test_default_catalog_is_banking_dwh_gold():
    assert render_statements()[0].startswith(
        "CREATE SCHEMA IF NOT EXISTS `banking_dwh`.`gold`"
    )


def test_split_statements_respects_quotes_and_comments():
    sql = "-- c; one\nSELECT 'a;b' AS x;\nSELECT \"q;\" -- trailing; comment\n;  \n"
    assert split_statements(sql) == ["SELECT 'a;b' AS x", 'SELECT "q;"']


def test_skip_flags_drop_schema_and_constraint_statements():
    full = render_statements("c", "s")
    tables_only = render_statements(
        "c", "s", include_schema=False, include_constraints=False
    )
    assert len(tables_only) == 4
    assert all(s.startswith("CREATE TABLE IF NOT EXISTS") for s in tables_only)
    assert not any(
        re.search(r"\bCONSTRAINT\b|PRIMARY KEY \(|FOREIGN KEY", s) for s in tables_only
    )
    assert len(full) > len(tables_only) + 1


def _constraint_statements() -> list[str]:
    return [" ".join(s.split()) for s in render_file("90_constraints.sql", "c", "s")]


def test_primary_keys_match_source():
    adds = [s for s in _constraint_statements() if "PRIMARY KEY (" in s and "ADD" in s]
    expected = {
        SOURCE_TO_GOLD[t]: [snake_case(c) for c in spec["pk"]]
        for t, spec in SOURCE.items()
    }
    actual = {}
    for stmt in adds:
        m = re.match(
            r"ALTER TABLE `c`\.`s`\.(\w+) ADD CONSTRAINT (pk_\w+) PRIMARY KEY \(([\w, ]+)\)$",
            stmt,
        )
        assert m, stmt
        table, name, cols = m.groups()
        assert name == f"pk_{table}"
        actual[table] = [c.strip() for c in cols.split(",")]
    assert actual == expected


def test_foreign_keys_match_source():
    adds = [s for s in _constraint_statements() if "FOREIGN KEY" in s]
    actual = set()
    for stmt in adds:
        m = re.match(
            r"ALTER TABLE `c`\.`s`\.(\w+) ADD CONSTRAINT (\w+) FOREIGN KEY \((\w+)\) "
            r"REFERENCES `c`\.`s`\.(\w+) \((\w+)\)$",
            stmt,
        )
        assert m, stmt
        actual.add(m.groups())
    expected = {
        (
            SOURCE_TO_GOLD[table],
            snake_case(name),
            snake_case(col),
            SOURCE_TO_GOLD[ref_table],
            snake_case(ref_col),
        )
        for table, spec in SOURCE.items()
        for name, col, ref_table, ref_col in spec["fks"]
    }
    assert actual == expected


def test_constraints_are_rerunnable():
    stmts = _constraint_statements()
    drops = [s for s in stmts if " DROP " in s]
    first_add = next(i for i, s in enumerate(stmts) if " ADD " in s)
    assert all(stmts.index(d) < first_add for d in drops)
    assert all("IF EXISTS" in d for d in drops)
    fk_names = {
        re.search(r"ADD CONSTRAINT (\w+) FOREIGN", s).group(1)
        for s in stmts
        if "FOREIGN" in s
    }
    assert fk_names == {
        d.split("IF EXISTS ")[1] for d in drops if "DROP CONSTRAINT" in d
    }
    assert {
        re.search(r"`s`\.(\w+) DROP PRIMARY", d).group(1)
        for d in drops
        if "PRIMARY" in d
    } == set(GOLD_TABLES)


# --------------------------------------------------------------------------- local Delta


@pytest.fixture(scope="module")
def ddl_spark(tmp_path_factory):
    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test_delta_ddl")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.repositories",
            "https://maven-central.storage-download.googleapis.com/maven2/",
        )
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark


@pytest.fixture(scope="module")
def gold_schema(ddl_spark):
    schema = f"gold_ddl_test_{uuid.uuid4().hex[:8]}"
    apply_ddl(
        ddl_spark, catalog="spark_catalog", schema=schema, include_constraints=False
    )
    yield schema
    ddl_spark.sql(f"DROP SCHEMA IF EXISTS spark_catalog.{schema} CASCADE")


def _delta_type(field) -> str:
    """Declared Delta type, recovering VARCHAR(n) that Spark exposes as STRING."""
    declared = field.metadata.get("__CHAR_VARCHAR_TYPE_STRING")
    return (declared or field.dataType.simpleString()).upper()


@pytest.mark.parametrize("source_table", sorted(SOURCE_TO_GOLD))
def test_delta_schema_matches_sql_server(ddl_spark, gold_schema, source_table):
    spec = SOURCE[source_table]
    gold = SOURCE_TO_GOLD[source_table]
    schema = ddl_spark.table(f"spark_catalog.{gold_schema}.{gold}").schema

    expected = [
        (snake_case(col), delta_type_for(sql_type), col not in spec["pk"])
        for col, sql_type in spec["columns"]
    ]
    actual = [(f.name, _delta_type(f), f.nullable) for f in schema.fields]
    assert actual == expected

    for field in schema.fields:
        assert field.metadata.get("comment"), f"{gold}.{field.name} has no comment"


def test_explicit_type_expectations(ddl_spark, gold_schema):
    from pyspark.sql.types import DateType, DecimalType, IntegerType, TimestampType

    account = ddl_spark.table(f"spark_catalog.{gold_schema}.dim_account").schema
    fact = ddl_spark.table(f"spark_catalog.{gold_schema}.fact_transaction").schema
    assert account["balance"].dataType == DecimalType(19, 4)
    assert account["date_opened"].dataType == DateType()
    assert account["account_id"].dataType == IntegerType()
    assert fact["amount"].dataType == DecimalType(19, 4)
    assert fact["transaction_date"].dataType == TimestampType()


@pytest.mark.parametrize("source_table", sorted(SOURCE_TO_GOLD))
def test_tables_are_delta_with_comments(ddl_spark, gold_schema, source_table):
    gold = SOURCE_TO_GOLD[source_table]
    detail = ddl_spark.sql(
        f"DESCRIBE DETAIL spark_catalog.{gold_schema}.{gold}"
    ).first()
    assert detail["format"] == "delta"
    assert detail["description"] and source_table in detail["description"]
    props = ddl_spark.sql(
        f"SHOW TBLPROPERTIES spark_catalog.{gold_schema}.{gold}"
    ).collect()
    props = {r["key"]: r["value"] for r in props}
    assert props["banking_etl.layer"] == "gold"
    assert props["banking_etl.source_table"] == f"DWH.dbo.{source_table}"


def test_ddl_is_idempotent(ddl_spark, gold_schema):
    before = {
        t: ddl_spark.table(f"spark_catalog.{gold_schema}.{t}").schema
        for t in GOLD_TABLES
    }
    apply_ddl(
        ddl_spark,
        catalog="spark_catalog",
        schema=gold_schema,
        include_constraints=False,
    )
    after = {
        t: ddl_spark.table(f"spark_catalog.{gold_schema}.{t}").schema
        for t in GOLD_TABLES
    }
    assert before == after


def test_money_and_datetime_round_trip(ddl_spark, gold_schema):
    table = f"spark_catalog.{gold_schema}.fact_transaction"
    ddl_spark.sql(
        f"INSERT INTO {table} VALUES "
        "(1, 10, TIMESTAMP'2024-01-18 15:21:30.123', 922337203685477.5807, 'Deposit', 2)"
    )
    row = ddl_spark.sql(
        f"SELECT CAST(amount AS STRING) a, CAST(transaction_date AS STRING) d FROM {table} "
        "WHERE transaction_id = 1"
    ).first()
    assert row["a"] == "922337203685477.5807"  # SQL Server MONEY max value
    assert row["d"] == "2024-01-18 15:21:30.123"


def test_primary_key_columns_reject_nulls(ddl_spark, gold_schema):
    with pytest.raises(Exception, match="(?i)null"):
        ddl_spark.sql(
            f"INSERT INTO spark_catalog.{gold_schema}.dim_branch VALUES (NULL, 'KCP', 'Jakarta')"
        )


def test_varchar_lengths_are_enforced(ddl_spark, gold_schema):
    table = f"spark_catalog.{gold_schema}.dim_customer"
    ddl_spark.sql(f"INSERT INTO {table} (customer_id, gender) VALUES (1, '{'x' * 10}')")
    with pytest.raises(Exception, match="(?i)varchar"):
        ddl_spark.sql(
            f"INSERT INTO {table} (customer_id, gender) VALUES (2, '{'x' * 11}')"
        )


def test_runner_cli_applies_ddl(ddl_spark):
    schema = f"gold_runner_test_{uuid.uuid4().hex[:8]}"
    try:
        statements = runner.main(
            ["--catalog", "spark_catalog", "--schema", schema, "--skip-constraints"],
            spark=ddl_spark,
        )
        assert len(statements) == 5
        tables = {
            r["tableName"]
            for r in ddl_spark.sql(f"SHOW TABLES IN spark_catalog.{schema}").collect()
        }
        assert tables == set(GOLD_TABLES)
    finally:
        ddl_spark.sql(f"DROP SCHEMA IF EXISTS spark_catalog.{schema} CASCADE")


def test_runner_defaults():
    args = runner.parse_args([])
    assert (args.catalog, args.schema, args.skip_constraints) == (
        "banking_dwh",
        "gold",
        False,
    )


def test_runner_bootstraps_sys_path_without_dunder_file(monkeypatch):
    """Serverless spark_python_task exec()s runner.py with no __file__ and no package on sys.path."""
    path = Path(runner.__file__)
    src_dir = str(path.resolve().parents[2])
    monkeypatch.setattr(
        sys, "path", [p for p in sys.path if Path(p).resolve() != Path(src_dir)]
    )
    for name in [
        m for m in sys.modules if m == "banking_etl" or m.startswith("banking_etl.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    namespace = {"__name__": "__exec__"}
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)  # noqa: S102
    assert namespace["parse_args"](["--schema", "s"]).schema == "s"
    assert sys.path[0] == src_dir
