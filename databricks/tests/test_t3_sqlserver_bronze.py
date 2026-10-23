"""Ticket 3: SQL Server (JDBC) -> Bronze.

Pure-Spark tests run against fixtures shaped like the SQL Server source tables. The
``mssql`` tests use a dockerised SQL Server with ``sample.bak`` restored and skip cleanly
when it (or the mssql-jdbc jar) is not available. Configure with ``BDWH_MSSQL_HOST``,
``BDWH_MSSQL_PORT``, ``BDWH_MSSQL_USER``, ``MSSQL_PASSWORD`` and ``MSSQL_JDBC_JAR``.
"""

from __future__ import annotations

import glob
import os
import re
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from banking_dwh.bronze import sqlserver as ss
from banking_dwh.contracts.tables import (
    BRONZE_SQLSERVER_ACCOUNT,
    BRONZE_SQLSERVER_CUSTOMER,
    BRONZE_SQLSERVER_TRANSACTION,
)

# --------------------------------------------------------------------------------------
# Fixtures shaped like sample.dbo.* (subset of the real rows + filler to the real counts)
# --------------------------------------------------------------------------------------
SOURCE_DDL = {
    "branch": "branch_id INT, branch_name STRING, branch_location STRING",
    "state": "state_id INT, state_name STRING",
    "city": "city_id INT, city_name STRING, state_id INT",
    "customer": "customer_id INT, customer_name STRING, address STRING, city_id INT, age STRING, gender STRING, email STRING",
    "account": "account_id INT, customer_id INT, account_type STRING, balance INT, date_opened TIMESTAMP, status STRING",
    "transaction_db": "transaction_id INT, account_id INT, transaction_date TIMESTAMP, amount INT, transaction_type STRING, branch_id INT",
}


def _rows(table: str, n: int) -> list[tuple]:
    ts = datetime(2022, 1, 1, 9, 10)
    make = {
        "branch": lambda i: (i, f"Branch {i}", f"Location {i}"),
        "state": lambda i: (i, f"State {i}"),
        "city": lambda i: (i, f"City {i}", (i % 9) + 1),
        "customer": lambda i: (i, f"Customer {i}", f"Address {i}", (i % 52) + 1, str(20 + i), "male", f"c{i}@x.com"),
        "account": lambda i: (i, (i % 20) + 1, "savings", 1000 * i, ts, "active"),
        "transaction_db": lambda i: (i, (i % 21) + 1, ts, 100000 * i, "Deposit" if i % 2 else "Withdrawal", 1),
    }[table]
    return [make(i) for i in range(1, n + 1)]


@pytest.fixture(scope="module")
def staged_root(spark, tmp_path_factory) -> str:
    root = tmp_path_factory.mktemp("landing") / "sqlserver"
    for table, n in ss.EXPECTED_ROW_COUNTS.items():
        spark.createDataFrame(_rows(table, n), SOURCE_DDL[table]).write.parquet(str(root / table))
    return str(root)


def _type_strings(spark, name: str) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in spark.table(name).schema.fields}


def _contract_types(contract) -> dict[str, str]:
    return {c.name: ss.spark_type(c.type).simpleString() for c in contract.columns}


# --------------------------------------------------------------------------------------
# Pure Python
# --------------------------------------------------------------------------------------
def test_contract_map_covers_configured_tables(local_cfg):
    assert set(ss.CONTRACTS) == set(local_cfg.jdbc.source_tables)
    assert set(ss.EXPECTED_ROW_COUNTS) == set(ss.CONTRACTS)
    for table, c in ss.CONTRACTS.items():
        assert c.name == f"sqlserver_{table}" and c.layer == "bronze" and c.owner_ticket == 3
        assert [x for x in c.column_names if x.startswith("_")] == list(ss.LINEAGE_COLUMNS)


def test_resolve_tables(local_cfg):
    assert ss.resolve_tables(local_cfg) == list(local_cfg.jdbc.source_tables)
    assert ss.resolve_tables(local_cfg, "all") == list(local_cfg.jdbc.source_tables)
    assert ss.resolve_tables(local_cfg, " branch, account,branch ") == ["branch", "account"]
    assert ss.resolve_tables(local_cfg, ["state"]) == ["state"]
    for bad in ("sysdiagrams", "orders", "branch;DROP TABLE x", "dbo.branch", " , ", [""]):
        with pytest.raises(ValueError):
            ss.resolve_tables(local_cfg, bad)


def test_source_query_matches_talend_tmssqlinput():
    # Load_FactTransaction tMSSqlInput QUERY: explicit dbo.<table>.<col> list, no WHERE.
    q = ss.source_query("transaction_db")
    assert re.sub(r"\s+", " ", q) == (
        "SELECT dbo.transaction_db.transaction_id, dbo.transaction_db.account_id, "
        "dbo.transaction_db.transaction_date, dbo.transaction_db.amount, "
        "dbo.transaction_db.transaction_type, dbo.transaction_db.branch_id FROM dbo.transaction_db"
    )
    assert "WHERE" not in ss.source_query("customer")
    with pytest.raises(ValueError):
        ss.source_query("sysdiagrams")
    with pytest.raises(ValueError):
        ss.source_query("branch", "dbo; DROP")


def test_local_jdbc_options_verify_tls_by_default(local_cfg):
    kw = dict(host="h", port=1433, database="sample", user="u", password="p")
    strict = ss.local_jdbc_options(local_cfg, **kw)
    assert "trustServerCertificate=false" in strict["url"] and "encrypt=true" in strict["url"]
    assert strict["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert "trustServerCertificate=true" in ss.local_jdbc_options(local_cfg, **kw, trust_server_certificate=True)["url"]


def test_landing_paths_for_each_table(local_cfg):
    for table in ss.CONTRACTS:
        assert local_cfg.landing_path("sqlserver", table) == (
            f"/Volumes/spark_catalog/t_bronze/landing/sqlserver/{table}"
        )


def test_batch_id_and_types():
    assert ss.validate_batch_id(ss.new_batch_id())
    assert ss.validate_batch_id("123456789")
    for bad in ("", "x' OR '1'='1", "a b", "x" * 129):
        with pytest.raises(ValueError):
            ss.validate_batch_id(bad)
    assert ss.spark_type("DECIMAL(19,4)").simpleString() == "decimal(19,4)"
    with pytest.raises(ValueError):
        ss.spark_type("GEOGRAPHY")


def test_ingest_argument_validation(spark, local_schemas):
    with pytest.raises(ValueError, match="source must be"):
        ss.ingest(spark, local_schemas, source="ftp")
    with pytest.raises(ValueError, match="JDBC options"):
        ss.ingest(spark, local_schemas, source="jdbc", jdbc=None)


# --------------------------------------------------------------------------------------
# Pure Spark: transform
# --------------------------------------------------------------------------------------
def test_conform_adds_lineage_and_matches_contract(spark):
    src = spark.createDataFrame(_rows("transaction_db", 3), SOURCE_DDL["transaction_db"])
    ts = datetime(2026, 10, 23, 12, 0, 0)
    out = ss.conform_to_contract(src, BRONZE_SQLSERVER_TRANSACTION, batch_id="b1", ingested_at=ts)
    assert out.columns == BRONZE_SQLSERVER_TRANSACTION.column_names
    assert {f.name: f.dataType.simpleString() for f in out.schema.fields} == _contract_types(BRONZE_SQLSERVER_TRANSACTION)
    rows = out.orderBy("transaction_id").collect()
    assert [r.transaction_id for r in rows] == [1, 2, 3]
    assert {r._batch_id for r in rows} == {"b1"} and {r._ingested_at for r in rows} == {ts}
    assert rows[1].amount == 200000 and rows[0].transaction_type == "Deposit"


def test_conform_casts_only_mismatched_types_and_is_case_insensitive(spark):
    # e.g. a staged file where balance landed as BIGINT and columns are upper-cased.
    src = spark.createDataFrame(
        [(1, 2, "savings", 5000, datetime(2020, 1, 1), "active", "extra")],
        "ACCOUNT_ID INT, Customer_Id INT, account_type STRING, balance BIGINT, date_opened TIMESTAMP, status STRING, junk STRING",
    )
    out = ss.conform_to_contract(src, BRONZE_SQLSERVER_ACCOUNT, batch_id="b", ingested_at=datetime(2026, 1, 1))
    assert out.columns == BRONZE_SQLSERVER_ACCOUNT.column_names  # extra column dropped
    assert dict(out.dtypes)["balance"] == "int"
    r = out.first()
    assert (r.account_id, r.customer_id, r.balance) == (1, 2, 5000)


def test_conform_keeps_age_as_string(spark):
    src = spark.createDataFrame(_rows("customer", 2), SOURCE_DDL["customer"])
    out = ss.conform_to_contract(src, BRONZE_SQLSERVER_CUSTOMER, batch_id="b", ingested_at=datetime(2026, 1, 1))
    assert dict(out.dtypes)["age"] == "string"
    assert out.orderBy("customer_id").first().age == "21"


def test_conform_missing_column_fails(spark):
    src = spark.createDataFrame([(1, "x")], "customer_id INT, customer_name STRING")
    with pytest.raises(ValueError, match="missing contract columns"):
        ss.conform_to_contract(src, BRONZE_SQLSERVER_CUSTOMER, batch_id="b", ingested_at=datetime(2026, 1, 1))


def test_null_key_count(spark):
    src = spark.createDataFrame([(None, 1, None, 1, "Deposit", 1), (2, 1, None, 1, "Deposit", 1)],
                                SOURCE_DDL["transaction_db"])
    out = ss.conform_to_contract(src, BRONZE_SQLSERVER_TRANSACTION, batch_id="b", ingested_at=datetime(2026, 1, 1))
    assert ss.null_key_count(out, BRONZE_SQLSERVER_TRANSACTION) == 1


def test_char_varchar_as_string_restores_conf(spark):
    before = spark.conf.get(ss._CHAR_AS_STRING, None)
    with ss.char_varchar_as_string(spark) as applied:
        assert applied and spark.conf.get(ss._CHAR_AS_STRING) == "true"
    assert spark.conf.get(ss._CHAR_AS_STRING, None) in (before, "false")


class _ConnectLikeConf:
    """Mimics Databricks serverless: the legacy conf cannot be set (CONFIG_NOT_AVAILABLE)."""

    def get(self, key, default=None):
        return default

    def set(self, key, value):
        raise RuntimeError("[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION] Configuration is not available.")


def test_char_varchar_as_string_tolerates_locked_conf():
    class FakeSpark:
        conf = _ConnectLikeConf()

    with ss.char_varchar_as_string(FakeSpark()) as applied:
        assert applied is False


# --------------------------------------------------------------------------------------
# Pure Spark: staged ingest into local Delta Bronze
# --------------------------------------------------------------------------------------
def test_staged_ingest_writes_all_contract_tables(spark, local_schemas, staged_root):
    cfg = local_schemas
    s = ss.ingest(spark, cfg, source="staged", staged_root=staged_root, batch_id="t3-batch-1",
                  assert_counts=True, unity_catalog=False)
    assert s["counts_ok"] and s["row_counts"] == ss.EXPECTED_ROW_COUNTS
    assert s["char_varchar_as_string"] is True
    assert s["source"] == "staged" and s["batch_id"] == "t3-batch-1"
    for table, contract in ss.CONTRACTS.items():
        name = cfg.table("bronze", f"sqlserver_{table}")
        assert name == f"spark_catalog.t_bronze.sqlserver_{table}"
        assert spark.table(name).columns == contract.column_names
        assert _type_strings(spark, name) == _contract_types(contract)
        assert spark.table(name).filter("_batch_id = 't3-batch-1'").count() == ss.EXPECTED_ROW_COUNTS[table]
        props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {name}").collect()}
        assert props["banking_dwh.owner_ticket"] == "3" and props["banking_dwh.layer"] == "bronze"


def test_staged_ingest_is_full_refresh(spark, local_schemas, staged_root):
    cfg = local_schemas
    ss.ingest(spark, cfg, source="staged", staged_root=staged_root, batch_id="r1", unity_catalog=False)
    s = ss.ingest(spark, cfg, source="staged", staged_root=staged_root, batch_id="r2",
                  tables="branch,transaction_db", unity_catalog=False)
    assert [t["source_table"] for t in s["tables"]] == ["branch", "transaction_db"]
    branch = spark.table(cfg.table("bronze", "sqlserver_branch"))
    assert branch.count() == 5 and {r._batch_id for r in branch.select("_batch_id").collect()} == {"r2"}
    # Tables not requested in this batch keep their previous snapshot.
    state = spark.table(cfg.table("bronze", "sqlserver_state"))
    assert {r._batch_id for r in state.select("_batch_id").collect()} == {"r1"}
    hist = spark.sql(f"DESCRIBE HISTORY {cfg.table('bronze', 'sqlserver_branch')}").collect()
    assert hist[0].operation in ("WRITE", "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT")


def _batch_rows(spark, cfg, table: str, batch_id: str) -> int:
    name = cfg.table("bronze", f"sqlserver_{table}")
    return spark.table(name).filter(f"_batch_id = '{batch_id}'").count() if spark.catalog.tableExists(name) else 0


def test_assert_counts_fails_on_mismatch(spark, local_schemas, staged_root):
    ss.ingest(spark, local_schemas, source="staged", staged_root=staged_root, tables="branch,state",
              batch_id="counts-ok", unity_catalog=False)
    with pytest.raises(AssertionError, match="Row-count mismatch.*no table was written"):
        ss.ingest(spark, local_schemas, source="staged", staged_root=staged_root, tables="branch,state",
                  batch_id="counts-bad", expected_counts={"state": 6}, assert_counts=True, unity_catalog=False)
    # Validation runs before any write: branch (count OK) still holds the previous batch.
    for t in ("branch", "state"):
        assert _batch_rows(spark, local_schemas, t, "counts-bad") == 0
        assert _batch_rows(spark, local_schemas, t, "counts-ok") == ss.EXPECTED_ROW_COUNTS[t]
    s = ss.ingest(spark, local_schemas, source="staged", staged_root=staged_root, tables="branch",
                  expected_counts={"branch": 6}, unity_catalog=False)
    assert s["counts_ok"] is False and s["tables"][0]["expected_rows"] == 6


def test_failing_table_leaves_earlier_bronze_tables_untouched(spark, local_schemas, staged_root, tmp_path):
    cfg = local_schemas
    root = tmp_path / "sqlserver"
    for table in ss.CONTRACTS:
        df = spark.read.parquet(f"{staged_root}/{table}")
        if table == "transaction_db":
            df = df.drop("amount")
        df.write.parquet(str(root / table))
    with pytest.raises(ValueError, match="missing contract columns"):
        ss.ingest(spark, cfg, source="staged", staged_root=str(root), batch_id="mixed", unity_catalog=False)
    assert all(_batch_rows(spark, cfg, t, "mixed") == 0 for t in ss.CONTRACTS)


class _FakeJdbc:
    """Stands in for SQL Server: serves fixture rows, optionally failing one table's read."""

    def __init__(self, spark, fail: str | None = None):
        self.spark, self.fail = spark, fail

    def read(self, spark, options, table, source_schema="dbo"):
        if table == self.fail:
            raise RuntimeError(f"connection reset while reading {table}")
        return spark.createDataFrame(_rows(table, ss.EXPECTED_ROW_COUNTS[table]), SOURCE_DDL[table])


def test_failed_jdbc_extract_keeps_previous_staged_snapshot(spark, local_schemas, tmp_path, monkeypatch):
    cfg, landing = local_schemas, tmp_path / "sqlserver"
    for table in ss.CONTRACTS:  # previous good snapshot: 2 rows per table
        spark.createDataFrame(_rows(table, 2), SOURCE_DDL[table]).write.parquet(str(landing / table))
    monkeypatch.setattr(ss, "list_source_tables", lambda *a, **k: sorted(ss.CONTRACTS))

    monkeypatch.setattr(ss, "read_jdbc", _FakeJdbc(spark, fail="transaction_db").read)
    with pytest.raises(RuntimeError, match="connection reset"):
        ss.ingest(spark, cfg, source="jdbc", jdbc={"url": "jdbc:sqlserver://fake"}, staged_root=str(landing),
                  batch_id="jdbc-fail", unity_catalog=False)
    assert {t: spark.read.parquet(str(landing / t)).count() for t in ss.CONTRACTS} == dict.fromkeys(ss.CONTRACTS, 2)
    assert not (landing / "_incoming").exists()
    assert all(_batch_rows(spark, cfg, t, "jdbc-fail") == 0 for t in ss.CONTRACTS)

    monkeypatch.setattr(ss, "read_jdbc", _FakeJdbc(spark).read)
    s = ss.ingest(spark, cfg, source="jdbc", jdbc={"url": "jdbc:sqlserver://fake"}, staged_root=str(landing),
                  batch_id="jdbc-ok", assert_counts=True, unity_catalog=False)
    assert s["counts_ok"] and all(t["staged_to"] == str(landing / t["source_table"]) for t in s["tables"])
    assert {t: spark.read.parquet(str(landing / t)).count() for t in ss.CONTRACTS} == ss.EXPECTED_ROW_COUNTS
    assert not (landing / "_incoming").exists()


# --------------------------------------------------------------------------------------
# Dockerised SQL Server (sample.bak restored): skipped when unavailable
# --------------------------------------------------------------------------------------
MSSQL_HOST = os.environ.get("BDWH_MSSQL_HOST", "localhost")
MSSQL_PORT = int(os.environ.get("BDWH_MSSQL_PORT", "1433"))
MSSQL_USER = os.environ.get("BDWH_MSSQL_USER", "sa")


def _mssql_jar() -> str | None:
    env = os.environ.get("MSSQL_JDBC_JAR")
    if env and Path(env).exists():
        return env
    hits = sorted(glob.glob(str(Path.home() / ".ivy2*" / "jars" / "*mssql-jdbc*.jar")))
    return hits[-1] if hits else None


def _mssql_reachable() -> bool:
    try:
        with socket.create_connection((MSSQL_HOST, MSSQL_PORT), timeout=1):
            return True
    except OSError:
        return False


mssql = pytest.mark.skipif(
    not (os.environ.get("MSSQL_PASSWORD") and _mssql_reachable() and _mssql_jar()),
    reason="dockerised SQL Server with sample.bak (MSSQL_PASSWORD, reachable host, mssql-jdbc jar) not available",
)


@pytest.fixture(scope="module")
def jdbc_opts(spark, local_cfg):
    spark.sql(f"ADD JAR {_mssql_jar()}")
    # Throwaway docker container with a self-signed certificate.
    return ss.local_jdbc_options(local_cfg, host=MSSQL_HOST, port=MSSQL_PORT, database="sample", user=MSSQL_USER,
                                 password=os.environ["MSSQL_PASSWORD"], trust_server_certificate=True)


@mssql
def test_mssql_lists_tables_without_sysdiagrams(spark, jdbc_opts):
    tables = ss.list_source_tables(spark, jdbc_opts)
    assert "sysdiagrams" not in tables
    assert set(ss.CONTRACTS) <= set(tables)


@mssql
def test_mssql_jdbc_ingest_to_bronze_and_staging(spark, local_schemas, jdbc_opts, tmp_path):
    cfg = local_schemas
    landing = str(tmp_path / "sqlserver")
    s = ss.ingest(spark, cfg, source="jdbc", jdbc=jdbc_opts, staged_root=landing, batch_id="jdbc-1",
                  assert_counts=True, unity_catalog=False)
    assert s["row_counts"] == ss.EXPECTED_ROW_COUNTS
    for table, contract in ss.CONTRACTS.items():
        name = cfg.table("bronze", f"sqlserver_{table}")
        # charVarcharAsString: varchar(n) columns land as STRING, not VARCHAR(n).
        assert _type_strings(spark, name) == _contract_types(contract)
        assert spark.read.parquet(f"{landing}/{table}").count() == ss.EXPECTED_ROW_COUNTS[table]
    first = spark.table(cfg.table("bronze", "sqlserver_transaction_db")).orderBy("transaction_id").first()
    assert (first.transaction_id, first.amount, first.transaction_type) == (1, 100000, "Deposit")
    assert first.transaction_date == datetime(2022, 1, 1, 9, 10)
    cust = {r.customer_id: r for r in spark.table(cfg.table("bronze", "sqlserver_customer")).collect()}
    assert all(isinstance(r.age, (str, type(None))) for r in cust.values())


@mssql
def test_mssql_jdbc_ingest_without_char_conf_keeps_string_columns(spark, local_schemas, jdbc_opts, monkeypatch):
    # Serverless path: the legacy conf is unavailable, so the contract DDL must keep STRING columns.
    from contextlib import contextmanager

    @contextmanager
    def locked(_spark):
        yield False

    monkeypatch.setattr(ss, "char_varchar_as_string", locked)
    cfg = local_schemas
    s = ss.ingest(spark, cfg, source="jdbc", jdbc=jdbc_opts, stage=False, batch_id="jdbc-noconf",
                  assert_counts=True, unity_catalog=False)
    assert s["char_varchar_as_string"] is False and s["counts_ok"]
    for table, contract in ss.CONTRACTS.items():
        name = cfg.table("bronze", f"sqlserver_{table}")
        assert _type_strings(spark, name) == _contract_types(contract)
        desc = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE {name}").collect()}
        assert not any("varchar" in str(v).lower() for v in desc.values()), desc


@mssql
def test_mssql_cli_extract_then_staged_ingest(spark, local_schemas, tmp_path):
    out = tmp_path / "staged"
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
           "MSSQL_JDBC_JAR": _mssql_jar()}
    proc = subprocess.run(
        [sys.executable, "-m", "banking_dwh.bronze.sqlserver", "extract", "--host", MSSQL_HOST,
         "--port", str(MSSQL_PORT), "--user", MSSQL_USER, "--out", str(out), "--trust-server-certificate"],
        env=env, capture_output=True, text=True, timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert '"transaction_db": 10' in proc.stdout
    s = ss.ingest(spark, local_schemas, source="staged", staged_root=str(out), batch_id="cli-1",
                  assert_counts=True, unity_catalog=False)
    assert s["counts_ok"]
