import csv
from datetime import datetime
from decimal import Decimal

import pytest
import yaml
from pyspark.sql.types import DecimalType, IntegerType, LongType, StringType, TimestampType

from banking_etl.bronze.files import read_transaction_csv, read_transaction_excel
from banking_etl.bronze.fixtures import stage_fixtures
from banking_etl.bronze.sqlserver import ingest_table
from banking_etl.config import Settings
from banking_etl.dims.account import load_dim_account, load_silver_account
from banking_etl.dims.branch import load_dim_branch, load_silver_branch
from banking_etl.facts.transaction import (
    FACT_COLUMNS,
    FK_REJECT_REASONS,
    REASON_COL,
    REJECT_INVALID_TRANSACTION_DATE,
    REJECT_MALFORMED_RECORD,
    REJECT_MISSING_DIM_ACCOUNT,
    REJECT_MISSING_DIM_BRANCH,
    REJECT_NULL_TRANSACTION_ID,
    SILVER_COLUMNS,
    bronze_tables,
    build_fact,
    conform_csv,
    conform_excel,
    conform_sqlserver,
    dim_tables,
    fact_transaction_table,
    load_fact_transaction,
    load_silver_transaction,
    rejects_table,
    silver_transaction_table,
    transform_transaction,
)

from conftest import DATA_SOURCES, FIXTURES, ROOT

PREFIX = "t8_"
PARITY_CSV = FIXTURES / "parity" / "fact_transaction.csv"

SQL_DDL = "transaction_id INT, account_id INT, transaction_date TIMESTAMP, amount INT, transaction_type STRING, branch_id INT, _ingested_at TIMESTAMP, _source STRING"
EXCEL_DDL = "transaction_id INT, account_id INT, transaction_date TIMESTAMP_NTZ, amount INT, transaction_type STRING, branch_id INT, _rescued_data STRING, _ingested_at TIMESTAMP, _source STRING, _source_file STRING"
CSV_DDL = "transaction_id INT, account_id INT, transaction_date STRING, amount INT, transaction_type STRING, branch_id INT, _rescued_data STRING, _ingested_at TIMESTAMP, _source STRING, _source_file STRING"
T0 = datetime(2026, 1, 1, 0, 0, 0)


def sql_df(spark, rows):
    return spark.createDataFrame([(*r, T0, "fixture") for r in rows], SQL_DDL)


def excel_df(spark, rows, rescued=None):
    return spark.createDataFrame([(*r, rescued, T0, "file_excel", "x.xlsx") for r in rows], EXCEL_DDL)


def csv_df(spark, rows, rescued=None, ingested=T0, file="a.csv"):
    return spark.createDataFrame([(*r, rescued, ingested, "file_csv", file) for r in rows], CSV_DDL)


def parity_rows():
    with PARITY_CSV.open(newline="", encoding="utf-8") as fh:
        return [
            (
                int(r["TransactionID"]),
                int(r["AccountID"]),
                datetime.fromisoformat(r["TransactionDate"]),
                Decimal(r["Amount"]),
                r["TransactionType"],
                int(r["BranchID"]),
            )
            for r in csv.DictReader(fh)
        ]


def source_csv(spark):
    return read_transaction_csv(spark, str(DATA_SOURCES / "transaction_csv.csv"))


def write_bronze(spark, settings, csv_frame=None, excel_frame=None):
    tables = bronze_tables(settings)
    if csv_frame is not None:
        csv_frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables["csv"])
    if excel_frame is not None:
        excel_frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables["excel"])


@pytest.fixture(scope="module")
def t8(spark, tmp_path_factory):
    """Bronze from the real sources (sample_db fixtures + data_sources files) and loaded dims."""
    settings = Settings(catalog=None, schema_prefix=PREFIX)
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.schema(layer)}")
    root = tmp_path_factory.mktemp("landing_t8") / "sample_db"
    stage_fixtures(root, ["transaction_db", "account", "branch"])
    for table in ("transaction_db", "account", "branch"):
        ingest_table(spark, settings, table, fixture_root=str(root))
    write_bronze(
        spark,
        settings,
        csv_frame=source_csv(spark),
        excel_frame=read_transaction_excel(spark, str(DATA_SOURCES / "transaction_excel.xlsx")),
    )
    load_silver_branch(spark, settings)
    load_dim_branch(spark, settings)
    load_silver_account(spark, settings)
    load_dim_account(spark, settings)
    yield settings
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"DROP DATABASE IF EXISTS {settings.schema(layer)} CASCADE")


def fact_tuples(spark, settings):
    rows = spark.table(fact_transaction_table(settings)).orderBy("TransactionID").collect()
    return [tuple(r[c] for c in FACT_COLUMNS[:6]) for r in rows]


# --------------------------------------------------------------------------- end to end (real sources)


def test_end_to_end_matches_sqlserver_parity(spark, t8):
    silver = load_silver_transaction(spark, t8)
    assert silver["input_rows"] == {"sqlserver": 10, "excel": 7, "csv": 12}
    assert silver["input_rejects"] == {}
    assert silver["duplicates_dropped"] == 4  # ids 6, 7 (Excel) and 14, 15 (CSV)
    assert silver["rows"] == 25
    assert silver["rows_by_source"] == {"csv": 10, "excel": 5, "sqlserver": 10}

    gold = load_fact_transaction(spark, t8)
    assert gold["rows"] == 22
    assert gold["fk_rejects"] == {REJECT_MISSING_DIM_ACCOUNT: 3}
    assert gold["total_rejects"] == 3
    # DWH.dbo.FactTransaction after the Talend-equivalent batch load on SQL Server 2022.
    assert fact_tuples(spark, t8) == parity_rows()

    rejects = spark.table(rejects_table(t8)).orderBy("TransactionID").collect()
    assert [(r.TransactionID, r.AccountID, r.reject_reason) for r in rejects] == [
        (23, 22, REJECT_MISSING_DIM_ACCOUNT),
        (24, 23, REJECT_MISSING_DIM_ACCOUNT),
        (25, 23, REJECT_MISSING_DIM_ACCOUNT),
    ]
    assert all(r.AccountKey is None and r.BranchKey is not None and r.rejected_at for r in rejects)


def test_surrogate_keys_come_from_dims(spark, t8):
    load_silver_transaction(spark, t8)
    load_fact_transaction(spark, t8)
    fact = spark.table(fact_transaction_table(t8))
    acct = spark.table(dim_tables(t8)["account"])
    branch = spark.table(dim_tables(t8)["branch"])
    mismatched = (
        fact.join(acct, "AccountID")
        .join(branch, "BranchID")
        .where((fact.AccountKey != acct.AccountKey) | (fact.BranchKey != branch.BranchKey))
    )
    assert mismatched.count() == 0
    assert fact.where("AccountKey IS NULL OR BranchKey IS NULL").count() == 0
    types = {f.name: f.dataType for f in fact.schema.fields}
    assert types["Amount"] == DecimalType(19, 4)
    assert types["TransactionDate"] == TimestampType()
    assert types["AccountKey"] == LongType()


def test_silver_schema(spark, t8):
    load_silver_transaction(spark, t8)
    df = spark.table(silver_transaction_table(t8))
    assert tuple(df.columns) == SILVER_COLUMNS
    types = {f.name: f.dataType for f in df.schema.fields}
    assert types["transaction_id"] == IntegerType()
    assert types["transaction_date"] == TimestampType()
    assert types["amount"] == DecimalType(19, 4)
    assert types["transaction_type"] == StringType()
    assert df.groupBy("transaction_id").count().where("count > 1").count() == 0


def test_full_refresh_is_idempotent_and_truncates(spark, t8):
    for _ in range(2):
        load_silver_transaction(spark, t8)
        gold = load_fact_transaction(spark, t8)
    assert gold["rows"] == 22 and gold["total_rejects"] == 3
    assert fact_tuples(spark, t8) == parity_rows()

    # Talend TRUNCATE + INSERT: a transaction that disappears from the sources disappears from the fact,
    # and stale rejects are replaced, not accumulated.
    try:
        write_bronze(spark, t8, csv_frame=source_csv(spark).where("transaction_id NOT IN (22, 25)"))
        load_silver_transaction(spark, t8)
        gold = load_fact_transaction(spark, t8)
        assert gold["rows"] == 21
        assert gold["fk_rejects"] == {REJECT_MISSING_DIM_ACCOUNT: 2}
        assert 22 not in [r[0] for r in fact_tuples(spark, t8)]
    finally:
        write_bronze(spark, t8, csv_frame=source_csv(spark))
    load_silver_transaction(spark, t8)
    assert load_fact_transaction(spark, t8)["rows"] == 22


def test_input_rejects_and_fk_rejects_are_replaced_independently(spark, t8):
    bad = csv_df(spark, [(90, 1, "31-02-2024 10:00:00", 5, "Deposit", 1)])
    try:
        write_bronze(spark, t8, csv_frame=source_csv(spark).unionByName(bad.select(*source_csv(spark).columns)))
        silver = load_silver_transaction(spark, t8)
        assert silver["input_rejects"] == {REJECT_INVALID_TRANSACTION_DATE: 1}
        load_fact_transaction(spark, t8)
        load_fact_transaction(spark, t8)  # gold rerun keeps the silver-owned rejects
        reasons = sorted(r.reject_reason for r in spark.table(rejects_table(t8)).collect())
        assert reasons == [REJECT_INVALID_TRANSACTION_DATE] + [REJECT_MISSING_DIM_ACCOUNT] * 3
    finally:
        write_bronze(spark, t8, csv_frame=source_csv(spark))
    load_silver_transaction(spark, t8)  # silver rerun clears its own rejects, keeps FK rejects
    reasons = sorted(r.reject_reason for r in spark.table(rejects_table(t8)).collect())
    assert reasons == [REJECT_MISSING_DIM_ACCOUNT] * 3


def test_empty_dims_raise(spark, tmp_path):
    settings = Settings(catalog=None, schema_prefix="t8_empty_")
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {settings.schema(layer)}")
    try:
        from banking_etl.gold.ddl import apply_star_schema

        apply_star_schema(spark, settings, unity_catalog=False)
        with pytest.raises(ValueError, match="empty"):
            load_fact_transaction(spark, settings)
    finally:
        for layer in ("bronze", "silver", "gold", "ops"):
            spark.sql(f"DROP DATABASE IF EXISTS {settings.schema(layer)} CASCADE")


# --------------------------------------------------------------------------- conform / date parsing


def test_csv_date_parsing(spark):
    df = conform_csv(
        csv_df(
            spark,
            [
                (1, 1, "21-01-2024 14:00:00", 100, "Deposit", 1),
                (2, 1, "31-12-2023 23:59:59", 100, "Deposit", 1),
                (3, 1, "32-01-2024 10:00:00", 100, "Deposit", 1),  # invalid day
                (4, 1, "2024-01-21 14:00:00", 100, "Deposit", 1),  # wrong pattern
                (5, 1, None, 100, "Deposit", 1),  # nullable column: NULL date is valid
                (6, 1, "", 100, "Deposit", 1),  # Talend: empty -> NULL
                (7, 1, "21-01-2024", 100, "Deposit", 1),  # time part missing
            ],
        )
    )
    got = {r.transaction_id: (r.transaction_date, r[REASON_COL]) for r in df.collect()}
    assert got[1] == (datetime(2024, 1, 21, 14, 0, 0), None)
    assert got[2] == (datetime(2023, 12, 31, 23, 59, 59), None)
    assert got[3] == (None, REJECT_INVALID_TRANSACTION_DATE)
    assert got[4] == (None, REJECT_INVALID_TRANSACTION_DATE)
    assert got[5] == (None, None)
    assert got[6] == (None, None)
    assert got[7] == (None, REJECT_INVALID_TRANSACTION_DATE)


def test_excel_and_sqlserver_dates_keep_wall_clock(spark):
    excel = conform_excel(excel_df(spark, [(11, 10, datetime(2024, 1, 20, 15, 0), 1000000, "Transfer", 1)])).first()
    assert excel.transaction_date == datetime(2024, 1, 20, 15, 0)
    assert excel.amount == Decimal("1000000.0000")
    sql = conform_sqlserver(sql_df(spark, [(1, 1, datetime(2022, 1, 1, 9, 10), 100000, "Deposit", 1)])).first()
    assert sql.transaction_date == datetime(2022, 1, 1, 9, 10)
    assert sql.amount == Decimal("100000.0000") and sql._source_order == 1 and sql[REASON_COL] is None


def test_excel_text_dates_use_talend_pattern(spark):
    ddl = EXCEL_DDL.replace("transaction_date TIMESTAMP_NTZ", "transaction_date STRING")
    rows = [(1, 1, "20-01-2024 15:00:00", 5, "Deposit", 1, None, T0, "file_excel", "x"), (2, 1, "bad", 5, "Deposit", 1, None, T0, "file_excel", "x")]
    got = {r.transaction_id: (r.transaction_date, r[REASON_COL]) for r in conform_excel(spark.createDataFrame(rows, ddl)).collect()}
    assert got == {1: (datetime(2024, 1, 20, 15, 0), None), 2: (None, REJECT_INVALID_TRANSACTION_DATE)}


def test_null_id_and_rescued_data_are_input_rejects(spark):
    df = conform_csv(csv_df(spark, [(None, 1, "bad", 5, "Deposit", 1)], rescued='{"amount":"12abc"}'))
    assert df.first()[REASON_COL] == ";".join(
        [REJECT_NULL_TRANSACTION_ID, REJECT_INVALID_TRANSACTION_DATE, REJECT_MALFORMED_RECORD]
    )


# --------------------------------------------------------------------------- tUnite + tUniqRow


def test_union_order_and_dedup_precedence(spark):
    sql = sql_df(spark, [(6, 6, datetime(2022, 2, 21, 13, 10), 50000, "Withdrawal", 1)])
    excel = excel_df(
        spark,
        [
            (6, 6, datetime(2024, 1, 18, 13, 10), 50000, "Withdrawal", 1),  # loses to SQL Server
            (14, 13, datetime(2024, 1, 21, 14, 0), 1500000, "Deposit", 4),  # beats CSV
        ],
    )
    csv_rows = csv_df(spark, [(14, 99, "21-01-2024 14:00:00", 1, "Deposit", 4), (16, 15, "22-01-2024 09:00:00", 100000, "Deposit", 1)])
    silver, rejects = transform_transaction(sql, excel, csv_rows)
    got = {r.transaction_id: r for r in silver.collect()}
    assert sorted(got) == [6, 14, 16]
    assert got[6]._source_system == "sqlserver" and got[6].transaction_date == datetime(2022, 2, 21, 13, 10)
    assert got[14]._source_system == "excel" and got[14].account_id == 13
    assert got[16]._source_system == "csv"
    assert rejects.count() == 0


def test_dedup_within_a_source_keeps_earliest_ingested(spark):
    early = csv_df(spark, [(30, 1, "01-02-2024 10:00:00", 1, "Deposit", 1)], ingested=datetime(2026, 1, 1), file="b.csv")
    late = csv_df(spark, [(30, 2, "01-02-2024 10:00:00", 2, "Deposit", 1)], ingested=datetime(2026, 1, 2), file="a.csv")
    silver, _ = transform_transaction(sql_df(spark, []), excel_df(spark, []), late.unionByName(early))
    assert [(r.transaction_id, r.account_id) for r in silver.collect()] == [(30, 1)]


def test_input_rejects_do_not_shadow_valid_duplicates(spark):
    # Talend drops an unparseable Excel row inside tFileInputExcel, before tUnite/tUniqRow,
    # so a valid CSV row with the same id survives.
    ddl = EXCEL_DDL.replace("transaction_date TIMESTAMP_NTZ", "transaction_date STRING")
    excel = spark.createDataFrame([(40, 1, "nope", 5, "Deposit", 1, None, T0, "file_excel", "x")], ddl)
    csv_rows = csv_df(spark, [(40, 2, "01-02-2024 10:00:00", 7, "Payment", 2)])
    silver, rejects = transform_transaction(sql_df(spark, []), excel, csv_rows)
    assert [(r.transaction_id, r._source_system, r.account_id) for r in silver.collect()] == [(40, "csv", 2)]
    assert [(r.transaction_id, r._source_system, r[REASON_COL]) for r in rejects.collect()] == [
        (40, "excel", REJECT_INVALID_TRANSACTION_DATE)
    ]


# --------------------------------------------------------------------------- lookups / FK rejects


def test_lookups_and_fk_rejects(spark):
    silver, _ = transform_transaction(
        sql_df(
            spark,
            [
                (1, 1, T0, 10, "Deposit", 1),  # matches both dims
                (2, 22, T0, 10, "Deposit", 1),  # unknown account
                (3, 1, T0, 10, "Deposit", 9),  # unknown branch
                (4, 22, T0, 10, "Deposit", 9),  # both unknown
                (5, None, T0, 10, "Deposit", None),  # NULL keys pass SQL Server FKs
            ],
        ),
        excel_df(spark, []),
        csv_df(spark, []),
    )
    dim_account = spark.createDataFrame([(101, 1)], "AccountKey BIGINT, AccountID INT")
    dim_branch = spark.createDataFrame([(201, 1)], "BranchKey BIGINT, BranchID INT")
    fact, rejects = build_fact(silver, dim_account, dim_branch)
    assert tuple(fact.columns) == FACT_COLUMNS
    assert {r.TransactionID: (r.AccountKey, r.BranchKey) for r in fact.collect()} == {1: (101, 201), 5: (None, None)}
    assert {r.TransactionID: (r.AccountKey, r.BranchKey, r.reject_reason) for r in rejects.collect()} == {
        2: (None, 201, REJECT_MISSING_DIM_ACCOUNT),
        3: (101, None, REJECT_MISSING_DIM_BRANCH),
        4: (None, None, f"{REJECT_MISSING_DIM_ACCOUNT};{REJECT_MISSING_DIM_BRANCH}"),
    }
    assert set(r.reject_reason for r in rejects.collect()) <= set(FK_REJECT_REASONS)


# --------------------------------------------------------------------------- bundle resource


def test_job_resource_is_serverless_and_ordered():
    doc = yaml.safe_load((ROOT / "resources" / "ticket-08-fact-transaction.yml").read_text())
    job = doc["resources"]["jobs"]["load_fact_transaction"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert list(tasks) == ["silver_transaction", "load_fact_transaction"]
    assert tasks["load_fact_transaction"]["depends_on"] == [{"task_key": "silver_transaction"}]
    for task in tasks.values():
        assert not {"new_cluster", "job_cluster_key", "existing_cluster_id"} & set(task)
        assert (ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve().exists()
    assert {p["name"] for p in job["parameters"]} == {"catalog", "schema_prefix"}
