"""Tests for banking_etl.load_dim_account, one group per rule of the Talend Load_DimAccount job."""

from __future__ import annotations

import datetime as dt
import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

DATABRICKS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DATABRICKS_ROOT / "src"))

pyspark = pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from banking_etl import load_dim_account as job  # noqa: E402

CATALOG = "spark_catalog"

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.IntegerType(), False),
        T.StructField("customer_id", T.IntegerType()),
        T.StructField("account_type", T.StringType()),
        T.StructField("balance", T.IntegerType()),
        T.StructField("date_opened", T.TimestampType()),
        T.StructField("status", T.StringType()),
    ]
)
NULLABLE_KEY_SCHEMA = T.StructType(
    [T.StructField(f.name, f.dataType, True) for f in BRONZE_SCHEMA.fields]
)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from delta import configure_spark_with_delta_pip

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("test_load_dim_account")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def config(spark):
    suffix = uuid.uuid4().hex[:8]
    cfg = job.DimAccountConfig(catalog=CATALOG, bronze_schema=f"bronze_{suffix}", gold_schema=f"gold_{suffix}")
    spark.sql(f"CREATE SCHEMA {CATALOG}.{cfg.bronze_schema}")
    yield cfg
    for schema in (cfg.bronze_schema, cfg.gold_schema):
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")


def ts(y, m, d, hh=0, mm=0, ss=0):
    return dt.datetime(y, m, d, hh, mm, ss)


SAMPLE_ROWS = [
    (1, 101, "Savings", 1500, ts(2020, 1, 15, 9, 30), "active"),
    (2, 102, "Checking", 250, ts(2021, 6, 1), "inactive"),
    (3, None, "Credit", None, None, None),
    (4, 101, "Checking", 0, ts(2019, 12, 31, 23, 59, 59), "closed"),
]


def write_bronze(spark, cfg, rows, schema=BRONZE_SCHEMA, mode="overwrite"):
    spark.createDataFrame(rows, schema).write.format("delta").mode(mode).option(
        "overwriteSchema", "true"
    ).saveAsTable(cfg.source_fqn)


def target_rows(spark, cfg):
    return {r.account_id: r.asDict() for r in spark.table(cfg.target_fqn).collect()}


# R1: source query is SELECT <6 cols> FROM dbo.account with no WHERE clause.
def test_r1_reads_every_source_row_without_filtering(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    job.run(spark, config)
    assert sorted(target_rows(spark, config)) == [1, 2, 3, 4]


def test_r1_ignores_extra_bronze_columns(spark, config):
    schema = T.StructType(BRONZE_SCHEMA.fields + [T.StructField("_source_file", T.StringType())])
    write_bronze(spark, config, [r + ("sample.bak",) for r in SAMPLE_ROWS], schema)
    job.run(spark, config)
    assert spark.table(config.target_fqn).columns == job.TARGET_COLUMNS


def test_r1_source_and_target_names_come_from_config():
    cfg = job.DimAccountConfig(catalog="cat", bronze_schema="b", gold_schema="g")
    assert cfg.source_fqn == "`cat`.`b`.`account`"
    assert cfg.target_fqn == "`cat`.`g`.`dim_account`"
    assert job.DimAccountConfig().target_fqn == "`banking_dwh`.`gold`.`dim_account`"


# R2: tMap is a straight row1.<col> copy for all six columns, nulls included.
def test_r2_tmap_is_a_one_to_one_column_copy(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    job.run(spark, config)
    rows = target_rows(spark, config)
    assert rows[1] == {
        "account_id": 1,
        "customer_id": 101,
        "account_type": "Savings",
        "balance": Decimal("1500.0000"),
        "date_opened": dt.date(2020, 1, 15),
        "status": "active",
    }
    assert rows[3] == {
        "account_id": 3,
        "customer_id": None,
        "account_type": "Credit",
        "balance": None,
        "date_opened": None,
        "status": None,
    }
    assert rows[2]["status"] == "inactive" and rows[4]["status"] == "closed"


# R3: id_Integer ids, Integer balance into MONEY, DATETIME2 date_opened into DATE.
def test_r3_target_schema_matches_dimaccount_ddl(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    job.run(spark, config)
    fields = {f.name: f for f in spark.table(config.target_fqn).schema.fields}
    assert fields["account_id"].dataType == T.IntegerType() and not fields["account_id"].nullable
    assert fields["customer_id"].dataType == T.IntegerType()
    assert fields["account_type"].dataType == T.StringType()
    assert fields["balance"].dataType == T.DecimalType(19, 4)
    assert fields["date_opened"].dataType == T.DateType()
    assert fields["status"].dataType == T.StringType()


def test_r3_datetime_is_truncated_to_date(spark, config):
    write_bronze(spark, config, [(1, 1, "Savings", 1, ts(2020, 1, 15, 23, 59, 59), "active")])
    job.run(spark, config)
    assert target_rows(spark, config)[1]["date_opened"] == dt.date(2020, 1, 15)


def test_r3_string_typed_bronze_is_cast(spark):
    schema = T.StructType([T.StructField(f.name, T.StringType()) for f in BRONZE_SCHEMA.fields])
    source = spark.createDataFrame(
        [
            ("1", "101", "Savings", "1500", "2020-01-15 09:30:00.0000000", "active"),
            ("2", "102", "Checking", "250", "01-06-2021", "active"),
            ("3", "abc", "Credit", "n/a", "not a date", "active"),
        ],
        schema,
    )
    rows = {r.account_id: r for r in job.transform(source).collect()}
    assert rows[1].customer_id == 101 and rows[1].balance == Decimal("1500.0000")
    assert rows[1].date_opened == dt.date(2020, 1, 15)
    assert rows[2].date_opened == dt.date(2021, 6, 1)
    assert rows[3].customer_id is None and rows[3].balance is None and rows[3].date_opened is None


# R4: TRIM_ALL_COLUMN=false and no per-column trim, so values keep their whitespace.
def test_r4_string_values_are_not_trimmed(spark, config):
    write_bronze(spark, config, [(1, 1, "  Savings ", 1, None, " active  ")])
    job.run(spark, config)
    row = target_rows(spark, config)[1]
    assert row["account_type"] == "  Savings " and row["status"] == " active  "


# R5: AccountID is the NOT NULL primary key; DIE_ON_ERROR=false rejects violating rows.
def test_r5_null_account_id_rows_are_rejected(spark, config):
    rows = SAMPLE_ROWS + [(None, 999, "Savings", 10, None, "active")]
    write_bronze(spark, config, rows, NULLABLE_KEY_SCHEMA)
    job.run(spark, config)
    rows_out = target_rows(spark, config)
    assert sorted(rows_out) == [1, 2, 3, 4]
    assert all(r["customer_id"] != 999 for r in rows_out.values())


def test_r5_duplicate_keys_collapse_to_one_row(spark, config):
    rows = SAMPLE_ROWS + [(1, 101, "Savings", 1500, ts(2020, 1, 15, 9, 30), "active")]
    write_bronze(spark, config, rows)
    job.run(spark, config)
    assert spark.table(config.target_fqn).count() == 4


def test_r5_latest_ingested_copy_wins_when_bronze_keeps_history(spark):
    schema = T.StructType(BRONZE_SCHEMA.fields + [T.StructField("_ingested_at", T.TimestampType())])
    source = spark.createDataFrame(
        [
            (1, 101, "Savings", 100, None, "active", ts(2024, 1, 1)),
            (1, 101, "Savings", 900, None, "closed", ts(2024, 3, 1)),
            (1, 101, "Savings", 500, None, "active", ts(2024, 2, 1)),
        ],
        schema,
    )
    rows = job.transform(source).collect()
    assert len(rows) == 1
    assert rows[0].balance == Decimal("900.0000") and rows[0].status == "closed"
    assert list(rows[0].asDict()) == job.TARGET_COLUMNS


# R6: TABLE_ACTION=CREATE_IF_NOT_EXISTS, DATA_ACTION=INSERT -> MERGE on account_id.
def test_r6_creates_gold_schema_and_table_when_missing(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    assert not spark.catalog.tableExists(f"{CATALOG}.{config.gold_schema}.dim_account")
    job.run(spark, config)
    assert spark.catalog.tableExists(f"{CATALOG}.{config.gold_schema}.dim_account")


def test_r6_existing_target_rows_are_kept_not_recreated(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    job.ensure_target(spark, config)
    spark.sql(
        f"INSERT INTO {config.target_fqn} VALUES (77, 7, 'Legacy', 5, DATE'2010-01-01', 'active')"
    )
    job.run(spark, config)
    rows = target_rows(spark, config)
    assert sorted(rows) == [1, 2, 3, 4, 77]
    assert rows[77]["account_type"] == "Legacy"


def test_r6_upsert_updates_changed_rows_and_inserts_new_keys(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    job.run(spark, config)
    changed = [(1, 101, "Savings", 2000, ts(2020, 1, 15), "inactive")] + SAMPLE_ROWS[1:] + [
        (5, 105, "Savings", 10, ts(2024, 5, 5), "active")
    ]
    write_bronze(spark, config, changed)
    job.run(spark, config)
    rows = target_rows(spark, config)
    assert sorted(rows) == [1, 2, 3, 4, 5]
    assert rows[1]["balance"] == Decimal("2000.0000") and rows[1]["status"] == "inactive"


def test_r6_insert_only_mode_matches_talend_insert_semantics(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    job.run(spark, config)
    write_bronze(
        spark,
        config,
        [(1, 101, "Savings", 2000, ts(2020, 1, 15), "inactive"), (5, 105, "Savings", 10, None, "active")],
    )
    insert_only = job.DimAccountConfig(
        catalog=config.catalog,
        bronze_schema=config.bronze_schema,
        gold_schema=config.gold_schema,
        load_mode="insert_only",
    )
    job.run(spark, insert_only)
    rows = target_rows(spark, config)
    assert sorted(rows) == [1, 2, 3, 4, 5]
    assert rows[1]["balance"] == Decimal("1500.0000") and rows[1]["status"] == "active"


def test_r6_rows_missing_from_source_are_not_deleted(spark, config):
    write_bronze(spark, config, SAMPLE_ROWS)
    job.run(spark, config)
    write_bronze(spark, config, SAMPLE_ROWS[:1])
    job.run(spark, config)
    assert sorted(target_rows(spark, config)) == [1, 2, 3, 4]


# Idempotency: re-running with unchanged bronze leaves gold byte-for-byte the same.
@pytest.mark.parametrize("load_mode", job.LOAD_MODES)
def test_rerun_is_idempotent(spark, config, load_mode):
    cfg = job.DimAccountConfig(
        catalog=config.catalog,
        bronze_schema=config.bronze_schema,
        gold_schema=config.gold_schema,
        load_mode=load_mode,
    )
    write_bronze(spark, cfg, SAMPLE_ROWS + [(1, 101, "Savings", 1500, ts(2020, 1, 15, 9, 30), "active")])
    job.run(spark, cfg)
    first = sorted(spark.table(cfg.target_fqn).collect())
    version_after_first = spark.sql(f"DESCRIBE HISTORY {cfg.target_fqn}").agg({"version": "max"}).first()[0]
    job.run(spark, cfg)
    job.run(spark, cfg)
    assert sorted(spark.table(cfg.target_fqn).collect()) == first
    assert len(first) == 4

    # Delta may skip the commit entirely for a no-op MERGE; any commit it does make must change nothing.
    reruns = spark.sql(f"DESCRIBE HISTORY {cfg.target_fqn}").where(f"version > {version_after_first}").collect()
    for commit in reruns:
        assert commit.operation == "MERGE"
        metrics = commit.operationMetrics
        for metric in ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted"):
            assert int(metrics.get(metric, "0")) == 0, (commit.version, metric, metrics)


# Job entry point and bundle resource.
def test_parse_args_maps_cli_to_config():
    cfg = job.parse_args(
        ["--catalog", "c", "--bronze-schema", "b", "--gold-schema", "g", "--load-mode", "insert_only"]
    )
    assert cfg == job.DimAccountConfig(catalog="c", bronze_schema="b", gold_schema="g", load_mode="insert_only")
    assert job.parse_args([]) == job.DimAccountConfig()


def test_invalid_load_mode_is_rejected():
    with pytest.raises(ValueError):
        job.DimAccountConfig(load_mode="truncate")


def test_bundle_resource_points_at_this_job():
    yaml = pytest.importorskip("yaml")
    resource_file = DATABRICKS_ROOT / "resources" / "load_dim_account.yml"
    spec = yaml.safe_load(resource_file.read_text())["resources"]["jobs"]["load_dim_account"]
    params = {p["name"]: p["default"] for p in spec["parameters"]}
    assert params["catalog"] == "${var.catalog}"
    assert params["bronze_schema"] == "bronze" and params["gold_schema"] == "gold"

    (task,) = spec["tasks"]
    python_file = (resource_file.parent / task["spark_python_task"]["python_file"]).resolve()
    assert python_file == Path(job.__file__).resolve()
    cli = task["spark_python_task"]["parameters"]
    assert cli == [
        "--catalog",
        "{{job.parameters.catalog}}",
        "--bronze-schema",
        "{{job.parameters.bronze_schema}}",
        "--gold-schema",
        "{{job.parameters.gold_schema}}",
        "--load-mode",
        "{{job.parameters.load_mode}}",
    ]
    resolved = [params.get(a[len("{{job.parameters.") : -2], a) if a.startswith("{{") else a for a in cli]
    parsed = job.parse_args([a.replace("${var.catalog}", "banking_dwh") for a in resolved])
    assert parsed == job.DimAccountConfig()
