"""Tests for banking_etl.load_dim_branch, one group per Talend Load_DimBranch rule (R1-R5)."""

import os
import sys
from pathlib import Path

import pytest
import yaml

DATABRICKS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DATABRICKS_ROOT / "src"))

from banking_etl import load_dim_branch as job  # noqa: E402

CATALOG = "spark_catalog"
BRONZE = "dimbranch_bronze"
GOLD = "dimbranch_gold"
SOURCE = f"{CATALOG}.{BRONZE}.branch"
TARGET = f"{CATALOG}.{GOLD}.dim_branch"
SOURCE_SCHEMA = "branch_id INT, branch_name STRING, branch_location STRING"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test_load_dim_branch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.repositories",
            "https://maven-central.storage-download.googleapis.com/maven2/",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def clean_schemas(spark):
    for schema in (BRONZE, GOLD, "alt_bronze", "alt_gold"):
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
        spark.sql(f"CREATE SCHEMA {CATALOG}.{schema}")
    yield


def seed_bronze(spark, rows, schema=SOURCE_SCHEMA, table=SOURCE):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(table)


def target_rows(spark, table=TARGET):
    return sorted(tuple(r) for r in spark.table(table).collect())


def run_job(spark, **kwargs):
    return job.run(spark, catalog=CATALOG, bronze_schema=BRONZE, gold_schema=GOLD, **kwargs)


BRANCHES = [
    (1, "Cabang Jakarta", "Jakarta"),
    (2, "Cabang Bandung", "Bandung"),
    (3, "Cabang Surabaya", "Surabaya"),
]


# R1: source query = SELECT branch_id, branch_name, branch_location FROM dbo.branch


def test_r1_reads_every_row_of_bronze_branch_without_filter(spark):
    seed_bronze(spark, BRANCHES)
    stats = run_job(spark)
    assert target_rows(spark) == sorted(BRANCHES)
    assert stats.source_rows == 3 and stats.inserted == 3


def test_r1_projects_only_the_three_query_columns(spark):
    seed_bronze(
        spark,
        [(1, "Cabang Jakarta", "Jakarta", "extra", "2026-01-01")],
        schema=SOURCE_SCHEMA + ", branch_manager STRING, _ingested_at STRING",
    )
    run_job(spark)
    assert spark.table(TARGET).columns == ["branch_id", "branch_name", "branch_location"]
    assert target_rows(spark) == [(1, "Cabang Jakarta", "Jakarta")]


# R2: branch_id is nullable="false" -> Talend aborts with "Null value in non-Nullable column"


def test_r2_null_branch_id_aborts_and_writes_nothing(spark):
    seed_bronze(spark, [(1, "Cabang Jakarta", "Jakarta"), (None, "Orphan", "Nowhere")])
    with pytest.raises(ValueError, match="Null value in non-Nullable column"):
        run_job(spark)
    assert not spark.catalog.tableExists(TARGET)


# R3: TRIM_ALL_COLUMN=false and all TRIM_COLUMN=false -> values pass through untouched


def test_r3_whitespace_is_not_trimmed(spark):
    seed_bronze(spark, [(1, "  Cabang Jakarta ", " Jakarta  "), (2, "", "\tBandung")])
    run_job(spark)
    assert target_rows(spark) == [(1, "  Cabang Jakarta ", " Jakarta  "), (2, "", "\tBandung")]


def test_r3_nullable_columns_keep_nulls(spark):
    seed_bronze(spark, [(1, None, "Jakarta"), (2, "Cabang Bandung", None), (3, None, None)])
    run_job(spark)
    assert target_rows(spark) == [(1, None, "Jakarta"), (2, "Cabang Bandung", None), (3, None, None)]


# R4: tMap is a 1:1 mapping (BranchID/BranchName/BranchLocation), INT key, string attributes


def test_r4_mapping_names_and_types(spark):
    seed_bronze(spark, BRANCHES)
    run_job(spark)
    fields = {f.name: f.dataType.simpleString() for f in spark.table(TARGET).schema.fields}
    assert fields == {"branch_id": "int", "branch_name": "string", "branch_location": "string"}
    assert [m[0] for m in job.COLUMN_MAPPING] == ["BranchID", "BranchName", "BranchLocation"]


def test_r4_casts_wider_numeric_key_to_int(spark):
    seed_bronze(
        spark,
        [(10, "Cabang Medan", "Medan")],
        schema="branch_id BIGINT, branch_name STRING, branch_location STRING",
    )
    run_job(spark)
    assert spark.table(TARGET).schema["branch_id"].dataType.simpleString() == "int"
    assert target_rows(spark) == [(10, "Cabang Medan", "Medan")]


def test_r4_transform_has_no_filter_or_expression(spark):
    source = spark.createDataFrame(BRANCHES, SOURCE_SCHEMA)
    assert sorted(tuple(r) for r in job.transform(source).collect()) == sorted(BRANCHES)


# R5: CREATE_IF_NOT_EXISTS + INSERT into DimBranch (PK, VARCHAR(100)/VARCHAR(255)), DIE_ON_ERROR=false


def test_r5_creates_target_when_missing(spark):
    seed_bronze(spark, BRANCHES)
    assert not spark.catalog.tableExists(TARGET)
    run_job(spark)
    assert spark.catalog.tableExists(TARGET)
    assert spark.table(TARGET).schema["branch_id"].nullable is False


def test_r5_keeps_existing_target_rows(spark):
    spark.sql(f"CREATE TABLE {TARGET} ({job.TARGET_DDL_COLUMNS}) USING DELTA")
    spark.sql(f"INSERT INTO {TARGET} VALUES (99, 'Cabang Lama', 'Bogor')")
    seed_bronze(spark, BRANCHES)
    run_job(spark)
    assert target_rows(spark) == sorted(BRANCHES + [(99, "Cabang Lama", "Bogor")])


def test_r5_existing_key_is_not_updated_insert_mode(spark):
    seed_bronze(spark, BRANCHES)
    run_job(spark)
    seed_bronze(spark, [(1, "Cabang Jakarta Pusat", "Jakarta Pusat"), (4, "Cabang Medan", "Medan")])
    stats = run_job(spark)
    assert stats.inserted == 1 and stats.updated == 0
    assert target_rows(spark) == sorted(BRANCHES + [(4, "Cabang Medan", "Medan")])


def test_r5_duplicate_key_in_batch_loads_one_row(spark):
    seed_bronze(spark, [(1, "Cabang B", "Jakarta"), (1, "Cabang A", "Jakarta"), (2, "Cabang Bandung", "Bandung")])
    stats = run_job(spark)
    assert stats.rejected_duplicate_key == 1
    assert target_rows(spark) == [(1, "Cabang A", "Jakarta"), (2, "Cabang Bandung", "Bandung")]


def test_r5_rows_over_dimbranch_widths_are_rejected(spark):
    seed_bronze(
        spark,
        [
            (1, "N" * 100, "L" * 255),
            (2, "N" * 101, "Bandung"),
            (3, "Cabang Surabaya", "L" * 256),
        ],
    )
    stats = run_job(spark)
    assert stats.rejected_width == 2 and stats.inserted == 1
    assert target_rows(spark) == [(1, "N" * 100, "L" * 255)]


# Idempotency: MERGE on branch_id makes re-runs no-ops


def test_rerun_is_idempotent(spark):
    seed_bronze(spark, BRANCHES)
    first = run_job(spark)
    after_first = target_rows(spark)
    second = run_job(spark)
    assert target_rows(spark) == after_first
    assert spark.table(TARGET).count() == 3
    assert (first.inserted, second.inserted, second.updated) == (3, 0, 0)


def test_update_existing_opt_in_refreshes_changed_rows_and_is_idempotent(spark):
    seed_bronze(spark, BRANCHES)
    run_job(spark)
    seed_bronze(spark, [(1, "Cabang Jakarta Pusat", "Jakarta Pusat"), (2, "Cabang Bandung", "Bandung")])
    first = run_job(spark, update_existing=True)
    second = run_job(spark, update_existing=True)
    assert (first.updated, first.inserted) == (1, 0)
    assert (second.updated, second.inserted) == (0, 0)
    assert target_rows(spark) == [
        (1, "Cabang Jakarta Pusat", "Jakarta Pusat"),
        (2, "Cabang Bandung", "Bandung"),
        (3, "Cabang Surabaya", "Surabaya"),
    ]


# Configuration: catalog/schema names are parameters, never hard-coded


def test_schema_names_are_configurable(spark):
    seed_bronze(spark, BRANCHES, table=f"{CATALOG}.alt_bronze.branch")
    job.run(spark, catalog=CATALOG, bronze_schema="alt_bronze", gold_schema="alt_gold")
    assert target_rows(spark, f"{CATALOG}.alt_gold.dim_branch") == sorted(BRANCHES)


def test_table_name_rejects_unsafe_identifiers():
    assert job.table_name("banking_dwh", "gold", "dim_branch") == "`banking_dwh`.`gold`.`dim_branch`"
    assert job.table_name(None, "gold", "dim_branch") == "`gold`.`dim_branch`"
    with pytest.raises(ValueError):
        job.table_name("banking_dwh; DROP TABLE x", "gold", "dim_branch")


def test_main_parses_job_parameters(spark):
    seed_bronze(spark, BRANCHES)
    stats = job.main(
        ["--catalog", CATALOG, "--bronze-schema", BRONZE, "--gold-schema", GOLD, "--update-existing", "false"]
    )
    assert stats.inserted == 3
    assert target_rows(spark) == sorted(BRANCHES)


# Bundle resource


def test_bundle_resource_definition():
    resource_file = DATABRICKS_ROOT / "resources" / "load_dim_branch.yml"
    jobs = yaml.safe_load(resource_file.read_text())["resources"]["jobs"]
    task = jobs["load_dim_branch"]["tasks"][0]
    python_file = (resource_file.parent / task["spark_python_task"]["python_file"]).resolve()
    assert python_file == (DATABRICKS_ROOT / "src" / "banking_etl" / "load_dim_branch.py").resolve()
    params = task["spark_python_task"]["parameters"]
    assert params[params.index("--catalog") + 1] == "${var.catalog}"
    assert params[params.index("--bronze-schema") + 1] == "bronze"
    assert params[params.index("--gold-schema") + 1] == "gold"
    assert task["environment_key"] in {e["environment_key"] for e in jobs["load_dim_branch"]["environments"]}
