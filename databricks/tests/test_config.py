import pytest

from banking_etl.config import Settings


def test_three_part_names_on_databricks():
    s = Settings(catalog="banking_etl_dev")
    assert s.table("gold", "dim_branch") == "banking_etl_dev.gold.dim_branch"
    assert s.landing_path("transactions", "csv") == "/Volumes/banking_etl_dev/bronze/landing/transactions/csv"


def test_schema_prefix_for_shared_catalog():
    s = Settings(catalog="migration_demo", schema_prefix="banking_etl_")
    assert s.schema("silver") == "migration_demo.banking_etl_silver"
    assert s.checkpoint_path("x") == "/Volumes/migration_demo/banking_etl_ops/checkpoints/x"


def test_local_two_part_names():
    assert Settings().table("bronze", "sqlserver_branch") == "bronze.sqlserver_branch"
    with pytest.raises(ValueError):
        Settings().landing_path()


def test_local_spark_delta_roundtrip(spark, settings):
    name = settings.table("ops", "smoke")
    spark.range(3).write.format("delta").mode("overwrite").saveAsTable(name)
    assert spark.table(name).count() == 3
