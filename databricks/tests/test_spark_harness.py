import pytest

pytestmark = pytest.mark.spark


def test_delta_round_trip_in_medallion_schema(spark, medallion_schemas):
    cfg = medallion_schemas
    target = cfg.table("gold", "dim_branch")
    rows = [(1, "KC Jakarta", "Jakarta"), (2, "KC Bandung", "Bandung")]
    spark.createDataFrame(rows, "branch_id INT, branch_name STRING, branch_location STRING") \
        .write.format("delta").mode("overwrite").saveAsTable(target)

    assert spark.table(target).count() == 2
    detail = spark.sql(f"DESCRIBE DETAIL {target}").collect()[0]
    assert detail["format"] == "delta"

    spark.sql(f"UPDATE {target} SET branch_location = 'Bogor' WHERE branch_id = 2")
    assert spark.table(target).where("branch_id = 2").first()["branch_location"] == "Bogor"
    assert spark.sql(f"DESCRIBE HISTORY {target}").count() == 2


def test_get_spark_reuses_active_session(spark):
    from banking_etl.spark import get_spark

    assert get_spark() is spark
