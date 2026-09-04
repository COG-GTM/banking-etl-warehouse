from databricks.jobs.dimensions.load_dimensions import (
    GOLD_TABLES,
    Config,
    parse_args,
    run,
)
from databricks.jobs.dimensions.transforms import (
    DIM_ACCOUNT_SCHEMA,
    DIM_BRANCH_SCHEMA,
    DIM_CUSTOMER_SCHEMA,
)


def test_parse_args_local_parquet():
    cfg = parse_args(["--format", "parquet", "--path", "/tmp/x", "--gold-schema", "g"])
    assert cfg.format == "parquet"
    assert cfg.path == "/tmp/x"
    assert cfg.gold_schema == "g"
    assert cfg.bronze_schema == "bronze"


def test_run_parquet_end_to_end(spark, tmp_path):
    base = str(tmp_path)
    spark.createDataFrame(
        [(1, "hq", "jakarta")], "branch_id int, branch_name string, branch_location string"
    ).write.parquet(f"{base}/bronze/branch")
    spark.createDataFrame(
        [(1, 1, "Savings", 100, "2021-01-01", "active")],
        "account_id int, customer_id int, account_type string, balance int, "
        "date_opened string, status string",
    ).write.parquet(f"{base}/bronze/account")
    spark.createDataFrame(
        [(1, "ann", "addr", 1, "20", "f", "a@b.c")],
        "customer_id int, customer_name string, address string, city_id int, "
        "age string, gender string, email string",
    ).write.parquet(f"{base}/bronze/customer")
    spark.createDataFrame(
        [(1, "jakarta", 1)], "city_id int, city_name string, state_id int"
    ).write.parquet(f"{base}/bronze/city")
    spark.createDataFrame(
        [(1, "dki")], "state_id int, state_name string"
    ).write.parquet(f"{base}/bronze/state")

    cfg = Config(format="parquet", path=base)
    run(spark, cfg)
    # run twice to prove overwrite is idempotent
    run(spark, cfg)

    expected = {
        "DimBranch": DIM_BRANCH_SCHEMA,
        "DimAccount": DIM_ACCOUNT_SCHEMA,
        "DimCustomer": DIM_CUSTOMER_SCHEMA,
    }
    for t in GOLD_TABLES:
        df = spark.read.parquet(f"{base}/gold/{t}")
        assert df.count() == 1
        assert [f.name for f in df.schema.fields] == [f.name for f in expected[t].fields]
        assert [f.dataType for f in df.schema.fields] == [
            f.dataType for f in expected[t].fields
        ]
    cust = spark.read.parquet(f"{base}/gold/DimCustomer").collect()[0]
    assert cust.CustomerName == "ANN"
    assert cust.StateName == "dki"
