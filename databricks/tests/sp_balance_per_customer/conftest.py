import json
import os
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
SRC = HERE.parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

FIXTURE = json.loads((HERE / "fixtures" / "fixture.json").read_text(encoding="utf-8"))
EXPECTED = json.loads((HERE / "fixtures" / "expected.json").read_text(encoding="utf-8"))

GOLD_SCHEMAS = {
    "dim_customer": "customer_id INT, customer_name STRING, address STRING, city_name STRING, "
                    "state_name STRING, age INT, gender STRING, email STRING",
    "dim_account": "account_id INT, customer_id INT, account_type STRING, balance DECIMAL(19,4), "
                   "date_opened DATE, status STRING",
    "dim_branch": "branch_id INT, branch_name STRING, branch_location STRING",
    "fact_transaction": "transaction_id INT, account_id INT, transaction_date TIMESTAMP, "
                        "amount DECIMAL(19,4), transaction_type STRING, branch_id INT",
}
_MONEY = {"balance", "amount"}


def _convert(column, value):
    if value is None:
        return None
    if column in _MONEY:
        return Decimal(value)
    if column == "date_opened":
        return date.fromisoformat(value)
    if column == "transaction_date":
        return datetime.fromisoformat(value)
    return value


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("sp_balance_per_customer-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    yield session


@pytest.fixture(scope="session")
def gold_schema(spark):
    """A throw-away schema in the session catalog holding the fixture's gold tables."""
    schema = f"bpc_gold_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE SCHEMA `{schema}`")
    for table, ddl in GOLD_SCHEMAS.items():
        columns = [c.split()[0] for c in ddl.split(", ")]
        rows = [tuple(_convert(c, r[c]) for c in columns) for r in FIXTURE[table]]
        spark.createDataFrame(rows, ddl).write.saveAsTable(f"`{schema}`.`{table}`")
    yield schema
    spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")


def normalize(df):
    """Rows as sorted list of dicts with MONEY rendered like the SQL Server oracle."""
    out = []
    for row in df.collect():
        d = row.asDict()
        for key in ("initial_balance", "current_balance"):
            if d[key] is not None:
                d[key] = str(Decimal(d[key]).quantize(Decimal("0.0001")))
        out.append(d)
    return sorted(out, key=lambda r: json.dumps(r, sort_keys=True))
