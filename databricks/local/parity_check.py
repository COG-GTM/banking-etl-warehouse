"""Compare the Delta analytics against the original T-SQL stored procedures.

Run after run_local.py. Creates the DWH database in SQL Server from sql_scripts/, copies
the Delta dims/fact into it, then runs sp_DailyTransaction / sp_BalancePerCustomer and the
PySpark equivalents with the same parameters and diffs the results.

Needs ``sqlcmd`` reachable through SQLCMD (default: the ``mssql`` Docker container).
"""

import os
import re
import shlex
import subprocess
import sys
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
DATABRICKS_DIR = os.path.dirname(HERE)
REPO_DIR = os.path.dirname(DATABRICKS_DIR)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(DATABRICKS_DIR, "jobs"))
sys.path.insert(0, os.path.join(DATABRICKS_DIR, "analytics"))

from run_local import build_spark  # noqa: E402

from balance_per_customer import balance_per_customer  # noqa: E402
from common import qualified  # noqa: E402
from daily_transaction import daily_transaction  # noqa: E402

SQLCMD = os.environ.get("SQLCMD", "docker exec -i mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -C -b")
DAILY_CASES = [("2024-01-18", "2024-01-20"), ("2022-01-01", "2024-12-31"), ("2024-01-22", "2024-01-22")]
BALANCE_CASES = ["Shelly", "shelly", "a", "Bobi", "nobody"]


def sqlcmd(sql: str, db: str = "master", nocount: bool = True) -> str:
    cmd = shlex.split(SQLCMD) + [
        "-U",
        os.environ["SQLSERVER_USER"],
        "-P",
        os.environ["SQLSERVER_PASSWORD"],
        "-d",
        db,
        "-h",
        "-1",
        "-W",
        "-s",
        "|",
    ]
    out = subprocess.run(
        cmd, input=f"{'SET NOCOUNT ON;' if nocount else ''}\n{sql}\nGO\n", capture_output=True, text=True, check=True
    )
    return out.stdout


def run_script(path: str) -> None:
    with open(path) as fh:
        for batch in re.split(r"^\s*GO\s*$", fh.read(), flags=re.M | re.I):
            # CREATE PROCEDURE must start its batch; the original script prefixes it with PRINT.
            batch = re.sub(r"^\s*PRINT[^\n]*\n(?=\s*CREATE PROCEDURE)", "", batch, flags=re.M | re.I)
            if batch.strip():
                sqlcmd(batch, "master" if "CREATE DATABASE" in batch else "DWH", nocount=False)


def rows(text: str) -> list:
    return sorted(tuple(c.strip() for c in line.split("|")) for line in text.strip().splitlines() if line.strip())


def norm(value) -> str:
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    return str(value)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    sqlcmd(
        "IF DB_ID('DWH') IS NOT NULL BEGIN ALTER DATABASE DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE DWH; END"
    )
    run_script(os.path.join(REPO_DIR, "sql_scripts", "01_create_tables.sql"))
    run_script(os.path.join(REPO_DIR, "sql_scripts", "02_create_procedures.sql"))

    url = f"jdbc:sqlserver://{os.environ['JDBC_HOST']}:1433;databaseName=DWH;encrypt=true;trustServerCertificate=true"
    for delta_table, sql_table in [
        ("dim_branch", "DimBranch"),
        ("dim_account", "DimAccount"),
        ("dim_customer", "DimCustomer"),
        ("fact_transaction", "FactTransaction"),
    ]:
        (
            spark.table(qualified(delta_table, spark))
            .write.format("jdbc")
            .mode("append")
            .option("url", url)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("dbtable", f"dbo.{sql_table}")
            .option("user", os.environ["SQLSERVER_USER"])
            .option("password", os.environ["SQLSERVER_PASSWORD"])
            .save()
        )

    failures = 0
    for start, end in DAILY_CASES:
        expected = rows(sqlcmd(f"EXEC sp_DailyTransaction @start_date='{start}', @end_date='{end}'", "DWH"))
        actual = sorted(tuple(norm(v) for v in r) for r in daily_transaction(start, end, spark).collect())
        ok = expected == actual
        failures += not ok
        print(f"sp_DailyTransaction {start}..{end}: {len(actual)} rows {'MATCH' if ok else 'DIFF'}")
        if not ok:
            print("  expected", expected, "\n  actual  ", actual)
    for name in BALANCE_CASES:
        expected = rows(sqlcmd(f"EXEC sp_BalancePerCustomer @customer_name='{name}'", "DWH"))
        actual = sorted(tuple(norm(v) for v in r) for r in balance_per_customer(name, spark).collect())
        ok = expected == actual
        failures += not ok
        print(f"sp_BalancePerCustomer '{name}': {len(actual)} rows {'MATCH' if ok else 'DIFF'}")
        if not ok:
            print("  expected", expected, "\n  actual  ", actual)
    print("PARITY OK" if failures == 0 else f"PARITY FAILED ({failures} cases)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
