"""Run the four ETL jobs in the README order:

    Load_DimBranch -> Load_DimAccount -> Load_DimCustomer -> Load_FactTransaction

Usage (from the repository root):

    spark-submit --packages com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 \\
        --py-files pyspark_jobs.zip pyspark_jobs/run_all.py [--jobs dim_branch,fact_transaction]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark_jobs import (
    load_dim_account,
    load_dim_branch,
    load_dim_customer,
    load_fact_transaction,
)
from pyspark_jobs.common.config import load_config
from pyspark_jobs.common.io import execute_jdbc_statement
from pyspark_jobs.common.spark_session import get_spark

log = logging.getLogger("pyspark_jobs.run_all")

JOBS = {
    "dim_branch": ("Load_DimBranch", load_dim_branch.run),
    "dim_account": ("Load_DimAccount", load_dim_account.run),
    "dim_customer": ("Load_DimCustomer", load_dim_customer.run),
    "fact_transaction": ("Load_FactTransaction", load_fact_transaction.run),
}
FK_DIMENSIONS = {"dim_branch", "dim_account"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--jobs",
        default=",".join(JOBS),
        help=f"Comma-separated subset of jobs to run, always executed in dependency order. Default: all ({', '.join(JOBS)})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    selected = [j.strip() for j in args.jobs.split(",") if j.strip()]
    unknown = sorted(set(selected) - set(JOBS))
    if unknown:
        log.error("Unknown job(s): %s. Valid: %s", unknown, list(JOBS))
        return 2

    config = load_config()
    spark = get_spark("banking-etl-warehouse-run-all")

    # DimAccount/DimBranch are referenced by FactTransaction's FKs, so a full
    # dimension reload has to clear the fact rows first. The fact load
    # replaces them at the end of the run.
    if config.dim_jdbc_mode == "delete_insert" and FK_DIMENSIONS & set(selected):
        fact = config.target_table(load_fact_transaction.TARGET_TABLE)
        deleted = execute_jdbc_statement(spark, config.target, f"DELETE FROM {fact}")
        log.info("Cleared %s rows from %s before reloading FK dimensions", deleted, fact)

    for key, (name, run) in JOBS.items():
        if key not in selected:
            continue
        started = time.monotonic()
        log.info("=== %s: start", name)
        rows = run(spark, config)
        log.info("=== %s: done, %s rows in %.1fs", name, rows, time.monotonic() - started)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
