"""Run the full DWH load: DimBranch -> DimAccount -> DimCustomer -> FactTransaction.

python -m etl.main
spark-submit --packages <see etl/requirements.txt> etl/main.py
"""

import logging
import sys
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import SparkSession

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.config import EtlConfig, load_config  # noqa: E402
from etl.jdbc import clear_table  # noqa: E402
from etl.jobs import (  # noqa: E402
    load_dim_account,
    load_dim_branch,
    load_dim_customer,
    load_fact_transaction,
)
from etl.spark_session import build_spark_session  # noqa: E402

log = logging.getLogger("etl")

Job = Callable[[SparkSession, EtlConfig, str | None], int]

JOBS: list[tuple[str, Job]] = [
    ("Load_DimBranch", load_dim_branch.run),
    ("Load_DimAccount", load_dim_account.run),
    ("Load_DimCustomer", load_dim_customer.run),
    ("Load_FactTransaction", load_fact_transaction.run),
]


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def run_all(spark: SparkSession, cfg: EtlConfig) -> dict[str, int]:
    if cfg.write_mode == "overwrite":
        # Clear in reverse dependency order so FactTransaction FKs never block the dim reloads.
        t = cfg.target_tables
        for table in (t.fact_transaction, t.dim_customer, t.dim_account, t.dim_branch):
            clear_table(spark, cfg.target, table)

    results: dict[str, int] = {}
    for name, job in JOBS:
        log.info("Running %s", name)
        results[name] = job(spark, cfg, "append")
        log.info("%s finished: %d rows", name, results[name])
    return results


def run_single(job: Job, cfg: EtlConfig) -> None:
    configure_logging()
    spark = build_spark_session(cfg.spark)
    try:
        job(spark, cfg, None)
    finally:
        spark.stop()


def main() -> int:
    configure_logging()
    cfg = load_config()
    spark = build_spark_session(cfg.spark)
    try:
        results = run_all(spark, cfg)
    finally:
        spark.stop()
    for name, rows in results.items():
        log.info("%-22s %d rows", name, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
