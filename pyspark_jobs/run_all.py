"""Runs the four ETL jobs in the README order: DimBranch -> DimAccount -> DimCustomer -> FactTransaction."""

from __future__ import annotations

import argparse
import logging
import time

if __package__ in (None, ""):  # allow `spark-submit pyspark_jobs/run_all.py` without --py-files
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark_jobs import load_dim_account, load_dim_branch, load_dim_customer, load_fact_transaction
from pyspark_jobs.common.config import EtlConfig
from pyspark_jobs.common.io import execute_jdbc
from pyspark_jobs.common.jobs import configure_logging
from pyspark_jobs.common.spark_session import get_spark

log = logging.getLogger("run_all")

JOBS = [
    ("Load_DimBranch", load_dim_branch),
    ("Load_DimAccount", load_dim_account),
    ("Load_DimCustomer", load_dim_customer),
    ("Load_FactTransaction", load_fact_transaction),
]


def full_refresh_reset(spark, cfg: EtlConfig) -> None:
    """Empties the DWH in reverse FK order so every table can be reloaded from scratch."""
    db = cfg.dwh_db
    execute_jdbc(
        spark,
        db,
        [
            f"TRUNCATE TABLE {db.qualified('FactTransaction')}",
            f"DELETE FROM {db.qualified('DimCustomer')}",
            f"DELETE FROM {db.qualified('DimAccount')}",
            f"DELETE FROM {db.qualified('DimBranch')}",
        ],
    )


def run_all(spark, cfg: EtlConfig, full_refresh: bool = False) -> dict[str, int]:
    if full_refresh:
        log.info("Full refresh: emptying FactTransaction and dimensions")
        full_refresh_reset(spark, cfg)
    results = {}
    for name, job in JOBS:
        started = time.monotonic()
        log.info("Starting %s", name)
        results[name] = job.run(spark, cfg)
        log.info("Finished %s: %d rows in %.1fs", name, results[name], time.monotonic() - started)
    return results


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Empty FactTransaction and all dimensions before loading (dimension rows are replaced, not just added)",
    )
    args = parser.parse_args(argv)
    configure_logging()
    cfg = EtlConfig.from_env()
    results = run_all(get_spark("run_all", cfg), cfg, args.full_refresh)
    log.info("ETL complete: %s", results)


if __name__ == "__main__":
    main()
