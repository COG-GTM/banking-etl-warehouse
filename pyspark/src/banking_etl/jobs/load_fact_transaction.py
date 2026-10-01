"""Load_FactTransaction: dbo.transaction_db (SQL Server) + transaction_excel.xlsx +
transaction_csv.csv -> dwh.fact_transaction (Delta).

Must run after the dimension loads: rows referencing unknown accounts/branches are
handled according to ``orphan_policy`` (the legacy foreign keys rejected them).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import SparkSession

from banking_etl import schemas
from banking_etl.config import EtlConfig
from banking_etl.readers import read_sqlserver_table, read_transaction_csv, read_transaction_excel
from banking_etl.runtime import base_parser, bootstrap, parse
from banking_etl.transforms import (
    deduplicate_transactions,
    split_orphan_facts,
    transform_fact_transaction,
    unite_transactions,
)
from banking_etl.writers import ensure_table, read_table, write_table

log = logging.getLogger(__name__)


class OrphanFactError(RuntimeError):
    pass


def run(spark: SparkSession, config: EtlConfig) -> int:
    sql_rows = read_sqlserver_table(spark, config.sqlserver, "transaction_db", schemas.SRC_TRANSACTION)
    excel_rows = read_transaction_excel(spark, config.transaction_excel, schemas.SRC_TRANSACTION)
    csv_rows = read_transaction_csv(spark, config.transaction_csv, schemas.SRC_TRANSACTION)

    fact = transform_fact_transaction(
        deduplicate_transactions(unite_transactions(sql_rows, excel_rows, csv_rows))
    )

    if config.orphan_policy != "keep":
        for spec in (schemas.DIM_ACCOUNT, schemas.DIM_BRANCH):
            ensure_table(spark, config.warehouse, spec)
        valid, rejected = split_orphan_facts(
            fact,
            read_table(spark, config.warehouse, schemas.DIM_ACCOUNT),
            read_table(spark, config.warehouse, schemas.DIM_BRANCH),
        )
        rejected_ids = sorted(r.TransactionID for r in rejected.select("TransactionID").collect())
        if rejected_ids:
            message = (
                f"{len(rejected_ids)} transaction(s) violate the fact_transaction keys "
                f"(unknown AccountID/BranchID or NULL TransactionID): {rejected_ids[:50]}"
            )
            if config.orphan_policy == "fail":
                raise OrphanFactError(message)
            log.warning("Rejected %s", message)
        fact = valid

    return write_table(
        spark,
        fact,
        config.warehouse,
        schemas.FACT_TRANSACTION,
        config.table_settings(schemas.FACT_TRANSACTION.name).write_mode,
    )


def main(argv: Sequence[str] | None = None) -> None:
    spark, config = bootstrap("load_fact_transaction", parse(base_parser(__doc__), argv))
    log.info("load_fact_transaction finished: %d rows", run(spark, config))


if __name__ == "__main__":
    main()
