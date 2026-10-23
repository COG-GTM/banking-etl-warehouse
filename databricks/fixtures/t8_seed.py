"""Seed a Ticket 8 scratch namespace with real-source Bronze rows and the dims needed for RI.

Writes, from the ``fixtures/t8_*.csv`` files (exports of ``data_sources/``, see ``t8_build_fixtures.py``):

* ``bronze.sqlserver_transaction_db`` (10 rows from ``sample.bak``),
  ``bronze.file_transaction_excel`` (7), ``bronze.file_transaction_csv`` (12);
* ``silver.dim_account`` (21) and ``silver.dim_branch`` (5) from ``sample.bak`` (stand-ins for Ticket 6).

Safety: only schema prefixes ending in ``t8_`` (e.g. ``bdwh_t8_``) are accepted, so the shared dev
namespace (``bdwh_``) and prod (no prefix) can never be seeded; nothing is written to ``gold`` and the
fact table itself is left to the load under test.
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from banking_dwh.common.config import EnvConfig
from banking_dwh.contracts.ddl import create_table_sql
from banking_dwh.contracts.tables import (
    BRONZE_FILE_TRANSACTION_CSV,
    BRONZE_FILE_TRANSACTION_EXCEL,
    BRONZE_SQLSERVER_TRANSACTION,
    DIM_ACCOUNT,
    DIM_BRANCH,
    TableContract,
)

FIXTURES_DIR = Path(__file__).resolve().parent
SEED_BATCH_ID = "t8-seed-sample-bak"
SEED_INGESTED_AT = datetime(2026, 10, 23, 0, 0, 0)
SEEDED_LAYERS = ("bronze", "silver")


def check_seed_target(cfg: EnvConfig) -> None:
    if not cfg.schema_prefix.endswith("t8_"):
        raise ValueError(
            f"Refusing to seed schema_prefix {cfg.schema_prefix!r}: Ticket 8 seeds only scratch "
            "namespaces whose prefix ends in 't8_' (e.g. bdwh_t8_)."
        )


def _parse(value: str, type_: str):
    if value == "" or value is None:
        return None
    t = type_.upper()
    if t == "INT":
        return int(value)
    if t.startswith("DECIMAL"):
        return Decimal(value)
    if t == "TIMESTAMP":
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    if t == "DATE":
        return date.fromisoformat(value)
    return value


def read_fixture(name: str, contract: TableContract, fixtures_dir: Path | str = FIXTURES_DIR) -> list[dict]:
    types = {c.name: c.type for c in contract.columns}
    with open(Path(fixtures_dir) / name, encoding="utf-8", newline="") as fh:
        return [{k: _parse(v, types[k]) for k, v in row.items()} for row in csv.DictReader(fh)]


def write_rows(spark, cfg: EnvConfig, contract: TableContract, rows: list[dict], *, unity_catalog: bool = True) -> int:
    """Create the contract table if needed and replace its contents with ``rows``."""
    if contract.layer not in SEEDED_LAYERS:
        raise ValueError(f"Refusing to seed layer {contract.layer!r}")
    spark.sql(create_table_sql(cfg, contract, unity_catalog=unity_catalog))
    names = contract.column_names
    df = spark.createDataFrame([tuple(r.get(c) for c in names) for r in rows], contract.ddl_schema())
    target = cfg.table(contract.layer, contract.name)
    df.write.insertInto(target, overwrite=True)
    return spark.table(target).count()


def seed_scratch(spark, cfg: EnvConfig, *, fixtures_dir: Path | str = FIXTURES_DIR,
                 unity_catalog: bool = True) -> dict[str, int]:
    check_seed_target(cfg)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    counts = {}

    sql_rows = read_fixture("t8_bronze_sqlserver_transaction_db.csv", BRONZE_SQLSERVER_TRANSACTION, fixtures_dir)
    for r in sql_rows:
        r.update(_ingested_at=SEED_INGESTED_AT, _batch_id=SEED_BATCH_ID)
    counts["bronze.sqlserver_transaction_db"] = write_rows(
        spark, cfg, BRONZE_SQLSERVER_TRANSACTION, sql_rows, unity_catalog=unity_catalog)

    for contract, fixture, landing, file_name in (
        (BRONZE_FILE_TRANSACTION_EXCEL, "t8_bronze_file_transaction_excel.csv", "excel_transaction", "transaction_excel.xlsx"),
        (BRONZE_FILE_TRANSACTION_CSV, "t8_bronze_file_transaction_csv.csv", "csv_transaction", "transaction_csv.csv"),
    ):
        rows = read_fixture(fixture, contract, fixtures_dir)
        source_file = cfg.landing_path(landing, file_name)
        for r in rows:
            r.update(_rescued_data=None, _source_file=source_file, _ingested_at=SEED_INGESTED_AT)
        counts[f"bronze.{contract.name}"] = write_rows(spark, cfg, contract, rows, unity_catalog=unity_catalog)

    for contract, fixture in ((DIM_BRANCH, "t8_silver_dim_branch.csv"), (DIM_ACCOUNT, "t8_silver_dim_account.csv")):
        rows = read_fixture(fixture, contract, fixtures_dir)
        for r in rows:
            r["_loaded_at"] = now
        counts[f"silver.{contract.name}"] = write_rows(spark, cfg, contract, rows, unity_catalog=unity_catalog)
    return counts
