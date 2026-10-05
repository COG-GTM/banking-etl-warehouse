#!/usr/bin/env python3
"""Provision UC schemas/volumes/grants from a laptop or CI through a SQL warehouse.

  python scripts/setup/provision_uc.py --catalog migration_demo --schema-prefix banking_etl_ \
      --warehouse-id 565cd2fd713738c4 [--create-catalog] [--data-engineers GROUP] [--jobs-principal APP_ID] [--dry-run]
"""
import argparse
import logging

import _bootstrap  # noqa: F401

from banking_etl.config import Settings
from banking_etl.setup.cli import warehouse_executor
from banking_etl.setup.provision import ProvisionOptions, plan, provision, summarize


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema-prefix", default="")
    ap.add_argument("--warehouse-id")
    ap.add_argument("--create-catalog", action="store_true")
    ap.add_argument("--data-engineers", default="")
    ap.add_argument("--jobs-principal", default="")
    ap.add_argument("--dry-run", action="store_true", help="print the SQL instead of running it")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    settings = Settings(catalog=a.catalog, schema_prefix=a.schema_prefix)
    options = ProvisionOptions(a.create_catalog, a.data_engineers, a.jobs_principal)
    if a.dry_run:
        for step in plan(settings, options):
            print(f"-- {step.name}{' (optional)' if step.optional else ''}")
            print(";\n".join(step.statements) + ";\n")
        return
    if not a.warehouse_id:
        ap.error("--warehouse-id is required unless --dry-run")
    print(summarize(provision(warehouse_executor(a.warehouse_id), settings, options)))


if __name__ == "__main__":
    main()
