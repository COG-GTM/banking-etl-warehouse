#!/usr/bin/env python3
"""Upload fixtures/sample_db/*.csv to the landing volume with the databricks CLI (dev fixture mode).

  python scripts/bronze/upload_fixtures.py --catalog migration_demo --schema-prefix banking_etl_ [--tables customer,city] [--dry-run]
"""
import argparse

import _bootstrap  # noqa: F401

from banking_etl.bronze.fixtures import fixture_files, fixture_targets, landing_root
from banking_etl.config import Settings
from banking_etl.setup.cli import run_cli


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema-prefix", default="")
    ap.add_argument("--tables", default=None, help="comma-separated (default: all six)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    root = landing_root(Settings(catalog=a.catalog, schema_prefix=a.schema_prefix))
    files = fixture_files(a.tables)
    for table, dest in fixture_targets(root, a.tables).items():
        mkdirs = ["fs", "mkdirs", f"dbfs:{dest.rsplit('/', 1)[0]}"]
        cp = ["fs", "cp", str(files[table]), f"dbfs:{dest}", "--overwrite"]
        for args in (mkdirs, cp):
            print("databricks", " ".join(args))
            if not a.dry_run:
                run_cli(args, parse_json=False)


if __name__ == "__main__":
    main()
