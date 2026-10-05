#!/usr/bin/env python3
"""Copy data_sources/transaction_csv.csv and transaction_excel.xlsx into the landing volume (laptop / CI).

  python scripts/bronze/land_transaction_files.py --catalog migration_demo --schema-prefix banking_etl_ \
      [--source-dir ../data_sources] [--sources csv,excel] [--overwrite] [--dry-run]

Uses ``databricks fs cp`` (CLI auth: DATABRICKS_HOST + OAuth M2M/PAT/profile). Existing files are skipped
unless --overwrite; Auto Loader never re-ingests a file it has already processed.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from banking_etl.bronze.files import land_transaction_files, landing_targets  # noqa: E402
from banking_etl.config import Settings  # noqa: E402
from banking_etl.setup.cli import run_cli  # noqa: E402

DEFAULT_SOURCE_DIR = Path(__file__).resolve().parents[3] / "data_sources"


def _dbfs(path: str) -> str:
    return f"dbfs:{path}"


def cli_exists(target: str) -> bool:
    try:
        run_cli(["fs", "ls", _dbfs(target)], parse_json=False)
        return True
    except RuntimeError:
        return False


def cli_copy(local: str, target: str) -> None:
    run_cli(["fs", "mkdirs", _dbfs(str(Path(target).parent))], parse_json=False)
    run_cli(["fs", "cp", local, _dbfs(target), "--overwrite"], parse_json=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema-prefix", default="")
    ap.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR))
    ap.add_argument("--sources", default="csv,excel")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    settings = Settings(catalog=a.catalog, schema_prefix=a.schema_prefix)
    if a.dry_run:
        for local, target in landing_targets(settings, a.source_dir, a.sources):
            print(f"{local} -> {target}")
        return
    for target, status in land_transaction_files(
        settings, a.source_dir, sources=a.sources, overwrite=a.overwrite, copy=cli_copy, exists=cli_exists
    ):
        print(f"{status:<18} {target}")


if __name__ == "__main__":
    main()
