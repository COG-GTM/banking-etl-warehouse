"""Stage the ``fixtures/sample_db/*.csv`` exports into the landing volume (dev fixture mode).

Layout: ``<landing>/sample_db/<table>/<table>.csv``, one folder per source table, which is
what :func:`banking_etl.bronze.sqlserver.read_fixture` reads.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable

from banking_etl.bronze.sqlserver import parse_tables
from banking_etl.config import Settings

FIXTURES_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "sample_db"


def fixture_files(tables: str | Iterable[str] | None = None, src_dir: Path | str = FIXTURES_DIR) -> dict[str, Path]:
    files = {t: Path(src_dir) / f"{t}.csv" for t in parse_tables(tables)}
    missing = [str(p) for p in files.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError(f"missing fixture files: {missing}")
    return files


def fixture_targets(dest_root: str, tables: str | Iterable[str] | None = None) -> dict[str, str]:
    root = dest_root.rstrip("/")
    return {t: f"{root}/{t}/{t}.csv" for t in parse_tables(tables)}


def stage_fixtures(
    dest_root: str | Path, tables: str | Iterable[str] | None = None, src_dir: Path | str = FIXTURES_DIR
) -> list[str]:
    """Copy fixtures with plain file I/O (local dirs or ``/Volumes/...`` on Databricks).

    Each table folder is emptied of other ``*.csv`` files first so it holds exactly one snapshot.
    """
    files = fixture_files(tables, src_dir)
    written = []
    for table, dest in fixture_targets(str(dest_root), tables).items():
        folder = Path(dest).parent
        folder.mkdir(parents=True, exist_ok=True)
        for stale in folder.glob("*.csv"):
            if stale.name != Path(dest).name:
                stale.unlink()
        shutil.copyfile(files[table], dest)
        written.append(dest)
    return written


def landing_root(settings: Settings) -> str:
    return settings.landing_path("sample_db")
