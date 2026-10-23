"""CSV transaction file -> Bronze with Auto Loader (Ticket 4).

Ports the ``tFileInputDelimited_1`` component of Talend ``Load_FactTransaction``
(``talend_jobs/Load_FactTransaction.zip``): header row 1, field separator ``,``, no
quote handling (``CSV_OPTION=false``: quotes stay literal), empty rows removed,
``transaction_date`` pattern ``dd-MM-yyyy HH:mm:ss``, ``DIE_ON_ERROR=false``. Talend's encoding ISO-8859-15 is rejected by Spark 4, so the reader
uses ISO-8859-1 (identical for the committed, ASCII-only file).

Talend sent unparsable rows to a REJECT flow and dropped them. Bronze keeps every row
instead: a value that does not fit the contract type lands as NULL and the raw text goes to
``_rescued_data`` (JSON, Auto Loader format). The Talend REJECT set is ``REJECT_PREDICATE``.

``transaction_date`` is read as STRING and parsed in ``to_bronze`` with
``try_to_timestamp(.., 'dd-MM-yyyy HH:mm:ss')``: a live probe showed Auto Loader's native CSV
timestamp parsing turns an impossible date (``32-01-2024``) into NULL *without* rescuing it,
so the date is parsed and rescued here instead. Other columns are typed by the reader.

Two readers share one parsing spec (``CSV_OPTIONS`` + ``reader_schema()``) and one
parse/lineage transform (``to_bronze``):

* ``read_csv_stream(..., autoloader=True)``: ``cloudFiles`` on Databricks; Auto Loader rescues
  bad values natively (``rescuedDataColumn``) and tracks processed files in the checkpoint.
* ``read_csv_batch`` / ``read_csv_stream(..., autoloader=False)``: OSS Spark (local tests),
  which has no ``rescuedDataColumn``; ``_rescued_data`` is rebuilt from the corrupt-record
  column with the same JSON shape. Like Auto Loader, a trailing value with no header column
  is dropped, and a short row yields NULLs without a rescue entry.

Land the committed sample for a demo with ``land_sample_csv`` (inside a notebook) or
``python -m banking_dwh.bronze.csv_autoloader land --target dev --schema-prefix bdwh_t4_``.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from ..common.config import EnvConfig
from ..contracts.ddl import create_table_sql
from ..contracts.tables import BRONZE_FILE_TRANSACTION_CSV

CONTRACT = BRONZE_FILE_TRANSACTION_CSV
STREAM_NAME = "csv_transaction"
LANDING = ("csv", "transaction")
RESCUED_COLUMN = "_rescued_data"
CORRUPT_COLUMN = "_corrupt_record"
TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"
FIELD_SEPARATOR = ","
# Sample CSV: next to src/ in a workspace import (<root>/data_sources), else the repo root
# (local checkout or Databricks Git folder, where src/ lives under databricks/).
_SAMPLE_CANDIDATES = [Path(__file__).resolve().parents[i] / "data_sources" / "transaction_csv.csv" for i in (3, 4)]
SAMPLE_CSV = next((p for p in _SAMPLE_CANDIDATES if p.is_file()), _SAMPLE_CANDIDATES[-1])

CSV_OPTIONS: dict[str, str] = {
    "header": "true",
    "sep": FIELD_SEPARATOR,
    # Talend CSV_OPTION=false: quotes are literal text, not field delimiters ("" disables quoting).
    "quote": "",
    # transaction_date is parsed in to_bronze with the same pattern (see module docstring).
    "timestampFormat": TIMESTAMP_FORMAT,
    "encoding": "ISO-8859-1",
    "mode": "PERMISSIVE",
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
    "pathGlobFilter": "*.csv",
}

# Rows Talend's tFileInputDelimited would have sent to REJECT (transaction_id is NOT NULL there).
REJECT_PREDICATE = f"{RESCUED_COLUMN} IS NOT NULL OR transaction_id IS NULL"


def source_columns() -> list[tuple[str, str]]:
    """(name, type) of the CSV columns, in file order, typed as in the Bronze contract."""
    return [(c.name, c.type) for c in CONTRACT.columns if not c.name.startswith("_")]


def source_schema() -> str:
    """Contract-typed DDL schema of the CSV columns, e.g. ``transaction_id INT, ...``."""
    return ", ".join(f"{n} {t}" for n, t in source_columns())


def reader_schema() -> str:
    """Explicit reader schema: contract types, except timestamps (parsed in ``to_bronze``)."""
    return ", ".join(f"{n} {'STRING' if t == 'TIMESTAMP' else t}" for n, t in source_columns())


def _timestamp_columns() -> list[str]:
    return [n for n, t in source_columns() if t == "TIMESTAMP"]


def autoloader_options(cfg: EnvConfig, *, schema_location: str | None = None) -> dict[str, str]:
    return {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaLocation": schema_location or cfg.checkpoint_path(STREAM_NAME, "schema"),
        # New / unexpected columns go to _rescued_data instead of stopping the stream.
        "cloudFiles.schemaEvolutionMode": "rescue",
        "rescuedDataColumn": RESCUED_COLUMN,
        **CSV_OPTIONS,
    }


def _emulate_rescue(df: Any) -> Any:
    """OSS Spark: rebuild Auto Loader's ``_rescued_data`` from the corrupt-record column.

    For a malformed line, every non-empty raw token whose typed column came back NULL is
    rescued under its column name, plus ``_file_path``.
    """
    from pyspark.sql import functions as F

    cols = [n for n, _ in source_columns()]
    tokens = F.split(F.col(CORRUPT_COLUMN), re.escape(FIELD_SEPARATOR))
    known = F.array(*[
        F.when(
            F.col(c).isNull() & (F.trim(F.get(tokens, i)) != ""),
            F.struct(F.lit(c).alias("k"), F.get(tokens, i).alias("v")),
        )
        for i, c in enumerate(cols)
    ])
    entries = F.filter(known, lambda e: e.isNotNull())
    with_path = F.concat(
        entries, F.array(F.struct(F.lit("_file_path").alias("k"), F.col("_metadata.file_path").alias("v")))
    )
    rescued = F.when(
        F.col(CORRUPT_COLUMN).isNotNull() & (F.size(entries) > 0), F.to_json(F.map_from_entries(with_path))
    )
    return df.withColumn(RESCUED_COLUMN, rescued).drop(CORRUPT_COLUMN)


def _parse_timestamps(df: Any) -> Any:
    """Parse STRING timestamp columns with ``TIMESTAMP_FORMAT``; rescue values that fail."""
    from pyspark.sql import functions as F

    for c in _timestamp_columns():
        if dict(df.dtypes).get(c) != "string":
            continue
        raw = F.col(c)
        parsed = F.expr(f"try_to_timestamp(`{c}`, '{TIMESTAMP_FORMAT}')")
        failed = raw.isNotNull() & (F.trim(raw) != "") & parsed.isNull()
        existing = F.from_json(F.col(RESCUED_COLUMN), "map<string,string>")
        merged = F.to_json(F.map_concat(
            F.coalesce(existing, F.create_map(F.lit("_file_path"), F.col("_metadata.file_path"))),
            F.create_map(F.lit(c), raw),
        ))
        df = df.withColumn(RESCUED_COLUMN, F.when(failed, merged).otherwise(F.col(RESCUED_COLUMN)))
        df = df.withColumn(c, parsed)
    return df


def to_bronze(df: Any) -> Any:
    """Shared parse + lineage transform: reader output -> Bronze contract shape.

    ``df`` must expose the source columns (``reader_schema()``), ``_rescued_data`` and the
    hidden ``_metadata`` file column (both ``spark.read.csv`` and ``cloudFiles`` provide it).
    """
    from pyspark.sql import functions as F

    if RESCUED_COLUMN not in df.columns:
        df = df.withColumn(RESCUED_COLUMN, F.lit(None).cast("string"))
    df = _parse_timestamps(df)
    have = dict(df.dtypes)
    out = []
    for c in CONTRACT.columns:
        if c.name == "_source_file":
            out.append(F.col("_metadata.file_path").alias(c.name))
        elif c.name == "_ingested_at":
            out.append(F.current_timestamp().alias(c.name))
        elif have.get(c.name, "").replace(" ", "").lower() == c.type.replace(" ", "").lower():
            out.append(F.col(c.name))
        else:
            out.append(F.col(c.name).cast(c.type).alias(c.name))
    return df.select(*out)


def read_csv_batch(spark: Any, path: str) -> Any:
    """Batch read (tests / ad-hoc backfills) with the same options, schema and rescue shape."""
    raw = (
        spark.read.options(**CSV_OPTIONS)
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(f"{reader_schema()}, {CORRUPT_COLUMN} STRING")
        .csv(path)
    )
    return to_bronze(_emulate_rescue(raw))


def read_csv_stream(
    spark: Any,
    cfg: EnvConfig,
    *,
    source_dir: str | None = None,
    schema_location: str | None = None,
    autoloader: bool = True,
) -> Any:
    """Streaming read of the landing folder, already shaped like the Bronze contract."""
    path = source_dir or cfg.landing_path(*LANDING)
    if autoloader:
        raw = (
            spark.readStream.format("cloudFiles")
            .options(**autoloader_options(cfg, schema_location=schema_location))
            .schema(reader_schema())
            .load(path)
        )
        return to_bronze(raw)
    raw = (
        spark.readStream.format("csv")
        .options(**CSV_OPTIONS)
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(f"{reader_schema()}, {CORRUPT_COLUMN} STRING")
        .load(path)
    )
    return to_bronze(_emulate_rescue(raw))


def _progress_rows(query: Any) -> int:
    total = 0
    for p in query.recentProgress or []:
        value = p["numInputRows"] if isinstance(p, dict) else getattr(p, "numInputRows", 0)
        total += int(value or 0)
    return total


def ingest(
    spark: Any,
    cfg: EnvConfig,
    *,
    source_dir: str | None = None,
    checkpoint_location: str | None = None,
    schema_location: str | None = None,
    autoloader: bool = True,
    unity_catalog: bool = True,
) -> dict[str, Any]:
    """Create the Bronze table if needed and run one ``availableNow`` micro-batch cycle.

    Exactly-once: files already recorded in the checkpoint are skipped, so a rerun with no
    new files appends 0 rows.
    """
    from pyspark.sql import functions as F

    target = cfg.table("bronze", CONTRACT.name)
    spark.sql(create_table_sql(cfg, CONTRACT, unity_catalog=unity_catalog))
    before = spark.table(target).count()
    query = (
        read_csv_stream(spark, cfg, source_dir=source_dir, schema_location=schema_location, autoloader=autoloader)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location or cfg.checkpoint_path(STREAM_NAME))
        .queryName(f"bronze_{CONTRACT.name}")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    table = spark.table(target)
    after = table.count()
    stats = table.agg(
        F.count(F.when(F.col(RESCUED_COLUMN).isNotNull(), 1)).alias("rescued"),
        F.countDistinct("_source_file").alias("files"),
    ).first()
    return {
        "table": target,
        "source_dir": source_dir or cfg.landing_path(*LANDING),
        "rows_before": before,
        "rows_added": after - before,
        "stream_input_rows": _progress_rows(query),
        "rows_total": after,
        "rescued_rows_total": int(stats["rescued"]),
        "files_total": int(stats["files"]),
        "column_types": dict(table.dtypes),
    }


# ----------------------------------------------------------------------------- demo landing
def land_sample_csv(dest_dir: str, source: str | Path = SAMPLE_CSV, *, overwrite: bool = False) -> dict[str, Any]:
    """Copy the committed CSV into a landing folder (a ``/Volumes/...`` path on Databricks)."""
    src = Path(source)
    if not src.is_file():
        raise FileNotFoundError(f"Sample CSV not found: {src}")
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / src.name
    if target.exists() and not overwrite:
        return {"copied": False, "path": str(target)}
    shutil.copyfile(src, target)
    return {"copied": True, "path": str(target)}


def resolve_sample_source(sample_source: str | Path | None = None) -> Path:
    """Sample CSV to land: an explicit path, else ``data_sources/transaction_csv.csv`` next to ``src/`` or at the repo root.

    In a workspace import that means ``<root>/data_sources/transaction_csv.csv`` (import the
    repo's ``data_sources`` folder alongside ``src``/``config``/``notebooks``, or use a Git folder).
    """
    src = Path(sample_source) if sample_source else SAMPLE_CSV
    if not src.is_file():
        raise FileNotFoundError(
            f"Sample CSV not found: {src}. Set the sample_source widget to the CSV path, or import "
            f"data_sources/transaction_csv.csv to {_SAMPLE_CANDIDATES[0].parent}."
        )
    return src


def land_sample_commands(cfg: EnvConfig, source: str | Path = SAMPLE_CSV, *, overwrite: bool = False) -> list[list[str]]:
    """Databricks CLI commands that upload the sample CSV into the target's landing volume."""
    dest = cfg.landing_path(*LANDING)
    cp = ["databricks", "fs", "cp", str(source), f"dbfs:{dest}/{Path(source).name}"]
    if overwrite:
        cp.append("--overwrite")
    return [["databricks", "fs", "mkdirs", f"dbfs:{dest}"], cp]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Land the sample transaction CSV in the Bronze landing volume.")
    p.add_argument("command", choices=["land"])
    p.add_argument("--target", default="dev")
    p.add_argument("--catalog")
    p.add_argument("--schema-prefix")
    p.add_argument("--source", default=str(SAMPLE_CSV))
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    cfg = EnvConfig.from_target(a.target, catalog=a.catalog, schema_prefix=a.schema_prefix)
    for cmd in land_sample_commands(cfg, a.source, overwrite=a.overwrite):
        print(" ".join(cmd))
        if not a.dry_run:
            subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
