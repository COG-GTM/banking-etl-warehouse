"""Load DimBranch, DimAccount and DimCustomer from bronze into gold.

Runs either as a Databricks job/notebook (widgets) or locally via argparse::

    python -m databricks.jobs.dimensions.load_dimensions \
        --bronze-catalog main --bronze-schema bronze \
        --gold-catalog main --gold-schema gold

    # local / CI run against parquet directories
    python -m databricks.jobs.dimensions.load_dimensions \
        --format parquet --path /tmp/dwh

With ``--format parquet --path P`` sources are read from ``P/bronze/<table>``
and gold tables are written to ``P/gold/<Table>``.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from .transforms import build_dim_account, build_dim_branch, build_dim_customer

SOURCE_TABLES = ("branch", "account", "customer", "city", "state")
GOLD_TABLES = ("DimBranch", "DimAccount", "DimCustomer")


@dataclass
class Config:
    bronze_catalog: str = "main"
    bronze_schema: str = "bronze"
    gold_catalog: str = "main"
    gold_schema: str = "gold"
    format: str = "delta"
    path: str | None = None


def _try_widgets() -> Config | None:
    """Read parameters from Databricks widgets when running inside a notebook."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
    except ImportError:
        return None
    try:
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        defaults = Config()
        for name, default in (
            ("bronze_catalog", defaults.bronze_catalog),
            ("bronze_schema", defaults.bronze_schema),
            ("gold_catalog", defaults.gold_catalog),
            ("gold_schema", defaults.gold_schema),
            ("format", defaults.format),
            ("path", ""),
        ):
            dbutils.widgets.text(name, default)
        return Config(
            bronze_catalog=dbutils.widgets.get("bronze_catalog"),
            bronze_schema=dbutils.widgets.get("bronze_schema"),
            gold_catalog=dbutils.widgets.get("gold_catalog"),
            gold_schema=dbutils.widgets.get("gold_schema"),
            format=dbutils.widgets.get("format") or defaults.format,
            path=dbutils.widgets.get("path") or None,
        )
    except Exception:  # noqa: BLE001 - widgets unavailable outside notebooks
        return None


def parse_args(argv: Sequence[str] | None = None) -> Config:
    p = argparse.ArgumentParser(description=__doc__)
    d = Config()
    p.add_argument("--bronze-catalog", default=d.bronze_catalog)
    p.add_argument("--bronze-schema", default=d.bronze_schema)
    p.add_argument("--gold-catalog", default=d.gold_catalog)
    p.add_argument("--gold-schema", default=d.gold_schema)
    p.add_argument("--format", default=d.format, choices=["delta", "parquet"])
    p.add_argument("--path", default=None, help="Base path for --format parquet local runs")
    a = p.parse_args(argv)
    return Config(
        bronze_catalog=a.bronze_catalog,
        bronze_schema=a.bronze_schema,
        gold_catalog=a.gold_catalog,
        gold_schema=a.gold_schema,
        format=a.format,
        path=a.path,
    )


def read_source(spark: SparkSession, cfg: Config, table: str) -> DataFrame:
    if cfg.path:
        return spark.read.format(cfg.format).load(f"{cfg.path}/bronze/{table}")
    return spark.read.table(f"{cfg.bronze_catalog}.{cfg.bronze_schema}.{table}")


def write_gold(df: DataFrame, cfg: Config, table: str) -> None:
    writer = df.write.format(cfg.format).mode("overwrite").option("overwriteSchema", "true")
    if cfg.path:
        writer.save(f"{cfg.path}/gold/{table}")
    else:
        writer.saveAsTable(f"{cfg.gold_catalog}.{cfg.gold_schema}.{table}")


def run(spark: SparkSession, cfg: Config) -> dict[str, DataFrame]:
    src = {t: read_source(spark, cfg, t) for t in SOURCE_TABLES}
    gold = {
        "DimBranch": build_dim_branch(src["branch"]),
        "DimAccount": build_dim_account(src["account"]),
        "DimCustomer": build_dim_customer(src["customer"], src["city"], src["state"]),
    }
    for name, df in gold.items():
        write_gold(df, cfg, name)
    return gold


def main(argv: Sequence[str] | None = None) -> None:
    cfg = _try_widgets() if argv is None and len(sys.argv) == 1 else None
    if cfg is None:
        cfg = parse_args(argv)
    spark = SparkSession.builder.appName("load_dimensions").getOrCreate()
    run(spark, cfg)


if __name__ == "__main__":
    main()
