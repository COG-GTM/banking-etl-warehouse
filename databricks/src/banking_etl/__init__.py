"""Banking ETL warehouse on Databricks (Unity Catalog medallion: bronze/silver/gold)."""

from banking_etl.config import DEFAULT_CATALOG, LAYERS, EtlConfig

__all__ = ["DEFAULT_CATALOG", "LAYERS", "EtlConfig"]
__version__ = "0.1.0"
