"""Ticket 6: Silver ``dim_account`` load (port of Talend ``Load_DimAccount``).

Talend job (``talend_jobs/Load_DimAccount.zip``): ``tMSSqlInput`` ``SELECT account_id, customer_id,
account_type, balance, date_opened, status FROM dbo.account`` -> ``tMap`` straight copy to
``AccountID`` / ``CustomerID`` / ``AccountType`` / ``Balance`` / ``DateOpened`` / ``Status`` ->
``tMSSqlOutput`` ``DimAccount``, ``DATA_ACTION=INSERT``, ``DIE_ON_ERROR=false``.

The only conversions are the ones SQL Server did implicitly on INSERT into the DWH table:
``balance`` int -> ``MONEY`` (here ``DECIMAL(19,4)``) and ``date_opened`` datetime2 -> ``DATE``
(the time of day is dropped). Then SCD-1 ``MERGE`` into Silver ``dim_account`` and the T2 PK checks.
"""

from __future__ import annotations

from datetime import datetime

from ..common.config import EnvConfig
from ..contracts.tables import BRONZE_SQLSERVER_ACCOUNT, DIM_ACCOUNT
from .merge import run_pk_checks, scd1_merge

SOURCE = BRONZE_SQLSERVER_ACCOUNT
TARGET = DIM_ACCOUNT


def transform(bronze_df):
    """tMap ``to_DimAccount`` plus the implicit SQL Server conversions into the DWH column types."""
    from pyspark.sql import functions as F

    return bronze_df.select(
        F.col("account_id").cast("int").alias("account_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("account_type").cast("string").alias("account_type"),
        F.col("balance").cast("decimal(19,4)").alias("balance"),
        F.to_date(F.col("date_opened")).alias("date_opened"),
        F.col("status").cast("string").alias("status"),
        F.col("_ingested_at"),
        F.col("_batch_id"),
    )


def load(spark, cfg: EnvConfig, *, loaded_at: datetime | None = None, dq_run_id: str | None = None,
         fail_on_error: bool = True, unity_catalog: bool = True) -> dict:
    bronze = spark.table(cfg.table(SOURCE.layer, SOURCE.name))
    result = scd1_merge(spark, cfg, TARGET, transform(bronze), loaded_at=loaded_at, unity_catalog=unity_catalog)
    dq = run_pk_checks(spark, cfg, TARGET, dq_run_id=dq_run_id, fail_on_error=fail_on_error)
    return {"table": TARGET.name, "source": cfg.table(SOURCE.layer, SOURCE.name), **result.as_dict(), "dq": dq}
