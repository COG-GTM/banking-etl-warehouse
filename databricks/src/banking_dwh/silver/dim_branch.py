"""Ticket 6: Silver ``dim_branch`` load (port of Talend ``Load_DimBranch``).

Talend job (``talend_jobs/Load_DimBranch.zip``): ``tMSSqlInput`` ``SELECT branch_id, branch_name,
branch_location FROM dbo.branch`` -> ``tMap`` straight copy to ``BranchID`` / ``BranchName`` /
``BranchLocation`` -> ``tMSSqlOutput`` ``DimBranch``, ``DATA_ACTION=INSERT``, ``DIE_ON_ERROR=false``.

Here: Bronze ``sqlserver_branch`` -> same straight copy -> SCD-1 ``MERGE`` into Silver ``dim_branch``
(see ``banking_dwh.silver.merge`` for why MERGE replaces the INSERT), then the T2 PK checks.
"""

from __future__ import annotations

from datetime import datetime

from ..common.config import EnvConfig
from ..contracts.tables import BRONZE_SQLSERVER_BRANCH, DIM_BRANCH
from .merge import run_pk_checks, scd1_merge

SOURCE = BRONZE_SQLSERVER_BRANCH
TARGET = DIM_BRANCH


def transform(bronze_df):
    """tMap ``to_DimBranch``: column-for-column copy, cast to the Silver contract types."""
    from pyspark.sql import functions as F

    return bronze_df.select(
        F.col("branch_id").cast("int").alias("branch_id"),
        F.col("branch_name").cast("string").alias("branch_name"),
        F.col("branch_location").cast("string").alias("branch_location"),
        F.col("_ingested_at"),
        F.col("_batch_id"),
    )


def load(spark, cfg: EnvConfig, *, loaded_at: datetime | None = None, dq_run_id: str | None = None,
         fail_on_error: bool = True, unity_catalog: bool = True) -> dict:
    bronze = spark.table(cfg.table(SOURCE.layer, SOURCE.name))
    result = scd1_merge(spark, cfg, TARGET, transform(bronze), loaded_at=loaded_at, unity_catalog=unity_catalog)
    dq = run_pk_checks(spark, cfg, TARGET, dq_run_id=dq_run_id, fail_on_error=fail_on_error)
    return {"table": TARGET.name, "source": cfg.table(SOURCE.layer, SOURCE.name), **result.as_dict(), "dq": dq}
