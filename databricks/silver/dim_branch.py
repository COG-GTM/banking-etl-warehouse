"""Port of the Talend job `Load_DimBranch`.

Legacy flow (IDX_INTERNSHIP/process/Load_DimBranch_0.1.item):

    tMSSqlInput "branch"  ->  tMap_1 (to_DimBranch)  ->  tMSSqlOutput DimBranch

The tMap holds three pass-through expressions and no filter or variable:

    BranchID       <- row1.branch_id       (id_Integer)
    BranchName     <- row1.branch_name     (id_String)
    BranchLocation <- row1.branch_location (id_String)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .common import DimensionConfig, clean_string, drop_ingest_metadata, to_int

DIM_BRANCH_CONFIG = DimensionConfig(
    bronze_tables={"branch": "sample_branch"},
    silver_table="dim_branch",
    gold_table="dim_branch",
    business_key="branch_id",
)


def transform_dim_branch(bronze_branch: DataFrame) -> DataFrame:
    """Conform `bronze.sample_branch` into the silver `dim_branch` contract."""
    return drop_ingest_metadata(bronze_branch).select(
        to_int(F.col("branch_id")).alias("branch_id"),
        clean_string(F.col("branch_name")).alias("branch_name"),
        clean_string(F.col("branch_location")).alias("branch_location"),
    )
