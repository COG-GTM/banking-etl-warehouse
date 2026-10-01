"""Pure DataFrame transformations replicating the Talend tMap / tUnite / tUniqRow logic."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from banking_etl.readers import conform
from banking_etl.schemas import DIM_ACCOUNT, DIM_BRANCH, DIM_CUSTOMER, FACT_TRANSACTION

SOURCE_PRIORITY_COL = "_source_priority"
SOURCE_ORDER_COL = "_source_order"


def transform_dim_branch(branch: DataFrame) -> DataFrame:
    """Load_DimBranch: one-to-one column mapping."""
    return conform(
        branch.select(
            F.col("branch_id").alias("BranchID"),
            F.col("branch_name").alias("BranchName"),
            F.col("branch_location").alias("BranchLocation"),
        ),
        DIM_BRANCH.struct,
    )


def transform_dim_account(account: DataFrame) -> DataFrame:
    """Load_DimAccount: one-to-one mapping; balance -> MONEY, date_opened -> DATE."""
    return conform(
        account.select(
            F.col("account_id").alias("AccountID"),
            F.col("customer_id").alias("CustomerID"),
            F.col("account_type").alias("AccountType"),
            F.col("balance").alias("Balance"),
            F.to_date("date_opened").alias("DateOpened"),
            F.col("status").alias("Status"),
        ),
        DIM_ACCOUNT.struct,
    )


def transform_dim_customer(customer: DataFrame, city: DataFrame, state: DataFrame) -> DataFrame:
    """Load_DimCustomer tMap.

    customer (main) LEFT JOIN city ON city_id LEFT JOIN state ON state_id (Talend
    lookups default to left outer joins), with StringHandling.UPCASE applied to
    customer_name, address and gender.
    """
    c = customer.alias("c")
    ct = city.alias("ct")
    st = state.alias("st")
    joined = c.join(ct, F.col("c.city_id") == F.col("ct.city_id"), "left").join(
        st, F.col("ct.state_id") == F.col("st.state_id"), "left"
    )
    return conform(
        joined.select(
            F.col("c.customer_id").alias("CustomerID"),
            F.upper("c.customer_name").alias("CustomerName"),
            F.upper("c.address").alias("Address"),
            F.col("ct.city_name").alias("CityName"),
            F.col("st.state_name").alias("StateName"),
            F.trim("c.age").cast("int").alias("Age"),
            F.upper("c.gender").alias("Gender"),
            F.col("c.email").alias("Email"),
        ),
        DIM_CUSTOMER.struct,
    )


def unite_transactions(*sources: DataFrame) -> DataFrame:
    """tUnite: union the transaction streams, tagging each row with its stream
    priority (tUnite merge order) and its position within the stream."""
    tagged = [
        df.withColumn(SOURCE_PRIORITY_COL, F.lit(priority)).withColumn(
            SOURCE_ORDER_COL, F.monotonically_increasing_id()
        )
        for priority, df in enumerate(sources, start=1)
    ]
    united = tagged[0]
    for df in tagged[1:]:
        united = united.unionByName(df)
    return united


def deduplicate_transactions(united: DataFrame, key: str = "transaction_id") -> DataFrame:
    """tUniqRow on ``key``: keep the first occurrence, i.e. a deterministic
    ``dropDuplicates([key])`` that prefers the SQL Server row, then Excel, then CSV."""
    window = Window.partitionBy(key).orderBy(SOURCE_PRIORITY_COL, SOURCE_ORDER_COL)
    return (
        united.withColumn("_rank", F.row_number().over(window))
        .where(F.col("_rank") == 1)
        .drop("_rank", SOURCE_PRIORITY_COL, SOURCE_ORDER_COL)
    )


def transform_fact_transaction(unique_transactions: DataFrame) -> DataFrame:
    """Load_FactTransaction output tMap: rename and cast to the fact schema."""
    return conform(
        unique_transactions.select(
            F.col("transaction_id").alias("TransactionID"),
            F.col("account_id").alias("AccountID"),
            F.col("transaction_date").alias("TransactionDate"),
            F.col("amount").alias("Amount"),
            F.col("transaction_type").alias("TransactionType"),
            F.col("branch_id").alias("BranchID"),
        ),
        FACT_TRANSACTION.struct,
    )


def split_orphan_facts(
    fact: DataFrame, dim_account: DataFrame, dim_branch: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Split facts into (valid, rejected) using the legacy FactTransaction
    constraints: non-null primary key and FK_FactTransaction_DimAccount /
    FK_FactTransaction_DimBranch (NULL foreign keys are allowed, as in SQL Server)."""
    accounts = dim_account.select(F.col("AccountID").alias("_fk_account")).distinct()
    branches = dim_branch.select(F.col("BranchID").alias("_fk_branch")).distinct()
    flagged = (
        fact.join(accounts, fact["AccountID"] == accounts["_fk_account"], "left")
        .join(branches, fact["BranchID"] == branches["_fk_branch"], "left")
        .withColumn(
            "_valid",
            F.col("TransactionID").isNotNull()
            & (F.col("AccountID").isNull() | F.col("_fk_account").isNotNull())
            & (F.col("BranchID").isNull() | F.col("_fk_branch").isNotNull()),
        )
    )
    columns = FACT_TRANSACTION.column_names
    return (
        flagged.where(F.col("_valid")).select(columns),
        flagged.where(~F.col("_valid")).select(columns),
    )
