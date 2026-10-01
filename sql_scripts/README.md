# Legacy SQL Server scripts (reference only)

These scripts define the original SQL Server warehouse and are kept for reference. The supported
implementation is the PySpark / Delta Lake project in [`../pyspark`](../pyspark):

| Legacy object                     | Replacement |
|-----------------------------------|-------------|
| `DimCustomer`, `DimAccount`, `DimBranch`, `FactTransaction` (`01_create_tables.sql`) | `dwh.dim_customer`, `dwh.dim_account`, `dwh.dim_branch`, `dwh.fact_transaction` (`banking_etl/schemas.py`) |
| `sp_DailyTransaction` (`02_create_procedures.sql`) | `banking_etl.analytics.daily_transaction` |
| `sp_BalancePerCustomer` (`02_create_procedures.sql`) | `banking_etl.analytics.balance_per_customer` |

`pyspark/tests/integration` still executes both scripts against SQL Server to check that the PySpark
analytics return the same results as the stored procedures.
