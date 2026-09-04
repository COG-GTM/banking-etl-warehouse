# Analytics procedures on Databricks

Translation of `sql_scripts/02_create_procedures.sql` (SQL Server stored
procedures) to Databricks. Databricks has no `EXEC sp_X` — the procedures
become plain DataFrame functions, parameterised Spark SQL files, and a job task
that materialises gold tables.

| T-SQL                                         | PySpark (`procedures.py`)                                          | Spark SQL file                                |
|-----------------------------------------------|--------------------------------------------------------------------|-----------------------------------------------|
| `EXEC sp_DailyTransaction '2024-01-01','2024-01-31'` | `daily_transaction(fact_df, "2024-01-01", "2024-01-31")`       | `daily_transaction.sql` (`:start_date`, `:end_date`) |
| `EXEC sp_BalancePerCustomer 'smith'`          | `balance_per_customer(customer_df, account_df, fact_df, "smith")`  | `balance_per_customer.sql` (`:customer_name`) |

## Calling options

**1. Python (notebook / job / tests)**

```python
from databricks.analytics.procedures import daily_transaction, balance_per_customer

fact = spark.table("dev_banking.gold.FactTransaction")
daily_transaction(fact, "2024-01-01", "2024-01-31").display()
```

**2. Spark SQL with named parameter markers**

```python
sql = open("databricks/analytics/balance_per_customer.sql").read()
sql = sql.replace("${catalog}", "dev_banking").replace("${schema}", "gold")
spark.sql(sql, args={"customer_name": "smith"}).display()
```

In a Databricks SQL editor/notebook, `:customer_name` becomes a widget/parameter
prompt; `${catalog}` / `${schema}` are substituted by the Asset Bundle variables
(or replaced by hand).

**3. Python UDTF (closest to `EXEC` from SQL)**

```python
from pyspark.sql.functions import udtf


@udtf(returnType="Date date, TotalTransactions long, TotalAmount decimal(19,4)")
class DailyTransactionUDTF:
    def eval(self, start_date: str, end_date: str):
        for r in daily_transaction(
            spark.table("dev_banking.gold.FactTransaction"), start_date, end_date
        ).collect():
            yield tuple(r)


spark.udtf.register("sp_DailyTransaction", DailyTransactionUDTF)
spark.sql("SELECT * FROM sp_DailyTransaction('2024-01-01', '2024-01-31')")
```

**4. Scheduled job** — `run_analytics.py` is the `run_analytics` task of the
`banking_dwh_job` bundle in `databricks/workflows/`. It runs both procedures and
overwrites the gold tables `DailyTransaction` and `BalancePerCustomer`
in `<catalog>.<schema>`. Parameters: `--catalog`, `--schema`, `--start-date`,
`--end-date`, `--customer-name` (empty string = all customers).

## Semantics preserved

* `MONEY` → `DECIMAL(19,4)`.
* SQL Server's default collation is case-insensitive, so `CustomerName LIKE
  '%name%'`, `Status = 'active'` and `TransactionType = 'Deposit'` are all
  matched case-insensitively (`lower()` on both sides). As in T-SQL, `%`/`_`
  inside the search string act as LIKE wildcards.
* `CAST(TransactionDate AS DATE) BETWEEN @start AND @end` is inclusive on both
  ends and truncates the time component before comparing.
* Accounts without transactions get `CurrentBalance = InitialBalance`
  (`LEFT JOIN` + `COALESCE(…, 0)`, mirroring `ISNULL`).
* `sp_BalancePerCustomer` has no `ORDER BY`; neither does the translation.

Tests: `pytest tests/analytics` (local SparkSession, no Databricks needed).
