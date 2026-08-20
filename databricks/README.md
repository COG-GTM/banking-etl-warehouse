# Databricks implementation

Databricks (Delta Lake + Spark SQL / PySpark) port of the SQL Server + Talend + T-SQL
warehouse. The legacy assets under `sql_scripts/` and `talend_jobs/` are kept for
reference; everything here is the modern equivalent.

| File | Replaces | Purpose |
|------|----------|---------|
| `01_create_tables.sql` | `sql_scripts/01_create_tables.sql` | Star-schema Delta tables (`DimCustomer`, `DimAccount`, `DimBranch`, `FactTransaction`) |
| `02_daily_transaction.py` | `sp_DailyTransaction` | Daily volume/amount **+ moving-average smoothing** |
| `03_balance_per_customer.py` | `sp_BalancePerCustomer` | Initial vs. current balance for active accounts |
| `04_ingest_sources.py` | Talend `Load_*` jobs | PySpark ingestion of the relational, CSV and Excel sources |
| `dwh_analytics.py` | — | Shared Spark functions imported by the analytics notebooks |
| `tests/test_dwh_analytics.py` | — | Local Spark tests (no cluster needed) |

The `.py` files are Databricks notebook sources (`# COMMAND ----------` cell markers) and
can be imported directly into a workspace or synced via Databricks Repos.

## Type mapping

| SQL Server | Delta / Spark |
|------------|---------------|
| `INT` | `INT` |
| `VARCHAR(n)` | `STRING` |
| `MONEY` | `DECIMAL(19,4)` |
| `DATE` | `DATE` |
| `DATETIME` | `TIMESTAMP` |

Delta Lake does not enforce PRIMARY KEY / FOREIGN KEY constraints. The original keys are
documented as informational column comments (and as optional Unity Catalog informational
constraints at the end of `01_create_tables.sql`); key columns are `NOT NULL`.

## Running

1. Create the schema and tables — run `01_create_tables.sql` with the `catalog` and
   `schema` parameters set (e.g. `main` / `dwh`).
2. Load data — run `04_ingest_sources.py`, pointing the widgets at the JDBC source and
   the CSV/Excel files (upload `data_sources/` to a volume or DBFS first). Reading
   `.xlsx` needs the `com.crealytics:spark-excel` library on the cluster; leave
   `excel_path` empty to skip it.
3. Analytics — run `02_daily_transaction.py` (widgets: `start_date`, `end_date`,
   `window_size`) and `03_balance_per_customer.py` (widget: `customer_name`).

Each analytics notebook also embeds the equivalent pure Spark SQL query if you would
rather run it from a SQL warehouse.

## Moving-average smoothing

`daily_transaction` adds `SmoothedTotalAmount`: a trailing moving average of the daily
`TotalAmount`, computed with

```sql
AVG(TotalAmount) OVER (ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```

The window size is configurable via the `window_size` widget / function argument and
defaults to 7 days. It is a trailing window over the days present in the result, so the
first `window_size - 1` rows are a running mean over fewer days. Results are ordered by
`Date`.

## Tests

```bash
pip install pyspark pytest
python -m pytest databricks/tests
```
