# Ticket 9: stored procedures -> Unity Catalog SQL table functions

`sql_scripts/02_create_procedures.sql` defines two read-only reporting procedures on the DWH star schema.
Both become **Unity Catalog SQL table-valued functions** in the gold schema, with a PySpark equivalent and
a widget notebook for ad-hoc use.

| T-SQL | Databricks SQL |
|---|---|
| `EXEC DWH.dbo.sp_DailyTransaction '2024-01-18', '2024-01-20'` | `SELECT * FROM <catalog>.<prefix>gold.fn_daily_transaction(DATE'2024-01-18', DATE'2024-01-20')` |
| `EXEC DWH.dbo.sp_BalancePerCustomer 'shelly'` | `SELECT * FROM <catalog>.<prefix>gold.fn_balance_per_customer('shelly')` |

| Piece | Path |
|---|---|
| Function DDL (`${...}` names from `Settings`) | `sql/analytics/01_fn_daily_transaction.sql`, `sql/analytics/02_fn_balance_per_customer.sql` |
| Create / call the functions | `banking_etl.analytics.functions` (`apply_analytics_functions`, `daily_transaction`, `balance_per_customer`) |
| PySpark DataFrame equivalents | `banking_etl.analytics.procedures` (`daily_transaction_df`, `balance_per_customer_df`) |
| Notebooks | `notebooks/analytics/create_analytics_functions.py` (creates both), `daily_transaction.py`, `balance_per_customer.py` (widget front ends) |
| Job | `resources/ticket-09-analytics.yml` (`banking_etl_analytics_functions`: create, then smoke-run both notebooks) |
| SQL Server expectations | `scripts/analytics/sqlserver_parity.py` -> `tests/analytics/expected/sqlserver_{dwh,dwh_edge}.json` |
| Live parity check | `scripts/analytics/live_parity.py` |

## Why SQL table functions

* **Same call shape.** Each procedure takes scalar parameters and returns one result set; a UC SQL TVF does
  the same. `SELECT * FROM fn(...)` replaces `EXEC`, and the result can also be joined or filtered further.
* **Governed like a table.** The functions live in gold next to the tables they read. `EXECUTE` is granted
  through the existing schema grants (ticket 1), and they run from SQL warehouses, notebooks, jobs and BI tools.
* **No extra state.** The procedures only read data. A view can't take parameters, and a materialized
  table or DLT pipeline would add storage and refresh cost for something that is computed on demand anyway.
* The PySpark functions are for pipelines that want a DataFrame. They follow the same rules, and the same
  parity tests check them.

## SQL Server semantics preserved

The expectations come from running the **original procedures on SQL Server 2022** with the default
collation `SQL_Latin1_General_CP1_CI_AS`, over two databases:
* `DWH`: loaded the way the Talend jobs load it.
* `DWH_EDGE`: synthetic edge rows.

| Behaviour (SQL Server) | Port |
|---|---|
| `CAST(TransactionDate AS DATE) BETWEEN @start AND @end` is inclusive; NULL bound or start > end -> no rows | same `BETWEEN` on `DATE` parameters |
| `COUNT(...)` is `INT`, `SUM(MONEY)` is `MONEY` | `INT`, `DECIMAL(19,4)` (ANSI mode errors on overflow like SQL Server) |
| `CustomerName LIKE '%' + @name + '%'` is case-insensitive | `ILIKE` |
| `%` and `_` in `@name` are wildcards | kept as wildcards; runs of `%` collapsed (same matches, avoids Java-regex backtracking) |
| T-SQL `LIKE` has no escape character (`\` is literal) | backslashes doubled before `ILIKE` |
| `@customer_name VARCHAR(100)` silently truncates | `left(customer_name, 100)` |
| `@name = NULL` -> no rows, `''` -> every non-NULL name | same |
| `Status = 'active'` and `TransactionType = 'Deposit'` ignore case and trailing spaces | `lower(rtrim(col)) = '...'` (a leading space still doesn't match) |
| Non-`Deposit` types, including NULL, subtract | `CASE ... ELSE -Amount` |
| NULL `Amount` / `Balance` propagate; accounts without transactions keep their balance (`ISNULL(sum, 0)`) | same (`COALESCE`) |
| Inner join customer -> account, left join transaction summary | same |

**Known gap:** T-SQL `LIKE` character classes (`[a-c]`, `[^x]`) are matched literally, because Spark `ILIKE`
has no bracket classes. No caller in the repo passes them.

Implementation notes:
* `sp_BalancePerCustomer` has no `ORDER BY`, so neither does the function.
* `fn_daily_transaction` orders by `Date`. The Python wrapper also orders its outer `SELECT`, because a
  function's `ORDER BY` isn't guaranteed to survive outer queries.
* OSS Spark 4.0 can't call a schema-qualified SQL table function. Local runs (`Settings(catalog=None)`)
  therefore create the same body as a session `TEMPORARY FUNCTION`.

## Evidence

* `tests/analytics/test_analytics.py`: loads each SQL Server scenario's tables into an isolated local gold
  schema. For all 34 cases (20 `DWH`, 14 `DWH_EDGE`), both the SQL function and the PySpark port must
  return SQL Server's rows **exactly**: same values, same `DECIMAL(19,4)` scale, same NULLs. The file also
  covers result types, rendering, idempotence, date-argument parsing and LIKE-pattern construction.
* Live (SQL warehouse): `scripts/analytics/live_parity.py` loads both scenarios into
  `migration_demo.banking_etl_t9_{dwh,dwh_edge}_gold`, creates the functions there and checks all 34 cases:
  0 mismatches. With `--gold-prefix banking_etl_` it also runs the 20 `DWH` cases against the functions in
  the migrated `migration_demo.banking_etl_gold`.
* The run URLs of the `jobs submit` that creates the functions in `migration_demo.banking_etl_gold` and runs
  both notebooks are in the PR description.

Recreate the expectations: start SQL Server 2022 in Docker, then run
`MSSQL_SA_PASSWORD=... python scripts/analytics/sqlserver_parity.py --container <name>`.
