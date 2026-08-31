# Stored procedure parity: T-SQL → dbt

The legacy DWH exposed two parameterized T-SQL stored procedures
(`sql_scripts/02_create_procedures.sql`). dbt has no stored procedures, so each one
becomes a model in `marts/reporting/`, and each runtime parameter becomes a dbt var
that is applied only when it is non-empty. That keeps the materialized table usable
unfiltered, so a BI layer can slice it freely, while still allowing a parameterized
build (`dbt run --vars '{...}'`) that reproduces the legacy call semantics.

## Parameter → var mapping

| Legacy procedure | Legacy parameter | dbt var | Default (`dbt_project.yml`) | Applied when |
| --- | --- | --- | --- | --- |
| `sp_DailyTransaction` | `@start_date DATE` | `start_date` | `'2024-01-01'` | var is non-empty |
| `sp_DailyTransaction` | `@end_date DATE` | `end_date` | `'2024-12-31'` | var is non-empty |
| `sp_BalancePerCustomer` | `@customer_name VARCHAR(100)` | `customer_name` | `''` (empty → no filter) | var is non-empty |

Example parameterized run:

```bash
dbt run --select rpt_daily_transaction --vars '{"start_date": "2024-03-01", "end_date": "2024-03-31"}'
dbt run --select rpt_balance_per_customer --vars '{"customer_name": "smith"}'
```

To build the unfiltered report, pass empty strings:

```bash
dbt run --select rpt_daily_transaction --vars '{"start_date": "", "end_date": ""}'
```

## 1. `sp_DailyTransaction` → `rpt_daily_transaction`

### Legacy T-SQL

```sql
CREATE PROCEDURE sp_DailyTransaction
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        CAST(TransactionDate AS DATE) AS [Date],
        COUNT(TransactionID) AS TotalTransactions,
        SUM(Amount) AS TotalAmount
    FROM FactTransaction
    WHERE CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date
    GROUP BY CAST(TransactionDate AS DATE)
    ORDER BY [Date];
END;
```

### dbt model (`dbt/models/marts/reporting/rpt_daily_transaction.sql`)

```sql
with transactions as (
    select transaction_id, transaction_date, amount
    from {{ ref('fct_transaction') }}
    where 1 = 1
    {% if var('start_date', '') %}
        and cast(transaction_date as date) >= cast('{{ var("start_date") }}' as date)
    {% endif %}
    {% if var('end_date', '') %}
        and cast(transaction_date as date) <= cast('{{ var("end_date") }}' as date)
    {% endif %}
)
select
    cast(transaction_date as date) as transaction_day,
    count(transaction_id) as total_transactions,
    sum(amount) as total_amount
from transactions
group by cast(transaction_date as date)
order by transaction_day
```

### Column mapping

| Legacy output | dbt output |
| --- | --- |
| `[Date]` | `transaction_day` |
| `TotalTransactions` | `total_transactions` |
| `TotalAmount` | `total_amount` |

## 2. `sp_BalancePerCustomer` → `rpt_balance_per_customer`

### Legacy T-SQL

```sql
CREATE PROCEDURE sp_BalancePerCustomer
    @customer_name VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    WITH TransactionSummary AS (
        SELECT
            AccountID,
            SUM(CASE WHEN TransactionType = 'Deposit' THEN Amount ELSE -Amount END)
                AS TotalTransactionAmount
        FROM FactTransaction
        GROUP BY AccountID
    )
    SELECT
        c.CustomerName,
        a.AccountType,
        a.Balance AS InitialBalance,
        a.Balance + ISNULL(ts.TotalTransactionAmount, 0) AS CurrentBalance
    FROM DimCustomer c
    JOIN DimAccount a ON c.CustomerID = a.CustomerID
    LEFT JOIN TransactionSummary ts ON a.AccountID = ts.AccountID
    WHERE c.CustomerName LIKE '%' + @customer_name + '%'
      AND a.Status = 'active';
END;
```

### dbt model (`dbt/models/marts/reporting/rpt_balance_per_customer.sql`)

```sql
with transaction_summary as (
    select
        account_id,
        sum(case when lower(transaction_type) = 'deposit' then amount else -amount end)
            as total_transaction_amount
    from {{ ref('fct_transaction') }}
    group by account_id
)
select
    c.customer_name,
    a.account_type,
    a.balance as initial_balance,
    a.balance + coalesce(ts.total_transaction_amount, 0) as current_balance
from {{ ref('dim_customer') }} as c
join {{ ref('dim_account') }} as a
    on c.customer_id = a.customer_id
left join transaction_summary as ts
    on a.account_id = ts.account_id
where lower(a.status) = 'active'
{% if var('customer_name', '') %}
    and lower(c.customer_name) like lower('%{{ var("customer_name") }}%')
{% endif %}
```

### Column mapping

| Legacy output | dbt output |
| --- | --- |
| `CustomerName` | `customer_name` |
| `AccountType` | `account_type` |
| `InitialBalance` | `initial_balance` |
| `CurrentBalance` | `current_balance` |

## Semantic differences

- **Parameters vs. vars.** A stored procedure is called per request with arguments; a dbt
  model is materialized once per run. The vars therefore behave as *build-time* filters.
  With the defaults above, `rpt_daily_transaction` is built for calendar year 2024 and
  `rpt_balance_per_customer` for every active account. Setting the date vars to `''`
  produces the full aggregate, which is the recommended shape when a BI tool does the
  filtering.
- **`ISNULL` → `coalesce`.** T-SQL's two-argument `ISNULL` has no Databricks equivalent;
  ANSI `coalesce` is used. One behavioural nuance: `ISNULL` coerces the result to the
  type of the *first* argument, while `coalesce` resolves to the common supertype of all
  arguments. With `DECIMAL(18,2)` and the integer literal `0` the result is
  `DECIMAL(18,2)` either way, so no rounding change is expected here.
- **`MONEY` rounding.** SQL Server `MONEY` is a fixed 4-decimal scaled integer, and
  arithmetic/aggregation on it truncates rather than rounds in some cases. The target
  type is `DECIMAL(18,2)` (per the agreed type mapping), so sums are exact at 2 decimals.
  Source values carrying 3–4 decimal places will be rounded during the staging cast, so
  `total_amount` / `current_balance` can differ from the legacy output by sub-cent
  amounts on such rows.
- **`LIKE '%' + @customer_name + '%'`.** SQL Server string concatenation with `+` becomes
  Jinja interpolation into a `like '%…%'` pattern. Two differences: (1) SQL Server
  collations are case-insensitive by default, so the legacy `LIKE` ignored case — the dbt
  version lowercases both sides explicitly to preserve that; (2) if `@customer_name` was
  `NULL`, the T-SQL concatenation yielded `NULL` and the predicate matched nothing,
  whereas an empty/unset dbt var removes the predicate entirely and matches everything.
  Wildcard characters (`%`, `_`) inside the var are still interpreted as wildcards, as in
  the original.
- **`transaction_type` casing.** The legacy comparison was `TransactionType = 'Deposit'`,
  which under a case-insensitive SQL Server collation also matched `DEPOSIT`/`deposit`.
  Databricks string comparison is case-sensitive, and staging (Ticket 5) may normalize
  the value's casing (the Talend `Load_DimCustomer` job uppercases text fields), so the
  model compares `lower(transaction_type) = 'deposit'`. The same reasoning applies to
  `a.status = 'active'`.
- **`ORDER BY` is not guaranteed.** The procedure's `ORDER BY [Date]` ordered a result
  set. `rpt_daily_transaction` is materialized as a Delta table, where row order is a
  physical detail and not preserved by a subsequent `select *`. The `order by` is kept
  because it influences file layout/clustering, but consumers that need ordered output
  must add their own `ORDER BY`.
- **Grain and uniqueness.** `rpt_daily_transaction` is one row per day (tested `unique` +
  `not_null` on `transaction_day`). `rpt_balance_per_customer` is one row per *active
  account*, not per customer — a customer with several active accounts produces several
  rows, exactly as in the legacy procedure.
