# Raw landing: how the 8 source systems reach Databricks

The legacy pipeline read from eight extraction points: six tables in the SQL Server OLTP
database `sample` (restored from `data_sources/sample.bak`), one CSV extract and one Excel
extract of transactions. On Databricks those eight land in the Unity Catalog schema
`banking.raw` in two different ways.

## 1. SQL-Server-sourced tables → Unity Catalog external tables

Declared in `dbt/models/staging/_sources.yml` as dbt source `sample`
(`database: banking`, `schema: raw`, which resolves to the Unity Catalog
catalog `banking` / schema `raw`):

| Source table | Grain | Legacy role |
| --- | --- | --- |
| `state` | one row per province | joined into `dim_customer` |
| `city` | one row per city | joined into `dim_customer` |
| `customer` | one row per customer | `Load_DimCustomer` |
| `account` | one row per account | `Load_DimAccount` |
| `branch` | one row per branch | `Load_DimBranch` |
| `transaction` | one row per transaction | first stream of `Load_FactTransaction` |

dbt does not create these tables — an upstream ingestion tool does (see
[section 4](#4-what-an-upstream-ingestion-tool-must-provide)). Source freshness is
deliberately **not** configured: there is no ingestion pipeline in this program to emit
loaded-at timestamps, so a freshness block would fail on day one. It should be added
together with the ingestion job.

### Table names in `sample.bak`

`sample.bak` was inspected by decoding its SQL Server data pages with a throwaway Python
script (no SQL Server instance was available). The user tables and their columns are:

```
state(state_id, state_name)
city(city_id, city_name, state_id)
customer(customer_id, customer_name, address, city_id, age, gender, email)
account(account_id, customer_id, account_type, balance, date_opened, status)
branch(branch_id, branch_name, branch_location)
transaction_bank / transaction_db(transaction_id, account_id, transaction_date, amount,
                                  transaction_type, branch_id)
```

The backup contains two sets of the transaction table: `transaction_bank` (an older snapshot,
dated 2022) and `transaction_db` (the current one, dated January 2024) — the table was renamed
between the two backup sets in the file. The dbt source declares it under the neutral name
`transaction`; the Unity Catalog external table is expected to be published under that name.

## 2. File extracts → dbt seeds

| Seed | Source file | Rows |
| --- | --- | --- |
| `raw_transaction_csv` | `data_sources/transaction_csv.csv` (verbatim copy) | 12 |
| `raw_transaction_excel` | `data_sources/transaction_excel.xlsx`, sheet `Sheet1`, converted with openpyxl | 7 |

Both keep the original column names and values. `transaction_date` stays text in the source
format `dd-MM-yyyy HH:mm:ss` and is parsed in the staging layer; `amount` lands as
`DECIMAL(18,2)` to match the `MONEY` → `DECIMAL(18,2)` mapping used throughout the warehouse.

The three transaction streams overlap on `transaction_id` (ids 6, 7 from the SQL table also
appear in the Excel extract; 14, 15 from the Excel extract also appear in the CSV). That is
exactly what the legacy `tUniqRow` component removed, so only the CSV seed carries a `unique`
test — the deduplication itself lives in the intermediate layer.

## 3. Sample seeds for the SQL-sourced tables

No data file exists in the repo for the six OLTP tables, and no Databricks workspace is
available, so the project would not be buildable end-to-end without them. Each therefore also
has a small seed, extracted from `data_sources/sample.bak`:

| Seed | Stands in for | Rows |
| --- | --- | --- |
| `raw_sample_state` | `banking.raw.state` | 9 |
| `raw_sample_city` | `banking.raw.city` | 52 |
| `raw_sample_customer` | `banking.raw.customer` | 20 |
| `raw_sample_account` | `banking.raw.account` | 23 |
| `raw_sample_branch` | `banking.raw.branch` | 5 |
| `raw_sample_transaction` | `banking.raw.transaction` | 10 |

**These seeds are sample data for development and CI only.** They are a point-in-time extract
of the demo backup, not a production feed, and models must keep reading the `sample` source —
not these seeds — so that a real deployment picks up live data. They are marked as sample data
in their `_seeds.yml` descriptions as well.

Timestamps in these seeds are ISO (`yyyy-MM-dd HH:mm:ss`) because they come from typed
SQL Server columns, unlike the text timestamps of the two file extracts.

## 4. What an upstream ingestion tool must provide

This program builds no ingestion. To run against real data, an ingestion tool
(Lakeflow Connect / Fivetran / ADF / Debezium — not decided here) must:

1. Create the Unity Catalog schema `banking.raw` and land one Delta table per OLTP table,
   named and cased exactly as declared in `_sources.yml`
   (`state`, `city`, `customer`, `account`, `branch`, `transaction`), with the source column
   names preserved — all renaming happens in `staging/`.
2. Preserve types per the agreed mapping: `MONEY` → `DECIMAL(18,2)`, `DATETIME`/`DATETIME2` →
   `TIMESTAMP`, `DATE` → `DATE`, `VARCHAR(n)` → `STRING`, `INT` → `INT`. `customer.age` is
   `VARCHAR` in SQL Server; landing it as `STRING` is fine, staging casts it.
3. Add an ingestion audit column (e.g. `_loaded_at`) so `freshness:` can be enabled on the
   source, and land the file extracts on a volume/autoloader path so the two transaction
   seeds can be replaced by real sources.
4. Deliver full snapshots (the dimensions are small) or CDC for `transaction`; the marts
   assume the raw tables represent the current state of the OLTP system.
