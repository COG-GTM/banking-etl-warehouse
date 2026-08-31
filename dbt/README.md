# banking_dwh — dbt on Databricks SQL

This dbt project re-platforms the legacy SQL Server + Talend banking data warehouse onto
Databricks SQL (Unity Catalog, Delta tables).

## Prerequisites

- Python 3.9+
- A Databricks SQL warehouse and a Unity Catalog catalog named `banking`

## Install

```bash
python3 -m venv ~/dbtvenv
~/dbtvenv/bin/pip install dbt-databricks
```

## Configure the connection

Copy the example profile into your dbt home and export the three environment variables it reads:

```bash
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml

export DATABRICKS_HOST='adb-1234567890.1.azuredatabricks.net'   # workspace host, no scheme
export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/<warehouse-id>'
export DATABRICKS_TOKEN='<personal-access-token>'
```

`profiles.yml` is git-ignored, so a local copy inside `dbt/` will never be committed.

## Run

```bash
cd dbt
dbt deps      # install packages from packages.yml
dbt parse     # compile-only check, no warehouse connection required
dbt build     # run models + tests + seeds against the target
```

Useful variable overrides (defaults live in `dbt_project.yml`):

```bash
dbt build --vars '{"start_date": "2024-01-01", "end_date": "2024-03-31", "customer_name": "ACME"}'
```

## Layer conventions

| Layer | Path | Materialization | Schema | Purpose |
| --- | --- | --- | --- | --- |
| staging | `models/staging/` | view | `staging` | 1:1 with a source: rename, cast, cleanse. No joins, no business logic. |
| intermediate | `models/intermediate/` | view | `intermediate` | Unions, dedup, joins — the Talend `tUnite`/`tUniqRow`/`tMap` logic. |
| marts | `models/marts/` | table | `marts` | Conformed `dim_*` / `fct_*` star schema. |
| reporting | `models/marts/reporting/` | table | `marts` | The ex-stored-procedure reports (`rpt_*`). |

Sources live in `models/staging/_sources.yml` (catalog `banking`, schema `raw`); small
reference/transaction extracts land as dbt seeds under `seeds/` (schema `raw`).

## Naming conventions

- Models are `snake_case`; legacy PascalCase columns map to snake_case (`CustomerID` → `customer_id`).
- Staging: `stg_<source>__<entity>` (e.g. `stg_sample__customer`).
- Intermediate: `int_<subject>_<verb>` (e.g. `int_transactions_unioned`).
- Marts: `dim_<entity>` / `fct_<event>` (e.g. `dim_customer`, `fct_transaction`).
- Reporting: `rpt_<report>` (e.g. `rpt_daily_transaction`, `rpt_balance_per_customer`).
- Tests and descriptions are declared in a `schema.yml` alongside the models they cover.

## Legacy → dbt mapping

### Types (T-SQL → Databricks)

| T-SQL | Databricks |
| --- | --- |
| `MONEY` | `DECIMAL(18,2)` |
| `DATETIME` | `TIMESTAMP` |
| `DATE` | `DATE` |
| `VARCHAR(n)` | `STRING` |
| `INT` | `INT` |

### Constraints

Databricks does not enforce primary or foreign keys, so constraints become dbt tests:

| Legacy constraint | dbt equivalent |
| --- | --- |
| `PRIMARY KEY` | `unique` + `not_null` tests on the column |
| `FOREIGN KEY` | `relationships` test (`to: ref('<parent>')`, `field: <parent_key>`) |
| `NOT NULL` | `not_null` test |
| Enumerated status/type columns | `accepted_values` test |

### Artifacts

| Legacy artifact | dbt equivalent |
| --- | --- |
| `sql_scripts/01_create_tables.sql` (star schema DDL) | `models/marts/` `dim_*` / `fct_*` models + `schema.yml` tests |
| `sp_DailyTransaction(@start_date, @end_date)` | `rpt_daily_transaction` model, parameters → vars `start_date` / `end_date` |
| `sp_BalancePerCustomer(@customer_name)` | `rpt_balance_per_customer` model, parameter → var `customer_name` |
| Talend `tMap` (join + uppercase cleansing) | staging/intermediate SQL (`upper()`, joins) |
| Talend `tUnite` (three transaction sources) | `int_transactions_unioned` (`union all`) |
| Talend `tUniqRow` on `transaction_id` | dedup in intermediate + `unique` test on `transaction_id` |
| SQL Server source tables | Unity Catalog sources in `models/staging/_sources.yml` |
| CSV / Excel extracts | dbt seeds under `seeds/` |

Stored-procedure parameters become dbt vars with defaults, and the reporting models are
also usable unfiltered (an empty `customer_name` matches all customers) so a BI layer can
apply its own filters.
