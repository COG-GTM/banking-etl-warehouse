# Legacy ↔ dbt parity harness

Proves that the dbt + Databricks SQL rebuild returns exactly the same numbers as
the legacy Talend + T-SQL warehouse, without needing SQL Server, Talend or a
Databricks workspace. Everything runs locally on DuckDB against the real data
committed in this repository.

## Run it

```bash
pip install -r parity/requirements.txt
python3 parity/run_parity.py
```

Exit code `0` means every scenario matched. `--update` rewrites the committed
expected outputs (only do this when a logic change is intended and reviewed).

## What it compares

For each scenario the harness builds two worlds from identical inputs and diffs
the result sets value-by-value:

| Side | Built by |
| --- | --- |
| Legacy | `sql/01_legacy_dwh.sql` replays the four Talend jobs, then `sql/02_*` / `sql/03_*` run DuckDB translations of `sp_DailyTransaction` and `sp_BalancePerCustomer` |
| dbt | `sql/04_dbt_models.sql` builds the staging → intermediate → marts DAG, then `sql/05_*` / `sql/06_*` run `rpt_daily_transaction` and `rpt_balance_per_customer` |

Column names differ on purpose (legacy `PascalCase` → dbt `snake_case`), so rows
are compared positionally after normalising decimals and timestamps to strings.

Scenarios:

| Scenario | Parameters / dbt vars |
| --- | --- |
| `daily_transaction__all_time` | `start_date=2000-01-01`, `end_date=2099-12-31` |
| `daily_transaction__default_vars_2024` | the `dbt_project.yml` defaults `2024-01-01` … `2024-12-31` |
| `daily_transaction__readme_example` | `2024-01-18` … `2024-01-20`, the example in the root README |
| `balance_per_customer__unfiltered` | `customer_name=''` (BI layer filters instead) |
| `balance_per_customer__shelly` | `customer_name='Shelly'` |
| `balance_per_customer__lowercase_input` | `customer_name='shelly juwita'` — guards the case-insensitivity fix below |

`sql/07_dbt_tests.sql` additionally runs the DuckDB equivalents of the
`unique` / `not_null` / `relationships` tests that replace the legacy PK/FK
constraints, and the harness diffs their failure counts against
`expected/dbt_tests.csv`.

## Inputs

| Relation | Source |
| --- | --- |
| `src_transaction_csv` | `data_sources/transaction_csv.csv`, parsed with the legacy `dd-MM-yyyy HH:mm:ss` date mask |
| `src_transaction_excel` | `data_sources/transaction_excel.xlsx`, read with a stdlib-only `.xlsx` reader (no extra dependency) |
| `src_state`, `src_city`, `src_customer`, `src_account`, `src_branch`, `src_transaction_db` | `fixtures/source/*.csv` — the six tables of the `sample` OLTP database, extracted from `data_sources/sample.bak` |

The fixtures are committed so the harness runs anywhere. Regenerate them with
`bash parity/fixtures/extract_sample_bak.sh` (Docker required — it restores
`sample.bak` into a throwaway SQL Server 2022 container and dumps each table).

## Behaviours the harness pins down

* **Dedup precedence.** `tUniqRow` keeps the first row per `transaction_id` in
  `tUnite` input order, so the SQL Server row wins over the Excel and CSV rows.
  Transactions `6` and `7` exist in both SQL Server (2022 dates) and Excel (2024
  dates); the legacy warehouse keeps the 2022 rows, and
  `int_transactions_unioned` reproduces that with
  `row_number() over (partition by transaction_id order by source_priority)`.
* **Case-insensitive customer filter.** SQL Server's default collation is
  case-insensitive, so `LIKE '%shelly juwita%'` matches the uppercased
  `SHELLY JUWITA` produced by the `Load_DimCustomer` cleansing. Databricks
  comparisons are case-sensitive, so the dbt model upper-cases both sides.
* **Known source defect.** Three CSV transactions reference `account_id` 22 and
  23, which do not exist in `DimAccount`. SQL Server's foreign key would have
  rejected them; Databricks does not enforce keys, so they surface as three
  failing rows of
  `relationships_fct_transaction_account_id__dim_account`. That count is
  committed in `expected/dbt_tests.csv` so the harness fails if it changes —
  see the cutover runbook for how to handle it.
