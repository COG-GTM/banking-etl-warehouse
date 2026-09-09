# Databricks conversion — `sp_BalancePerCustomer`

Converted workload from `sql_scripts/02_create_procedures.sql` lines 50-90. The T-SQL and Talend
sources are unchanged; this directory is additive. Rationale, anti-pattern analysis and the wave
plan live in `docs/databricks-migration-assessment.md`.

```
src/banking_dwh/balance_per_customer.py   PySpark/Delta conversion + Workflow entrypoint
src/banking_dwh/tsql_reference.py         SQLite transliteration of the T-SQL, used as oracle
tests/                                    pytest equivalence suite + CSV fixtures
databricks.yml, resources/                Databricks Asset Bundle and job definition
```

## Why the procedure is split in two

The procedure aggregates the whole fact table (lines 58-72) to answer a question about one
customer (lines 86-89). `account_balance()` keeps the aggregation and runs on a schedule into the
Delta table `gold.account_balance`; the `@customer_name` predicate becomes `serve_from_gold()`, a
filter over that much smaller table. `balance_per_customer()` composes both and is what the
equivalence tests assert against, so the split is proved not to change the result set.

## Run the tests

```bash
cd databricks
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

Expected: `16 passed`. The suite runs local Spark (`local[2]`) against CSV fixtures and compares
row-for-row with the SQLite reference. Read the module docstring in `src/banking_dwh/tsql_reference.py`
before drawing conclusions from a green run — it states exactly which SQL Server behaviours the
reference can and cannot stand in for.

Delta is not required to run the tests: they exercise the DataFrame transformation directly. The
Delta write path (`refresh_gold_table()`) needs a Databricks runtime or `delta-spark` and has not
been executed in this repository.

## Deploy

```bash
databricks bundle deploy -t dev
databricks bundle run refresh_account_balance -t dev
```
