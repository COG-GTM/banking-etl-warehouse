# Slice 3 — Silver dimension transforms (Talend → PySpark)

Source of truth for this port is the exported Talend XML, not the README:

- `talend_jobs/Load_DimBranch.zip` → `IDX_INTERNSHIP/process/Load_DimBranch_0.1.item`
- `talend_jobs/Load_DimAccount.zip` → `IDX_INTERNSHIP/process/Load_DimAccount_0.1.item`
- `talend_jobs/Load_DimCustomer.zip` → `IDX_INTERNSHIP/process/Load_DimCustomer_0.1.item`

All three jobs point at `Sample_DB_Connection` (`localhost:1433`, db `sample`, schema `dbo`)
for input and `DWH_DB_Connection` (db `DWH`) for output.

## Asset mapping

| Legacy asset | Databricks asset |
| --- | --- |
| `Load_DimBranch` job | `databricks/silver/dim_branch.py::transform_dim_branch` |
| `Load_DimAccount` job | `databricks/silver/dim_account.py::transform_dim_account` |
| `Load_DimCustomer` job | `databricks/silver/dim_customer.py::transform_dim_customer` |
| `tMSSqlInput` (JDBC `SELECT ... FROM dbo.<t>`) | `spark.table("banking.bronze.sample_<t>")` (bronze owned by slice 2) |
| `tMSSqlOutput` (`CREATE_IF_NOT_EXISTS` + `INSERT`) | `databricks/silver/publish.py::merge_dimension` (Delta `MERGE`) / `full_refresh_dimension` |
| Job context var `namaTabel` (`"DimBranch"`, …) | `DimensionConfig.silver_table` / `.gold_table` |
| Talend run configuration | `databricks/silver/run_silver_dimensions.py` (widgets or CLI args) |

Transforms are pure `(DataFrame, ...) -> DataFrame`; the only Databricks-specific code
(`dbutils` widgets, Delta writes, `spark.table`) sits in `run_silver_dimensions.py` and
`publish.py`.

## Load_DimBranch

`tMSSqlInput "branch"` → `tMap_1` (output table `to_DimBranch`) → `tMSSqlOutput DimBranch`.

| tMap expression | PySpark |
| --- | --- |
| `BranchID = row1.branch_id` (`id_Integer`) | `to_int(col("branch_id")).alias("branch_id")` |
| `BranchName = row1.branch_name` (`id_String`) | `clean_string(col("branch_name"))` |
| `BranchLocation = row1.branch_location` (`id_String`) | `clean_string(col("branch_location"))` |

No filter, no variables, no lookups.

## Load_DimAccount

`tMSSqlInput "account"` → `tMap_1` (`to_DimAccount`) → `tMSSqlOutput DimAccount`.

| tMap expression | PySpark |
| --- | --- |
| `AccountID = row1.account_id` (`id_Integer`) | `to_int(col("account_id"))` |
| `CustomerID = row1.customer_id` (`id_Integer`) | `to_int(col("customer_id"))` |
| `AccountType = row1.account_type` (`id_String`) | `clean_string(col("account_type"))` |
| `Balance = row1.balance` (`id_Integer` in tMap, `MONEY` in DDL) | `to_decimal(col("balance"))` → `DECIMAL(19,4)` |
| `DateOpened = row1.date_opened` (`id_Date`) | `to_date(col("date_opened"))` → `DATE` |
| `Status = row1.status` (`id_String`) | `clean_string(col("status"))` |

## Load_DimCustomer

`tMSSqlInput "customer"` (main, `row1`), `tMSSqlInput "city"` (`row2`), `tMSSqlInput "state"`
(`row3`) → `tMap_1` (`to_DimCustomer`) → `tMSSqlOutput DimCustomer`.

Join, read off the `inputTables` entries:

| tMap lookup | Join model in XML | PySpark |
| --- | --- | --- |
| `row2.city_id = row1.city_id` | no `innerJoin` attribute ⇒ Talend default **left outer join**; `matchingMode="UNIQUE_MATCH"`, `lookupMode="LOAD_ONCE"` | `customer.join(city, "city_id", "left")` after `deduplicate_on_key(city, "city_id")` |
| `row3.state_id = row2.state_id` | same (left outer, unique match) | `.join(state, "state_id", "left")` after `deduplicate_on_key(state, "state_id")` |

So a customer whose `city_id` is missing (or whose city points at a missing state) is kept,
with `city_name`/`state_name` NULL. `UNIQUE_MATCH` means at most one lookup row per key, so a
duplicated city row cannot fan out the customer grain; `deduplicate_on_key` reproduces that
(pass `unique_match=False` to see the fan-out instead).

| tMap output expression | PySpark |
| --- | --- |
| `CustomerID = row1.customer_id` | `to_int(col("customer_id"))` |
| `CustomerName = StringHandling.UPCASE(row1.customer_name)` | `upcase(col("customer_name"))` |
| `Address = StringHandling.UPCASE(row1.address)` | `upcase(col("address"))` |
| `Age = row1.age` (`id_String` in tMap, `INT` in DDL) | `to_int(col("age"))` |
| `Gender = StringHandling.UPCASE(row1.gender)` | `upcase(col("gender"))` |
| `Email = row1.email` (no UPCASE) | `clean_string(col("email"))` |
| `CityName = row2.city_name` (no UPCASE) | `clean_string(col("city_name"))` |
| `StateName = row3.state_name` (no UPCASE) | `clean_string(col("state_name"))` |

`StringHandling.UPCASE` is null-safe in Talend and returns null for null input, which matches
`F.upper` on a null column.

## Output contract

Silver tables carry exactly the gold column set (snake_case), typed to the agreed contract:

- `silver.dim_branch(branch_id INT, branch_name STRING, branch_location STRING)`
- `silver.dim_account(account_id INT, customer_id INT, account_type STRING, balance DECIMAL(19,4), date_opened DATE, status STRING)`
- `silver.dim_customer(customer_id INT, customer_name STRING, address STRING, city_name STRING, state_name STRING, age INT, gender STRING, email STRING)`

Bronze bookkeeping columns (`_ingest_ts`, `_source_file`, `_batch_id`) are dropped at the silver
boundary. Table names are never hard-coded: `DimensionConfig` carries catalog/schema/table and
`run_silver_dimensions.py` overrides them from widgets or CLI flags.

## What does not map 1:1

1. **Truncate/insert → MERGE.** `tMSSqlOutput` runs with `TABLE_ACTION=CREATE_IF_NOT_EXISTS` and
   `DATA_ACTION=INSERT`: a blind append that collides with the `PRIMARY KEY` on rerun, so in
   practice the target had to be emptied first. The default here is an idempotent `MERGE` on the
   business key (`branch_id` / `account_id` / `customer_id`) with `UPDATE SET *` / `INSERT *`;
   rerunning the same batch leaves row counts unchanged. `full_refresh_dimension` keeps the legacy
   truncate/insert behaviour available (`--write-mode full_refresh`), and `merge_dimension` refuses
   a source with duplicate or NULL business keys rather than performing a non-deterministic merge.
2. **`Balance` widened.** The tMap types `Balance` as `id_Integer` even though both `dbo.account.balance`
   and `DimAccount.Balance` are `MONEY`, so the Talend job truncated the fractional part.
   We follow the agreed gold contract (`DECIMAL(19,4)`) and keep the cents. This is an intentional
   behaviour change.
3. **`Age` typed.** The tMap types `Age` as `id_String` while `DimCustomer.Age` is `INT`; we cast to
   `INT` in silver, matching the DDL and the gold contract.
4. **Casting is null-tolerant.** Bronze columns are raw strings. `to_int`/`to_decimal`/`to_date` use
   `try_cast`/`try_to_timestamp`, so an unparseable value lands as NULL instead of failing the job
   (Talend would have thrown on the JDBC read, but the values were already typed there). Empty and
   whitespace-only strings become NULL, and numeric/date strings are trimmed before casting;
   `$`/`,` are stripped from money strings. Talend applied no trim (`TRIM_ALL_COLUMN=false` on all
   inputs), so no case or content change is applied to string payloads beyond the UPCASE rules above.
5. **Lookup determinism.** Talend's `UNIQUE_MATCH` keeps the last lookup row loaded for a key. The
   Spark equivalent orders by `_ingest_ts` when present, otherwise by input order, and keeps the
   last row. Ordering is only observable when bronze holds genuinely duplicated lookup keys.
6. **No surrogate keys.** Both the legacy DWH and the target model use the source business keys as
   primary keys; nothing here generates surrogate keys, and there is no SCD history.

## Local validation

No Databricks workspace or SQL Server instance exists in the session, so validation is local
PySpark against in-memory fixtures that mirror the bronze shapes (all business columns `STRING`
plus the three ingest metadata columns).

```bash
pip install pyspark==4.2.0 pytest
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 python3 -m pytest databricks/tests/silver -q
```

Coverage: type casting to the gold contract, cleansing/UPCASE rules, the customer join fan-out
(with and without `UNIQUE_MATCH`), missing city/state lookups, null and unparseable values, and
MERGE idempotency (merge twice, assert stable row counts and stable values).

`delta-spark` could not be installed against the local Spark build, so the MERGE tests assert
against `upsert_dataframes`, a pure-DataFrame implementation of the same semantics
(anti-join + union = update-on-match, insert-otherwise). `merge_dimension` issues the real Delta
`MERGE INTO ... WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` and is the code
path that runs on a cluster; it is the one piece of this slice that is unverified off-cluster.
