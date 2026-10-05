# Ticket 7: Load_DimCustomer -> silver.customer -> gold.dim_customer

Source job: `talend_jobs/Load_DimCustomer.zip` (`process/Load_DimCustomer_0.1.item`).
Code: `src/banking_etl/dims/customer.py`. Notebooks: `notebooks/silver/silver_customer.py`,
`notebooks/gold/load_dim_customer.py`. Job: `dim_customer` in `resources/ticket-07-dim-customer.yml`.

```
bronze.sqlserver_customer ─┐ (row1, main)
bronze.sqlserver_city ─────┤ LEFT JOIN on city_id    (row2, UNIQUE_MATCH)
bronze.sqlserver_state ────┘ LEFT JOIN on state_id   (row3, UNIQUE_MATCH, keyed on row2.state_id)
        │  UPPER(customer_name, address, gender); age varchar(3) -> INT
        ├─> silver.customer          (overwrite, valid rows)
        └─> ops.dim_customer_rejects (overwrite, rows SQL Server would refuse)
silver.customer ── merge_scd1 on CustomerID ──> gold.dim_customer (CustomerKey identity, stable)
```

## Talend -> Databricks mapping

| Talend (`tMap_1` / `tDBOutput_1`) | Databricks |
|---|---|
| `row2` city lookup, `row3` state lookup: `UNIQUE_MATCH`, `LOAD_ONCE`, no `innerJoin` flag (= left outer), no reject outputs, no filters | `left` joins; unmatched city -> `city_name`, `state_id`, `state_name` NULL; unmatched state -> `state_name` NULL. The customer is always kept. |
| `UNIQUE_MATCH` keeps one lookup row per key (last loaded) | `unique_lookup`: one row per key with a deterministic tie-break (the source PKs make this a no-op on real data) |
| `StringHandling.UPCASE(row1.customer_name / address / gender)` (null-safe) | `F.upper` (null-safe) on the same three columns |
| `email`, `age`, `city_name`, `state_name` passed as-is; `TRIM_COLUMN=false` on every input | passed as-is, no trimming |
| `Age` = `row1.age` (String) inserted into `DimCustomer.Age INT` -> SQL Server implicit conversion | `parse_age`: trim, `''` -> 0, NULL -> NULL, else `^[+-]?[0-9]+$` within INT range -> INT, otherwise reject (regex instead of `try_cast` because `Column.try_cast` is missing from the serverless Spark Connect client; verified against SQL Server 2022: `''`/`'   '` -> 0, `' 25'` -> 25, `'+7'` -> 7, `'abc'`/`'2.5'` -> conversion error) |
| Output mapping `CustomerID, CustomerName, Address, Age, Gender, Email, CityName, StateName` | `GOLD_MAPPING` silver -> gold PascalCase |
| `TABLE_ACTION=CREATE_IF_NOT_EXISTS` | `ensure_dim_customer` runs ticket 4's `apply_star_schema` only if `gold.dim_customer` is missing |
| `DATA_ACTION=INSERT`, `DIE_ON_ERROR=false`: rows SQL Server refuses are skipped and logged | Same rows are kept out of silver/gold and written to `ops.dim_customer_rejects` with `reject_reason` |
| Re-run = INSERT again (all PK violations, nothing changes) | SCD-1 `merge_scd1` (README contract): changed attributes are updated, new ids inserted, no deletes; re-run is a no-op |

### Reject reasons (`ops.dim_customer_rejects.reject_reason`, `;`-separated)
| Reason | Legacy behaviour it reproduces |
|---|---|
| `null_customer_id` | `CustomerID INT PRIMARY KEY` refuses NULL |
| `duplicate_customer_id` | PK violation. Talend would keep the first row inserted (source order); Spark has no stable order, so **all** rows of a duplicated id are rejected (deviation; impossible on the real source, `customer_id` is the PK) |
| `age_not_int` | varchar -> INT conversion error |
| `age_out_of_range` | **Deviation**: SQL Server would load it, but gold has the enforced `ck_dim_customer_age` CHECK (0..150), which would fail the whole MERGE |
| `too_long_<column>` | `String or binary data would be truncated` for `DimCustomer` VARCHAR limits (CustomerName 100, Address 255 (source is varchar(max)), CityName 100, StateName 100, Gender 10, Email 100) |

## Tables
- `silver.customer`: `customer_id INT, customer_name STRING, address STRING, city_id INT, city_name STRING, state_id INT, state_name STRING, age INT, gender STRING, email STRING, _loaded_at TIMESTAMP`. Full overwrite.
- `ops.dim_customer_rejects`: the silver columns (all nullable) + `age_raw STRING, reject_reason STRING, rejected_at TIMESTAMP`. Full overwrite (latest run only; time travel keeps history).
- `gold.dim_customer`: ticket 4 DDL, unchanged.

## Parity evidence
`tests/dims/expected_dim_customer_sqlserver.csv` is `DWH.dbo.DimCustomer` produced on SQL Server 2022 from the
restored `data_sources/sample.bak`, with `sql_scripts/01_create_tables.sql` and this T-SQL equivalent of the tMap:

```sql
INSERT INTO DWH.dbo.DimCustomer (CustomerID, CustomerName, Address, CityName, StateName, Age, Gender, Email)
SELECT c.customer_id, UPPER(c.customer_name), UPPER(c.address), ci.city_name, st.state_name, c.age, UPPER(c.gender), c.email
FROM sample.dbo.customer c
LEFT JOIN sample.dbo.city ci ON ci.city_id = c.city_id
LEFT JOIN sample.dbo.state st ON st.state_id = ci.state_id;
```

`test_gold_parity_idempotent_and_scd1` asserts `gold.dim_customer` (built from the fixtures) equals it row for row (20 rows).
