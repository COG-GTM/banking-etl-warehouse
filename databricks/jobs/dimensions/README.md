# Dimension loaders (Talend → PySpark)

Replaces the Talend jobs `Load_DimBranch`, `Load_DimAccount` and
`Load_DimCustomer` with a single PySpark job that reads bronze source tables
and writes gold Delta dimension tables.

| File | Purpose |
|------|---------|
| `transforms.py` | Pure DataFrame transforms: `build_dim_branch`, `build_dim_account`, `build_dim_customer` + expected `StructType`s |
| `load_dimensions.py` | I/O wrapper: reads bronze, applies transforms, overwrites gold. Widgets or argparse. |
| `tests/dimensions/` | pytest suite using a local SparkSession |

## Component mapping

| Talend component | Job(s) | PySpark equivalent |
|------------------|--------|--------------------|
| `tDBInput` (`SELECT ... FROM dbo.branch/account/customer/city/state`) | all | `spark.read.table("<catalog>.<schema>.<table>")` in `read_source` |
| `tMap` column rename / pass-through | all | `df.select(F.col("x").alias("Y"))` |
| `tMap` `StringHandling.UPCASE(...)` on `customer_name`, `address`, `gender` | Load_DimCustomer | `F.upper(col)` (null in → null out, like Talend) |
| `tMap` lookup `row2.city_id = row1.city_id`, `row3.state_id = row2.state_id` (default left outer, unique match) | Load_DimCustomer | `customer.join(city, ..., "left").join(state, ..., "left")` after `dropDuplicates` on the lookup key |
| Implicit Java type coercion (`Integer` balance → `MONEY`, `String` age → `INT`) | Load_DimAccount, Load_DimCustomer | `.cast(DecimalType(19,4))`, `.cast(IntegerType())` in `_conform` |
| `tDBOutput` (`CREATE_IF_NOT_EXISTS` + `INSERT`) | all | `df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(...)` |
| `context.namaTabel` / `context.namaTabelTujuan` | Load_DimBranch, Load_DimAccount | `--gold-catalog/--gold-schema` args or widgets |

Note: the Talend jobs used `INSERT` (append) on a `CREATE_IF_NOT_EXISTS`
table; the PySpark job uses a full overwrite of each dimension, which is the
idempotent equivalent for a full reload.

## Running

Databricks job (widgets `bronze_catalog`, `bronze_schema`, `gold_catalog`,
`gold_schema` are created with defaults `main.bronze` / `main.gold`):

```
python -m databricks.jobs.dimensions.load_dimensions --bronze-schema bronze --gold-schema gold
```

Local run without Delta/Unity Catalog, reading `P/bronze/<table>` parquet and
writing `P/gold/<Table>`:

```
python -m databricks.jobs.dimensions.load_dimensions --format parquet --path /tmp/dwh
```

Tests:

```
pip install pyspark pytest
pytest tests/dimensions
```
