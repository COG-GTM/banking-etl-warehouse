# Banking ETL warehouse on Databricks

Databricks Asset Bundle for migrating the Talend + SQL Server star-schema warehouse
(`sql_scripts/`, `talend_jobs/`, `data_sources/`) to a Unity Catalog medallion
lakehouse.

## Layout

```
databricks/
├── databricks.yml            # bundle: name, dev/prod targets, variables, wheel artifact, include resources/*.yml
├── resources/                # one job/pipeline definition per workload (picked up by include)
│   └── uc_setup.yml          # job: create catalog, schemas, landing volume
├── src/banking_etl/          # Python package (built into a wheel by the bundle)
│   ├── config.py             # EtlConfig: catalog/schema/table/volume naming (never hard-code names)
│   ├── spark.py              # get_spark(): active Databricks session, else local Spark + Delta
│   └── uc_setup.py           # idempotent Unity Catalog DDL (wheel entry point `uc_setup`)
├── tests/                    # pytest; conftest.py provides a local Spark + Delta fixture
└── pyproject.toml            # package + build config, `dev` extra, pytest settings
```

Other workloads add their own module under `src/banking_etl/`, tests under `tests/`
and job/pipeline file under `resources/`; `databricks.yml` does not need to change.

## Unity Catalog

| Object | Default name | Bundle variable / `EtlConfig` field |
|---|---|---|
| Catalog | `banking_dwh` | `catalog` |
| Bronze schema (raw: `bronze.<sample.bak table>`, `bronze.transaction_csv`, `bronze.transaction_excel`) | `bronze` | `bronze_schema` |
| Silver schema (cleansed/conformed) | `silver` | `silver_schema` |
| Gold schema (`dim_account`, `dim_branch`, `dim_customer`, `fact_transaction`) | `gold` | `gold_schema` |
| Landing volume for raw files | `/Volumes/banking_dwh/bronze/landing` | `landing_volume` |

`schema_prefix` (default empty) prefixes every schema, so an isolated copy can run inside
a shared catalog, e.g. `--schema-prefix=ci_42_` → `ci_42_bronze`, `ci_42_silver`, ...

The schemas are created by the `uc_setup` job rather than as bundle `schemas:` resources
so that dev-mode name prefixing doesn't rename them and `bundle destroy` never drops
warehouse data.

```python
from banking_etl.config import EtlConfig

cfg = EtlConfig.from_args()          # --catalog/--schema-prefix/... > BANKING_ETL_* env > defaults
cfg.table("gold", "dim_account")     # banking_dwh.gold.dim_account
cfg.landing_file("transaction_csv.csv")  # /Volumes/banking_dwh/bronze/landing/transaction_csv.csv
```

Entry points can reuse the same flags with `EtlConfig.add_arguments(parser)` and
`EtlConfig.from_namespace(args)`.

## Deploy and run

```bash
cd databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev          # builds the wheel with `uv build --wheel`
databricks bundle run uc_setup -t dev    # once per environment, before ingestion
# Catalog already provisioned (no CREATE CATALOG grant):
databricks bundle deploy -t dev --var="create_catalog=false"
```

Preview the DDL without a workspace:

```bash
PYTHONPATH=src python -m banking_etl.uc_setup --catalog banking_dwh --print-sql
```

## Tests

Spark tests need Java 17 (set `JAVA_HOME`; `/usr/lib/jvm/java-17-openjdk-amd64` is picked
up automatically).

```bash
cd databricks
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Fixtures in `tests/conftest.py`: `spark` (session-scoped local Spark + Delta),
`etl_config` (catalog `spark_catalog`, per-test schema prefix), `medallion_schemas`
(creates and drops the bronze/silver/gold schemas for `etl_config`), `bundle_root`,
`repo_root`, `data_sources_dir`. If Maven Central is rate-limited, the Delta jars are
fetched from `BANKING_ETL_MAVEN_REPO` (defaults to Google's Maven Central mirror).

CI (`.github/workflows/databricks-ci.yml`) runs `pytest` on every change under
`databricks/` and `databricks bundle validate` for both targets when the
`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` secrets are set.
