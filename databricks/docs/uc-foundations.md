# Ticket 1 – Unity Catalog foundations

## What gets provisioned
| Step | Template | Notes |
|---|---|---|
| catalog (optional) | `sql/setup/00_catalog.sql` | Only with `create_catalog=true`; failure (no metastore admin) is logged and skipped. |
| schemas | `sql/setup/01_schemas.sql` | `bronze`, `silver`, `gold`, `ops`, each prefixed with `schema_prefix`. |
| volumes | `sql/setup/02_volumes.sql` | `bronze.landing`, `ops.checkpoints` (managed). Landing folders `transactions/csv`, `transactions/excel`, `sample_db` are created by the notebook. |
| grants_catalog (optional) | `sql/setup/10_grants_catalog.sql` | `USE CATALOG` for data engineers; only together with `create_catalog`. |
| grants_data_engineers | `sql/setup/11_grants_data_engineers.sql` | `USE SCHEMA, SELECT, EXECUTE` on silver + gold. Skipped if no group given. |
| grants_jobs_principal | `sql/setup/12_grants_jobs_principal.sql` | `ALL PRIVILEGES` on all four schemas. Skipped if no principal given. |

Templates use `${catalog}`, `${bronze_schema}`, … placeholders, rendered by
`banking_etl.setup.provision.plan()` with backtick-quoted identifiers built from `Settings`.
Every statement is `IF NOT EXISTS` or `GRANT`, so runs are idempotent.

## How to run
- **Bundle job** `banking_etl_setup` (`resources/setup.yml`, serverless):
  `databricks bundle run banking_etl_setup -t dev --var catalog=migration_demo --var schema_prefix=banking_etl_ --var data_engineers_group="account users" --var jobs_principal=<sp-app-id>`
- **From a laptop/CI through a SQL warehouse**:
  `python scripts/setup/provision_uc.py --catalog migration_demo --schema-prefix banking_etl_ --warehouse-id 565cd2fd713738c4 [--data-engineers G] [--jobs-principal P] [--create-catalog] [--dry-run]`

## Secret scope
`scripts/setup/create_secret_scope.py [--scope banking-etl-sqlserver] [--reader <principal>]` creates the
scope if missing and (re)puts `jdbc-host`, `jdbc-port`, `jdbc-database`, `jdbc-user`, `jdbc-password` from
`BANKING_ETL_JDBC_HOST`, `BANKING_ETL_JDBC_PORT`, `BANKING_ETL_JDBC_DATABASE`, `BANKING_ETL_JDBC_USER`,
`BANKING_ETL_JDBC_PASSWORD`. Values are piped through stdin (never argv, never committed). Read them in
code with `dbutils.secrets.get(settings.secret_scope, "jdbc-host")`.

## Cluster policy
`resources/policies/banking_etl_job_cluster.json` (`banking-etl-job-cluster`): `cluster_type=job`, DBR
17.3 LTS (standard or Photon), `SINGLE_USER`/dedicated access mode, nodes `i3.xlarge`, `i3.2xlarge`,
`m5d.large`, `m5d.xlarge`, `r5d.large`, up to 4 fixed / 8 autoscale workers, autotermination 10–60 min
(job clusters terminate at run end anyway), spot-with-fallback, fixed tags `project`, `managed_by`, plus
`environment` (dev|prod) and `cost_center`. A workspace admin applies it with
`scripts/setup/apply_cluster_policies.py [--group G] [--service-principal APP_ID]` (create-or-edit by name,
optional CAN_USE grants). It is deliberately not a bundle resource because the jobs SP cannot create
policies, and the dev workspace is serverless-only (see README “Workspace facts”).
