# Databricks Migration (Talend → Lakehouse)

Target of the migration of the Talend/SQL Server banking data warehouse onto
Databricks with Delta Lake and a medallion architecture.

## Unity Catalog naming

All objects live in the Unity Catalog catalog **`dwh`**. Each medallion layer is
a schema inside that catalog:

| Layer    | Schema       | Contents                                                                 |
| -------- | ------------ | ------------------------------------------------------------------------ |
| Bronze   | `dwh.bronze` | Raw, as-is landing of source systems (SQL Server `sample` DB, CSV, Excel) |
| Silver   | `dwh.silver` | Cleansed, conformed, deduplicated entities                                |
| Gold     | `dwh.gold`   | Star schema: `DimAccount`, `DimBranch`, `DimCustomer`, `FactTransaction`  |

Fully qualified names are therefore `dwh.<layer>.<table>`, e.g.
`dwh.bronze.account`, `dwh.gold.FactTransaction`.

## Directory layout

```
databricks/
├── bronze/      # Ingestion jobs landing raw sources into dwh.bronze
├── silver/      # Cleansing / conforming jobs producing dwh.silver
├── gold/        # Star-schema dimension and fact loads into dwh.gold
├── analytics/   # Replacements for the T-SQL stored procedures (reporting queries)
├── workflows/   # Databricks Workflows job definitions / orchestration
└── ddl/         # Delta table DDL for the warehouse layers
```

Only `bronze/` and `ddl/` are populated by the foundational tickets
(TICKET-2/3/4); silver, gold, analytics and workflows are delivered by
subsequent tickets.

## Mapping from the Talend solution

| Talend job             | Databricks replacement                                           |
| ---------------------- | ---------------------------------------------------------------- |
| `Load_DimBranch`       | bronze ingest of `branch` → silver → `dwh.gold.DimBranch`         |
| `Load_DimAccount`      | bronze ingest of `account` → silver → `dwh.gold.DimAccount`       |
| `Load_DimCustomer`     | bronze ingest of `customer`/`city`/`state` → joined `DimCustomer` |
| `Load_FactTransaction` | union of SQL Server / CSV / Excel transactions, deduplicated      |

Referential integrity that SQL Server enforced with PRIMARY KEY / FOREIGN KEY
constraints is not enforced by Delta; it is replaced by deduplication (MERGE on
the business key) and data-quality checks in the silver/gold loads. See
`ddl/01_create_delta_tables.sql`.
