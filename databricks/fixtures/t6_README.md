# Ticket 6 fixtures

* `t6_sqlserver_branch.csv` (5 rows), `t6_sqlserver_account.csv` (21 rows): `dbo.branch` / `dbo.account` from
  `data_sources/sample.bak`, restored on SQL Server 2022 (Docker) and exported unchanged (`date_opened` as
  `yyyy-MM-dd HH:mm:ss`). Tests and `t6_seed_bronze` turn them into rows of the T3 Bronze contracts.
* `t6_legacy_dim_branch.csv`, `t6_legacy_dim_account.csv`: `DimBranch` / `DimAccount` after running
  `sql_scripts/01_create_tables.sql` and the Talend-equivalent `INSERT INTO Dim… SELECT … FROM dbo.…` on the same
  restore. Parity oracle for the initial Silver load (`Balance` MONEY, `DateOpened` DATE).
* `t6_seed_bronze.py`: Databricks notebook that seeds a ticket-6 scratch Bronze (`schema_prefix` must end in `t6_`)
  for live validation; `mode=reset|append_change`.
