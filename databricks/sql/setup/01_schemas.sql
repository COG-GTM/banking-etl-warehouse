CREATE SCHEMA IF NOT EXISTS ${bronze_schema}
  COMMENT 'Raw, source-shaped extracts (SQL Server snapshots, CSV/Excel Auto Loader).';
CREATE SCHEMA IF NOT EXISTS ${silver_schema}
  COMMENT 'Typed, cleansed, conformed entities.';
CREATE SCHEMA IF NOT EXISTS ${gold_schema}
  COMMENT 'Star schema (dims + fact) with legacy PascalCase column names.';
CREATE SCHEMA IF NOT EXISTS ${ops_schema}
  COMMENT 'Audit, data quality, reconciliation, rejects and streaming checkpoints.';
