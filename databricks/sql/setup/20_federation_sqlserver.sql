-- GUARDED / OPTIONAL (ticket 2): Lakehouse Federation for SQL Server, the prod alternative to Spark JDBC.
-- NOT executed by banking_etl.setup.provision or the setup job. Render + run explicitly:
--   python scripts/bronze/create_federation.py --host <sqlserver-host> [--execute --warehouse-id <id>]
-- Needs CREATE CONNECTION and CREATE FOREIGN CATALOG on the metastore (admin), and network access
-- from serverless/SQL warehouses to SQL Server. Credentials come from the JDBC secret scope.
CREATE CONNECTION IF NOT EXISTS ${connection} TYPE sqlserver
OPTIONS (
  host ${host},
  port ${port},
  user secret(${secret_scope}, 'jdbc-user'),
  password secret(${secret_scope}, 'jdbc-password'),
  trustServerCertificate 'true'
)
COMMENT 'SQL Server source (Talend tMSSqlInput) for banking-etl-warehouse';

CREATE FOREIGN CATALOG IF NOT EXISTS ${foreign_catalog} USING CONNECTION ${connection}
COMMENT 'Read-only federated view of the SQL Server sample database (dbo.customer, city, state, account, branch, transaction_db)'
OPTIONS (database ${database});
