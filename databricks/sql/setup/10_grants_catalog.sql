-- Catalog-level grants: only applied together with 00_catalog.sql (needs ownership/MANAGE on the catalog).
GRANT USE CATALOG ON CATALOG ${catalog} TO ${data_engineers};
