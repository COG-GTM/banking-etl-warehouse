-- Data engineers: read-only on the curated layers.
GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA ${silver_schema} TO ${data_engineers};
GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA ${gold_schema} TO ${data_engineers};
