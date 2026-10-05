-- Jobs service principal: full control of every layer (tables and volumes inherit).
GRANT ALL PRIVILEGES ON SCHEMA ${bronze_schema} TO ${jobs_principal};
GRANT ALL PRIVILEGES ON SCHEMA ${silver_schema} TO ${jobs_principal};
GRANT ALL PRIVILEGES ON SCHEMA ${gold_schema} TO ${jobs_principal};
GRANT ALL PRIVILEGES ON SCHEMA ${ops_schema} TO ${jobs_principal};
