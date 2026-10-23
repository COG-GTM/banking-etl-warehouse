# Ticket 7 fixture: `sample.bak` customer / city / state

Extracted from `data_sources/sample.bak` restored in SQL Server 2022 (Docker):
`SELECT * FROM dbo.<table> ORDER BY 1` for `customer` (20 rows), `city` (52) and `state` (9).
Columns match the Bronze contracts `BRONZE_SQLSERVER_{CUSTOMER,CITY,STATE}` (minus ingestion metadata).

`expected_dim_customer.csv` is the legacy result, produced on the same SQL Server by creating
`DimCustomer` from `sql_scripts/01_create_tables.sql` and running the Talend-equivalent insert:

```sql
INSERT INTO DimCustomer (CustomerID, CustomerName, Address, Age, Gender, Email, CityName, StateName)
SELECT c.customer_id, UPPER(c.customer_name), UPPER(c.address), c.age, UPPER(c.gender), c.email,
       ci.city_name, s.state_name
FROM customer c
LEFT JOIN city ci ON ci.city_id = c.city_id
LEFT JOIN state s ON s.state_id = ci.state_id;
```

Used by `tests/test_t7_dim_customer.py` (parity) and by `notebooks/silver/03_load_dim_customer.py`
with `seed_bronze_fixture=true` to seed a ticket scratch Bronze schema (`bdwh_t7_bronze`) for live runs.
