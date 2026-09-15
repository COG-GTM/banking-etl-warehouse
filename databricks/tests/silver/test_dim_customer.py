from __future__ import annotations

from conftest import bronze_df

from databricks.silver.dim_customer import transform_dim_customer

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "address",
    "city_id",
    "age",
    "gender",
    "email",
]
CITY_COLUMNS = ["city_id", "city_name", "state_id"]
STATE_COLUMNS = ["state_id", "state_name"]


def customers(spark, rows):
    return bronze_df(spark, rows, CUSTOMER_COLUMNS)


def cities(spark, rows):
    return bronze_df(spark, rows, CITY_COLUMNS)


def states(spark, rows):
    return bronze_df(spark, rows, STATE_COLUMNS)


def test_join_applies_talend_upcase_rules_and_types(spark):
    result = transform_dim_customer(
        customers(
            spark,
            [
                {
                    "customer_id": "1",
                    "customer_name": "budi santoso",
                    "address": "jl. sudirman 10",
                    "city_id": "5",
                    "age": "34",
                    "gender": "male",
                    "email": "Budi@Example.com",
                }
            ],
        ),
        cities(spark, [{"city_id": "5", "city_name": "bandung", "state_id": "2"}]),
        states(spark, [{"state_id": "2", "state_name": "jawa barat"}]),
    )

    assert result.columns == [
        "customer_id",
        "customer_name",
        "address",
        "city_name",
        "state_name",
        "age",
        "gender",
        "email",
    ]
    assert dict(result.dtypes)["age"] == "int"
    row = result.collect()[0]
    assert (row.customer_name, row.address, row.gender) == (
        "BUDI SANTOSO",
        "JL. SUDIRMAN 10",
        "MALE",
    )
    # UPCASE is applied to name/address/gender only, per the tMap expressions.
    assert row.email == "Budi@Example.com"
    assert (row.city_name, row.state_name) == ("bandung", "jawa barat")
    assert row.age == 34


def test_missing_lookups_keep_the_customer_row(spark):
    """The tMap lookups have no innerJoin flag, so they are left outer joins."""
    result = transform_dim_customer(
        customers(
            spark,
            [
                {
                    "customer_id": "2",
                    "customer_name": "siti",
                    "address": "jl. mawar",
                    "city_id": "99",
                    "age": "28",
                    "gender": "female",
                    "email": "siti@example.com",
                },
                {
                    "customer_id": "3",
                    "customer_name": "agus",
                    "address": None,
                    "city_id": None,
                    "age": None,
                    "gender": None,
                    "email": None,
                },
                {
                    "customer_id": "4",
                    "customer_name": "dewi",
                    "address": "jl. melati",
                    "city_id": "7",
                    "age": "41",
                    "gender": "female",
                    "email": "dewi@example.com",
                },
            ],
        ),
        # city 7 exists but points at a state that does not.
        cities(
            spark,
            [
                {"city_id": "5", "city_name": "bandung", "state_id": "2"},
                {"city_id": "7", "city_name": "medan", "state_id": "404"},
            ],
        ),
        states(spark, [{"state_id": "2", "state_name": "jawa barat"}]),
    )

    rows = {r.customer_id: r for r in result.collect()}
    assert set(rows) == {2, 3, 4}
    assert (rows[2].city_name, rows[2].state_name) == (None, None)
    assert (rows[3].city_name, rows[3].state_name) == (None, None)
    assert rows[3].age is None
    assert (rows[4].city_name, rows[4].state_name) == ("medan", None)


def test_unique_match_prevents_lookup_fan_out(spark):
    duplicated_cities = cities(
        spark,
        [
            {"city_id": "5", "city_name": "bandung", "state_id": "2"},
            {"city_id": "5", "city_name": "BANDUNG DUPLICATE", "state_id": "2"},
        ],
    )
    customer = customers(
        spark,
        [
            {
                "customer_id": "1",
                "customer_name": "budi",
                "address": "jl. sudirman",
                "city_id": "5",
                "age": "34",
                "gender": "male",
                "email": "budi@example.com",
            }
        ],
    )
    state = states(spark, [{"state_id": "2", "state_name": "jawa barat"}])

    assert transform_dim_customer(customer, duplicated_cities, state).count() == 1
    # Without UNIQUE_MATCH the duplicate lookup row fans the customer grain out.
    assert transform_dim_customer(customer, duplicated_cities, state, unique_match=False).count() == 2
