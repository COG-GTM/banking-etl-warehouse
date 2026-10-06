"""Load_DimCustomer: customer LEFT JOIN city LEFT JOIN state -> dwh.dim_customer.

Mirrors the Talend tMap: city and state are lookups (left joins, unique match) and text
fields are upper-cased (StringHandling.UPCASE). The columns to upper-case are set by the
``upper_columns`` parameter.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import functions as F  # noqa: E402

from common import get_param, get_spark, qualified, read_jdbc_table, write_delta  # noqa: E402

DEFAULT_UPPER_COLUMNS = "CustomerName,Address,CityName,StateName,Gender"


def main() -> None:
    spark = get_spark()
    upper_columns = {
        c.strip() for c in get_param("upper_columns", DEFAULT_UPPER_COLUMNS, spark).split(",") if c.strip()
    }

    customer = read_jdbc_table("customer", spark).alias("cu")
    city = read_jdbc_table("city", spark).dropDuplicates(["city_id"]).alias("ci")
    state = read_jdbc_table("state", spark).dropDuplicates(["state_id"]).alias("st")

    joined = customer.join(city, F.col("cu.city_id") == F.col("ci.city_id"), "left").join(
        state, F.col("ci.state_id") == F.col("st.state_id"), "left"
    )

    columns = {
        "CustomerID": F.col("cu.customer_id"),
        "CustomerName": F.col("cu.customer_name"),
        "Address": F.col("cu.address"),
        "CityName": F.col("ci.city_name"),
        "StateName": F.col("st.state_name"),
        "Age": F.col("cu.age"),
        "Gender": F.col("cu.gender"),
        "Email": F.col("cu.email"),
    }
    df = joined.select([(F.upper(c) if name in upper_columns else c).alias(name) for name, c in columns.items()])
    write_delta(df, qualified("dim_customer", spark), key="CustomerID", spark=spark)


if __name__ == "__main__":
    main()
