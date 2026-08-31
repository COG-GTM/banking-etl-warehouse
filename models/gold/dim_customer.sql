select
    customer_id   as CustomerID,
    customer_name as CustomerName,
    address       as Address,
    city_name     as CityName,
    state_name    as StateName,
    age           as Age,
    gender        as Gender,
    email         as Email
from {{ ref('slv_customer') }}
