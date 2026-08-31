select
    cast(customer_id as int)      as customer_id,
    cast(customer_name as string) as customer_name,
    cast(address as string)       as address,
    cast(city_id as int)          as city_id,
    cast(age as int)              as age,
    cast(gender as string)        as gender,
    cast(email as string)         as email
from {{ source('banking_raw', 'customer') }}
