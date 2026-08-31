select
    cast(city_id as int)          as city_id,
    cast(city_name as string)     as city_name,
    cast(state_id as int)         as state_id
from {{ source('banking_raw', 'city') }}
