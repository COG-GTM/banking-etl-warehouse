select
    cast(state_id as int)          as state_id,
    cast(state_name as string)     as state_name
from {{ source('banking_raw', 'state') }}
