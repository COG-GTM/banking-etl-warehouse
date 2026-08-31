select
    c.customer_id,
    upper(c.customer_name) as customer_name,
    upper(c.address)       as address,
    upper(ci.city_name)    as city_name,
    upper(s.state_name)    as state_name,
    c.age,
    c.gender,
    c.email
from {{ ref('bronze_customer') }} c
left join {{ ref('bronze_city') }} ci
    on c.city_id = ci.city_id
left join {{ ref('bronze_state') }} s
    on ci.state_id = s.state_id
