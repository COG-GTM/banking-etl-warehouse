{{ config(materialized='table') }}

-- Replaces the Talend job Load_DimCustomer (tMap join of customer x city x state)
-- and the T-SQL table DimCustomer.
-- LEFT JOINs are used deliberately: Talend's tMap inner join dropped customers
-- whose city/state lookup failed. Keeping them preserves dimension completeness so
-- fct_transaction never loses a customer to a missing geography reference.

with customer as (

    select * from {{ ref('stg_sample__customer') }}

),

city as (

    select * from {{ ref('stg_sample__city') }}

),

state as (

    select * from {{ ref('stg_sample__state') }}

),

joined as (

    select
        c.customer_id,
        upper(trim(c.customer_name))    as customer_name,
        upper(trim(c.address))          as address,
        upper(trim(ci.city_name))       as city_name,
        upper(trim(s.state_name))       as state_name,
        c.age,
        upper(trim(c.gender))           as gender,
        lower(trim(c.email))            as email

    from customer as c
    left join city  as ci on c.city_id = ci.city_id
    left join state as s  on ci.state_id = s.state_id

)

select
    cast(customer_id as int)        as customer_id,
    cast(customer_name as string)   as customer_name,
    cast(address as string)         as address,
    cast(city_name as string)       as city_name,
    cast(state_name as string)      as state_name,
    cast(age as int)                as age,
    cast(gender as string)          as gender,
    cast(email as string)           as email
from joined
