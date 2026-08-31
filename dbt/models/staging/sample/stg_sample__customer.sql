with source as (

    select * from {{ source('sample', 'customer') }}

),

renamed as (

    select
        cast(customer_id as int)                                as customer_id,
        upper(trim(cast(customer_name as string)))              as customer_name,
        upper(trim(cast(address as string)))                    as address,
        cast(city_id as int)                                    as city_id,
        cast(trim(cast(age as string)) as int)                  as age,
        upper(trim(cast(gender as string)))                     as gender,
        trim(cast(email as string))                             as email

    from source

)

select * from renamed
