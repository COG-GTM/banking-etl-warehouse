with source as (

    select * from {{ source('sample', 'city') }}

),

renamed as (

    select
        cast(city_id as int)                        as city_id,
        upper(trim(cast(city_name as string)))      as city_name,
        cast(state_id as int)                       as state_id

    from source

)

select * from renamed
