with source as (

    select * from {{ source('sample', 'state') }}

),

renamed as (

    select
        cast(state_id as int)                       as state_id,
        upper(trim(cast(state_name as string)))     as state_name

    from source

)

select * from renamed
