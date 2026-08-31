with source as (

    select * from {{ source('sample', 'branch') }}

),

renamed as (

    select
        cast(branch_id as int)              as branch_id,
        cast(branch_name as string)         as branch_name,
        cast(branch_location as string)     as branch_location

    from source

)

select * from renamed
