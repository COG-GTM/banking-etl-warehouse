with source as (

    select * from {{ source('sample', 'account') }}

),

renamed as (

    select
        cast(account_id as int)                     as account_id,
        cast(customer_id as int)                    as customer_id,
        cast(account_type as string)                as account_type,
        cast(balance as decimal(18, 2))             as balance,
        cast(date_opened as date)                   as date_opened,
        lower(trim(cast(status as string)))         as status

    from source

)

select * from renamed
