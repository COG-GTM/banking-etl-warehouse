with source as (

    select * from {{ source('sample', 'transaction') }}

),

renamed as (

    select
        cast(transaction_id as int)                     as transaction_id,
        cast(account_id as int)                         as account_id,
        cast(transaction_date as timestamp)             as transaction_date,
        cast(amount as decimal(18, 2))                  as amount,
        initcap(trim(cast(transaction_type as string))) as transaction_type,
        cast(branch_id as int)                          as branch_id,
        'sample' as source_system

    from source

)

select * from renamed
