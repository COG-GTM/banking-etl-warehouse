{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

with transactions as (

    select * from {{ ref('int_transactions_unioned') }}

)

select
    cast(transaction_id as int)              as transaction_id,
    cast(account_id as int)                  as account_id,
    cast(transaction_date as timestamp)      as transaction_date,
    cast(amount as decimal(18, 2))           as amount,
    cast(transaction_type as string)         as transaction_type,
    cast(branch_id as int)                   as branch_id,
    cast(source_system as string)            as source_system

from transactions
