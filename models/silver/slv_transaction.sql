{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

with unioned as (

    select *, 'sql_server' as source_system, 1 as source_priority
    from {{ ref('bronze_transaction_db') }}

    union all

    select *, 'csv' as source_system, 2 as source_priority
    from {{ ref('bronze_transaction_csv') }}

    union all

    select *, 'excel' as source_system, 3 as source_priority
    from {{ ref('bronze_transaction_excel') }}

)

select
    transaction_id,
    account_id,
    transaction_date,
    amount,
    transaction_type,
    branch_id,
    source_system
from unioned
{% if is_incremental() %}
where transaction_date > (select max(transaction_date) from {{ this }})
{% endif %}
qualify row_number() over (
    partition by transaction_id
    order by source_priority
) = 1
