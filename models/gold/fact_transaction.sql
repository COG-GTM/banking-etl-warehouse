{{
    config(
        materialized='incremental',
        unique_key='TransactionID',
        incremental_strategy='merge',
        file_format='delta'
    )
}}

select
    transaction_id   as TransactionID,
    account_id       as AccountID,
    transaction_date as TransactionDate,
    amount           as Amount,
    transaction_type as TransactionType,
    branch_id        as BranchID
from {{ ref('slv_transaction') }}
{% if is_incremental() %}
where transaction_date > (select max(TransactionDate) from {{ this }})
{% endif %}
