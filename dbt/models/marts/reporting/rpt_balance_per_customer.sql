{{ config(materialized='table') }}

-- Replaces the legacy T-SQL stored procedure sp_BalancePerCustomer(@customer_name).
-- The name filter is optional: when the dbt var customer_name is empty every active
-- account is emitted so a BI layer can filter freely.

with transaction_summary as (

    select
        account_id,
        sum(
            case
                when lower(transaction_type) = 'deposit' then amount
                else -amount
            end
        ) as total_transaction_amount
    from {{ ref('fct_transaction') }}
    group by account_id

)

select
    c.customer_name,
    a.account_type,
    a.balance as initial_balance,
    a.balance + coalesce(ts.total_transaction_amount, 0) as current_balance
from {{ ref('dim_customer') }} as c
join {{ ref('dim_account') }} as a
    on c.customer_id = a.customer_id
left join transaction_summary as ts
    on a.account_id = ts.account_id
where lower(a.status) = 'active'
{% if var('customer_name', '') %}
    and lower(c.customer_name) like lower('%{{ var("customer_name") }}%')
{% endif %}
