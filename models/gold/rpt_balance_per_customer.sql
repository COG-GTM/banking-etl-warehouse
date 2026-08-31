{% set customer_name = var('balance_customer_name', none) %}

with transaction_summary as (

    select
        AccountID,
        sum(
            case
                when TransactionType = 'Deposit' then Amount
                else -Amount
            end
        ) as TotalTransactionAmount
    from {{ ref('fact_transaction') }}
    group by AccountID

)

select
    c.CustomerID,
    c.CustomerName,
    a.AccountID,
    a.AccountType,
    a.Balance                                          as InitialBalance,
    a.Balance + coalesce(ts.TotalTransactionAmount, 0) as CurrentBalance
from {{ ref('dim_customer') }} c
join {{ ref('dim_account') }} a
    on c.CustomerID = a.CustomerID
left join transaction_summary ts
    on a.AccountID = ts.AccountID
where a.Status = 'active'
{% if customer_name %}
  and c.CustomerName like '%{{ customer_name | upper }}%'
{% endif %}
