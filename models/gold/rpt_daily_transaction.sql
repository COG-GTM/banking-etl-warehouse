{% set start_date = var('daily_transaction_start_date', none) %}
{% set end_date = var('daily_transaction_end_date', none) %}

select
    cast(TransactionDate as date) as `Date`,
    count(TransactionID)          as TotalTransactions,
    sum(Amount)                   as TotalAmount
from {{ ref('fact_transaction') }}
{% if start_date and end_date %}
where cast(TransactionDate as date) between '{{ start_date }}' and '{{ end_date }}'
{% endif %}
group by cast(TransactionDate as date)
