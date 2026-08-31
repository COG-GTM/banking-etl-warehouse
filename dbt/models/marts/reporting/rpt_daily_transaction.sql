{{ config(materialized='table') }}

-- Replaces the legacy T-SQL stored procedure sp_DailyTransaction(@start_date, @end_date).
-- The date range is optional: when the dbt vars start_date / end_date are empty the
-- full history is emitted so a BI layer can filter freely.

with transactions as (

    select
        transaction_id,
        transaction_date,
        amount
    from {{ ref('fct_transaction') }}
    where 1 = 1
    {% if var('start_date', '') %}
        and cast(transaction_date as date) >= cast('{{ var("start_date") }}' as date)
    {% endif %}
    {% if var('end_date', '') %}
        and cast(transaction_date as date) <= cast('{{ var("end_date") }}' as date)
    {% endif %}

)

select
    cast(transaction_date as date) as transaction_day,
    count(transaction_id) as total_transactions,
    sum(amount) as total_amount
from transactions
group by cast(transaction_date as date)
order by transaction_day
