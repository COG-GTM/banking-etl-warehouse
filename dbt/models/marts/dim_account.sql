{{ config(materialized='table') }}

-- Replaces the Talend job Load_DimAccount and the T-SQL table DimAccount.
-- Type mapping: MONEY -> DECIMAL(18,2), DATE -> DATE, VARCHAR(n) -> STRING, INT -> INT.

with account as (

    select * from {{ ref('stg_sample__account') }}

)

select
    cast(account_id as int)              as account_id,
    cast(customer_id as int)             as customer_id,
    cast(account_type as string)         as account_type,
    cast(balance as decimal(18, 2))      as balance,
    cast(date_opened as date)            as date_opened,
    cast(status as string)               as status
from account
