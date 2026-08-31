{{
    config(
        materialized='view'
    )
}}

/*
    Talend equivalent: Load_FactTransaction -> tUnite + tUniqRow.

    The three transaction feeds are unioned with an explicit, aligned column
    list, then reduced to one row per transaction_id. Source precedence:
    SQL Server (1) > CSV (2) > Excel (3), tie-broken by transaction_date.
*/

with sample_source as (

    select
        cast(transaction_id as int)             as transaction_id,
        cast(account_id as int)                 as account_id,
        cast(transaction_date as timestamp)     as transaction_date,
        cast(amount as decimal(18, 2))          as amount,
        cast(transaction_type as string)        as transaction_type,
        cast(branch_id as int)                  as branch_id,
        'sample_sqlserver'                      as record_source,
        1                                       as source_precedence
    from {{ ref('stg_sample__transaction') }}

),

csv_source as (

    select
        cast(transaction_id as int)             as transaction_id,
        cast(account_id as int)                 as account_id,
        cast(transaction_date as timestamp)     as transaction_date,
        cast(amount as decimal(18, 2))          as amount,
        cast(transaction_type as string)        as transaction_type,
        cast(branch_id as int)                  as branch_id,
        'transaction_csv'                       as record_source,
        2                                       as source_precedence
    from {{ ref('stg_files__transaction_csv') }}

),

excel_source as (

    select
        cast(transaction_id as int)             as transaction_id,
        cast(account_id as int)                 as account_id,
        cast(transaction_date as timestamp)     as transaction_date,
        cast(amount as decimal(18, 2))          as amount,
        cast(transaction_type as string)        as transaction_type,
        cast(branch_id as int)                  as branch_id,
        'transaction_excel'                     as record_source,
        3                                       as source_precedence
    from {{ ref('stg_files__transaction_excel') }}

),

unioned as (

    select * from sample_source
    union all
    select * from csv_source
    union all
    select * from excel_source

),

deduped as (

    {{ dedupe_by('unioned', 'transaction_id', ['source_precedence', 'transaction_date']) }}

)

select
    transaction_id,
    account_id,
    transaction_date,
    amount,
    transaction_type,
    branch_id,
    record_source
from deduped
