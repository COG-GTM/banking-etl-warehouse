-- Dedup / row-count assertion for int_transactions_unioned.
-- The model must contain exactly one row per distinct transaction_id present
-- across the three staging feeds: no duplicates and no rows dropped.

with model_counts as (

    select
        count(*) as row_count,
        count(distinct transaction_id) as distinct_ids
    from {{ ref('int_transactions_unioned') }}

),

source_ids as (

    select transaction_id from {{ ref('stg_sample__transaction') }}
    union
    select transaction_id from {{ ref('stg_files__transaction_csv') }}
    union
    select transaction_id from {{ ref('stg_files__transaction_excel') }}

),

source_counts as (

    select count(*) as expected_ids from source_ids

)

select
    model_counts.row_count,
    model_counts.distinct_ids,
    source_counts.expected_ids
from model_counts
cross join source_counts
where model_counts.row_count <> model_counts.distinct_ids
   or model_counts.row_count <> source_counts.expected_ids
