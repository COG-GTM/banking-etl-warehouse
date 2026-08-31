-- Replacement for the legacy FactTransaction foreign key constraints
-- FK_FactTransaction_DimAccount and FK_FactTransaction_DimBranch, which
-- Databricks cannot enforce. Returns one row per fact row whose account_id or
-- branch_id has no matching dimension member, labelled with the offending key.

with fact as (

    select
        transaction_id,
        account_id,
        branch_id
    from {{ ref('fct_transaction') }}

),

accounts as (

    select account_id from {{ ref('dim_account') }}

),

branches as (

    select branch_id from {{ ref('dim_branch') }}

)

select
    f.transaction_id,
    f.account_id,
    f.branch_id,
    case
        when a.account_id is null and b.branch_id is null then 'account_id,branch_id'
        when a.account_id is null then 'account_id'
        else 'branch_id'
    end as orphan_key

from fact as f
left join accounts as a on f.account_id = a.account_id
left join branches as b on f.branch_id = b.branch_id
where a.account_id is null
   or b.branch_id is null
