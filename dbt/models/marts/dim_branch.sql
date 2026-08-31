{{ config(materialized='table') }}

-- Replaces the Talend job Load_DimBranch and the T-SQL table DimBranch.
-- Type mapping: VARCHAR(n) -> STRING, INT -> INT.

with branch as (

    select * from {{ ref('stg_sample__branch') }}

)

select
    cast(branch_id as int)               as branch_id,
    cast(branch_name as string)          as branch_name,
    cast(branch_location as string)      as branch_location
from branch
