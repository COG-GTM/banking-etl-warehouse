select
    branch_id,
    branch_name,
    branch_location
from {{ ref('bronze_branch') }}
