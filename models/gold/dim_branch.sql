select
    branch_id       as BranchID,
    branch_name     as BranchName,
    branch_location as BranchLocation
from {{ ref('slv_branch') }}
