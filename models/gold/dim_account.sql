select
    account_id   as AccountID,
    customer_id  as CustomerID,
    account_type as AccountType,
    balance      as Balance,
    date_opened  as DateOpened,
    status       as Status
from {{ ref('slv_account') }}
