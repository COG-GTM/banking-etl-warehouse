select
    account_id,
    customer_id,
    trim(account_type)        as account_type,
    balance,
    date_opened,
    lower(trim(status))       as status
from {{ ref('bronze_account') }}
