select
    claim_id,
    policy_id,
    claim_date,
    claim_amount,
    is_amount_valid,
    claim_type,
    status
from {{ ref('stg_claims') }}
